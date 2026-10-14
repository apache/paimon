# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
from typing import Callable, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import polars

from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import predicate_supports_arrow_filter
from pypaimon.read.reader.iface.record_batch_reader import (
    InternalRowWrapperIterator,
    RecordBatchReader,
    _polars_arrow_table,
)
from pypaimon.schema.data_types import DataField, PyarrowFieldParser

logger = logging.getLogger(__name__)


def _comparison_fields(predicate):
    if predicate.method in ('and', 'or'):
        return set().union(*(_comparison_fields(child) for child in predicate.literals))
    if predicate.method in ('isNull', 'isNotNull'):
        # Null checks see the cast values, so they agree with comparisons.
        return {predicate.field}
    literals = predicate.literals or []
    # NOT IN with a NULL literal never matches, so skip the cast.
    if predicate.method == 'notIn' and any(value is None for value in literals):
        return set()
    if (predicate.method in ('equal', 'notEqual', 'lessThan', 'lessOrEqual', 'greaterThan',
                             'greaterOrEqual', 'in', 'notIn', 'between', 'notBetween',
                             'startsWith', 'endsWith', 'contains', 'like')
            and any(value is not None for value in literals)):
        return {predicate.field}
    return set()


def _try_cast(column, data_type):
    # Like SQL TRY_CAST: values that do not fit the declared type compare as NULL.
    try:
        return column.cast(data_type)
    except pa.ArrowNotImplementedError:
        return pa.nulls(len(column), data_type)
    except pa.ArrowInvalid:
        values = column.unique()
        cast = []
        for index in range(len(values)):
            try:
                cast.append(values.slice(index, 1).cast(data_type))
            except pa.ArrowInvalid:
                cast.append(pa.nulls(1, data_type))
        return pa.concat_arrays(cast).take(pc.index_in(column, value_set=values))


class FilterRecordBatchReader(RecordBatchReader):
    """
    Wraps a RecordBatchReader and filters each batch by predicate.
    Used for data evolution read where predicate cannot be pushed down to
    individual file readers. Only used when predicate columns are in read schema.
    """

    def __init__(
        self,
        reader: RecordBatchReader,
        predicate: Predicate,
        field_names: Optional[List[str]] = None,
        schema_fields: Optional[List[DataField]] = None,
        filter_view: Optional[Callable[[pa.RecordBatch], pa.RecordBatch]] = None,
    ):
        self.reader = reader
        self.predicate = predicate
        self.field_names = field_names
        self.schema_fields = schema_fields
        # The predicate tests this view; rows are taken from the original batch.
        self._filter_view = filter_view
        comparison_fields = _comparison_fields(predicate)
        self._comparison_types = {
            field.name: PyarrowFieldParser.from_paimon_type(field.type)
            for field in schema_fields or [] if field.name in comparison_fields}
        self._use_arrow_filter = predicate_supports_arrow_filter(predicate)
        self._adopt_metadata(reader)

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        while True:
            batch = self.reader.read_arrow_batch()
            if batch is None:
                return None
            self._refresh_blob_view_lookup(self.reader)
            if batch.num_rows == 0:
                return batch
            filtered = self._filter_batch(batch)
            if filtered is not None and filtered.num_rows > 0:
                return filtered
            continue

    def _filter_batch(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        view = batch if self._filter_view is None else self._filter_view(batch)
        filter_batch = self._coerce_comparison_types(view, batch)
        if not self._use_arrow_filter:
            return self._filter_batch_by_row(batch, filter_batch)
        expr = self.predicate.to_arrow(filter_batch.schema)
        if expr is None:
            return self._filter_batch_by_row(batch, filter_batch)
        if filter_batch is not batch:
            position = '_paimon_filter_position'
            while position in filter_batch.schema.names:
                position += '_'
            data = pa.Table.from_batches([filter_batch]).append_column(
                position, pa.array(range(batch.num_rows), type=pa.int64()))
            selected = ds.InMemoryDataset(data).scanner(columns=[position], filter=expr).to_table()
            return batch.take(selected.column(0).combine_chunks()) if selected.num_rows else None
        result = ds.InMemoryDataset(pa.Table.from_batches([batch])).scanner(
            filter=expr
        ).to_table()
        if result.num_rows == 0:
            return None
        batches = result.to_batches()
        if not batches:
            return None
        if len(batches) == 1:
            return batches[0]
        concat_batches = getattr(pa, "concat_batches", None)
        if concat_batches is not None:
            return concat_batches(batches)
        return pa.RecordBatch.from_arrays(
            [result.column(i) for i in range(result.num_columns)],
            schema=result.schema,
        )

    def _coerce_comparison_types(self, view, batch):
        # Cast columns a mask retyped, for the filter only; the output is unchanged.
        columns = None
        for name, data_type in self._comparison_types.items():
            column = view.column(name)
            if column.type != data_type and column.type != batch.column(name).type:
                if columns is None:
                    columns, fields = list(view.columns), list(view.schema)
                index = view.schema.get_field_index(name)
                columns[index] = _try_cast(column, data_type)
                fields[index] = fields[index].with_type(data_type)
        if columns is None:
            return view
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields, metadata=view.schema.metadata))

    def _filter_batch_by_row(self, batch: pa.RecordBatch, filter_batch=None) -> Optional[pa.RecordBatch]:
        filter_batch = batch if filter_batch is None else filter_batch
        # Predicate indices address logical fields. A record-reader adapter
        # may prepend _row_kind, which is metadata rather than an operand.
        columns = (self.field_names if self.field_names is not None else
                   [field.name for field in self.schema_fields] if self.schema_fields is not None else
                   [name for name in filter_batch.schema.names if name != "_row_kind"])
        df = polars.from_arrow(_polars_arrow_table(filter_batch).select(columns))
        iterator = InternalRowWrapperIterator(
            self._iter_df_rows(df),
            df.width,
            self.file_io,
            self.blob_field_indices,
            self.vector_field_indices,
            self.descriptor_field_indices,
            self.blob_view_lookup,
        )
        selected = []
        pos = 0
        for row in iter(iterator.next, None):
            if self.predicate.test(row):
                selected.append(pos)
            pos += 1
        if not selected:
            return None
        return batch.take(pa.array(selected, type=pa.int64()))

    def return_batch_pos(self) -> int:
        pos = getattr(self.reader, 'return_batch_pos', lambda: 0)()
        return pos if pos is not None else 0

    def close(self) -> None:
        self.reader.close()
