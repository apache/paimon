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
from typing import List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
import polars

from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import predicate_supports_arrow_filter
from pypaimon.read.reader.iface.record_batch_reader import (
    InternalRowWrapperIterator,
    RecordBatchReader,
)
from pypaimon.schema.data_types import DataField

logger = logging.getLogger(__name__)


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
    ):
        self.reader = reader
        self.predicate = predicate
        self.field_names = field_names
        self.schema_fields = schema_fields
        self._use_arrow_filter = predicate_supports_arrow_filter(predicate)
        self._adopt_metadata(reader)

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        while True:
            batch = self.reader.read_arrow_batch()
            if batch is None:
                return None
            if batch.num_rows == 0:
                return batch
            filtered = self._filter_batch(batch)
            if filtered is not None and filtered.num_rows > 0:
                return filtered
            continue

    def _filter_batch(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        if not self._use_arrow_filter:
            return self._filter_batch_by_row(batch)
        expr = self.predicate.to_arrow()
        if expr is None:
            return self._filter_batch_by_row(batch)
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

    def _filter_batch_by_row(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        df = polars.from_arrow(pa.Table.from_batches([batch]))
        iterator = InternalRowWrapperIterator(
            self._iter_df_rows(df),
            df.width,
            self.file_io,
            self.blob_field_indices,
            self.vector_field_indices,
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
