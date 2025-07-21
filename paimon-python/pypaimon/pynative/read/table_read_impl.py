################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import List, Optional, TYPE_CHECKING, Iterator
import pandas as pd
import pyarrow as pa

from pypaimon.api import TableRead, Split
from pypaimon.pynative.common.data_field import DataField
from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.common.predicate import PredicateImpl
from pypaimon.pynative.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.pynative.row.offset_row import OffsetRow
from pypaimon.pynative.read.split_impl import SplitImpl
from pypaimon.pynative.read.split_read import SplitRead, RawFileSplitRead, MergeFileSplitRead
from pypaimon.pynative.table import schema_util

if TYPE_CHECKING:
    import ray
    from duckdb.duckdb import DuckDBPyConnection


class TableReadImpl(TableRead):
    """Implementation of TableRead for native Python reading."""

    def __init__(self, table, predicate: Optional[PredicateImpl], read_type: List[DataField]):
        from pypaimon.pynative.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.read_type = read_type

    def to_iterator(self, splits: List[Split]) -> Iterator:
        def _record_generator():
            for split in splits:
                if not isinstance(split, SplitImpl):
                    raise TypeError(f"Expected SplitImpl, but got {type(split).__name__}")
                reader = self._create_split_read(split).create_reader()
                try:
                    for batch in iter(reader.read_batch, None):
                        yield from iter(batch.next, None)
                finally:
                    reader.close()

        return _record_generator()

    def to_arrow(self, splits: List[Split]) -> pa.Table:
        chunk_size = 65536
        schema = schema_util.convert_data_fields_to_pa_schema(self.read_type)
        arrow_batches = []

        for split in splits:
            if not isinstance(split, SplitImpl):
                raise TypeError(f"Expected SplitImpl, but got {type(split).__name__}")
            reader = self._create_split_read(split).create_reader()
            try:
                if isinstance(reader, RecordBatchReader):
                    for batch in iter(reader.read_arrow_batch, None):
                        arrow_batches.append(batch)
                else:
                    row_tuple_chunk = []
                    for iterator in iter(reader.read_batch, None):
                        for row in iter(iterator.next, None):
                            if not isinstance(row, OffsetRow):
                                raise TypeError(f"Expected OffsetRow, but got {type(row).__name__}")
                            row_tuple_chunk.append(row.row_tuple[row.offset: row.offset + row.arity])

                            if len(row_tuple_chunk) >= chunk_size:
                                batch = convert_rows_to_arrow_batch(row_tuple_chunk, schema)
                                arrow_batches.append(batch)
                                row_tuple_chunk = []

                    if row_tuple_chunk:
                        batch = convert_rows_to_arrow_batch(row_tuple_chunk, schema)
                        arrow_batches.append(batch)
            finally:
                reader.close()

        if not arrow_batches:
            return pa.Table.from_arrays([], schema=schema)

        unified_schema = pa.unify_schemas([b.schema for b in arrow_batches])
        casted_batches = [b.cast(target_schema=unified_schema) for b in arrow_batches]
        return pa.Table.from_batches(casted_batches)

    def to_arrow_batch_reader(self, splits: List[Split]) -> pa.RecordBatchReader:
        raise PyNativeNotImplementedError("to_arrow_batch_reader")

    def to_pandas(self, splits: List[Split]) -> pd.DataFrame:
        arrow_table = self.to_arrow(splits)
        return arrow_table.to_pandas()

    def to_duckdb(self, splits: List[Split], table_name: str,
                  connection: Optional["DuckDBPyConnection"] = None) -> "DuckDBPyConnection":
        raise PyNativeNotImplementedError("to_duckdb")

    def to_ray(self, splits: List[Split]) -> "ray.data.dataset.Dataset":
        raise PyNativeNotImplementedError("to_ray")

    def _create_split_read(self, split: SplitImpl) -> SplitRead:
        if self.table.is_primary_key_table and not split.raw_convertible:
            return MergeFileSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=self.read_type,
                split=split
            )
        else:
            return RawFileSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=self.read_type,
                split=split
            )


def convert_rows_to_arrow_batch(row_tuples: List[tuple], schema: pa.Schema) -> pa.RecordBatch:
    columns_data = zip(*row_tuples)
    pydict = {name: list(column) for name, column in zip(schema.names, columns_data)}
    return pa.RecordBatch.from_pydict(pydict, schema=schema)
