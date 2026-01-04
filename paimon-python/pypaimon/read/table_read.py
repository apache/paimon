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
from typing import Iterator, List, Optional

import pandas
import pyarrow

from pypaimon.common.predicate import Predicate
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.split import Split
from pypaimon.read.split_read import (DataEvolutionSplitRead,
                                      MergeFileSplitRead, RawFileSplitRead,
                                      SplitRead)
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.offset_row import OffsetRow


class TableRead:
    """Implementation of TableRead for native Python reading."""

    def __init__(self, table, predicate: Optional[Predicate], read_type: List[DataField]):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.read_type = read_type

    def to_iterator(self, splits: List[Split]) -> Iterator:
        def _record_generator():
            for split in splits:
                reader = self._create_split_read(split).create_reader()
                try:
                    for batch in iter(reader.read_batch, None):
                        yield from iter(batch.next, None)
                finally:
                    reader.close()

        return _record_generator()

    def to_arrow_batch_reader(self, splits: List[Split]) -> pyarrow.ipc.RecordBatchReader:
        schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        batch_iterator = self._arrow_batch_generator(splits, schema)
        return pyarrow.ipc.RecordBatchReader.from_batches(schema, batch_iterator)

    @staticmethod
    def _try_to_pad_batch_by_schema(batch: pyarrow.RecordBatch, target_schema):
        if batch.schema.names == target_schema.names:
            return batch

        columns = []
        num_rows = batch.num_rows

        for field in target_schema:
            if field.name in batch.column_names:
                col = batch.column(field.name)
            else:
                col = pyarrow.nulls(num_rows, type=field.type)
            columns.append(col)

        return pyarrow.RecordBatch.from_arrays(columns, schema=target_schema)

    def to_arrow(self, splits: List[Split]) -> Optional[pyarrow.Table]:
        batch_reader = self.to_arrow_batch_reader(splits)

        schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        table_list = []
        for batch in iter(batch_reader.read_next_batch, None):
            if batch.num_rows == 0:
                continue
            table_list.append(self._try_to_pad_batch_by_schema(batch, schema))

        if not table_list:
            return pyarrow.Table.from_arrays([pyarrow.array([], type=field.type) for field in schema], schema=schema)
        else:
            return pyarrow.Table.from_batches(table_list)

    def _arrow_batch_generator(self, splits: List[Split], schema: pyarrow.Schema) -> Iterator[pyarrow.RecordBatch]:
        chunk_size = 65536

        for split in splits:
            reader = self._create_split_read(split).create_reader()
            try:
                if isinstance(reader, RecordBatchReader):
                    yield from iter(reader.read_arrow_batch, None)
                else:
                    row_tuple_chunk = []
                    for row_iterator in iter(reader.read_batch, None):
                        for row in iter(row_iterator.next, None):
                            if not isinstance(row, OffsetRow):
                                raise TypeError(f"Expected OffsetRow, but got {type(row).__name__}")
                            row_tuple_chunk.append(row.row_tuple[row.offset: row.offset + row.arity])

                            if len(row_tuple_chunk) >= chunk_size:
                                batch = self.convert_rows_to_arrow_batch(row_tuple_chunk, schema)
                                yield batch
                                row_tuple_chunk = []

                    if row_tuple_chunk:
                        batch = self.convert_rows_to_arrow_batch(row_tuple_chunk, schema)
                        yield batch
            finally:
                reader.close()

    def to_pandas(self, splits: List[Split]) -> pandas.DataFrame:
        arrow_table = self.to_arrow(splits)
        return arrow_table.to_pandas()

    def to_duckdb(self, splits: List[Split], table_name: str,
                  connection: Optional["DuckDBPyConnection"] = None) -> "DuckDBPyConnection":
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow(splits))
        return con

    def to_ray(self, splits: List[Split], parallelism: int = 1) -> "ray.data.dataset.Dataset":
        """Convert Paimon table data to Ray Dataset."""
        import ray

        if not splits:
            schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
            empty_table = pyarrow.Table.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema],
                schema=schema
            )
            return ray.data.from_arrow(empty_table)

        # Validate parallelism parameter
        if parallelism < 1:
            raise ValueError(f"parallelism must be at least 1, got {parallelism}")

        from pypaimon.read.ray_datasource import PaimonDatasource
        datasource = PaimonDatasource(self, splits)
        return ray.data.read_datasource(datasource, parallelism=parallelism)

    def _create_split_read(self, split: Split) -> SplitRead:
        if self.table.is_primary_key_table and not split.raw_convertible:
            return MergeFileSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=self.read_type,
                split=split,
                row_tracking_enabled=False
            )
        elif self.table.options.data_evolution_enabled():
            return DataEvolutionSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=self.read_type,
                split=split,
                row_tracking_enabled=True
            )
        else:
            return RawFileSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=self.read_type,
                split=split,
                row_tracking_enabled=self.table.options.row_tracking_enabled()
            )

    @staticmethod
    def convert_rows_to_arrow_batch(row_tuples: List[tuple], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
        columns_data = zip(*row_tuples)
        pydict = {name: list(column) for name, column in zip(schema.names, columns_data)}
        return pyarrow.RecordBatch.from_pydict(pydict, schema=schema)
