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
from typing import Any, Dict, Iterator, List, Optional

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

ROW_KIND_COLUMN = "_row_kind"


class TableRead:
    """Implementation of TableRead for native Python reading."""

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        read_type: List[DataField],
        include_row_kind: bool = False
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.read_type = read_type
        self.include_row_kind = include_row_kind

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
        if self.include_row_kind:
            schema = self._add_row_kind_to_schema(schema)
        batch_iterator = self._arrow_batch_generator(splits, schema)
        return pyarrow.ipc.RecordBatchReader.from_batches(schema, batch_iterator)

    @staticmethod
    def _add_row_kind_to_schema(schema: pyarrow.Schema) -> pyarrow.Schema:
        """Add _row_kind column to the schema as the first column."""
        row_kind_field = pyarrow.field(ROW_KIND_COLUMN, pyarrow.string())
        return pyarrow.schema([row_kind_field] + list(schema))

    @staticmethod
    def _try_to_pad_batch_by_schema(batch: pyarrow.RecordBatch, target_schema):
        if batch.schema.names == target_schema.names:
            return batch

        columns = []
        num_rows = batch.num_rows

        for field in target_schema:
            if field.name in batch.schema.names:
                col = batch.column(field.name)
            else:
                col = pyarrow.nulls(num_rows, type=field.type)
            columns.append(col)

        return pyarrow.RecordBatch.from_arrays(columns, schema=target_schema)

    def to_arrow(self, splits: List[Split]) -> Optional[pyarrow.Table]:
        batch_reader = self.to_arrow_batch_reader(splits)

        schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        if self.include_row_kind:
            schema = self._add_row_kind_to_schema(schema)

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
                    # Add row kind column if requested (default to +I for RecordBatchReader)
                    if self.include_row_kind:
                        for batch in iter(reader.read_arrow_batch, None):
                            yield self._add_row_kind_column_to_batch(batch, "+I")
                    else:
                        yield from iter(reader.read_arrow_batch, None)
                else:
                    row_tuple_chunk = []
                    row_kind_chunk = []
                    for row_iterator in iter(reader.read_batch, None):
                        for row in iter(row_iterator.next, None):
                            if not isinstance(row, OffsetRow):
                                raise TypeError(f"Expected OffsetRow, but got {type(row).__name__}")
                            row_tuple_chunk.append(row.row_tuple[row.offset: row.offset + row.arity])
                            if self.include_row_kind:
                                row_kind_chunk.append(row.get_row_kind().to_string())

                            if len(row_tuple_chunk) >= chunk_size:
                                batch = self._convert_rows_to_arrow_batch_with_row_kind(
                                    row_tuple_chunk, row_kind_chunk, schema
                                )
                                yield batch
                                row_tuple_chunk = []
                                row_kind_chunk = []

                    if row_tuple_chunk:
                        batch = self._convert_rows_to_arrow_batch_with_row_kind(
                            row_tuple_chunk, row_kind_chunk, schema
                        )
                        yield batch
            finally:
                reader.close()

    def _convert_rows_to_arrow_batch_with_row_kind(
        self,
        row_tuples: List[tuple],
        row_kinds: List[str],
        schema: pyarrow.Schema
    ) -> pyarrow.RecordBatch:
        """Convert rows to Arrow batch, optionally including row kind column."""
        if not self.include_row_kind or not row_kinds:
            # No row kind - use original schema (without _row_kind column)
            data_schema = schema
            columns_data = zip(*row_tuples)
            pydict = {name: list(column) for name, column in zip(data_schema.names, columns_data)}
        else:
            # Include row kind as first column
            # Schema already has _row_kind as first field
            data_field_names = [f.name for f in schema if f.name != ROW_KIND_COLUMN]
            columns_data = zip(*row_tuples)
            pydict = {ROW_KIND_COLUMN: row_kinds}
            for name, column in zip(data_field_names, columns_data):
                pydict[name] = list(column)
        return pyarrow.RecordBatch.from_pydict(pydict, schema=schema)

    def _add_row_kind_column_to_batch(
        self,
        batch: pyarrow.RecordBatch,
        default_row_kind: str = "+I"
    ) -> pyarrow.RecordBatch:
        """Add a _row_kind column to an existing batch."""
        row_kind_array = pyarrow.array([default_row_kind] * batch.num_rows, type=pyarrow.string())
        new_schema = self._add_row_kind_to_schema(batch.schema)
        columns = [row_kind_array] + [batch.column(i) for i in range(batch.num_columns)]
        return pyarrow.RecordBatch.from_arrays(columns, schema=new_schema)

    def to_pandas(self, splits: List[Split]) -> pandas.DataFrame:
        arrow_table = self.to_arrow(splits)
        return arrow_table.to_pandas()

    def to_duckdb(self, splits: List[Split], table_name: str,
                  connection: Optional["DuckDBPyConnection"] = None) -> "DuckDBPyConnection":
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow(splits))
        return con

    def to_ray(
        self,
        splits: List[Split],
        *,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        override_num_blocks: Optional[int] = None,
        **read_args,
    ) -> "ray.data.dataset.Dataset":
        """Convert Paimon table data to Ray Dataset.
        Args:
            splits: List of splits to read from the Paimon table.
            ray_remote_args: Optional kwargs passed to :func:`ray.remote` in read tasks.
                For example, ``{"num_cpus": 2, "max_retries": 3}``.
            concurrency: Optional max number of Ray tasks to run concurrently.
                By default, dynamically decided based on available resources.
            override_num_blocks: Optional override for the number of output blocks.
                You needn't manually set this in most cases.
            **read_args: Additional kwargs passed to the datasource.
                For example, ``per_task_row_limit`` (Ray 2.52.0+).

        See `Ray Data API <https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html>`_
        for details.
        """
        import ray

        if not splits:
            schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
            empty_table = pyarrow.Table.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema],
                schema=schema
            )
            return ray.data.from_arrow(empty_table)

        if override_num_blocks is not None and override_num_blocks < 1:
            raise ValueError(f"override_num_blocks must be at least 1, got {override_num_blocks}")

        from pypaimon.read.datasource.ray_datasource import RayDatasource
        datasource = RayDatasource(self, splits)
        return ray.data.read_datasource(
            datasource,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **read_args
        )

    def to_torch(
        self,
        splits: List[Split],
        streaming: bool = False,
        prefetch_concurrency: int = 1,
    ) -> "torch.utils.data.Dataset":
        """Wrap Paimon table data to PyTorch Dataset."""
        if streaming:
            from pypaimon.read.datasource.torch_dataset import TorchIterDataset
            dataset = TorchIterDataset(self, splits, prefetch_concurrency)
            return dataset
        else:
            from pypaimon.read.datasource.torch_dataset import TorchDataset
            dataset = TorchDataset(self, splits)
            return dataset

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
