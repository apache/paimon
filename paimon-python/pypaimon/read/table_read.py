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
from pypaimon.globalindex.indexed_split import IndexedSplit
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
        i = 0
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
            # Get row ranges if this is an IndexedSplit
            row_ranges = None
            actual_split = split
            if isinstance(split, IndexedSplit):
                row_ranges = split.row_ranges()
                actual_split = split.data_split()

            reader = self._create_split_read(actual_split).create_reader()
            try:
                if isinstance(reader, RecordBatchReader):
                    # For IndexedSplit, filter batches by row ranges
                    if row_ranges:
                        yield from self._filter_batches_by_row_ranges(
                            iter(reader.read_arrow_batch, None),
                            actual_split,
                            row_ranges
                        )
                    else:
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

    def _filter_batches_by_row_ranges(
        self,
        batch_iterator: Iterator[pyarrow.RecordBatch],
        split: Split,
        row_ranges: List
    ) -> Iterator[pyarrow.RecordBatch]:
        """Filter batches to only include rows within the given row ranges."""
        import numpy as np

        # Build a set of allowed row IDs for fast lookup
        allowed_row_ids = set()
        for r in row_ranges:
            for row_id in range(r.from_, r.to + 1):
                allowed_row_ids.add(row_id)

        # Track current row ID based on file's first_row_id
        current_row_id = 0
        for file in split.files:
            if file.first_row_id is not None:
                current_row_id = file.first_row_id
                break

        for batch in batch_iterator:
            if batch.num_rows == 0:
                continue

            # Build mask for rows that are in allowed_row_ids
            mask = np.zeros(batch.num_rows, dtype=bool)
            for i in range(batch.num_rows):
                if current_row_id + i in allowed_row_ids:
                    mask[i] = True

            current_row_id += batch.num_rows

            # Filter batch
            if np.any(mask):
                filtered_batch = batch.filter(pyarrow.array(mask))
                if filtered_batch.num_rows > 0:
                    yield filtered_batch

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

    def to_torch(self, splits: List[Split], streaming: bool = False) -> "torch.utils.data.Dataset":
        """Wrap Paimon table data to PyTorch Dataset."""
        if streaming:
            from pypaimon.read.datasource.torch_dataset import TorchIterDataset
            dataset = TorchIterDataset(self, splits)
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
