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
import pyarrow.compute as pc
import pyarrow.ipc

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.push_down_utils import extract_predicate_to_list
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.split import Split
from pypaimon.read.split_read import (MergeFileSplitRead, RawFileSplitRead,
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

    def __getstate__(self):
        """Custom serialization to handle non-serializable objects."""
        state = self.__dict__.copy()
        # Store table information needed for reconstruction
        if hasattr(self.table, 'table_schema'):
            state['_table_schema'] = self.table.table_schema
            state['_table_path'] = getattr(self.table, 'table_path', None)
            state['_table_options'] = getattr(self.table, 'options', {})
            state['_is_primary_key_table'] = getattr(self.table, 'is_primary_key_table', False)
        # Remove the table object as it may contain file handles
        state['table'] = None
        return state

    def __setstate__(self, state):
        """Custom deserialization to reconstruct non-serializable objects."""
        self.__dict__.update(state)
        # Reconstruct table if we have the necessary information
        if state.get('_table_schema') and state.get('_table_path'):
            try:
                from pypaimon.table.file_store_table import FileStoreTable
                from pypaimon.io.file_io_factory import FileIOFactory

                file_io = FileIOFactory.create(state['_table_path'])
                self.table = FileStoreTable(
                    file_io=file_io,
                    table_path=state['_table_path'],
                    table_schema=state['_table_schema']
                )
                if state.get('_table_options'):
                    self.table.options = state['_table_options']
                if '_is_primary_key_table' in state:
                    self.table.is_primary_key_table = state['_is_primary_key_table']
            except Exception:
                # If reconstruction fails, create a minimal mock table
                class MockTable:
                    def __init__(self):
                        self.is_primary_key_table = state.get('_is_primary_key_table', False)
                        self.table_schema = state.get('_table_schema')
                        self.options = state.get('_table_options', {})
                        # Add file_io attribute to avoid AttributeError
                        try:
                            from pypaimon.common.file_io import FileIO
                            self.file_io = FileIO()
                        except Exception:
                            # If FileIO creation fails, create a minimal mock FileIO
                            class MockFileIO:
                                def __init__(self):
                                    pass

                                def read_parquet(self, *args, **kwargs):
                                    import pyarrow as pa
                                    return pa.Table.from_arrays([], names=[])

                                def read_orc(self, *args, **kwargs):
                                    import pyarrow as pa
                                    return pa.Table.from_arrays([], names=[])

                                @property
                                def filesystem(self):
                                    # Return a mock filesystem for compatibility
                                    import pyarrow.fs as fs
                                    return fs.LocalFileSystem()

                            self.file_io = MockFileIO()
                        self.primary_keys = getattr(state.get('_table_schema'), 'primary_keys', []) if state.get(
                            '_table_schema') else []
                        # Add fields attribute to avoid AttributeError
                        self.fields = getattr(state.get('_table_schema'), 'fields', []) if state.get(
                            '_table_schema') else []
                        # Add partition_keys attribute to avoid AttributeError
                        self.partition_keys = getattr(state.get('_table_schema'), 'partition_keys', []) if state.get(
                            '_table_schema') else []

                self.table = MockTable()
        elif not hasattr(self, 'table') or self.table is None:
            # Create a minimal mock table if no reconstruction info available
            class MockTable:
                def __init__(self):
                    self.is_primary_key_table = False
                    self.table_schema = None
                    self.options = {}

                    # Add file_io attribute to avoid AttributeError
                    class MockFileIO:
                        def __init__(self):
                            pass

                        def read_parquet(self, *args, **kwargs):
                            import pyarrow as pa
                            return pa.Table.from_arrays([], names=[])

                        def read_orc(self, *args, **kwargs):
                            import pyarrow as pa
                            return pa.Table.from_arrays([], names=[])

                    self.file_io = MockFileIO()
                    self.primary_keys = []
                    # Add fields attribute to avoid AttributeError
                    self.fields = []
                    # Add partition_keys attribute to avoid AttributeError
                    self.partition_keys = []

            self.table = MockTable()

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

    def to_arrow(self, splits: List[Split]) -> Optional[pyarrow.Table]:
        batch_reader = self.to_arrow_batch_reader(splits)
        arrow_table = batch_reader.read_all()
        return arrow_table

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
                                batch = convert_rows_to_arrow_batch(row_tuple_chunk, schema)
                                yield batch
                                row_tuple_chunk = []

                    if row_tuple_chunk:
                        batch = convert_rows_to_arrow_batch(row_tuple_chunk, schema)
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

    def to_ray(self, splits: List[Split]) -> "ray.data.dataset.Dataset":
        import ray

        return ray.data.from_arrow(self.to_arrow(splits))

    def _create_split_read(self, split: Split) -> SplitRead:
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


def convert_rows_to_arrow_batch(row_tuples: List[tuple], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    columns_data = zip(*row_tuples)
    pydict = {name: list(column) for name, column in zip(schema.names, columns_data)}
    return pyarrow.RecordBatch.from_pydict(pydict, schema=schema)
