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
from collections import defaultdict
from typing import List, Optional

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite


class TableWrite:
    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.table_pyarrow_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        self.file_store_write = FileStoreWrite(self.table, commit_user)
        self.row_key_extractor = self.table.create_row_key_extractor()
        self.commit_user = commit_user

    def write_arrow(self, table: pa.Table, row_kinds: Optional[List[int]] = None):
        """Write Arrow table with optional RowKind information.

        Args:
            table: PyArrow table to write
            row_kinds: Optional list of RowKind values (0-3) for each row.
                      If provided, a '__row_kind__' column will be added to the table.
                      0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE
        """
        if row_kinds is not None:
            table = self._add_row_kind_column(table, row_kinds)

        batches_iterator = table.to_batches()
        for batch in batches_iterator:
            self.write_arrow_batch(batch)

    def write_arrow_batch(self, data: pa.RecordBatch, row_kinds: Optional[List[int]] = None):
        """Write Arrow record batch with optional RowKind information.

        Args:
            data: PyArrow record batch to write
            row_kinds: Optional list of RowKind values for each row.
                      If provided, a '__row_kind__' column will be added to the batch.
        """
        if row_kinds is not None:
            data = self._add_row_kind_to_batch(data, row_kinds)

        self._validate_pyarrow_schema(data.schema)
        partitions, buckets = self.row_key_extractor.extract_partition_bucket_batch(data)

        partition_bucket_groups = defaultdict(list)
        for i in range(data.num_rows):
            partition_bucket_groups[(tuple(partitions[i]), buckets[i])].append(i)

        for (partition, bucket), row_indices in partition_bucket_groups.items():
            indices_array = pa.array(row_indices, type=pa.int64())
            sub_table = pa.compute.take(data, indices_array)
            self.file_store_write.write(partition, bucket, sub_table)

    def write_pandas(self, dataframe):
        pa_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        record_batch = pa.RecordBatch.from_pandas(dataframe, schema=pa_schema)
        return self.write_arrow_batch(record_batch)

    def with_write_type(self, write_cols: List[str]):
        for col in write_cols:
            if col not in self.table_pyarrow_schema.names:
                raise ValueError(f"Column {col} is not in table schema.")
        if len(write_cols) == len(self.table_pyarrow_schema.names):
            write_cols = None
        self.file_store_write.write_cols = write_cols
        return self

    def close(self):
        self.file_store_write.close()

    def _add_row_kind_column(self, table: pa.Table, row_kinds: List[int]) -> pa.Table:
        """Add '__row_kind__' column to the table.

        Args:
            table: PyArrow table
            row_kinds: List of RowKind values (0-3) for each row

        Returns:
            Table with '__row_kind__' column added

        Raises:
            ValueError: If row_kinds length doesn't match table rows or contains invalid values
        """
        if len(row_kinds) != table.num_rows:
            raise ValueError(
                f"row_kinds length ({len(row_kinds)}) must match table rows ({table.num_rows})"
            )

        # Validate RowKind values
        for i, rk in enumerate(row_kinds):
            if rk not in [0, 1, 2, 3]:
                raise ValueError(
                    f"Invalid RowKind value: {rk} at index {i}. "
                    f"Valid values are 0(INSERT), 1(UPDATE_BEFORE), 2(UPDATE_AFTER), 3(DELETE)"
                )

        row_kind_array = pa.array(row_kinds, type=pa.int32())
        return table.append_column('__row_kind__', row_kind_array)

    def _add_row_kind_to_batch(self, batch: pa.RecordBatch, row_kinds: List[int]) -> pa.RecordBatch:
        """Add '__row_kind__' column to the record batch.

        Args:
            batch: PyArrow record batch
            row_kinds: List of RowKind values (0-3) for each row

        Returns:
            Batch with '__row_kind__' column added

        Raises:
            ValueError: If row_kinds length doesn't match batch rows or contains invalid values
        """
        if len(row_kinds) != batch.num_rows:
            raise ValueError(
                f"row_kinds length ({len(row_kinds)}) must match batch rows ({batch.num_rows})"
            )

        # Validate RowKind values
        for i, rk in enumerate(row_kinds):
            if rk not in [0, 1, 2, 3]:
                raise ValueError(
                    f"Invalid RowKind value: {rk} at index {i}. "
                    f"Valid values are 0(INSERT), 1(UPDATE_BEFORE), 2(UPDATE_AFTER), 3(DELETE)"
                )

        row_kind_array = pa.array(row_kinds, type=pa.int32())
        return batch.append_column('__row_kind__', row_kind_array)

    def _validate_pyarrow_schema(self, data_schema: pa.Schema):
        if data_schema != self.table_pyarrow_schema and data_schema.names != self.file_store_write.write_cols:
            raise ValueError(f"Input schema isn't consistent with table schema and write cols. "
                             f"Input schema is: {data_schema} "
                             f"Table schema is: {self.table_pyarrow_schema} "
                             f"Write cols is: {self.file_store_write.write_cols}")


class BatchTableWrite(TableWrite):
    def __init__(self, table, commit_user):
        super().__init__(table, commit_user)
        self.batch_committed = False

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        return self.file_store_write.prepare_commit(BATCH_COMMIT_IDENTIFIER)


class StreamTableWrite(TableWrite):

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        return self.file_store_write.prepare_commit(commit_identifier)
