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

from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.blob import BlobConsumer
from pypaimon.write.row_utils import require_columns, row_to_named_values
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite

if TYPE_CHECKING:
    from ray.data import Dataset


class TableWrite:
    def __init__(self, table, commit_user, static_partition: Optional[dict] = None):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.table_pyarrow_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        self.file_store_write = FileStoreWrite(self.table, commit_user)
        self.row_key_extractor = self.table.create_row_key_extractor()
        self.commit_user = commit_user
        self.static_partition = static_partition

    def write_arrow(self, table: pa.Table):
        batches_iterator = table.to_batches()
        for batch in batches_iterator:
            self.write_arrow_batch(batch)

    def write_arrow_batch(self, data: pa.RecordBatch):
        self._validate_pyarrow_schema(data.schema)
        partitions, buckets = self.row_key_extractor.extract_partition_bucket_batch(data)

        partition_bucket_groups = defaultdict(list)
        for i in range(data.num_rows):
            partition_bucket_groups[(tuple(partitions[i]), buckets[i])].append(i)

        for (partition, bucket), row_indices in partition_bucket_groups.items():
            if len(row_indices) == data.num_rows:
                # Every input row belongs to the same partition/bucket. Passing the
                # original batch through avoids copying large BLOB values through
                # Arrow take before the dedicated BLOB writer consumes them.
                sub_table = data
            elif row_indices[-1] - row_indices[0] + 1 == len(row_indices):
                # Contiguous groups can share the original Arrow buffers instead of
                # gathering their rows into newly allocated buffers with take.
                sub_table = data.slice(row_indices[0], len(row_indices))
            else:
                indices_array = pa.array(row_indices, type=pa.int64())
                sub_table = pa.compute.take(data, indices_array)
            self.file_store_write.write(partition, bucket, sub_table)

    def write_row(self, row):
        values_by_name = row_to_named_values(row, self.table.table_schema.fields)
        column_names = (
            self.file_store_write.write_cols
            if self.file_store_write.write_cols is not None
            else list(self.table.field_names)
        )
        require_columns(values_by_name, column_names, "write_row")
        require_columns(values_by_name, self.table.partition_keys, "write_row")
        partition, bucket = (
            self.row_key_extractor.extract_partition_bucket_row(values_by_name)
        )
        self.file_store_write.write_row(partition, bucket, row, values_by_name)

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

    def with_blob_consumer(self, blob_consumer: BlobConsumer):
        if self.file_store_write.data_writers:
            raise RuntimeError(
                "with_blob_consumer must be called before any write operation."
            )
        self.file_store_write.blob_consumer = blob_consumer
        return self

    def write_ray(
        self,
        dataset: "Dataset",
        overwrite: bool = False,
        concurrency: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        hash_fixed_precluster: str = "auto",
        static_partition: Optional[dict] = None,
    ) -> None:
        """
        Write a Ray Dataset to Paimon table.
        
        Args:
            dataset: Ray Dataset to write. This is a distributed data collection
                from Ray Data (ray.data.Dataset).
            overwrite: Whether to overwrite existing data. Defaults to False.
                Builder-level or static_partition overwrite mode takes precedence.
            concurrency: Optional max number of Ray tasks to run concurrently.
                By default, dynamically decided based on available resources.
            ray_remote_args: Optional kwargs passed to :func:`ray.remote` in write tasks.
                For example, ``{"num_cpus": 2, "max_retries": 3}``.
            hash_fixed_precluster: HASH_FIXED pre-clustering mode. ``"auto"``
                and ``"off"`` write append-only HASH_FIXED tables directly
                and reject HASH_FIXED primary-key tables. ``"map_groups"``
                writes each HASH_FIXED primary-key group in one task and
                preserves the legacy single-group memory bound.
            static_partition: Optional partition spec to overwrite. When set,
                the Ray write runs in overwrite mode for this partition and
                overrides any builder-level partition spec.
        """
        from pypaimon.write.ray_datasink import write_paimon_dataset

        overwrite_partition = self.static_partition
        if static_partition is not None:
            overwrite_partition = static_partition

        write_paimon_dataset(
            dataset,
            self.table,
            overwrite=overwrite,
            static_partition=overwrite_partition,
            concurrency=concurrency,
            ray_remote_args=ray_remote_args,
            hash_fixed_precluster=hash_fixed_precluster,
        )

    def close(self):
        self.file_store_write.close()

    def abort(self):
        self.file_store_write.abort()

    def _validate_pyarrow_schema(self, data_schema: pa.Schema):
        if self._is_compatible_pyarrow_schema(data_schema, self.table_pyarrow_schema):
            return

        write_cols = self.file_store_write.write_cols
        if write_cols is not None:
            write_cols_schema = self._write_cols_pyarrow_schema(write_cols)
            if self._is_compatible_pyarrow_schema(data_schema, write_cols_schema):
                return

        self._raise_inconsistent_schema(data_schema)

    def _is_compatible_pyarrow_schema(
            self, data_schema: pa.Schema, expected_schema: pa.Schema) -> bool:
        # Allow compatible binary types: binary, fixed_size_binary[N] are interchangeable
        if data_schema.names != expected_schema.names:
            return False
        for i in range(len(data_schema)):
            input_type = data_schema.field(i).type
            expected_type = expected_schema.field(i).type
            if input_type == expected_type:
                continue
            if self._is_binary_family(input_type) and self._is_binary_family(expected_type):
                continue
            return False
        return True

    def _write_cols_pyarrow_schema(self, write_cols: List[str]) -> pa.Schema:
        table_fields = {
            field.name: field for field in self.table_pyarrow_schema
        }
        return pa.schema([table_fields[col] for col in write_cols])

    def _raise_inconsistent_schema(self, data_schema: pa.Schema):
        raise ValueError(f"Input schema isn't consistent with table schema and write cols. "
                         f"Input schema is: {data_schema} "
                         f"Table schema is: {self.table_pyarrow_schema} "
                         f"Write cols is: {self.file_store_write.write_cols}")

    @staticmethod
    def _is_binary_family(arrow_type) -> bool:
        return pa.types.is_binary(arrow_type) or pa.types.is_fixed_size_binary(arrow_type)


class BatchTableWrite(TableWrite):
    def __init__(self, table, commit_user, static_partition: Optional[dict] = None):
        super().__init__(table, commit_user, static_partition)
        self.batch_committed = False

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        return self.file_store_write.prepare_commit(BATCH_COMMIT_IDENTIFIER)


class StreamTableWrite(TableWrite):

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        return self.file_store_write.prepare_commit(commit_identifier)
