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

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.blob import BlobConsumer
from pypaimon.write.commit_identifier import validate_commit_identifier
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import validate_write_columns

if TYPE_CHECKING:
    from ray.data import Dataset


class TableWrite:
    """Public writer API backed by the selected execution implementation."""

    def __init__(self, table, commit_user, static_partition: Optional[dict] = None, *, restore_snapshot_id=None):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.__init__(
            self, table, commit_user, static_partition, restore_snapshot_id=restore_snapshot_id)

    def write_arrow(self, data, bucket: Optional[int] = None):
        """Write Arrow data, optionally to a bucket assigned upstream."""
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.write_arrow(self, data, bucket)

    def write_arrow_batch(self, data: pa.RecordBatch, bucket: Optional[int] = None):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.write_arrow_batch(self, data, bucket)

    def roll_before_group_if_needed(self, row_count: int):
        """Roll current files before the next logical write group if needed."""
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.roll_before_group_if_needed(self, row_count)

    def write_row(self, row):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.write_row(self, row)

    def write_pandas(self, dataframe):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.write_pandas(self, dataframe)

    def with_write_type(self, write_cols: List[str]):
        write_cols = validate_write_columns(write_cols)
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.with_write_type(self, write_cols)

    def with_blob_consumer(self, blob_consumer: BlobConsumer):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.with_blob_consumer(self, blob_consumer)

    def with_blob_uri_reader_factory(self, uri_reader_factory):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.with_blob_uri_reader_factory(self, uri_reader_factory)

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
            hash_fixed_precluster: Pre-clustering mode. ``"auto"`` follows
                table options, ``"off"`` disables it, and ``"map_groups"``
                explicitly enables HASH_FIXED grouping.
            static_partition: Optional partition spec to overwrite. When set,
                the Ray write runs in overwrite mode for this partition and
                overrides any builder-level partition spec.
        """
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.write_ray(self, dataset, overwrite, concurrency,
                                               ray_remote_args, hash_fixed_precluster, static_partition)

    def close(self):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.close(self)

    def abort(self):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin.abort(self)

    def _create_file_store_write(self, commit_user):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._create_file_store_write(self, commit_user)

    def _create_row_key_extractor(self, static_partition):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._create_row_key_extractor(self, static_partition)

    def _write_partition_bucket_batch(self, partition, bucket, data):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._write_partition_bucket_batch(self, partition, bucket, data)

    def _write_precomputed_bucket(self, data, bucket):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._write_precomputed_bucket(self, data, bucket)

    def _write_partition_bucket_row(self, partition, bucket, row, values_by_name):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._write_partition_bucket_row(self, partition, bucket, row, values_by_name)

    def _distributed_write_options(self, *, overwrite=False):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._distributed_write_options(self, overwrite=overwrite)

    def _prepare_commit(self, commit_identifier):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._prepare_commit(self, commit_identifier)

    def _release_prepared_indexes(self):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._release_prepared_indexes(self)

    def _prepare_arrow_data(self, data):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._prepare_arrow_data(self, data)

    def _prepare_arrow_schema(self, data):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._prepare_arrow_schema(self, data)

    def _normalize_arrow_write_schema(self, data):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._normalize_arrow_write_schema(self, data)

    def _validate_pyarrow_schema(self, data_schema, *, check_nested_nullability=True):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._validate_pyarrow_schema(
            self, data_schema, check_nested_nullability=check_nested_nullability)

    def _is_compatible_pyarrow_schema(self, data_schema, expected_schema, *, check_nested_nullability=True):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._is_compatible_pyarrow_schema(
            self, data_schema, expected_schema, check_nested_nullability=check_nested_nullability)

    def _write_cols_pyarrow_schema(self, write_cols):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._write_cols_pyarrow_schema(self, write_cols)

    def _raise_inconsistent_schema(self, data_schema):
        from pypaimon.execution.python.write.table_write import PythonTableWriteMixin
        return PythonTableWriteMixin._raise_inconsistent_schema(self, data_schema)


class BatchTableWrite(TableWrite):
    def __init__(self, table, commit_user, static_partition: Optional[dict] = None,
                 *, restore_snapshot_id=None):
        super().__init__(table, commit_user, static_partition,
                         restore_snapshot_id=restore_snapshot_id)
        self.batch_committed = False

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        return self._prepare_commit(BATCH_COMMIT_IDENTIFIER)


class StreamTableWrite(TableWrite):

    def __init__(self, table, commit_user, *, restore_snapshot_id=None):
        super().__init__(table, commit_user, None,
                         restore_snapshot_id=restore_snapshot_id)

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        return self._prepare_commit(validate_commit_identifier(commit_identifier))
