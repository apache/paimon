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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import PostponeFixedBucketFileStoreWrite
from pypaimon.write.postpone_bucket import PostponeBucketPlanner
from pypaimon.write.row_key_extractor import PostponeFixedBucketRowKeyExtractor
from pypaimon.write.row_utils import (
    require_columns,
    row_to_named_values,
    row_values_to_arrow_table,
)
from pypaimon.write.table_write import BatchTableWrite
from pypaimon.write.write_builder import BatchWriteBuilder


class PostponeFixedBucketWriteBuilder(BatchWriteBuilder):
    """Write builder for fixed-bucket batches on a postpone table."""

    def __init__(self, table):
        if table.bucket_mode() != BucketMode.POSTPONE_MODE:
            raise ValueError(
                "Postpone fixed-bucket write requires a postpone-bucket table"
            )
        super().__init__(table)
        self._bucket_plan = None

    def with_bucket_plan(self, bucket_plan):
        """Use a precomputed partition bucket plan."""
        self._bucket_plan = bucket_plan
        return self

    def new_write(self):
        return PostponeFixedBucketBatchTableWrite(
            self.table,
            self.commit_user,
            self.static_partition,
            self._bucket_plan,
        )


class PostponeFixedBucketBatchTableWrite(BatchTableWrite):
    """Batch writer which plans postpone rows before routing new partitions."""

    def __init__(
        self,
        table,
        commit_user,
        static_partition: Optional[dict] = None,
        bucket_plan=None,
    ):
        self._planner = PostponeBucketPlanner(
            table,
            known_num_buckets=(
                bucket_plan.as_dict() if bucket_plan is not None else None
            ),
        )
        self._bucket_plan = (
            bucket_plan
            if bucket_plan is not None
            else self._planner.current_plan()
        )
        self._plan_provided = bucket_plan is not None
        self._pending_inputs = []
        super().__init__(table, commit_user, static_partition)

    def _create_file_store_write(self, commit_user):
        return PostponeFixedBucketFileStoreWrite(self.table, commit_user)

    def _create_row_key_extractor(self, static_partition):
        return PostponeFixedBucketRowKeyExtractor(
            self.table, self._bucket_plan)

    def _write_partition_bucket_batch(self, partition, bucket, data):
        self.file_store_write.write(
            partition,
            bucket,
            data,
            self.row_key_extractor.num_buckets(partition),
        )

    def _write_partition_bucket_row(
        self, partition, bucket, row, values_by_name
    ):
        self.file_store_write.write_row(
            partition,
            bucket,
            row,
            values_by_name,
            self.row_key_extractor.num_buckets(partition),
        )

    def _buffer_input(self, data) -> bool:
        if self._plan_provided:
            return False
        return any(
            not self._bucket_plan.contains(partition)
            for partition in self._planner.input_partition_stats(data)
        )

    def write_arrow(self, table: pa.Table):
        if not self._buffer_input(table):
            return super().write_arrow(table)
        self._validate_pyarrow_schema(table.schema)
        self._pending_inputs.extend(
            ("batch", batch) for batch in table.to_batches())

    def write_arrow_batch(self, data: pa.RecordBatch):
        if not self._buffer_input(data):
            return super().write_arrow_batch(data)
        self._validate_pyarrow_schema(data.schema)
        self._pending_inputs.append(("batch", data))

    def write_row(self, row):
        if self._plan_provided:
            return super().write_row(row)

        values_by_name = row_to_named_values(
            row, self.table.table_schema.fields)
        column_names = (
            self.file_store_write.write_cols
            if self.file_store_write.write_cols is not None
            else list(self.table.field_names)
        )
        require_columns(values_by_name, column_names, "write_row")
        require_columns(values_by_name, self.table.partition_keys, "write_row")
        partition = tuple(
            values_by_name[key] for key in self.table.partition_keys)
        if self._bucket_plan.contains(partition):
            return super().write_row(row)

        arrow_row = row_values_to_arrow_table(
            values_by_name, self.table.table_schema.fields, column_names)
        size = self._planner.input_partition_stats(arrow_row)[partition][1]
        self._pending_inputs.append(
            ("row", (row, partition, size)))

    def _flush_pending_inputs(self):
        if not self._pending_inputs:
            return

        partition_stats = {}
        for input_type, value in self._pending_inputs:
            if input_type == "batch":
                stats_by_partition = (
                    self._planner.input_partition_stats(value))
            else:
                _, partition, size = value
                stats_by_partition = {partition: (1, size)}
            for partition, stats in stats_by_partition.items():
                rows, size = partition_stats.get(partition, (0, 0))
                partition_stats[partition] = (
                    rows + stats[0], size + stats[1])

        self._bucket_plan = self._planner.plan(
            partition_stats,
            include_postpone_rows=self.static_partition is None,
        )
        self.row_key_extractor.with_bucket_plan(self._bucket_plan)
        inputs = self._pending_inputs
        self._pending_inputs = []
        for input_type, value in inputs:
            if input_type == "batch":
                super().write_arrow_batch(value)
            else:
                super().write_row(value[0])

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError(
                "BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        self._flush_pending_inputs()
        return self._prepare_commit(BATCH_COMMIT_IDENTIFIER)

    def _distributed_write_options(self) -> Dict[str, Any]:
        return {"postpone_bucket_planner": self._planner}

    def close(self):
        self._pending_inputs = []
        super().close()

    def abort(self):
        self._pending_inputs = []
        super().abort()
