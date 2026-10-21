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

from pypaimon.write.commit_message import CommitMessage
from pypaimon.execution.python.write.file_store_write import PostponeFixedBucketFileStoreWrite
from pypaimon.write.postpone_bucket import PostponeBucketPlanner
from pypaimon.execution.python.write.table_write import PythonTableWrite
from pypaimon.execution.python.write.row_key_extractor import PostponeFixedBucketRowKeyExtractor
from pypaimon.write.row_utils import (
    require_columns,
    row_to_named_values,
    row_values_to_arrow_table,
)


class PythonPostponeFixedBucketWrite(PythonTableWrite):
    """Batch writer which plans postpone rows before routing new partitions."""

    def initialize(
        self,
        table,
        commit_user,
        static_partition: Optional[dict] = None,
        bucket_plan=None,
    ):
        writer = self.writer
        writer._planner = PostponeBucketPlanner(
            table,
            known_num_buckets=(
                bucket_plan.as_dict() if bucket_plan is not None
                else {} if static_partition is not None else None
            ),
        )
        writer._bucket_plan = (
            bucket_plan
            if bucket_plan is not None
            else writer._planner.current_plan()
        )
        writer._plan_provided = bucket_plan is not None
        writer._pending_inputs = []
        PythonTableWrite(writer).initialize(table, commit_user, static_partition)

    def _create_file_store_write(self, commit_user):
        writer = self.writer
        return PostponeFixedBucketFileStoreWrite(writer.table, commit_user)

    def _create_row_key_extractor(self, static_partition):
        writer = self.writer
        return PostponeFixedBucketRowKeyExtractor(
            writer.table, writer._bucket_plan)

    def _write_partition_bucket_batch(self, partition, bucket, data):
        writer = self.writer
        writer.file_store_write.write(
            partition,
            bucket,
            data,
            writer.row_key_extractor.num_buckets(partition),
        )

    def _write_partition_bucket_row(
        self, partition, bucket, row, values_by_name
    ):
        writer = self.writer
        writer.file_store_write.write_row(
            partition,
            bucket,
            row,
            values_by_name,
            writer.row_key_extractor.num_buckets(partition),
        )

    def _buffer_input(self, data) -> bool:
        writer = self.writer
        if writer._plan_provided:
            return False
        return any(
            not writer._bucket_plan.contains(partition)
            for partition in writer._planner.input_partition_stats(data)
        )

    def write_arrow(self, table: pa.Table, bucket=None):
        writer = self.writer
        if bucket is not None:
            raise ValueError('Precomputed bucket writes require HASH_FIXED or HASH_DYNAMIC tables')
        table = writer._prepare_arrow_data(table)
        if not writer._buffer_input(table):
            return PythonTableWrite(writer).write_arrow(table)
        writer._pending_inputs.extend(
            ("batch", batch) for batch in table.to_batches())

    def write_arrow_batch(self, data: pa.RecordBatch, bucket=None):
        writer = self.writer
        if bucket is not None:
            raise ValueError('Precomputed bucket writes require HASH_FIXED or HASH_DYNAMIC tables')
        data = writer._prepare_arrow_data(data)
        if not writer._buffer_input(data):
            return PythonTableWrite(writer).write_arrow_batch(data)
        writer._pending_inputs.append(("batch", data))

    def write_row(self, row):
        writer = self.writer
        if writer._plan_provided:
            return PythonTableWrite(writer).write_row(row)

        values_by_name = row_to_named_values(
            row, writer.table.table_schema.fields)
        column_names = (
            writer.file_store_write.write_cols
            if writer.file_store_write.write_cols is not None
            else list(writer.table.field_names)
        )
        require_columns(values_by_name, column_names, "write_row")
        require_columns(values_by_name, writer.table.partition_keys, "write_row")
        from pypaimon.write.row_kind import skip_write_row

        if skip_write_row(writer.table, values_by_name, row.get_row_kind(), writer.file_store_write.write_cols):
            return
        partition = tuple(
            values_by_name[key] for key in writer.table.partition_keys)
        if writer._bucket_plan.contains(partition):
            return PythonTableWrite(writer).write_row(row)

        arrow_row = row_values_to_arrow_table(
            values_by_name, writer.table.table_schema.fields, column_names)
        size = writer._planner.input_partition_stats(arrow_row)[partition][1]
        writer._pending_inputs.append(
            ("row", (row, partition, size)))

    def _flush_pending_inputs(self):
        writer = self.writer
        if not writer._pending_inputs:
            return

        partition_stats = {}
        for input_type, value in writer._pending_inputs:
            if input_type == "batch":
                stats_by_partition = (
                    writer._planner.input_partition_stats(value))
            else:
                _, partition, size = value
                stats_by_partition = {partition: (1, size)}
            for partition, stats in stats_by_partition.items():
                rows, size = partition_stats.get(partition, (0, 0))
                partition_stats[partition] = (
                    rows + stats[0], size + stats[1])

        writer._bucket_plan = writer._planner.plan(
            partition_stats,
        )
        writer.row_key_extractor.with_bucket_plan(writer._bucket_plan)
        inputs = writer._pending_inputs
        writer._pending_inputs = []
        for input_type, value in inputs:
            if input_type == "batch":
                PythonTableWrite(writer).write_arrow_batch(value)
            else:
                PythonTableWrite(writer).write_row(value[0])

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        writer = self.writer
        writer._flush_pending_inputs()
        return PythonTableWrite(writer).prepare_commit(commit_identifier)

    def _distributed_write_options(self, *, overwrite=False) -> Dict[str, Any]:
        writer = self.writer
        if overwrite and not writer._plan_provided:
            return {"postpone_bucket_planner": PostponeBucketPlanner(writer.table, known_num_buckets={})}
        return {"postpone_bucket_planner": writer._planner}

    def close(self):
        writer = self.writer
        writer._pending_inputs = []
        PythonTableWrite(writer).close()

    def abort(self):
        writer = self.writer
        writer._pending_inputs = []
        PythonTableWrite(writer).abort()
