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

from typing import List, Optional

import pyarrow as pa

from pypaimon.write.commit_message import CommitMessage

from pypaimon.write.backend_dispatch import create_write
from pypaimon.table.bucket_mode import BucketMode
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
        return create_write(
            self.table, self.commit_user, self.static_partition,
            fixed_bucket=True, bucket_plan=self._bucket_plan,
            restore_snapshot_id=self.restore_snapshot_id)


class PostponeFixedBucketBatchTableWrite(BatchTableWrite):
    """Batch writer which plans postpone rows before routing new partitions."""

    def __init__(self, table, commit_user, static_partition: Optional[dict] = None, bucket_plan=None):
        self._python_execution().initialize(table, commit_user, static_partition, bucket_plan)
        self.batch_committed = False

    def _python_execution(self):
        from pypaimon.execution.python.write.postpone_batch_table_write import PythonPostponeFixedBucketWrite
        return PythonPostponeFixedBucketWrite(self)

    def write_arrow(self, table: pa.Table, bucket=None):
        return self._python_execution().write_arrow(table, bucket)

    def write_arrow_batch(self, data: pa.RecordBatch, bucket=None):
        return self._python_execution().write_arrow_batch(data, bucket)

    def write_row(self, row):
        return self._python_execution().write_row(row)

    def prepare_commit(self) -> List[CommitMessage]:
        return super().prepare_commit()

    def _distributed_write_options(self, *, overwrite=False):
        return self._python_execution()._distributed_write_options(overwrite=overwrite)

    def close(self):
        return self._python_execution().close()

    def abort(self):
        return self._python_execution().abort()
