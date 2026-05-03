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

from typing import Any, Dict, List, Tuple

from pypaimon.compact.rewriter.append_compact_rewriter import AppendCompactRewriter
from pypaimon.compact.task.compact_task import CompactTask, register_compact_task
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.write.commit_message import CommitMessage


@register_compact_task
class AppendCompactTask(CompactTask):
    """Compact a single (partition, bucket) of an append-only table.

    The driver attaches the in-process FileStoreTable so LocalExecutor can run
    without rebuilding catalog state. Distributed executors (RayExecutor, added
    in Phase 4) must instead populate the loader fields so the worker can
    rebuild the table — see to_dict()/from_dict() for the contract.
    """

    TYPE = "append-compact"

    def __init__(
        self,
        partition: Tuple,
        bucket: int,
        files: List[DataFileMeta],
        table=None,
    ):
        self.partition = tuple(partition)
        self.bucket = bucket
        self.files = list(files)
        self._table = table

    def with_table(self, table) -> "AppendCompactTask":
        self._table = table
        return self

    def run(self) -> CommitMessage:
        table = self._resolve_table()
        rewriter = AppendCompactRewriter(table)
        after = rewriter.rewrite(self.partition, self.bucket, self.files)
        return CommitMessage(
            partition=self.partition,
            bucket=self.bucket,
            compact_before=list(self.files),
            compact_after=list(after),
        )

    def to_dict(self) -> Dict[str, Any]:
        # Distributed executors will be wired up in Phase 4 (Ray). At that
        # point we'll replace this stub with catalog-options + identifier so
        # workers can rebuild the table; until then the LocalExecutor path
        # never serializes a task and this method should raise loudly.
        raise NotImplementedError(
            "AppendCompactTask.to_dict() is reserved for Phase 4 distributed "
            "execution; LocalExecutor runs tasks in-process without serialization."
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AppendCompactTask":
        raise NotImplementedError(
            "AppendCompactTask.from_dict() is reserved for Phase 4 distributed "
            "execution; LocalExecutor runs tasks in-process without serialization."
        )

    def _resolve_table(self):
        if self._table is None:
            raise RuntimeError(
                "AppendCompactTask has no table attached. The CompactJob/driver "
                "must call with_table(table) before handing tasks to an executor."
            )
        return self._table
