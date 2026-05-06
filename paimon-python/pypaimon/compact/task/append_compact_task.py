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
from pypaimon.manifest.schema.data_file_meta import (DataFileMeta, decode_value,
                                                     encode_value)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.compact_increment import CompactIncrement


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
            total_buckets=table.total_buckets,
            compact_increment=CompactIncrement(
                compact_before=list(self.files),
                compact_after=list(after),
            ),
        )

    def _to_payload(self) -> Dict[str, Any]:
        return {
            "partition": [encode_value(v) for v in self.partition],
            "bucket": self.bucket,
            "files": [f.to_dict() for f in self.files],
        }

    @classmethod
    def _from_payload(cls, payload: Dict[str, Any]) -> "AppendCompactTask":
        return cls(
            partition=tuple(decode_value(v) for v in payload.get("partition") or []),
            bucket=payload["bucket"],
            files=[DataFileMeta.from_dict(f) for f in payload.get("files") or []],
        )

    def _resolve_table(self):
        if self._table is not None:
            return self._table
        return self._resolve_table_via_loader()
