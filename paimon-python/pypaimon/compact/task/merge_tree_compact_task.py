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

from pypaimon.compact.rewriter.merge_tree_compact_rewriter import \
    MergeTreeCompactRewriter
from pypaimon.compact.task.compact_task import CompactTask, register_compact_task
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.interval_partition import IntervalPartition
from pypaimon.read.reader.merge_function import \
    create_merge_function_factory
from pypaimon.write.commit_message import CommitMessage


@register_compact_task
class MergeTreeCompactTask(CompactTask):
    """Compact a single (partition, bucket) of a primary-key table.

    Carries the picked CompactUnit's files plus the strategy-decided
    output_level and drop_delete flag. The driver attaches the in-process
    FileStoreTable; Phase 4 will plumb the loader fields for distributed
    execution.
    """

    TYPE = "merge-tree-compact"

    def __init__(
        self,
        partition: Tuple,
        bucket: int,
        files: List[DataFileMeta],
        output_level: int,
        drop_delete: bool,
        table=None,
    ):
        self.partition = tuple(partition)
        self.bucket = bucket
        self.files = list(files)
        self.output_level = output_level
        self.drop_delete = drop_delete
        self._table = table

    def with_table(self, table) -> "MergeTreeCompactTask":
        self._table = table
        return self

    def run(self) -> CommitMessage:
        if self._table is None:
            raise RuntimeError(
                "MergeTreeCompactTask has no table attached. The CompactJob/driver "
                "must call with_table(table) before handing tasks to an executor."
            )

        # IntervalPartition reproduces split_read.MergeFileSplitRead.create_reader's
        # section grouping so the rewriter sees the same "non-overlapping
        # SortedRuns per section" layout it would on a normal scan.
        sections = IntervalPartition(self.files).partition()

        rewriter = MergeTreeCompactRewriter(
            table=self._table,
            mf_factory=create_merge_function_factory(self._table.options),
        )
        after = rewriter.rewrite(
            partition=self.partition,
            bucket=self.bucket,
            output_level=self.output_level,
            sections=sections,
            drop_delete=self.drop_delete,
        )

        return CommitMessage(
            partition=self.partition,
            bucket=self.bucket,
            compact_before=list(self.files),
            compact_after=list(after),
        )

    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError(
            "MergeTreeCompactTask.to_dict() is reserved for Phase 4 distributed "
            "execution."
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MergeTreeCompactTask":
        raise NotImplementedError(
            "MergeTreeCompactTask.from_dict() is reserved for Phase 4 distributed "
            "execution."
        )
