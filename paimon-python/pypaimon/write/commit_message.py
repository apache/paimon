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

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.write.compact_increment import CompactIncrement
from pypaimon.write.data_increment import DataIncrement


@dataclass
class CommitMessage:
    """File committable for sink.

    Direct port of org.apache.paimon.table.sink.CommitMessageImpl. Carries
    everything one (partition, bucket) writer or compactor contributes to a
    snapshot, packaged as a (data_increment, compact_increment) pair so the
    same message type can describe both pure writes and compaction results.

    - partition / bucket: identify the (partition, bucket) the message
      applies to.
    - total_buckets: number of buckets the table had at write time, used by
      the commit path to detect bucket-count changes.
    - data_increment: ADD/DELETE/changelog/index deltas from a normal write.
    - compact_increment: ADD/DELETE/changelog/index deltas from compaction.
    - check_from_snapshot: row-tracking conflict-detection anchor; -1 means
      "no check" (default).
    """

    partition: Tuple
    bucket: int
    total_buckets: Optional[int] = None
    data_increment: DataIncrement = field(default_factory=DataIncrement)
    compact_increment: CompactIncrement = field(default_factory=CompactIncrement)
    check_from_snapshot: Optional[int] = -1

    # ---- Convenience accessors ---------------------------------------------
    # Mirror Java's CommitMessageImpl shape: callers usually want the
    # individual file lists rather than reaching through the increment.

    @property
    def new_files(self) -> List[DataFileMeta]:
        return self.data_increment.new_files

    @property
    def deleted_files(self) -> List[DataFileMeta]:
        return self.data_increment.deleted_files

    @property
    def changelog_files(self) -> List[DataFileMeta]:
        return self.data_increment.changelog_files

    @property
    def compact_before(self) -> List[DataFileMeta]:
        return self.compact_increment.compact_before

    @property
    def compact_after(self) -> List[DataFileMeta]:
        return self.compact_increment.compact_after

    @property
    def compact_changelog_files(self) -> List[DataFileMeta]:
        return self.compact_increment.changelog_files

    def is_empty(self) -> bool:
        return self.data_increment.is_empty() and self.compact_increment.is_empty()
