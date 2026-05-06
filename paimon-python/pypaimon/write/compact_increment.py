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
from typing import List

from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


@dataclass
class CompactIncrement:
    """Files changed before and after compaction, with changelog produced during compaction.

    Direct port of org.apache.paimon.io.CompactIncrement.

    - compact_before: input files consumed by compaction (DELETE entries).
    - compact_after: rewritten output files (ADD entries).
    - changelog_files: changelog files emitted while compacting (used by the
      full-compaction changelog producer; empty in the basic dedup path).
    - new_index_files / deleted_index_files: index file deltas attributable
      to this compaction (deletion vectors / global index updates). Empty
      lists by default.
    """

    compact_before: List[DataFileMeta] = field(default_factory=list)
    compact_after: List[DataFileMeta] = field(default_factory=list)
    changelog_files: List[DataFileMeta] = field(default_factory=list)
    new_index_files: List[IndexFileMeta] = field(default_factory=list)
    deleted_index_files: List[IndexFileMeta] = field(default_factory=list)

    def is_empty(self) -> bool:
        return (
            not self.compact_before
            and not self.compact_after
            and not self.changelog_files
            and not self.new_index_files
            and not self.deleted_index_files
        )

    @classmethod
    def empty(cls) -> "CompactIncrement":
        return cls()
