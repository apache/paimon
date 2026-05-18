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
class DataIncrement:
    """Increment of data files, changelog files and index files produced by a write.

    Direct port of org.apache.paimon.io.DataIncrement. Carries everything one
    write attempt contributes to a snapshot, so a CommitMessage can be
    constructed from a (DataIncrement, CompactIncrement) pair just like the
    Java side.

    - new_files: data files this write created (ADD entries).
    - deleted_files: data files this write removed without compaction
      (e.g. row-level delete in data-evolution tables); ADD/DELETE asymmetry
      is preserved by giving each list its own slot.
    - changelog_files: changelog data files associated with this write.
    - new_index_files / deleted_index_files: index file deltas (deletion
      vectors, global index, ...). Empty lists by default.
    """

    new_files: List[DataFileMeta] = field(default_factory=list)
    deleted_files: List[DataFileMeta] = field(default_factory=list)
    changelog_files: List[DataFileMeta] = field(default_factory=list)
    new_index_files: List[IndexFileMeta] = field(default_factory=list)
    deleted_index_files: List[IndexFileMeta] = field(default_factory=list)

    def is_empty(self) -> bool:
        return (
            not self.new_files
            and not self.deleted_files
            and not self.changelog_files
            and not self.new_index_files
            and not self.deleted_index_files
        )

    @classmethod
    def empty(cls) -> "DataIncrement":
        return cls()
