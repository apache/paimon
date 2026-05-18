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

from pypaimon.compact.levels import LevelSortedRun
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


@dataclass
class CompactUnit:
    """One unit of compaction work picked by a CompactStrategy.

    `output_level` is the LSM level the rewriter should write the merged
    output at. `file_rewrite=False` is a hint that the rewriter may simply
    upgrade files in place (no key merging needed) — used by the merge-tree
    rewriter for large non-overlapping inputs. The append-only path ignores
    it.
    """

    output_level: int
    files: List[DataFileMeta] = field(default_factory=list)
    file_rewrite: bool = False

    @classmethod
    def from_level_runs(cls, output_level: int, runs: List[LevelSortedRun]) -> "CompactUnit":
        files: List[DataFileMeta] = []
        for run in runs:
            files.extend(run.run.files)
        return cls(output_level=output_level, files=files, file_rewrite=False)

    @classmethod
    def from_files(
        cls,
        output_level: int,
        files: List[DataFileMeta],
        file_rewrite: bool = False,
    ) -> "CompactUnit":
        return cls(output_level=output_level, files=list(files), file_rewrite=file_rewrite)
