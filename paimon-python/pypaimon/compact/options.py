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

from dataclasses import dataclass
from typing import Any, Dict, Optional

# Defaults mirror Java's append-only compaction options where possible. Only the
# subset that drives append/PK planning is exposed here; per-table options
# (file format, compression, target_file_size, open_file_cost) still come from
# CoreOptions on the table itself.
DEFAULT_MIN_FILE_NUM = 5
DEFAULT_FORCE_FULL = False


@dataclass
class CompactOptions:
    """Knobs that drive compaction planning.

    target_file_size and open_file_cost are intentionally absent — they are
    sourced from the table's own CoreOptions (target_file_size via DataWriter
    rolling, open_file_cost as the per-file overhead added when computing
    bin size). This keeps a job's output and packing decisions consistent
    with what the regular write path would produce.

    The Java AppendCompactCoordinator's `compactionFileNumLimit` /
    per-task max-file-count knobs aren't surfaced here: the size-based
    bin-packing in `_pick_files_for_bucket` naturally caps each task at
    ~2x target_file_size of input, which is the same shape Java produces.
    """

    min_file_num: int = DEFAULT_MIN_FILE_NUM
    full_compaction: bool = DEFAULT_FORCE_FULL

    def __post_init__(self):
        if self.min_file_num < 1:
            raise ValueError(f"min_file_num must be >= 1, got {self.min_file_num}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_file_num": self.min_file_num,
            "full_compaction": self.full_compaction,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "CompactOptions":
        if not data:
            return cls()
        return cls(
            min_file_num=data.get("min_file_num", DEFAULT_MIN_FILE_NUM),
            full_compaction=data.get("full_compaction", DEFAULT_FORCE_FULL),
        )
