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
# (file format, compression, target_file_size) still come from CoreOptions on
# the table itself.
DEFAULT_MIN_FILE_NUM = 5
DEFAULT_MAX_FILE_NUM = 50
DEFAULT_FORCE_FULL = False


@dataclass
class CompactOptions:
    """Knobs that drive compaction planning.

    target_file_size is intentionally absent — it is sourced from the table's
    own CoreOptions (via DataWriter rolling) so a job inherits whatever the
    writer would use, keeping output sizes consistent across write/compact.
    """

    min_file_num: int = DEFAULT_MIN_FILE_NUM
    max_file_num: int = DEFAULT_MAX_FILE_NUM
    full_compaction: bool = DEFAULT_FORCE_FULL

    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_file_num": self.min_file_num,
            "max_file_num": self.max_file_num,
            "full_compaction": self.full_compaction,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "CompactOptions":
        if not data:
            return cls()
        return cls(
            min_file_num=data.get("min_file_num", DEFAULT_MIN_FILE_NUM),
            max_file_num=data.get("max_file_num", DEFAULT_MAX_FILE_NUM),
            full_compaction=data.get("full_compaction", DEFAULT_FORCE_FULL),
        )
