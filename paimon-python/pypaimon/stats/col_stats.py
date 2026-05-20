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

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ColStats:
    """Per-column statistics. Mirrors Java org.apache.paimon.stats.ColStats."""

    col_id: int
    distinct_count: Optional[int] = None
    min: Optional[str] = None
    max: Optional[str] = None
    null_count: Optional[int] = None
    avg_len: Optional[int] = None
    max_len: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {"colId": self.col_id}
        if self.distinct_count is not None:
            result["distinctCount"] = self.distinct_count
        if self.min is not None:
            result["min"] = self.min
        if self.max is not None:
            result["max"] = self.max
        if self.null_count is not None:
            result["nullCount"] = self.null_count
        if self.avg_len is not None:
            result["avgLen"] = self.avg_len
        if self.max_len is not None:
            result["maxLen"] = self.max_len
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColStats':
        return cls(
            col_id=data["colId"],
            distinct_count=data.get("distinctCount"),
            min=data.get("min"),
            max=data.get("max"),
            null_count=data.get("nullCount"),
            avg_len=data.get("avgLen"),
            max_len=data.get("maxLen"),
        )
