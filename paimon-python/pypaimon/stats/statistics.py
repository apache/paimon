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

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from pypaimon.stats.col_stats import ColStats


@dataclass
class Statistics:
    """Table-level statistics. Mirrors Java org.apache.paimon.stats.Statistics."""

    snapshot_id: int
    schema_id: int
    merged_record_count: Optional[int] = None
    merged_record_size: Optional[int] = None
    col_stats: Dict[str, ColStats] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "snapshotId": self.snapshot_id,
            "schemaId": self.schema_id,
        }
        if self.merged_record_count is not None:
            result["mergedRecordCount"] = self.merged_record_count
        if self.merged_record_size is not None:
            result["mergedRecordSize"] = self.merged_record_size
        result["colStats"] = {
            name: cs.to_dict() for name, cs in self.col_stats.items()
        }
        return result

    @classmethod
    def from_json(cls, json_str: str) -> 'Statistics':
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Statistics':
        col_stats = {
            name: ColStats.from_dict(cs_data)
            for name, cs_data in data.get("colStats", {}).items()
        }
        return cls(
            snapshot_id=data["snapshotId"],
            schema_id=data["schemaId"],
            merged_record_count=data.get("mergedRecordCount"),
            merged_record_size=data.get("mergedRecordSize"),
            col_stats=col_stats,
        )
