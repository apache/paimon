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

import re
from dataclasses import asdict, dataclass, fields
from typing import Any, Dict, Optional


@dataclass
class Snapshot:
    version: int
    id: int
    schema_id: int
    base_manifest_list: str
    delta_manifest_list: str
    commit_user: str
    commit_identifier: int
    commit_kind: str
    time_millis: int
    log_offsets: Dict[int, int]

    changelog_manifest_list: Optional[str] = None
    index_manifest: Optional[str] = None
    total_record_count: Optional[int] = None
    delta_record_count: Optional[int] = None
    changelog_record_count: Optional[int] = None
    watermark: Optional[int] = None
    statistics: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]):
        known_fields = {field.name for field in fields(Snapshot)}
        processed_data = {
            camel_to_snake(key): value
            for key, value in data.items()
            if camel_to_snake(key) in known_fields
        }
        return Snapshot(**processed_data)

    def to_json(self) -> Dict[str, Any]:
        snake_case_dict = asdict(self)
        return {
            snake_to_camel(key): value
            for key, value in snake_case_dict.items()
        }


def camel_to_snake(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(name: str) -> str:
    parts = name.split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])
