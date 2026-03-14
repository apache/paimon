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
from typing import Optional

from pypaimon.common.json_util import json_field, optional_json_field

BATCH_COMMIT_IDENTIFIER = 0x7fffffffffffffff


@dataclass
class Snapshot:
    # Required fields
    version: int = json_field("version")
    id: int = json_field("id")
    schema_id: int = json_field("schemaId")
    base_manifest_list: str = json_field("baseManifestList")
    delta_manifest_list: str = json_field("deltaManifestList")
    total_record_count: int = json_field("totalRecordCount")
    delta_record_count: int = json_field("deltaRecordCount")
    commit_user: str = json_field("commitUser")
    commit_identifier: int = json_field("commitIdentifier")
    commit_kind: str = json_field("commitKind")
    time_millis: int = json_field("timeMillis")
    # Optional fields with defaults
    changelog_manifest_list: Optional[str] = optional_json_field("changelogManifestList", "non_null")
    index_manifest: Optional[str] = optional_json_field("indexManifest", "non_null")
    changelog_record_count: Optional[int] = optional_json_field("changelogRecordCount", "non_null")
    watermark: Optional[int] = optional_json_field("watermark", "non_null")
    statistics: Optional[str] = optional_json_field("statistics", "non_null")
    next_row_id: Optional[int] = optional_json_field("nextRowId", "non_null")
