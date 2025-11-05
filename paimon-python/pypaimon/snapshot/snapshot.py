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
from typing import Dict, Optional

from pypaimon.common.json_util import json_field

BATCH_COMMIT_IDENTIFIER = 0x7fffffffffffffff


@dataclass
class Snapshot:
    # Required fields
    id: int = json_field("id")
    schema_id: int = json_field("schemaId")
    base_manifest_list: str = json_field("baseManifestList")
    delta_manifest_list: str = json_field("deltaManifestList")
    commit_user: str = json_field("commitUser")
    commit_identifier: int = json_field("commitIdentifier")
    commit_kind: str = json_field("commitKind")
    time_millis: int = json_field("timeMillis")
    # Optional fields with defaults
    version: Optional[int] = json_field("version", default=None)
    log_offsets: Optional[Dict[int, int]] = json_field("logOffsets", default_factory=dict)
    changelog_manifest_list: Optional[str] = json_field("changelogManifestList", default=None)
    index_manifest: Optional[str] = json_field("indexManifest", default=None)
    total_record_count: Optional[int] = json_field("totalRecordCount", default=None)
    delta_record_count: Optional[int] = json_field("deltaRecordCount", default=None)
    changelog_record_count: Optional[int] = json_field("changelogRecordCount", default=None)
    watermark: Optional[int] = json_field("watermark", default=None)
    statistics: Optional[str] = json_field("statistics", default=None)
    next_row_id: Optional[int] = json_field("nextRowId", default=None)
