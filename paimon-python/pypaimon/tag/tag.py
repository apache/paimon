#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pypaimon.common.json_util import json_field, optional_json_field
from pypaimon.snapshot.snapshot import Snapshot


@dataclass
class Tag:
    """
    Tag with tagCreateTime and tagTimeRetained.
    
    A Tag is essentially a Snapshot with additional metadata about when the tag
    was created and how long it should be retained.
    """
    # Required fields from Snapshot
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

    # Optional fields from Snapshot
    changelog_manifest_list: Optional[str] = optional_json_field("changelogManifestList", "non_null")
    index_manifest: Optional[str] = optional_json_field("indexManifest", "non_null")
    changelog_record_count: Optional[int] = optional_json_field("changelogRecordCount", "non_null")
    watermark: Optional[int] = optional_json_field("watermark", "non_null")
    statistics: Optional[str] = optional_json_field("statistics", "non_null")
    next_row_id: Optional[int] = optional_json_field("nextRowId", "non_null")

    # Tag-specific fields TODO support auto create and expire
    tag_create_time: Optional[str] = optional_json_field("tagCreateTime", "non_null")
    tag_time_retained: Optional[str] = optional_json_field("tagTimeRetained", "non_null")

    def trim_to_snapshot(self) -> Snapshot:
        """Convert this Tag to a Snapshot by removing tag-specific fields."""
        return Snapshot(
            version=self.version,
            id=self.id,
            schema_id=self.schema_id,
            base_manifest_list=self.base_manifest_list,
            delta_manifest_list=self.delta_manifest_list,
            total_record_count=self.total_record_count,
            delta_record_count=self.delta_record_count,
            commit_user=self.commit_user,
            commit_identifier=self.commit_identifier,
            commit_kind=self.commit_kind,
            time_millis=self.time_millis,
            changelog_manifest_list=self.changelog_manifest_list,
            index_manifest=self.index_manifest,
            changelog_record_count=self.changelog_record_count,
            watermark=self.watermark,
            statistics=self.statistics,
            next_row_id=self.next_row_id
        )

    @staticmethod
    def from_snapshot(
            snapshot: Snapshot,
            tag_time_retained: Optional[timedelta] = None,
            tag_create_time: Optional[datetime] = None
    ) -> 'Tag':
        """
        Create a Tag from a Snapshot with optional tag TTL.
        
        Args:
            snapshot: The snapshot to create the tag from
            tag_time_retained: Optional duration for how long the tag should be retained
            tag_create_time: Optional creation time, defaults to current time if tag_time_retained is set
            
        Returns:
            A new Tag instance
        """
        create_time_str = None
        retained_str = None

        if tag_time_retained is not None:
            if tag_create_time is None:
                tag_create_time = datetime.now()
            create_time_str = tag_create_time.isoformat()
            # Convert timedelta to ISO 8601 duration format (e.g., "PT1H" for 1 hour)
            total_seconds = int(tag_time_retained.total_seconds())
            retained_str = f"PT{total_seconds}S"

        return Tag(
            version=snapshot.version,
            id=snapshot.id,
            schema_id=snapshot.schema_id,
            base_manifest_list=snapshot.base_manifest_list,
            delta_manifest_list=snapshot.delta_manifest_list,
            total_record_count=snapshot.total_record_count,
            delta_record_count=snapshot.delta_record_count,
            commit_user=snapshot.commit_user,
            commit_identifier=snapshot.commit_identifier,
            commit_kind=snapshot.commit_kind,
            time_millis=snapshot.time_millis,
            changelog_manifest_list=snapshot.changelog_manifest_list,
            index_manifest=snapshot.index_manifest,
            changelog_record_count=snapshot.changelog_record_count,
            watermark=snapshot.watermark,
            statistics=snapshot.statistics,
            next_row_id=snapshot.next_row_id,
            tag_create_time=create_time_str,
            tag_time_retained=retained_str
        )
