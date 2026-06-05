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

"""The ``$snapshots`` system table — every committed snapshot's metadata."""

from typing import List, Optional

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "snapshot_id", AtomicType("BIGINT", nullable=False)),
    DataField(1, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(2, "commit_user", AtomicType("STRING", nullable=False)),
    DataField(3, "commit_identifier", AtomicType("BIGINT", nullable=False)),
    DataField(4, "commit_kind", AtomicType("STRING", nullable=False)),
    DataField(5, "commit_time", AtomicType("TIMESTAMP(3)", nullable=False)),
    DataField(6, "base_manifest_list", AtomicType("STRING", nullable=False)),
    DataField(7, "delta_manifest_list", AtomicType("STRING", nullable=False)),
    DataField(8, "changelog_manifest_list", AtomicType("STRING", nullable=True)),
    DataField(9, "total_record_count", AtomicType("BIGINT", nullable=True)),
    DataField(10, "delta_record_count", AtomicType("BIGINT", nullable=True)),
    DataField(11, "changelog_record_count", AtomicType("BIGINT", nullable=True)),
    DataField(12, "watermark", AtomicType("BIGINT", nullable=True)),
    DataField(13, "next_row_id", AtomicType("BIGINT", nullable=True)),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")


class SnapshotsTable(SystemTable):
    """The ``$snapshots`` system table."""

    def system_table_name(self) -> str:
        return "snapshots"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["snapshot_id"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshots = self.base_table.snapshot_manager().list_snapshots()

        snapshot_ids: List[int] = []
        schema_ids: List[int] = []
        commit_users: List[str] = []
        commit_identifiers: List[int] = []
        commit_kinds: List[str] = []
        commit_times: List[int] = []
        base_manifest_lists: List[str] = []
        delta_manifest_lists: List[str] = []
        changelog_manifest_lists: List[Optional[str]] = []
        total_record_counts: List[Optional[int]] = []
        delta_record_counts: List[Optional[int]] = []
        changelog_record_counts: List[Optional[int]] = []
        watermarks: List[Optional[int]] = []
        next_row_ids: List[Optional[int]] = []

        for snap in snapshots:
            snapshot_ids.append(int(snap.id))
            schema_ids.append(int(snap.schema_id))
            commit_users.append(snap.commit_user)
            commit_identifiers.append(int(snap.commit_identifier))
            commit_kinds.append(snap.commit_kind)
            commit_times.append(int(snap.time_millis))
            base_manifest_lists.append(snap.base_manifest_list)
            delta_manifest_lists.append(snap.delta_manifest_list)
            changelog_manifest_lists.append(snap.changelog_manifest_list)
            total_record_counts.append(
                None if snap.total_record_count is None
                else int(snap.total_record_count))
            delta_record_counts.append(
                None if snap.delta_record_count is None
                else int(snap.delta_record_count))
            changelog_record_counts.append(
                None if snap.changelog_record_count is None
                else int(snap.changelog_record_count))
            watermarks.append(
                None if snap.watermark is None else int(snap.watermark))
            next_row_ids.append(
                None if snap.next_row_id is None else int(snap.next_row_id))

        return pyarrow.table({
            "snapshot_id": pyarrow.array(snapshot_ids, type=pyarrow.int64()),
            "schema_id": pyarrow.array(schema_ids, type=pyarrow.int64()),
            "commit_user": pyarrow.array(commit_users, type=pyarrow.string()),
            "commit_identifier": pyarrow.array(
                commit_identifiers, type=pyarrow.int64()),
            "commit_kind": pyarrow.array(commit_kinds, type=pyarrow.string()),
            "commit_time": pyarrow.array(commit_times, type=_TIMESTAMP_TYPE),
            "base_manifest_list": pyarrow.array(
                base_manifest_lists, type=pyarrow.string()),
            "delta_manifest_list": pyarrow.array(
                delta_manifest_lists, type=pyarrow.string()),
            "changelog_manifest_list": pyarrow.array(
                changelog_manifest_lists, type=pyarrow.string()),
            "total_record_count": pyarrow.array(
                total_record_counts, type=pyarrow.int64()),
            "delta_record_count": pyarrow.array(
                delta_record_counts, type=pyarrow.int64()),
            "changelog_record_count": pyarrow.array(
                changelog_record_counts, type=pyarrow.int64()),
            "watermark": pyarrow.array(watermarks, type=pyarrow.int64()),
            "next_row_id": pyarrow.array(next_row_ids, type=pyarrow.int64()),
        })
