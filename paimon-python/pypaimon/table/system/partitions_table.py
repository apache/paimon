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

"""The ``$partitions`` system table — aggregated partition stats."""

from typing import List, Optional

import pyarrow

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "partition", AtomicType("STRING", nullable=True)),
    DataField(1, "record_count", AtomicType("BIGINT", nullable=False)),
    DataField(2, "file_size_in_bytes", AtomicType("BIGINT", nullable=False)),
    DataField(3, "file_count", AtomicType("BIGINT", nullable=False)),
    DataField(4, "last_update_time", AtomicType("TIMESTAMP(3)", nullable=True)),
    DataField(5, "created_at", AtomicType("TIMESTAMP(3)", nullable=True)),
    DataField(6, "created_by", AtomicType("STRING", nullable=True)),
    DataField(7, "updated_by", AtomicType("STRING", nullable=True)),
    DataField(8, "options", AtomicType("STRING", nullable=True)),
    DataField(9, "total_buckets", AtomicType("INT", nullable=False)),
    DataField(10, "done", AtomicType("BOOLEAN", nullable=False)),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")


class PartitionsTable(SystemTable):
    """The ``$partitions`` system table.

    The filesystem flow aggregates from manifest entries; catalog-owned
    columns (``created_at``, ``created_by``, ``updated_by``, ``options``,
    ``done``) are filled with placeholders because the filesystem
    catalog does not track them.
    """

    def system_table_name(self) -> str:
        return "partitions"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["partition"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return self._empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        manifest_file_manager = ManifestFileManager(self.base_table)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=True)

        partition_map: dict = {}
        for entry in entries:
            spec_items = tuple(
                (field.name, str(value))
                for field, value in zip(
                    entry.partition.fields, entry.partition.values))
            spec_key = spec_items

            stats = partition_map.get(spec_key)
            if stats is None:
                stats = {
                    "spec_items": spec_items,
                    "record_count": 0,
                    "file_size_in_bytes": 0,
                    "file_count": 0,
                    "last_update_time": None,
                    "buckets": set(),
                }
                partition_map[spec_key] = stats

            stats["record_count"] += int(entry.file.row_count)
            stats["file_size_in_bytes"] += int(entry.file.file_size)
            stats["file_count"] += 1
            if entry.file.creation_time is not None:
                ct_ms = entry.file.creation_time.get_millisecond()
                if (stats["last_update_time"] is None
                        or ct_ms > stats["last_update_time"]):
                    stats["last_update_time"] = ct_ms
            stats["buckets"].add(int(entry.bucket))

        partition_strings: List[Optional[str]] = []
        record_counts: List[int] = []
        file_sizes: List[int] = []
        file_counts: List[int] = []
        last_update_times: List[Optional[int]] = []
        total_buckets: List[int] = []

        for stats in partition_map.values():
            partition_strings.append(_render_partition(stats["spec_items"]))
            record_counts.append(stats["record_count"])
            file_sizes.append(stats["file_size_in_bytes"])
            file_counts.append(stats["file_count"])
            last_update_times.append(stats["last_update_time"])
            total_buckets.append(len(stats["buckets"]))

        n = len(partition_map)
        return pyarrow.table({
            "partition": pyarrow.array(
                partition_strings, type=pyarrow.string()),
            "record_count": pyarrow.array(
                record_counts, type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array(
                file_sizes, type=pyarrow.int64()),
            "file_count": pyarrow.array(file_counts, type=pyarrow.int64()),
            "last_update_time": pyarrow.array(
                last_update_times, type=_TIMESTAMP_TYPE),
            "created_at": pyarrow.array([None] * n, type=_TIMESTAMP_TYPE),
            "created_by": pyarrow.array([None] * n, type=pyarrow.string()),
            "updated_by": pyarrow.array([None] * n, type=pyarrow.string()),
            "options": pyarrow.array([None] * n, type=pyarrow.string()),
            "total_buckets": pyarrow.array(total_buckets, type=pyarrow.int32()),
            "done": pyarrow.array([False] * n, type=pyarrow.bool_()),
        })

    @staticmethod
    def _empty_table() -> pyarrow.Table:
        return pyarrow.table({
            "partition": pyarrow.array([], type=pyarrow.string()),
            "record_count": pyarrow.array([], type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array([], type=pyarrow.int64()),
            "file_count": pyarrow.array([], type=pyarrow.int64()),
            "last_update_time": pyarrow.array([], type=_TIMESTAMP_TYPE),
            "created_at": pyarrow.array([], type=_TIMESTAMP_TYPE),
            "created_by": pyarrow.array([], type=pyarrow.string()),
            "updated_by": pyarrow.array([], type=pyarrow.string()),
            "options": pyarrow.array([], type=pyarrow.string()),
            "total_buckets": pyarrow.array([], type=pyarrow.int32()),
            "done": pyarrow.array([], type=pyarrow.bool_()),
        })


def _render_partition(spec_items) -> Optional[str]:
    """Render a partition spec as ``pt=v/pt2=v2`` or None when empty."""
    if not spec_items:
        return None
    return "/".join("{}={}".format(name, value) for name, value in spec_items)
