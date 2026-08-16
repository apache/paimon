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

"""The ``$buckets`` system table — per-bucket aggregated stats."""

from typing import List, Optional

import pyarrow

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "partition", AtomicType("STRING", nullable=True)),
    DataField(1, "bucket", AtomicType("INT", nullable=False)),
    DataField(2, "record_count", AtomicType("BIGINT", nullable=False)),
    DataField(3, "file_size_in_bytes", AtomicType("BIGINT", nullable=False)),
    DataField(4, "file_count", AtomicType("BIGINT", nullable=False)),
    DataField(5, "last_update_time", AtomicType("TIMESTAMP(3)", nullable=True)),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")


class BucketsTable(SystemTable):
    """The ``$buckets`` system table.

    Aggregates manifest entries by (partition, bucket) to show per-bucket
    record counts, file sizes, file counts and last update times.
    """

    def system_table_name(self) -> str:
        return "buckets"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["partition", "bucket"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return self._empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        manifest_file_manager = ManifestFileManager(self.base_table)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=True)

        _NULL = object()

        bucket_map: dict = {}
        for entry in entries:
            raw_key = tuple(
                (field.name, _NULL if value is None else value)
                for field, value in zip(
                    entry.partition.fields, entry.partition.values))
            bucket_id = int(entry.bucket)
            key = (raw_key, bucket_id)

            stats = bucket_map.get(key)
            if stats is None:
                render_items = tuple(
                    (name, str(val) if val is not _NULL else None)
                    for name, val in raw_key)
                stats = {
                    "render_items": render_items,
                    "bucket": bucket_id,
                    "record_count": 0,
                    "file_size_in_bytes": 0,
                    "file_count": 0,
                    "last_update_time": None,
                }
                bucket_map[key] = stats

            stats["record_count"] += int(entry.file.row_count)
            stats["file_size_in_bytes"] += int(entry.file.file_size)
            stats["file_count"] += 1
            ct_ms = entry.file.creation_time_epoch_millis()
            if ct_ms is not None:
                if (stats["last_update_time"] is None
                        or ct_ms > stats["last_update_time"]):
                    stats["last_update_time"] = ct_ms

        sorted_keys = sorted(
            bucket_map.keys(),
            key=lambda k: (
                _render_partition(bucket_map[k]["render_items"]) or "",
                k[1]))

        partition_strings: List[Optional[str]] = []
        buckets: List[int] = []
        record_counts: List[int] = []
        file_sizes: List[int] = []
        file_counts: List[int] = []
        last_update_times: List[Optional[int]] = []

        for key in sorted_keys:
            stats = bucket_map[key]
            partition_strings.append(_render_partition(stats["render_items"]))
            buckets.append(stats["bucket"])
            record_counts.append(stats["record_count"])
            file_sizes.append(stats["file_size_in_bytes"])
            file_counts.append(stats["file_count"])
            last_update_times.append(stats["last_update_time"])

        return pyarrow.table({
            "partition": pyarrow.array(
                partition_strings, type=pyarrow.string()),
            "bucket": pyarrow.array(buckets, type=pyarrow.int32()),
            "record_count": pyarrow.array(
                record_counts, type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array(
                file_sizes, type=pyarrow.int64()),
            "file_count": pyarrow.array(file_counts, type=pyarrow.int64()),
            "last_update_time": pyarrow.array(
                last_update_times, type=_TIMESTAMP_TYPE),
        })

    @staticmethod
    def _empty_table() -> pyarrow.Table:
        return pyarrow.table({
            "partition": pyarrow.array([], type=pyarrow.string()),
            "bucket": pyarrow.array([], type=pyarrow.int32()),
            "record_count": pyarrow.array([], type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array([], type=pyarrow.int64()),
            "file_count": pyarrow.array([], type=pyarrow.int64()),
            "last_update_time": pyarrow.array([], type=_TIMESTAMP_TYPE),
        })


def _render_partition(spec_items) -> Optional[str]:
    """Render a partition spec as ``pt=v/pt2=v2`` or None when empty.

    Null partition values are rendered as ``__NULL__`` to distinguish them
    from the literal string ``"None"``.  A partition whose value is
    literally ``"__NULL__"`` will produce the same rendered string —
    aggregation keys are still distinct, but the displayed partition
    column will collide.  This is a display-only limitation.
    """
    if not spec_items:
        return None
    return "/".join(
        "{}={}".format(name, "__NULL__" if value is None else value)
        for name, value in spec_items)
