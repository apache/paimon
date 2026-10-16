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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.files_table import _render_value
from pypaimon.table.system.system_table import (
    SystemTable, metadata_snapshot, metadata_local_millis, metadata_epoch_millis)
from pypaimon.table.row.generic_row import GenericRowSerializer
from pypaimon.utils.file_store_path_factory import _is_null_or_whitespace_only


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
    DataField(11, "deleted_record_count", AtomicType("BIGINT", nullable=True)),
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
        snapshot = metadata_snapshot(self.base_table)
        if snapshot is None:
            return self._empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        manifest_file_manager = ManifestFileManager(self.base_table)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=True)
        deleted_record_counts = self._deleted_record_counts(snapshot)

        partition_map: dict = {}
        for entry in entries:
            partition_key = bytes(GenericRowSerializer.to_bytes(entry.partition))
            spec_key = partition_key
            partition_string = _render_partition(
                entry.partition, self.base_table.options.options.get(CoreOptions.PARTITION_DEFAULT_NAME))

            stats = partition_map.get(spec_key)
            if stats is None:
                stats = {
                    "partition_string": partition_string,
                    "partition_key": partition_key,
                    "record_count": 0,
                    "file_size_in_bytes": 0,
                    "file_count": 0,
                    "last_update_time": None,
                    "total_buckets": entry.total_buckets,
                }
                partition_map[spec_key] = stats

            stats["record_count"] += int(entry.file.row_count)
            stats["file_size_in_bytes"] += int(entry.file.file_size)
            stats["file_count"] += 1
            if entry.file.creation_time is not None:
                ct_ms = metadata_epoch_millis(entry.file.creation_time.get_millisecond())
                if (stats["last_update_time"] is None
                        or ct_ms > stats["last_update_time"]):
                    stats["last_update_time"] = ct_ms
            stats["total_buckets"] = entry.total_buckets

        partition_strings: List[Optional[str]] = []
        record_counts: List[int] = []
        file_sizes: List[int] = []
        file_counts: List[int] = []
        last_update_times: List[Optional[int]] = []
        total_buckets: List[int] = []
        deleted_counts: List[Optional[int]] = []

        for stats in sorted(partition_map.values(), key=lambda stats: stats["partition_string"]):
            partition_strings.append(stats["partition_string"])
            record_counts.append(stats["record_count"])
            file_sizes.append(stats["file_size_in_bytes"])
            file_counts.append(stats["file_count"])
            last_update_times.append(None if stats["last_update_time"] is None
                                     else metadata_local_millis(stats["last_update_time"]))
            total_buckets.append(stats["total_buckets"])
            deleted_counts.append(
                deleted_record_counts.get(stats["partition_key"], 0))

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
            "deleted_record_count": pyarrow.array(
                deleted_counts, type=pyarrow.int64()),
        })

    def _deleted_record_counts(self, snapshot) -> dict:
        if not self.base_table.options.deletion_vectors_enabled(False):
            return {}

        entries = IndexFileHandler(self.base_table).scan(
            snapshot,
            lambda entry: entry.kind == 0 and entry.index_file.index_type
            == IndexManifestFile.DELETION_VECTORS_INDEX,
        )
        result = {}
        for entry in entries:
            partition_key = bytes(GenericRowSerializer.to_bytes(entry.partition))
            if partition_key in result and result[partition_key] is None:
                continue

            dv_ranges = entry.index_file.dv_ranges
            if not dv_ranges:
                continue

            count = result.get(partition_key, 0)
            for dv_meta in dv_ranges.values():
                if dv_meta.cardinality is None:
                    result[partition_key] = None
                    break
                count += int(dv_meta.cardinality)
            else:
                result[partition_key] = count

        return result

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
            "deleted_record_count": pyarrow.array([], type=pyarrow.int64()),
        })


def _render_partition(partition, default_name) -> str:
    """Java PartitionsTable uses typed SQL casts and an unescaped key=value spec."""
    parts = []
    for index, field in enumerate(partition.fields):
        value = partition.get_field(index)
        text = default_name if value is None else _render_value(value, field.type)
        if _is_null_or_whitespace_only(text):
            text = default_name
        parts.append("{}={}".format(field.name, text))
    return "/".join(parts)
