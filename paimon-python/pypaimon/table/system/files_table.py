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

"""The ``$files`` system table — per-data-file detail of the latest snapshot."""

import json
from typing import Any, List, Optional

import pyarrow

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        RowType)
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "partition", AtomicType("STRING", nullable=True)),
    DataField(1, "bucket", AtomicType("INT", nullable=False)),
    DataField(2, "file_path", AtomicType("STRING", nullable=False)),
    DataField(3, "file_format", AtomicType("STRING", nullable=False)),
    DataField(4, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(5, "level", AtomicType("INT", nullable=False)),
    DataField(6, "record_count", AtomicType("BIGINT", nullable=False)),
    DataField(7, "file_size_in_bytes", AtomicType("BIGINT", nullable=False)),
    DataField(8, "min_key", AtomicType("STRING", nullable=True)),
    DataField(9, "max_key", AtomicType("STRING", nullable=True)),
    DataField(10, "null_value_counts", AtomicType("STRING", nullable=False)),
    DataField(11, "min_value_stats", AtomicType("STRING", nullable=False)),
    DataField(12, "max_value_stats", AtomicType("STRING", nullable=False)),
    DataField(13, "min_sequence_number", AtomicType("BIGINT", nullable=True)),
    DataField(14, "max_sequence_number", AtomicType("BIGINT", nullable=True)),
    DataField(15, "creation_time", AtomicType("TIMESTAMP(3)", nullable=True)),
    # ``deleteRowCount`` is intentionally camelCase to keep the on-wire
    # column name stable.
    DataField(16, "deleteRowCount", AtomicType("BIGINT", nullable=True)),
    DataField(17, "file_source", AtomicType("STRING", nullable=True)),
    DataField(18, "first_row_id", AtomicType("BIGINT", nullable=True)),
    DataField(
        19,
        "write_cols",
        ArrayType(nullable=True,
                  element_type=AtomicType("STRING", nullable=True))),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")
_WRITE_COLS_TYPE = pyarrow.list_(pyarrow.string())


def _to_json(obj: Any) -> str:
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False,
                      default=str)


def _stringify_path(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return str(value)


def _stats_columns(file_meta, table_field_names: List[str]) -> List[str]:
    cols = getattr(file_meta, "value_stats_cols", None)
    if cols:
        return list(cols)
    return list(table_field_names)


def _to_python(value: Any) -> Any:
    """Render an internal-row cell value into a JSON-safe primitive."""
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, (int, float, bool, str, list, dict)):
        return value
    return str(value)


def _render_key(row) -> Optional[str]:
    if row is None:
        return None
    values = getattr(row, "values", None)
    if not values:
        return None
    return _to_json([_to_python(v) for v in values])


def _render_partition(partition_row) -> Optional[str]:
    if partition_row is None:
        return None
    fields = getattr(partition_row, "fields", None) or []
    values = getattr(partition_row, "values", None) or []
    if not fields:
        return None
    return "/".join("{}={}".format(field.name, value)
                    for field, value in zip(fields, values))


def _render_stats_map(values: List[Any], columns: List[str]) -> str:
    pairs = {}
    n = min(len(columns), len(values) if values is not None else 0)
    for i in range(n):
        pairs[columns[i]] = _to_python(values[i])
    return _to_json(pairs)


def _render_null_counts(null_counts: Optional[List[int]],
                        columns: List[str]) -> str:
    pairs = {}
    if null_counts:
        n = min(len(columns), len(null_counts))
        for i in range(n):
            pairs[columns[i]] = (None if null_counts[i] is None
                                 else int(null_counts[i]))
    return _to_json(pairs)


class FilesTable(SystemTable):
    """The ``$files`` system table."""

    def system_table_name(self) -> str:
        return "files"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["file_path"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return self._empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        manifest_file_manager = ManifestFileManager(self.base_table)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=False)

        file_format = self.base_table.options.file_format()
        table_field_names = list(self.base_table.field_names)

        rows = {
            "partition": [],
            "bucket": [],
            "file_path": [],
            "file_format": [],
            "schema_id": [],
            "level": [],
            "record_count": [],
            "file_size_in_bytes": [],
            "min_key": [],
            "max_key": [],
            "null_value_counts": [],
            "min_value_stats": [],
            "max_value_stats": [],
            "min_sequence_number": [],
            "max_sequence_number": [],
            "creation_time": [],
            "deleteRowCount": [],
            "file_source": [],
            "first_row_id": [],
            "write_cols": [],
        }

        for entry in entries:
            meta = entry.file
            rows["partition"].append(_render_partition(entry.partition))
            rows["bucket"].append(int(entry.bucket))
            rows["file_path"].append(_stringify_path(
                meta.file_path or meta.file_name))
            rows["file_format"].append(file_format)
            rows["schema_id"].append(int(meta.schema_id))
            rows["level"].append(int(meta.level))
            rows["record_count"].append(int(meta.row_count))
            rows["file_size_in_bytes"].append(int(meta.file_size))
            rows["min_key"].append(_render_key(meta.min_key))
            rows["max_key"].append(_render_key(meta.max_key))

            stats_cols = _stats_columns(meta, table_field_names)
            value_stats = meta.value_stats
            rows["null_value_counts"].append(
                _render_null_counts(value_stats.null_counts, stats_cols))
            rows["min_value_stats"].append(_render_stats_map(
                getattr(value_stats.min_values, "values", []) or [],
                stats_cols))
            rows["max_value_stats"].append(_render_stats_map(
                getattr(value_stats.max_values, "values", []) or [],
                stats_cols))

            rows["min_sequence_number"].append(int(meta.min_sequence_number))
            rows["max_sequence_number"].append(int(meta.max_sequence_number))
            rows["creation_time"].append(meta.creation_time_epoch_millis())
            rows["deleteRowCount"].append(
                None if meta.delete_row_count is None
                else int(meta.delete_row_count))
            rows["file_source"].append(
                None if meta.file_source is None else str(meta.file_source))
            rows["first_row_id"].append(
                None if meta.first_row_id is None
                else int(meta.first_row_id))
            rows["write_cols"].append(
                list(meta.write_cols) if meta.write_cols else None)

        return pyarrow.table({
            "partition": pyarrow.array(
                rows["partition"], type=pyarrow.string()),
            "bucket": pyarrow.array(rows["bucket"], type=pyarrow.int32()),
            "file_path": pyarrow.array(
                rows["file_path"], type=pyarrow.string()),
            "file_format": pyarrow.array(
                rows["file_format"], type=pyarrow.string()),
            "schema_id": pyarrow.array(
                rows["schema_id"], type=pyarrow.int64()),
            "level": pyarrow.array(rows["level"], type=pyarrow.int32()),
            "record_count": pyarrow.array(
                rows["record_count"], type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array(
                rows["file_size_in_bytes"], type=pyarrow.int64()),
            "min_key": pyarrow.array(rows["min_key"], type=pyarrow.string()),
            "max_key": pyarrow.array(rows["max_key"], type=pyarrow.string()),
            "null_value_counts": pyarrow.array(
                rows["null_value_counts"], type=pyarrow.string()),
            "min_value_stats": pyarrow.array(
                rows["min_value_stats"], type=pyarrow.string()),
            "max_value_stats": pyarrow.array(
                rows["max_value_stats"], type=pyarrow.string()),
            "min_sequence_number": pyarrow.array(
                rows["min_sequence_number"], type=pyarrow.int64()),
            "max_sequence_number": pyarrow.array(
                rows["max_sequence_number"], type=pyarrow.int64()),
            "creation_time": pyarrow.array(
                rows["creation_time"], type=_TIMESTAMP_TYPE),
            "deleteRowCount": pyarrow.array(
                rows["deleteRowCount"], type=pyarrow.int64()),
            "file_source": pyarrow.array(
                rows["file_source"], type=pyarrow.string()),
            "first_row_id": pyarrow.array(
                rows["first_row_id"], type=pyarrow.int64()),
            "write_cols": pyarrow.array(
                rows["write_cols"], type=_WRITE_COLS_TYPE),
        })

    @staticmethod
    def _empty_table() -> pyarrow.Table:
        return pyarrow.table({
            "partition": pyarrow.array([], type=pyarrow.string()),
            "bucket": pyarrow.array([], type=pyarrow.int32()),
            "file_path": pyarrow.array([], type=pyarrow.string()),
            "file_format": pyarrow.array([], type=pyarrow.string()),
            "schema_id": pyarrow.array([], type=pyarrow.int64()),
            "level": pyarrow.array([], type=pyarrow.int32()),
            "record_count": pyarrow.array([], type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array([], type=pyarrow.int64()),
            "min_key": pyarrow.array([], type=pyarrow.string()),
            "max_key": pyarrow.array([], type=pyarrow.string()),
            "null_value_counts": pyarrow.array([], type=pyarrow.string()),
            "min_value_stats": pyarrow.array([], type=pyarrow.string()),
            "max_value_stats": pyarrow.array([], type=pyarrow.string()),
            "min_sequence_number": pyarrow.array([], type=pyarrow.int64()),
            "max_sequence_number": pyarrow.array([], type=pyarrow.int64()),
            "creation_time": pyarrow.array([], type=_TIMESTAMP_TYPE),
            "deleteRowCount": pyarrow.array([], type=pyarrow.int64()),
            "file_source": pyarrow.array([], type=pyarrow.string()),
            "first_row_id": pyarrow.array([], type=pyarrow.int64()),
            "write_cols": pyarrow.array([], type=_WRITE_COLS_TYPE),
        })
