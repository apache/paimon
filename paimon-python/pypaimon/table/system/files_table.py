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

from typing import Any, List, Optional

import pyarrow

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        RowType)
from pypaimon.table.system.system_table import SystemTable, metadata_snapshot


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


def _render_value(value, data_type=None) -> str:
    if value is None:
        return "null"
    if data_type is not None:
        from pypaimon.casting.row_to_string import cast_value_to_string
        from pypaimon.casting.numeric_utils import java_floating_string
        type_name = str(getattr(data_type, 'type', data_type)).upper()
        if type_name in ('FLOAT', 'REAL', 'DOUBLE'):
            return java_floating_string(value, type_name != 'DOUBLE')
        if type_name in ('BYTES', 'BINARY', 'VARBINARY') or type_name.startswith(('BINARY(', 'VARBINARY(')):
            from pypaimon.utils.file_store_path_factory import _decode_java_utf8
            return _decode_java_utf8(value)
        if 'TIMESTAMP_LTZ' in type_name or 'WITH LOCAL TIME ZONE' in type_name:
            from pypaimon.casting.temporal_string import format_timestamp_parts
            from pypaimon.casting.temporal_utils import _epoch_day_to_date, _local_offset
            from pypaimon.table.row.generic_row import (
                _datetime_to_millis_and_nanos, _normalize_ltz, _parse_type_precision_scale)
            # BinaryRow timestamps need not fit Arrow's i64 nanosecond range.
            value = _normalize_ltz(value)
            millis, nanos = _datetime_to_millis_and_nanos(value)
            seconds = millis // 1000
            day, second_of_day = divmod(seconds + _local_offset(seconds), 86400)
            year, month, day = _epoch_day_to_date(day)
            hour, second_of_day = divmod(second_of_day, 3600)
            minute, second = divmod(second_of_day, 60)
            nanos += millis % 1000 * 1000000 + getattr(value, 'nanosecond', 0)
            precision, _ = _parse_type_precision_scale(data_type)
            return format_timestamp_parts(year, month, day, hour, minute, second, nanos, precision)
        return cast_value_to_string(value, data_type)
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


def _render_object(value, data_type=None) -> str:
    # Java InternalRowUtils.get returns integer DATE/TIME and a Timestamp
    # whose toString is ISO LocalDateTime, including for LTZ key/stats cells.
    if value is None:
        return 'null'
    type_name = str(getattr(data_type, 'type', data_type)).upper()
    if type_name == 'DATE':
        from datetime import date
        return str((value - date(1970, 1, 1)).days)
    if type_name.startswith('TIME') and not type_name.startswith('TIMESTAMP'):
        return str(((value.hour * 60 + value.minute) * 60 + value.second) * 1000 + value.microsecond // 1000)
    if type_name.startswith('TIMESTAMP'):
        from datetime import timezone
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        nanos = value.microsecond * 1000 + getattr(value, 'nanosecond', 0)
        text = '{:04d}-{:02d}-{:02d}T{:02d}:{:02d}'.format(
            value.year, value.month, value.day, value.hour, value.minute)
        if value.second or nanos:
            text += ':{:02d}'.format(value.second)
        if nanos:
            precision = 3 if nanos % 1000000 == 0 else 6 if nanos % 1000 == 0 else 9
            text += '.' + '{:09d}'.format(nanos)[:precision]
        return text
    return _render_value(value, data_type)


def _render_key(row) -> Optional[str]:
    if row is None or not len(row):
        return None
    fields = getattr(row, 'fields', [])
    return '[' + ', '.join(_render_object(row.get_field(i), fields[i].type) for i in range(len(row))) + ']'


def _render_partition(partition_row) -> Optional[str]:
    if partition_row is None:
        return None
    fields = getattr(partition_row, 'fields', [])
    return '{' + ', '.join(_render_value(partition_row.get_field(i), field.type)
                           for i, field in enumerate(fields)) + '}'


def _metadata_file_path(table, entry):
    if entry.file.external_path is not None:
        return entry.file.external_path
    # Display Java's logical bucket path; the IO path may have decoded URI
    # escapes or dropped file: to reach a literal escaped local directory.
    from pypaimon.utils.path import resolve_path
    relative_bucket = table.path_factory().relative_bucket_path(
        tuple(entry.partition.values), int(entry.bucket), canonical_partition=True)
    bucket = resolve_path(str(table.table_path), relative_bucket)
    return bucket.rstrip('/') + '/' + entry.file.file_name


def _data_file_format(file_name):
    parts = file_name.rsplit('.', 2)
    if len(parts) < 2:
        raise ValueError('{} is not a legal file name'.format(file_name))
    if parts[-1].lower() in ('gz', 'bz2', 'deflate', 'snappy', 'lz4', 'zst'):
        if len(parts) < 3:
            raise ValueError('{} is not a legal file name'.format(file_name))
        return parts[-2]
    return parts[-1]


def _row_values(row) -> List[Any]:
    return [row.get_field(i) for i in range(len(row))]


def _stats_fields(stats_row) -> List[str]:
    # The manifest decodes a stats row with the fields its file stored,
    # resolved against the file's own schema, so they name the row's positions.
    return [field.name for field in stats_row.fields]


def _render_stats_map(values: List[Any], columns: List[str], fields=None) -> str:
    values = values or []
    pairs = {name: _render_object(values[i] if i < len(values) else None,
                                  fields[i].type if fields else None)
             for i, name in enumerate(columns)}
    return '{' + ', '.join(name + '=' + pairs[name] for name in sorted(pairs)) + '}'


def _render_null_counts(null_counts: Optional[List[int]], columns: List[str]) -> str:
    return _render_stats_map(null_counts, columns)


def _file_stats(table, meta):
    from pypaimon.casting.schema_evolution import cast_array_for_schema_evolution
    from pypaimon.schema.data_types import PyarrowFieldParser
    file_schema = table.schema_manager.get_schema(meta.schema_id)
    physical = {field.id: field for field in file_schema.data_file_fields(meta.write_cols)}
    stats = meta.value_stats
    names = _stats_fields(stats.min_values)
    nulls, lower, upper = [], [], []
    for field in table.fields:
        source = physical.get(field.id)
        if source is None:
            nulls.append(meta.row_count)
            lower.append(None)
            upper.append(None)
            continue
        index = names.index(source.name) if source.name in names else None
        nulls.append(stats.null_counts[index] if index is not None and stats.null_counts is not None
                     and index < len(stats.null_counts) else None)
        for row, output in ((stats.min_values, lower), (stats.max_values, upper)):
            value = row.get_field(index) if index is not None and index < len(row) else None
            if value is not None and source.type != field.type:
                value = cast_array_for_schema_evolution(
                    pyarrow.array([value], type=PyarrowFieldParser.from_paimon_type(source.type)),
                    PyarrowFieldParser.from_paimon_type(field.type), source.type, field.type)[0].as_py()
            output.append(value)
    return nulls, lower, upper


class FilesTable(SystemTable):
    """The ``$files`` system table."""

    def system_table_name(self) -> str:
        return "files"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["file_path"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = metadata_snapshot(self.base_table)
        if snapshot is None:
            return self._empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        manifest_file_manager = ManifestFileManager(self.base_table)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=False)

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
            rows["file_path"].append(_metadata_file_path(self.base_table, entry))
            rows["file_format"].append(_data_file_format(meta.file_name))
            rows["schema_id"].append(int(meta.schema_id))
            rows["level"].append(int(meta.level))
            rows["record_count"].append(int(meta.row_count))
            rows["file_size_in_bytes"].append(int(meta.file_size))
            rows["min_key"].append(_render_key(meta.min_key))
            rows["max_key"].append(_render_key(meta.max_key))

            nulls, lower, upper = _file_stats(self.base_table, meta)
            columns = [field.name for field in self.base_table.fields]
            rows["null_value_counts"].append(_render_null_counts(nulls, columns))
            rows["min_value_stats"].append(_render_stats_map(lower, columns, self.base_table.fields))
            rows["max_value_stats"].append(_render_stats_map(upper, columns, self.base_table.fields))

            rows["min_sequence_number"].append(int(meta.min_sequence_number))
            rows["max_sequence_number"].append(int(meta.max_sequence_number))
            rows["creation_time"].append(
                None if meta.creation_time is None else meta.creation_time.get_millisecond())
            rows["deleteRowCount"].append(
                None if meta.delete_row_count is None
                else int(meta.delete_row_count))
            rows["file_source"].append(
                None if meta.file_source is None else
                {0: "APPEND", 1: "COMPACT"}.get(meta.file_source, str(meta.file_source)))
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
