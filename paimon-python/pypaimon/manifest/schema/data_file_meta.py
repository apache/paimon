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
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from base64 import b64decode, b64encode
from typing import Any, Dict, List, Optional
import time

from pypaimon.utils.range import Range
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.simple_stats import (KEY_STATS_SCHEMA, VALUE_STATS_SCHEMA,
                                                   SimpleStats)
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import RowKind
from pypaimon.utils.file_store_path_factory import _is_null_or_whitespace_only


@dataclass
class DataFileMeta:
    file_name: str
    file_size: int
    row_count: int
    min_key: GenericRow
    max_key: GenericRow
    key_stats: SimpleStats
    value_stats: SimpleStats
    min_sequence_number: int
    max_sequence_number: int
    schema_id: int
    level: int
    extra_files: List[str]

    creation_time: Optional[Timestamp] = None
    delete_row_count: Optional[int] = None
    embedded_index: Optional[bytes] = None
    file_source: Optional[int] = None
    value_stats_cols: Optional[List[str]] = None
    external_path: Optional[str] = None
    first_row_id: Optional[int] = None
    write_cols: Optional[List[str]] = None

    # not a schema field, just for internal usage
    file_path: str = None

    def row_id_range(self) -> Optional[Range]:
        if self.first_row_id is None:
            return None
        return Range(self.first_row_id, self.first_row_id + self.row_count - 1)

    def get_creation_time(self) -> Optional[Timestamp]:
        return self.creation_time

    def creation_time_epoch_millis(self) -> Optional[int]:
        if self.creation_time is None:
            return None
        local_dt = self.creation_time.to_local_date_time()
        local_time_struct = local_dt.timetuple()
        local_timestamp = time.mktime(local_time_struct)
        utc_timestamp = time.mktime(time.gmtime(local_timestamp))
        tz_offset_seconds = int(local_timestamp - utc_timestamp)
        return int((local_timestamp - tz_offset_seconds) * 1000)

    def creation_time_as_datetime(self) -> Optional[datetime]:
        if self.creation_time is None:
            return None
        return self.creation_time.to_local_date_time()

    @classmethod
    def create(
        cls,
        file_name: str,
        file_size: int,
        row_count: int,
        min_key: GenericRow,
        max_key: GenericRow,
        key_stats: SimpleStats,
        value_stats: SimpleStats,
        min_sequence_number: int,
        max_sequence_number: int,
        schema_id: int,
        level: int,
        extra_files: List[str],
        creation_time: Optional[Timestamp] = None,
        delete_row_count: Optional[int] = None,
        embedded_index: Optional[bytes] = None,
        file_source: Optional[int] = None,
        value_stats_cols: Optional[List[str]] = None,
        external_path: Optional[str] = None,
        first_row_id: Optional[int] = None,
        write_cols: Optional[List[str]] = None,
        file_path: Optional[str] = None,
    ) -> 'DataFileMeta':
        if creation_time is None:
            creation_time = Timestamp.now()

        return cls(
            file_name=file_name,
            file_size=file_size,
            row_count=row_count,
            min_key=min_key,
            max_key=max_key,
            key_stats=key_stats,
            value_stats=value_stats,
            min_sequence_number=min_sequence_number,
            max_sequence_number=max_sequence_number,
            schema_id=schema_id,
            level=level,
            extra_files=extra_files,
            creation_time=creation_time,
            delete_row_count=delete_row_count,
            embedded_index=embedded_index,
            file_source=file_source,
            value_stats_cols=value_stats_cols,
            external_path=external_path,
            first_row_id=first_row_id,
            write_cols=write_cols,
            file_path=file_path,
        )

    def set_file_path(
            self, table_path: str, partition: GenericRow, bucket: int,
            default_part_value: str = "__DEFAULT_PARTITION__"):
        path_builder = table_path.rstrip('/')
        partition_dict = partition.to_dict()
        for field_name, field_value in partition_dict.items():
            part_value = default_part_value if _is_null_or_whitespace_only(field_value) else str(field_value)
            path_builder = f"{path_builder}/{field_name}={part_value}"
        path_builder = f"{path_builder}/bucket-{str(bucket)}/{self.file_name}"
        self.file_path = path_builder

    def copy_without_stats(self) -> 'DataFileMeta':
        """Create a new DataFileMeta without value statistics."""
        return DataFileMeta(
            file_name=self.file_name,
            file_size=self.file_size,
            row_count=self.row_count,
            min_key=self.min_key,
            max_key=self.max_key,
            key_stats=self.key_stats,
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=self.min_sequence_number,
            max_sequence_number=self.max_sequence_number,
            schema_id=self.schema_id,
            level=self.level,
            extra_files=self.extra_files,
            creation_time=self.creation_time,
            delete_row_count=self.delete_row_count,
            embedded_index=self.embedded_index,
            file_source=self.file_source,
            value_stats_cols=[],
            external_path=self.external_path,
            first_row_id=self.first_row_id,
            write_cols=self.write_cols,
            file_path=self.file_path
        )

    @staticmethod
    def is_blob_file(file_name: str) -> bool:
        return file_name.endswith(".blob")

    def assign_first_row_id(self, first_row_id: int) -> 'DataFileMeta':
        """Create a new DataFileMeta with the assigned first_row_id."""
        return DataFileMeta(
            file_name=self.file_name,
            file_size=self.file_size,
            row_count=self.row_count,
            min_key=self.min_key,
            max_key=self.max_key,
            key_stats=self.key_stats,
            value_stats=self.value_stats,
            min_sequence_number=self.min_sequence_number,
            max_sequence_number=self.max_sequence_number,
            schema_id=self.schema_id,
            level=self.level,
            extra_files=self.extra_files,
            creation_time=self.creation_time,
            delete_row_count=self.delete_row_count,
            embedded_index=self.embedded_index,
            file_source=self.file_source,
            value_stats_cols=self.value_stats_cols,
            external_path=self.external_path,
            first_row_id=first_row_id,
            write_cols=self.write_cols,
            file_path=self.file_path
        )

    def assign_sequence_number(self, min_sequence_number: int, max_sequence_number: int) -> 'DataFileMeta':
        """Create a new DataFileMeta with the assigned sequence numbers."""
        return DataFileMeta(
            file_name=self.file_name,
            file_size=self.file_size,
            row_count=self.row_count,
            min_key=self.min_key,
            max_key=self.max_key,
            key_stats=self.key_stats,
            value_stats=self.value_stats,
            min_sequence_number=min_sequence_number,
            max_sequence_number=max_sequence_number,
            schema_id=self.schema_id,
            level=self.level,
            extra_files=self.extra_files,
            creation_time=self.creation_time,
            delete_row_count=self.delete_row_count,
            embedded_index=self.embedded_index,
            file_source=self.file_source,
            value_stats_cols=self.value_stats_cols,
            external_path=self.external_path,
            first_row_id=self.first_row_id,
            write_cols=self.write_cols,
            file_path=self.file_path
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-friendly dict for cross-process transport (e.g. Ray task payloads).

        Field types preserved via tagged objects (see encode_value/decode_value).
        """
        return {
            "file_name": self.file_name,
            "file_size": self.file_size,
            "row_count": self.row_count,
            "min_key": _generic_row_to_dict(self.min_key),
            "max_key": _generic_row_to_dict(self.max_key),
            "key_stats": _simple_stats_to_dict(self.key_stats),
            "value_stats": _simple_stats_to_dict(self.value_stats),
            "min_sequence_number": self.min_sequence_number,
            "max_sequence_number": self.max_sequence_number,
            "schema_id": self.schema_id,
            "level": self.level,
            "extra_files": list(self.extra_files) if self.extra_files is not None else [],
            "creation_time": _timestamp_to_dict(self.creation_time),
            "delete_row_count": self.delete_row_count,
            "embedded_index": _bytes_to_str(self.embedded_index),
            "file_source": self.file_source,
            "value_stats_cols": list(self.value_stats_cols) if self.value_stats_cols is not None else None,
            "external_path": self.external_path,
            "first_row_id": self.first_row_id,
            "write_cols": list(self.write_cols) if self.write_cols is not None else None,
            "file_path": self.file_path,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataFileMeta":
        return cls(
            file_name=data["file_name"],
            file_size=data["file_size"],
            row_count=data["row_count"],
            min_key=_generic_row_from_dict(data.get("min_key")),
            max_key=_generic_row_from_dict(data.get("max_key")),
            key_stats=_simple_stats_from_dict(data.get("key_stats")),
            value_stats=_simple_stats_from_dict(data.get("value_stats")),
            min_sequence_number=data["min_sequence_number"],
            max_sequence_number=data["max_sequence_number"],
            schema_id=data["schema_id"],
            level=data["level"],
            extra_files=list(data.get("extra_files") or []),
            creation_time=_timestamp_from_dict(data.get("creation_time")),
            delete_row_count=data.get("delete_row_count"),
            embedded_index=_bytes_from_str(data.get("embedded_index")),
            file_source=data.get("file_source"),
            value_stats_cols=list(data["value_stats_cols"]) if data.get("value_stats_cols") is not None else None,
            external_path=data.get("external_path"),
            first_row_id=data.get("first_row_id"),
            write_cols=list(data["write_cols"]) if data.get("write_cols") is not None else None,
            file_path=data.get("file_path"),
        )


def _bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return b64encode(value).decode("ascii")


def _bytes_from_str(value: Optional[str]) -> Optional[bytes]:
    if value is None:
        return None
    return b64decode(value.encode("ascii"))


def _timestamp_to_dict(ts: Optional[Timestamp]) -> Optional[Dict[str, int]]:
    if ts is None:
        return None
    return {"ms": ts.get_millisecond(), "ns": ts.get_nano_of_millisecond()}


def _timestamp_from_dict(data: Optional[Dict[str, int]]) -> Optional[Timestamp]:
    if data is None:
        return None
    return Timestamp(data["ms"], data.get("ns", 0))


def encode_value(value: Any) -> Any:
    """Encode a GenericRow / SimpleStats / partition field value into a JSON-friendly form.

    Tagged dicts mark non-JSON-native types so decode_value can round-trip them.
    Public so that callers serializing other field-bearing structures (e.g. partitions
    in CommitMessage) can reuse the same tagged encoding.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"__t__": "bytes", "v": b64encode(value).decode("ascii")}
    if isinstance(value, Decimal):
        return {"__t__": "decimal", "v": str(value)}
    if isinstance(value, Timestamp):
        return {"__t__": "ts", "ms": value.get_millisecond(), "ns": value.get_nano_of_millisecond()}
    if isinstance(value, datetime):
        return {"__t__": "datetime", "v": value.isoformat()}
    if isinstance(value, date):
        return {"__t__": "date", "v": value.isoformat()}
    if isinstance(value, dt_time):
        return {"__t__": "time", "v": value.isoformat()}
    raise TypeError(
        f"Unsupported value type for DataFileMeta serialization: {type(value).__name__}"
    )


def decode_value(value: Any) -> Any:
    if not isinstance(value, dict) or "__t__" not in value:
        return value
    tag = value["__t__"]
    if tag == "bytes":
        return b64decode(value["v"].encode("ascii"))
    if tag == "decimal":
        return Decimal(value["v"])
    if tag == "ts":
        return Timestamp(value["ms"], value.get("ns", 0))
    if tag == "datetime":
        return datetime.fromisoformat(value["v"])
    if tag == "date":
        return date.fromisoformat(value["v"])
    if tag == "time":
        return dt_time.fromisoformat(value["v"])
    raise ValueError(f"Unknown tagged value type: {tag}")


def _generic_row_to_dict(row) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    # GenericRow exposes .values directly; BinaryRow lazily decodes per field
    # via get_field(i). Normalize both into a list of decoded Python values
    # so the dict format stays uniform.
    if hasattr(row, "values"):
        values = row.values
    else:
        values = [row.get_field(i) for i in range(len(row))]
    fields = getattr(row, "fields", None)
    return {
        "values": [encode_value(v) for v in values],
        "fields": [f.to_dict() for f in fields] if fields else [],
        "row_kind": row.get_row_kind().value if hasattr(row, "get_row_kind") else 0,
    }


def _generic_row_from_dict(data: Optional[Dict[str, Any]]) -> Optional[GenericRow]:
    if data is None:
        return None
    fields = [DataField.from_dict(f) for f in data.get("fields", [])]
    values = [decode_value(v) for v in data.get("values", [])]
    row_kind = RowKind(data.get("row_kind", RowKind.INSERT.value))
    return GenericRow(values, fields, row_kind)


def _simple_stats_to_dict(stats: Optional[SimpleStats]) -> Optional[Dict[str, Any]]:
    if stats is None:
        return None
    # null_counts may be a Python list (writer path) or a pyarrow Array-like
    # (manifest reader path). Normalize to a plain list of ints.
    nc = stats.null_counts
    if nc is None:
        null_counts = []
    elif hasattr(nc, "to_pylist"):
        null_counts = nc.to_pylist()
    else:
        null_counts = list(nc)
    return {
        "min_values": _generic_row_to_dict(stats.min_values),
        "max_values": _generic_row_to_dict(stats.max_values),
        "null_counts": null_counts,
    }


def _simple_stats_from_dict(data: Optional[Dict[str, Any]]) -> Optional[SimpleStats]:
    if data is None:
        return None
    return SimpleStats(
        min_values=_generic_row_from_dict(data.get("min_values")),
        max_values=_generic_row_from_dict(data.get("max_values")),
        null_counts=list(data.get("null_counts") or []),
    )


DATA_FILE_META_SCHEMA = {
    "type": "record",
    "name": "DataFileMeta",
    "fields": [
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_ROW_COUNT", "type": "long"},
        {"name": "_MIN_KEY", "type": "bytes"},
        {"name": "_MAX_KEY", "type": "bytes"},
        {"name": "_KEY_STATS", "type": KEY_STATS_SCHEMA},
        {"name": "_VALUE_STATS", "type": VALUE_STATS_SCHEMA},
        {"name": "_MIN_SEQUENCE_NUMBER", "type": "long"},
        {"name": "_MAX_SEQUENCE_NUMBER", "type": "long"},
        {"name": "_SCHEMA_ID", "type": "long"},
        {"name": "_LEVEL", "type": "int"},
        {"name": "_EXTRA_FILES", "type": {"type": "array", "items": "string"}},
        {"name": "_CREATION_TIME",
         "type": [
             "null",
             {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
        {"name": "_DELETE_ROW_COUNT", "type": ["null", "long"], "default": None},
        {"name": "_EMBEDDED_FILE_INDEX", "type": ["null", "bytes"], "default": None},
        {"name": "_FILE_SOURCE", "type": ["null", "int"], "default": None},
        {"name": "_VALUE_STATS_COLS",
         "type": ["null", {"type": "array", "items": "string"}],
         "default": None},
        {"name": "_EXTERNAL_PATH", "type": ["null", "string"], "default": None},
        {"name": "_FIRST_ROW_ID", "type": ["null", "long"], "default": None},
        {"name": "_WRITE_COLS",
         "type": ["null", {"type": "array", "items": "string"}],
         "default": None},
    ]
}
