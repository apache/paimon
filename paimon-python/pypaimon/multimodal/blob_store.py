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

import io
import re
from dataclasses import dataclass
from typing import BinaryIO, Dict, Iterable, List, Mapping, Optional, Sequence

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor


_RANGE_PATTERN = re.compile(r"^bytes=(\d*)-(\d*)$")
_MAX_OVERWRITE_ATTEMPTS = 3


class NoSuchKey(KeyError):
    """Raised when a blob key does not exist in a multimodal blob store."""


@dataclass(frozen=True)
class PutObjectResult:
    key: object
    size: Optional[int]
    descriptor: Optional[BlobDescriptor]
    snapshot_id: Optional[int]


@dataclass(frozen=True)
class ObjectInfo:
    key: object
    size: Optional[int]
    descriptor: Optional[BlobDescriptor]
    columns: Dict[str, object]
    snapshot_id: Optional[int]


@dataclass(frozen=True)
class BlobObject:
    key: object
    descriptor: BlobDescriptor
    columns: Dict[str, object]
    file_io: object
    range_header: Optional[str] = None
    snapshot_id: Optional[int] = None

    @property
    def size(self) -> int:
        return self.descriptor.length

    @property
    def content_length(self) -> int:
        descriptor = _descriptor_for_range(self.descriptor, self.range_header)
        return descriptor.length

    def open(self) -> BinaryIO:
        descriptor = _descriptor_for_range(self.descriptor, self.range_header)
        uri_reader = self.file_io.uri_reader_factory.create(descriptor.uri)
        return Blob.from_descriptor(uri_reader, descriptor).new_input_stream()

    def read(self) -> bytes:
        with self.open() as stream:
            return stream.read()


class BlobStore:
    """S3-like object facade over a Paimon multimodal table BLOB column."""

    def __init__(
            self,
            table,
            column: Optional[str] = None,
            key_column: Optional[str] = None):
        self._table = table
        self._raw_table = table.raw_table
        self.column = column or self._infer_blob_column()
        self.key_column = key_column or self._infer_key_column()
        self._validate_columns()

    def put_object(
            self,
            key,
            body=None,
            *,
            uri: Optional[str] = None,
            offset: int = 0,
            length: int = -1,
            descriptor: Optional[BlobDescriptor] = None,
            columns: Optional[Mapping[str, object]] = None) -> PutObjectResult:
        return self.put_objects([
            {
                "key": key,
                "body": body,
                "uri": uri,
                "offset": offset,
                "length": length,
                "descriptor": descriptor,
                "columns": dict(columns or {}),
            }
        ])[0]

    def put_objects(self, objects: Iterable[Mapping[str, object]]) -> List[PutObjectResult]:
        rows = []
        for obj in objects:
            key = _require_key(obj)
            row = dict(obj.get("columns") or {})
            row[self.key_column] = key
            row[self.column] = _object_to_blob_value(obj)
            rows.append(row)
        if not rows:
            return []
        self._validate_unique_keys([row[self.key_column] for row in rows])
        snapshot_id = self._overwrite_rows(rows)
        return [
            self._put_result(row[self.key_column], snapshot_id)
            for row in rows
        ]

    def get_object(
            self,
            key,
            *,
            range: Optional[str] = None,
            columns: Optional[Sequence[str]] = None) -> BlobObject:
        info = self.head_object(key, columns=columns)
        if info.descriptor is None:
            raise NoSuchKey(key)
        _descriptor_for_range(info.descriptor, range)
        return BlobObject(
            key=info.key,
            descriptor=info.descriptor,
            columns=info.columns,
            file_io=self._raw_table.file_io,
            range_header=range,
            snapshot_id=info.snapshot_id,
        )

    def head_object(
            self,
            key,
            *,
            columns: Optional[Sequence[str]] = None) -> ObjectInfo:
        rows = self._read_rows(
            keys=[key],
            include_blob=True,
            columns=columns,
        )
        if not rows:
            raise NoSuchKey(key)
        if len(rows) > 1:
            raise ValueError("Multiple rows found for blob key %r." % key)
        return self._row_to_info(rows[0])

    def list_objects(
            self,
            *,
            prefix: Optional[str] = None,
            limit: Optional[int] = None,
            columns: Optional[Sequence[str]] = None) -> List[ObjectInfo]:
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be greater than or equal to 0.")
            if limit == 0:
                return []
        rows = self._read_rows(
            include_blob=True,
            columns=columns,
        )
        objects = []
        for row in rows:
            key = row[self.key_column]
            if prefix is not None and not str(key).startswith(prefix):
                continue
            objects.append(self._row_to_info(row))
            if limit is not None and len(objects) >= limit:
                break
        return objects

    def delete_object(self, key) -> None:
        self.delete_objects([key])

    def delete_objects(self, keys: Sequence[object]) -> None:
        if not keys:
            return
        self._validate_unique_keys(keys)
        write_builder = self._raw_table.new_batch_write_builder()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.delete_by_predicate(
                self._keys_predicate(keys))
            table_commit.commit(messages)
        finally:
            table_commit.close()

    def _overwrite_rows(self, rows: List[Mapping[str, object]]) -> Optional[int]:
        keys = [row[self.key_column] for row in rows]
        snapshot_id = self._commit_upsert_once(rows)
        if self._has_unique_rows(keys):
            return snapshot_id
        for _ in range(_MAX_OVERWRITE_ATTEMPTS - 1):
            snapshot_id = self._commit_replace_once(rows, keys)
            if self._has_unique_rows(keys):
                return snapshot_id
        raise ValueError(
            "Concurrent blob put conflict left duplicate rows for keys: %s"
            % list(keys)
        )

    def _commit_upsert_once(self, rows: List[Mapping[str, object]]) -> Optional[int]:
        arrow_table = self._rows_to_arrow(rows)
        write_builder = self._raw_table.new_batch_write_builder()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.upsert_by_arrow_with_key(
                arrow_table, [self.key_column])
            table_commit.commit(messages)
        finally:
            table_commit.close()
        latest_snapshot = self._raw_table.snapshot_manager().get_latest_snapshot()
        return latest_snapshot.id if latest_snapshot is not None else None

    def _commit_replace_once(
            self,
            rows: List[Mapping[str, object]],
            keys: Sequence[object]) -> Optional[int]:
        arrow_table = self._rows_to_arrow(rows)
        write_builder = self._raw_table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.delete_by_predicate(
                self._keys_predicate(keys))
            table_write.write_arrow(arrow_table)
            messages.extend(table_write.prepare_commit())
            table_commit.commit(messages)
        finally:
            table_write.close()
            table_commit.close()
        latest_snapshot = self._raw_table.snapshot_manager().get_latest_snapshot()
        return latest_snapshot.id if latest_snapshot is not None else None

    def _has_unique_rows(self, keys: Sequence[object]) -> bool:
        rows = self._read_rows(keys=keys, columns=[])
        counts = {key: 0 for key in keys}
        for row in rows:
            key = row[self.key_column]
            counts[key] = counts.get(key, 0) + 1
        return all(counts.get(key, 0) == 1 for key in keys)

    def _put_result(self, key, snapshot_id: Optional[int]) -> PutObjectResult:
        info = self.head_object(key)
        return PutObjectResult(
            key=key,
            size=info.size,
            descriptor=info.descriptor,
            snapshot_id=snapshot_id,
        )

    def _rows_to_arrow(self, rows: List[Mapping[str, object]]) -> pa.Table:
        schema = PyarrowFieldParser.from_paimon_schema(
            self._raw_table.table_schema.fields)
        names = []
        arrays = []
        for field in schema:
            values = [row.get(field.name) for row in rows]
            arrays.append(pa.array(values, type=field.type))
            names.append(field.name)
        return pa.Table.from_arrays(arrays, names=names)

    def _read_rows(
            self,
            keys: Optional[Sequence[object]] = None,
            include_blob: bool = False,
            columns: Optional[Sequence[str]] = None) -> List[dict]:
        read_table = self._raw_table.copy({
            CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"
        }) if include_blob else self._raw_table
        read_builder = read_table.new_read_builder()
        projection = self._projection(include_blob, columns)
        read_builder = read_builder.with_projection(projection)
        if keys is not None:
            read_builder = read_builder.with_filter(self._keys_predicate(keys))
        plan = read_builder.new_scan().plan()
        table = read_builder.new_read().to_arrow(plan.splits())
        return table.to_pylist()

    def _projection(
            self,
            include_blob: bool,
            selected_columns: Optional[Sequence[str]]) -> List[str]:
        projection = [self.key_column]
        if include_blob:
            projection.append(self.column)
        projection.extend(self._selected_columns(selected_columns))
        return projection

    def _row_to_info(self, row: Mapping[str, object]) -> ObjectInfo:
        descriptor_bytes = row.get(self.column)
        descriptor = None
        if descriptor_bytes is not None:
            descriptor = BlobDescriptor.deserialize(_bytes_value(descriptor_bytes))
        columns = {
            name: value
            for name, value in row.items()
            if name not in (self.key_column, self.column)
        }
        snapshot = self._raw_table.snapshot_manager().get_latest_snapshot()
        return ObjectInfo(
            key=row[self.key_column],
            size=descriptor.length if descriptor is not None else None,
            descriptor=descriptor,
            columns=columns,
            snapshot_id=snapshot.id if snapshot is not None else None,
        )

    def _keys_predicate(self, keys: Sequence[object]):
        builder = PredicateBuilder(self._raw_table.fields)
        values = list(keys)
        if len(values) == 1:
            return builder.equal(self.key_column, values[0])
        return builder.is_in(self.key_column, values)

    def _infer_blob_column(self) -> str:
        blob_columns = [
            field.name for field in self._raw_table.fields
            if _is_blob_field(field)
        ]
        if len(blob_columns) != 1:
            raise ValueError(
                "Blob column is required when table has %d BLOB columns: %s."
                % (len(blob_columns), blob_columns)
            )
        return blob_columns[0]

    def _infer_key_column(self) -> str:
        for candidate in ("key", "object_key", "path"):
            if candidate in self._raw_table.field_names:
                return candidate
        raise ValueError(
            "key_column is required; no default key/object_key/path column "
            "exists in table schema."
        )

    def _validate_columns(self):
        if self.key_column not in self._raw_table.field_names:
            raise ValueError("key_column %r is not in table schema." % self.key_column)
        if self.column not in self._raw_table.field_names:
            raise ValueError("blob column %r is not in table schema." % self.column)
        if not _is_blob_field(self._raw_table.field_dict[self.column]):
            raise ValueError("Column %r is not a BLOB column." % self.column)
        if self.key_column == self.column:
            raise ValueError("key_column and blob column must be different.")

    def _selected_columns(
            self,
            columns: Optional[Sequence[str]]) -> List[str]:
        if columns is None:
            return [
                name for name in self._raw_table.field_names
                if name not in (self.key_column, self.column)
            ]
        if isinstance(columns, str):
            names = [columns]
        else:
            names = list(columns)
        self._validate_unique_selected_columns(names)
        for name in names:
            if name in (self.key_column, self.column):
                raise ValueError(
                    "columns must not include key or blob column %r." % name
                )
            if name not in self._raw_table.field_names:
                raise ValueError("Column %r is not in table schema." % name)
        return names

    @staticmethod
    def _validate_unique_selected_columns(names: Sequence[str]):
        seen = set()
        duplicates = set()
        for name in names:
            if name in seen:
                duplicates.add(name)
            seen.add(name)
        if duplicates:
            raise ValueError("Duplicate columns: %s" % sorted(duplicates))

    @staticmethod
    def _validate_unique_keys(keys: Sequence[object]):
        seen = set()
        duplicates = set()
        for key in keys:
            if key in seen:
                duplicates.add(key)
            seen.add(key)
        if duplicates:
            raise ValueError("Duplicate blob keys: %s" % sorted(duplicates))


def _require_key(obj: Mapping[str, object]):
    if "key" not in obj:
        raise ValueError("Blob object spec requires 'key'.")
    key = obj["key"]
    if key is None:
        raise ValueError("Blob object key must not be None.")
    return key


def _object_to_blob_value(obj: Mapping[str, object]) -> bytes:
    has_body = "body" in obj and obj.get("body") is not None
    has_reference = obj.get("descriptor") is not None or obj.get("uri") is not None
    if has_body and has_reference:
        raise ValueError(
            "Blob object spec must use either 'body' or descriptor/uri, not both."
        )
    if has_reference:
        return _coerce_descriptor(obj).serialize()
    if not has_body:
        raise ValueError("Blob object spec requires 'body', 'descriptor', or 'uri'.")
    return _body_to_bytes(obj.get("body"))


def _body_to_bytes(body) -> bytes:
    if isinstance(body, Blob):
        try:
            return body.to_descriptor().serialize()
        except RuntimeError:
            return body.to_data()
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)
    if isinstance(body, memoryview):
        return body.tobytes()
    if isinstance(body, str):
        return body.encode("utf-8")
    if isinstance(body, io.IOBase) or hasattr(body, "read"):
        data = body.read()
        if isinstance(data, str):
            return data.encode("utf-8")
        return bytes(data)
    raise ValueError("Unsupported blob body type: %r." % type(body))


def _coerce_descriptor(obj: Mapping[str, object]) -> BlobDescriptor:
    descriptor = obj.get("descriptor")
    if descriptor is not None:
        if isinstance(descriptor, BlobDescriptor):
            return descriptor
        if isinstance(descriptor, (bytes, bytearray)):
            return BlobDescriptor.deserialize(bytes(descriptor))
        raise ValueError("descriptor must be BlobDescriptor or serialized bytes.")

    uri = obj.get("uri")
    if not uri:
        raise ValueError("Reference object spec requires 'uri' or 'descriptor'.")
    return BlobDescriptor(
        uri=str(uri),
        offset=int(obj.get("offset", 0)),
        length=int(obj.get("length", -1)),
    )


def _descriptor_for_range(
        descriptor: BlobDescriptor,
        range_header: Optional[str]) -> BlobDescriptor:
    if range_header is None:
        return descriptor
    start, length = _parse_range(range_header, descriptor.length)
    return BlobDescriptor(descriptor.uri, descriptor.offset + start, length)


def _parse_range(range_header: str, total_length: int):
    match = _RANGE_PATTERN.match(range_header)
    if match is None:
        raise ValueError("Range must use S3 syntax like 'bytes=0-1023'.")
    start_text, end_text = match.groups()
    if not start_text and not end_text:
        raise ValueError("Range must specify a start or end byte.")

    if start_text:
        start = int(start_text)
        if end_text:
            end = int(end_text)
            if end < start:
                raise ValueError("Range end must be greater than or equal to start.")
            length = end - start + 1
        else:
            if total_length < 0:
                length = -1
            else:
                length = max(total_length - start, 0)
    else:
        if total_length < 0:
            raise ValueError("Suffix ranges require a known object length.")
        suffix_length = int(end_text)
        if suffix_length < 0:
            raise ValueError("Suffix range length must be non-negative.")
        length = min(suffix_length, total_length)
        start = total_length - length

    if total_length >= 0 and start > total_length:
        raise ValueError("Range start is past the end of the object.")
    return start, length


def _bytes_value(value) -> bytes:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, bytearray):
        value = bytes(value)
    if not isinstance(value, bytes):
        raise ValueError("Expected serialized BlobDescriptor bytes.")
    return value


def _is_blob_field(field) -> bool:
    return getattr(field.type, "type", None) == "BLOB"
