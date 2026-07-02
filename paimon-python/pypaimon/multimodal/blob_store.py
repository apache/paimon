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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.blob import Blob, BlobData, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


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
            row[self.column] = self._object_to_blob(obj)
            rows.append(row)
        if not rows:
            return []
        self._validate_unique_keys([row[self.key_column] for row in rows])
        return self._overwrite_rows(rows)

    def update_object_columns(
            self,
            key,
            columns: Mapping[str, object]) -> ObjectInfo:
        return self.update_objects_columns([
            {"key": key, "columns": columns}
        ])[0]

    def update_objects_columns(
            self,
            objects: Iterable[Mapping[str, object]]) -> List[ObjectInfo]:
        updates = []
        keys = []
        for obj in objects:
            key = _require_key(obj)
            columns = dict(obj.get("columns") or {})
            if not columns:
                raise ValueError("columns must not be empty.")
            self._selected_update_columns(columns.keys())
            updates.append((key, columns))
            keys.append(key)
        if not updates:
            return []
        self._validate_unique_keys(keys)
        self._commit_column_updates(updates)
        return [
            self.head_object(key)
            for key in keys
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

    def _overwrite_rows(self, rows: List[Mapping[str, object]]) -> List[PutObjectResult]:
        keys = [row[self.key_column] for row in rows]
        snapshot_id = self._commit_upsert_once(rows)
        results = self._read_put_results(keys, snapshot_id)
        if results is not None:
            return results
        for _ in range(_MAX_OVERWRITE_ATTEMPTS - 1):
            snapshot_id = self._commit_replace_once(rows, keys)
            results = self._read_put_results(keys, snapshot_id)
            if results is not None:
                return results
        raise ValueError(
            "Concurrent blob put conflict left non-unique rows for keys: %s"
            % list(keys)
        )

    def _commit_upsert_once(self, rows: List[Mapping[str, object]]) -> Optional[int]:
        generic_rows = self._rows_to_generic_rows(rows)
        write_builder = self._raw_table.new_batch_write_builder()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.upsert_by_key(
                generic_rows, [self.key_column])
            table_commit.commit(messages)
        finally:
            table_commit.close()
        latest_snapshot = self._raw_table.snapshot_manager().get_latest_snapshot()
        return latest_snapshot.id if latest_snapshot is not None else None

    def _commit_replace_once(
            self,
            rows: List[Mapping[str, object]],
            keys: Sequence[object]) -> Optional[int]:
        write_builder = self._raw_table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.delete_by_predicate(
                self._keys_predicate(keys))
            for row in self._rows_to_generic_rows(rows):
                table_write.write_row(row)
            messages.extend(table_write.prepare_commit())
            table_commit.commit(messages)
        finally:
            table_write.close()
            table_commit.close()
        latest_snapshot = self._raw_table.snapshot_manager().get_latest_snapshot()
        return latest_snapshot.id if latest_snapshot is not None else None

    def _commit_column_updates(self, updates: Sequence[tuple]) -> Optional[int]:
        update_columns = self._column_update_names(updates)
        targets = self._read_update_targets(
            [key for key, _ in updates],
            update_columns,
        )
        rows = []
        row_ids_by_row = []
        for key, columns in updates:
            target = targets[key]
            row = dict(target["columns"])
            row.update(columns)
            row[self.key_column] = key
            rows.append(self._row_to_generic_row(row))
            row_ids_by_row.append([target["row_id"]])

        write_builder = self._raw_table.new_batch_write_builder()
        table_commit = write_builder.new_commit()
        try:
            messages = TableUpdateByRowId(
                self._raw_table,
                write_builder.commit_user,
                BATCH_COMMIT_IDENTIFIER,
            ).update_rows_columns(rows, row_ids_by_row, update_columns)
            if messages:
                table_commit.commit(messages)
        finally:
            table_commit.close()
        latest_snapshot = self._raw_table.snapshot_manager().get_latest_snapshot()
        return latest_snapshot.id if latest_snapshot is not None else None

    def _column_update_names(self, updates: Sequence[tuple]) -> List[str]:
        requested = set()
        for _, columns in updates:
            requested.update(columns)
        return [
            name for name in self._raw_table.field_names
            if name in requested
        ]

    def _read_update_targets(
            self,
            keys: Sequence[object],
            columns: Sequence[str]) -> Dict[object, dict]:
        read_builder = self._raw_table.new_read_builder()
        projection = [self.key_column, SpecialFields.ROW_ID.name]
        projection.extend(columns)
        read_builder = read_builder.with_projection(projection)
        read_builder = read_builder.with_filter(self._keys_predicate(keys))
        plan = read_builder.new_scan().plan()
        table = read_builder.new_read().to_arrow(plan.splits())

        targets = {}
        duplicates = []
        for row in table.to_pylist():
            key = row[self.key_column]
            if key in targets:
                duplicates.append(key)
                continue
            targets[key] = {
                "row_id": row[SpecialFields.ROW_ID.name],
                "columns": {
                    name: row.get(name)
                    for name in columns
                },
            }

        missing = [key for key in keys if key not in targets]
        if missing:
            raise NoSuchKey(missing[0])
        if duplicates:
            raise ValueError("Multiple rows found for blob keys: %s" % duplicates)
        return targets

    def _read_put_results(
            self,
            keys: Sequence[object],
            snapshot_id: Optional[int]) -> Optional[List[PutObjectResult]]:
        rows = self._read_rows(keys=keys, include_blob=True)
        rows_by_key = {}
        for row in rows:
            key = row[self.key_column]
            if key in rows_by_key:
                return None
            rows_by_key[key] = row

        results = []
        for key in keys:
            row = rows_by_key.get(key)
            if row is None:
                return None
            info = self._row_to_info(row)
            if info.descriptor is None:
                return None
            results.append(PutObjectResult(
                key=key,
                size=info.size,
                descriptor=info.descriptor,
                snapshot_id=snapshot_id,
            ))
        return results

    def _rows_to_generic_rows(self, rows: List[Mapping[str, object]]) -> List[GenericRow]:
        return [
            self._row_to_generic_row(row)
            for row in rows
        ]

    def _row_to_generic_row(self, row: Mapping[str, object]) -> GenericRow:
        return GenericRow(
            [row.get(field.name) for field in self._raw_table.fields],
            self._raw_table.fields,
        )

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

    def _object_to_blob(self, obj: Mapping[str, object]) -> Blob:
        has_body = "body" in obj and obj.get("body") is not None
        has_reference = obj.get("descriptor") is not None or obj.get("uri") is not None
        if has_body and has_reference:
            raise ValueError(
                "Blob object spec must use either 'body' or descriptor/uri, not both."
            )
        if has_reference:
            descriptor = _coerce_descriptor(obj)
            uri_reader = self._raw_table.file_io.uri_reader_factory.create(descriptor.uri)
            return Blob.from_descriptor(uri_reader, descriptor)
        if not has_body:
            raise ValueError("Blob object spec requires 'body', 'descriptor', or 'uri'.")
        body = obj.get("body")
        if isinstance(body, Blob):
            try:
                descriptor = body.to_descriptor()
            except RuntimeError:
                return body
            uri_reader = self._raw_table.file_io.uri_reader_factory.create(descriptor.uri)
            return Blob.from_descriptor(uri_reader, descriptor)
        return BlobData(_body_to_bytes(body))

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

    def _selected_update_columns(self, columns: Sequence[str]) -> List[str]:
        names = self._selected_columns(columns)
        partition_keys = set(self._raw_table.partition_keys)
        for name in names:
            if name in partition_keys:
                raise ValueError(
                    "columns must not include partition column %r." % name
                )
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


def _body_to_bytes(body) -> bytes:
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
        if total_length >= 0 and start >= total_length:
            raise ValueError("Range start is past the end of the object.")
        if end_text:
            end = int(end_text)
            if end < start:
                raise ValueError("Range end must be greater than or equal to start.")
            if total_length >= 0:
                end = min(end, total_length - 1)
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
