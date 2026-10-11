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

"""Java ``CommitMessageSerializer`` v14 body, without an outer version header.

The caller must carry version 14 alongside these bytes, as Java's
``ManifestCommittableSerializer`` does. The data and compaction increments are
kept separate on the wire even when only the data increment is populated.
"""

from dataclasses import replace
import struct
from typing import List, Optional

from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.read.split_serializer import (
    _DFM_FIELDS, _Reader, _Writer, _binary_array_header,
    _datafilemeta_from_row, _round_to_word,
    _serialize_data_file_meta,
)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.binary_row import BinaryRow
from pypaimon.table.row.generic_row import (
    GenericRow, GenericRowDeserializer, GenericRowSerializer,
)
from pypaimon.write.commit_message import CommitMessage

VERSION = 14


def _fields(types):
    return [DataField(i, str(i), AtomicType(t)) for i, t in enumerate(types)]


_INDEX_FIELDS = _fields(['STRING', 'STRING', 'BIGINT', 'BIGINT', 'BYTES',
                         'STRING', 'BYTES'])
_DV_FIELDS = _fields(['STRING', 'INT', 'INT', 'BIGINT'])
_GLOBAL_FIELDS = _fields(['BIGINT', 'BIGINT', 'INT', 'BYTES', 'BYTES', 'BYTES'])


def _row(values, fields):
    return GenericRowSerializer.to_bytes(GenericRow(values, fields))[4:]


def _array_rows(rows: List[bytes]) -> bytes:
    count = len(rows)
    header = _binary_array_header(count)
    data = bytearray(_round_to_word(header + count * 8))
    struct.pack_into('<i', data, 0, count)
    for i, row in enumerate(rows):
        offset = len(data)
        data.extend(row)
        data.extend(b'\x00' * (_round_to_word(len(row)) - len(row)))
        struct.pack_into('<Q', data, header + i * 8,
                         (offset << 32) | len(row))
    return bytes(data)


def _read_array_rows(data: bytes) -> List[bytes]:
    if len(data) < 4:
        raise ValueError('truncated BinaryArray<row>')
    count = struct.unpack_from('<i', data)[0]
    if count < 0:
        raise ValueError('negative BinaryArray<row> count')
    header = _binary_array_header(count)
    fixed = _round_to_word(header + count * 8)
    if fixed > len(data):
        raise ValueError('truncated BinaryArray<row> fixed part')
    result = []
    for i in range(count):
        if data[4 + i // 8] & (1 << (i % 8)):
            raise ValueError('null BinaryArray<row> element')
        slot = struct.unpack_from('<Q', data, header + i * 8)[0]
        offset, length = slot >> 32, slot & 0xFFFFFFFF
        if offset < fixed or offset + length > len(data):
            raise ValueError('BinaryArray<row> element outside buffer')
        result.append(data[offset:offset + length])
    return result


def _int_array(values: List[int]) -> bytes:
    header = _binary_array_header(len(values))
    data = bytearray(_round_to_word(header + len(values) * 4))
    struct.pack_into('<i', data, 0, len(values))
    for i, value in enumerate(values):
        struct.pack_into('<i', data, header + i * 4, value)
    return bytes(data)


def _read_int_array(data: bytes) -> List[int]:
    if len(data) < 4:
        raise ValueError('truncated BinaryArray<int>')
    count = struct.unpack_from('<i', data)[0]
    header = _binary_array_header(count)
    if count < 0 or header + count * 4 > len(data):
        raise ValueError('invalid BinaryArray<int> size')
    return [struct.unpack_from('<i', data, header + i * 4)[0]
            for i in range(count)]


def _index_row(meta: IndexFileMeta) -> bytes:
    dv = None
    if meta.dv_ranges is not None:
        dv = _array_rows([
            _row([value.data_file_name, value.offset, value.length,
                  value.cardinality], _DV_FIELDS)
            for value in meta.dv_ranges.values()
        ])
    global_meta = None
    if meta.global_index_meta is not None:
        value = meta.global_index_meta
        global_meta = _row([
            value.row_range_start, value.row_range_end, value.index_field_id,
            _int_array(value.extra_field_ids)
            if value.extra_field_ids is not None else None,
            value.index_meta, value.source_meta,
        ], _GLOBAL_FIELDS)
    return _row([meta.index_type, meta.file_name, meta.file_size,
                 meta.row_count, dv, meta.external_path, global_meta],
                _INDEX_FIELDS)


def _index_from_row(data: bytes) -> IndexFileMeta:
    row = BinaryRow(struct.pack('>i', 7) + data, _INDEX_FIELDS)
    dv = None
    if row.get_field(4) is not None:
        dv = {}
        for raw in _read_array_rows(row.get_field(4)):
            entry = BinaryRow(struct.pack('>i', 4) + raw, _DV_FIELDS)
            value = DeletionVectorMeta(entry.get_field(0), entry.get_field(1),
                                       entry.get_field(2), entry.get_field(3))
            dv[value.data_file_name] = value
    global_meta = None
    if row.get_field(6) is not None:
        value = BinaryRow(struct.pack('>i', 6) + row.get_field(6), _GLOBAL_FIELDS)
        extra = value.get_field(3)
        global_meta = GlobalIndexMeta(
            value.get_field(0), value.get_field(1), value.get_field(2),
            _read_int_array(extra) if extra is not None else None,
            value.get_field(4), value.get_field(5))
    return IndexFileMeta(row.get_field(0), row.get_field(1), row.get_field(2),
                         row.get_field(3), dv, row.get_field(5), global_meta)


def _write_list(writer: _Writer, values, encode):
    writer.i32(len(values))
    for value in values:
        row = encode(value)
        writer.i32(len(row))
        writer.take(row)


def _read_list(reader: _Reader, decode):
    count = reader.i32()
    if count < 0:
        raise ValueError('negative CommitMessage list count')
    return [decode(reader.take(reader.i32())) for _ in range(count)]


def _file_row(meta):
    # A local file_path is not Java's externalPath field.
    return _serialize_data_file_meta(replace(meta, file_path=None), '')


def _file_from_row(raw, key_fields):
    if not key_fields:
        row = BinaryRow(struct.pack('>i', len(_DFM_FIELDS)) + raw, _DFM_FIELDS)
        for pos in (3, 4):
            key = row.get_field(pos)
            if key is not None and (len(key) < 4 or struct.unpack_from('>i', key)[0] != 0):
                raise ValueError('key_fields are required to decode primary-key files')
    meta = _datafilemeta_from_row(raw, '', len(_DFM_FIELDS), key_fields)
    meta.file_path = None
    return meta


def serialize_commit_message(message: CommitMessage,
                             partition_fields: List[DataField]) -> bytes:
    """Return the unframed Java v14 ``CommitMessageSerializer.serialize`` bytes."""
    if len(message.partition) != len(partition_fields):
        raise ValueError('partition arity does not match partition fields')
    writer = _Writer()
    partition = GenericRowSerializer.to_bytes(
        GenericRow(list(message.partition), partition_fields))
    writer.i32(len(partition))
    writer.take(partition)
    writer.i32(message.bucket)
    writer.u8(1 if message.total_buckets is not None else 0)
    if message.total_buckets is not None:
        writer.i32(message.total_buckets)
    for values in (message.new_files, message.deleted_files,
                   message.changelog_files):
        _write_list(writer, values, _file_row)
    for values in (message.index_adds, message.index_deletes):
        _write_list(writer, values, lambda entry: _index_row(entry.index_file))
    for values in (message.compact_before, message.compact_after,
                   message.compact_changelog_files):
        _write_list(writer, values, _file_row)
    for values in (message.compact_index_adds, message.compact_index_deletes):
        _write_list(writer, values, lambda entry: _index_row(entry.index_file))
    check = message.check_from_snapshot
    writer.u8(0 if check is None else 1)
    if check is not None:
        writer.i64(check)
    return writer.finish()


def deserialize_commit_message(data: bytes, partition_fields: List[DataField],
                               key_fields: Optional[List[DataField]] = None,
                               version: int = VERSION) -> CommitMessage:
    """Read a Java v14 body; ``version`` is supplied by the enclosing format."""
    if version != VERSION:
        raise ValueError('unsupported CommitMessage version %d' % version)
    reader = _Reader(data)
    partition_bytes = reader.take(reader.i32())
    if (len(partition_bytes) < 4 or
            struct.unpack_from('>i', partition_bytes)[0] != len(partition_fields)):
        raise ValueError('partition arity does not match partition fields')
    partition = tuple(GenericRowDeserializer.from_bytes(
        partition_bytes, partition_fields).values)
    bucket = reader.i32()
    total_buckets = reader.i32() if reader.u8() else None
    file_reader = lambda raw: _file_from_row(raw, key_fields)
    new_files = _read_list(reader, file_reader)
    deleted_files = _read_list(reader, file_reader)
    changelog_files = _read_list(reader, file_reader)

    def index_list(kind):
        return [IndexManifestEntry(kind, GenericRow(list(partition), partition_fields),
                                   bucket, meta)
                for meta in _read_list(reader, _index_from_row)]

    index_adds, index_deletes = index_list(0), index_list(1)
    compact_before = _read_list(reader, file_reader)
    compact_after = _read_list(reader, file_reader)
    compact_changelog_files = _read_list(reader, file_reader)
    compact_index_adds, compact_index_deletes = index_list(0), index_list(1)
    check = reader.i64() if reader.u8() else None
    reader.finish()
    return CommitMessage(
        partition=partition, bucket=bucket, new_files=new_files,
        check_from_snapshot=check, deleted_files=deleted_files,
        index_adds=index_adds, index_deletes=index_deletes,
        changelog_files=changelog_files, total_buckets=total_buckets,
        compact_before=compact_before, compact_after=compact_after,
        compact_changelog_files=compact_changelog_files,
        compact_index_adds=compact_index_adds,
        compact_index_deletes=compact_index_deletes)
