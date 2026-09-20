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

"""Read and write the cross-language ``SplitSerializer`` v1 binary format.

This is the stable bridge between Java, PyPaimon and ``pypaimon_rust``. It
supports DataSplit and IndexedSplit (including scores on the Python side) and
the current DataSplit v9/DataFileMeta v21 layout. The reader remains compatible
with DataSplit v8.
"""

import struct
from typing import List, Optional

from pypaimon.data.timestamp import Timestamp
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.split import DataSplit, Split
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.binary_row import BinaryRow
from pypaimon.table.row.generic_row import (
    GenericRow, GenericRowDeserializer, GenericRowSerializer)
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.range import Range

# Frame magics/versions, mirroring the Rust/Java constants.
_SPLIT_SER_MAGIC = 0x53504C49545F5631  # "SPLIT_V1"
_SPLIT_SER_VERSION = 1
_TYPE_DATA_SPLIT = 1
_TYPE_INDEXED_SPLIT = 3
_INDEXED_SPLIT_MAGIC = -938472394838495695
_INDEXED_SPLIT_VERSION = 1
_SPLIT_MAGIC = -2394839472490812314
_DFM_ARITY_BY_VERSION = {8: 20, 9: 21}
_SPLIT_VERSION = 9


def _f(idx, name, dtype):
    return DataField(idx, name, dtype)


# DataFileMeta layout (order/types mirror DataFileMetaSerializer#toRow).
_DFM_FIELDS: List[DataField] = [
    _f(0, '_FILE_NAME', AtomicType('STRING')),
    _f(1, '_FILE_SIZE', AtomicType('BIGINT')),
    _f(2, '_ROW_COUNT', AtomicType('BIGINT')),
    _f(3, '_MIN_KEY', AtomicType('BYTES')),
    _f(4, '_MAX_KEY', AtomicType('BYTES')),
    _f(5, '_KEY_STATS', AtomicType('BYTES')),
    _f(6, '_VALUE_STATS', AtomicType('BYTES')),
    _f(7, '_MIN_SEQUENCE_NUMBER', AtomicType('BIGINT')),
    _f(8, '_MAX_SEQUENCE_NUMBER', AtomicType('BIGINT')),
    _f(9, '_SCHEMA_ID', AtomicType('BIGINT')),
    _f(10, '_LEVEL', AtomicType('INT')),
    _f(11, '_EXTRA_FILES', AtomicType('BYTES')),      # BinaryArray<string>, decoded below
    _f(12, '_CREATION_TIME', AtomicType('BIGINT')),   # compact millis; read raw, wrap Timestamp
    _f(13, '_DELETE_ROW_COUNT', AtomicType('BIGINT')),
    _f(14, '_EMBEDDED_FILE_INDEX', AtomicType('BYTES')),
    _f(15, '_FILE_SOURCE', AtomicType('TINYINT')),
    _f(16, '_VALUE_STATS_COLS', AtomicType('BYTES')),
    _f(17, '_EXTERNAL_PATH', AtomicType('STRING')),
    _f(18, '_FIRST_ROW_ID', AtomicType('BIGINT')),
    _f(19, '_WRITE_COLS', AtomicType('BYTES')),
    # Added in v9 for compaction/index refresh; unused by the Python reader.
    _f(20, '_WRITE_COLS_SEQUENCES', AtomicType('BYTES')),
]

assert len(_DFM_FIELDS) == max(_DFM_ARITY_BY_VERSION.values())

_SIMPLE_STATS_FIELDS: List[DataField] = [
    _f(0, '_MIN_VALUES', AtomicType('BYTES')),
    _f(1, '_MAX_VALUES', AtomicType('BYTES')),
    _f(2, '_NULL_COUNTS', AtomicType('BYTES')),
]


def _round_to_word(size: int) -> int:
    return (size + 7) & ~7


def _binary_array_header(size: int) -> int:
    return 4 + ((size + 31) // 32) * 4


def _encode_str_array(values: List[str]) -> bytes:
    """Encode non-null strings using Paimon's BinaryArray layout."""
    header = _binary_array_header(len(values))
    fixed_size = _round_to_word(header + len(values) * 8)
    data = bytearray(fixed_size)
    struct.pack_into('<i', data, 0, len(values))
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise TypeError("BinaryArray string values must be strings")
        encoded = value.encode('utf-8')
        element_offset = header + index * 8
        if len(encoded) <= 7:
            data[element_offset:element_offset + len(encoded)] = encoded
            data[element_offset + 7] = 0x80 | len(encoded)
        else:
            variable_offset = len(data)
            data.extend(encoded)
            data.extend(b'\x00' * (_round_to_word(len(encoded)) - len(encoded)))
            struct.pack_into(
                '<Q', data, element_offset,
                (variable_offset << 32) | len(encoded))
    return bytes(data)


def _encode_long_array(values: List[Optional[int]]) -> bytes:
    """Encode nullable BIGINTs using Paimon's BinaryArray layout."""
    header = _binary_array_header(len(values))
    data = bytearray(_round_to_word(header + len(values) * 8))
    struct.pack_into('<i', data, 0, len(values))
    for index, value in enumerate(values):
        if value is None:
            data[4 + index // 8] |= 1 << (index % 8)
        else:
            struct.pack_into('<q', data, header + index * 8, value)
    return bytes(data)


def _decode_long_array(data: Optional[bytes]) -> Optional[List[Optional[int]]]:
    if data is None:
        return None
    if len(data) < 4:
        raise ValueError("BinaryArray<long> is shorter than its header")
    size = struct.unpack_from('<i', data, 0)[0]
    if size < 0:
        raise ValueError("negative BinaryArray<long> size")
    header = _binary_array_header(size)
    if header + size * 8 > len(data):
        raise ValueError("BinaryArray<long> elements exceed its buffer")
    result = []
    for index in range(size):
        is_null = data[4 + index // 8] & (1 << (index % 8))
        result.append(
            None if is_null else struct.unpack_from(
                '<q', data, header + index * 8)[0])
    return result


def _decode_str_array(b: Optional[bytes]) -> Optional[List[str]]:
    """Decode a Paimon ``BinaryArray<string>`` (non-null elements)."""
    if b is None:
        return None
    if len(b) < 4:
        raise ValueError("BinaryArray<string> is shorter than its header")
    n = struct.unpack_from('<i', b, 0)[0]
    if n < 0:
        raise ValueError("negative BinaryArray<string> size")
    if n == 0:
        return []
    header = _binary_array_header(n)
    fixed_size = _round_to_word(header + n * 8)
    if fixed_size > len(b):
        raise ValueError("BinaryArray<string> elements exceed its buffer")
    out = []
    for k in range(n):
        if b[4 + k // 8] & (1 << (k % 8)):
            raise ValueError("array<string not null> contains a null element")
        eo = header + k * 8
        if b[eo + 7] & 0x80:  # inline: value in first 7 bytes, len in low 7 bits of byte 7
            length = b[eo + 7] & 0x7F
            if length > 7:
                raise ValueError("invalid inline BinaryArray<string> length")
            out.append(b[eo:eo + length].decode('utf-8'))
        else:  # pointer: (var_off << 32) | len
            slot = struct.unpack_from('<Q', b, eo)[0]
            var_off = (slot >> 32) & 0xFFFFFFFF
            length = slot & 0xFFFFFFFF
            if var_off < fixed_size or var_off + length > len(b):
                raise ValueError("BinaryArray<string> value exceeds its buffer")
            out.append(b[var_off:var_off + length].decode('utf-8'))
    return out


def _decode_modified_utf8(raw: bytes) -> str:
    """Decode Java writeUTF modified UTF-8 (NUL as C0 80, 3-byte for >= U+0800;
    supplementary chars as a UTF-16 surrogate pair, recombined below)."""
    units = []
    i, n = 0, len(raw)
    while i < n:
        b = raw[i]
        if b < 0x80:
            units.append(b)
            i += 1
        elif b & 0xE0 == 0xC0:
            if i + 1 >= n or raw[i + 1] & 0xC0 != 0x80:
                raise ValueError("invalid modified UTF-8 two-byte sequence")
            units.append(((b & 0x1F) << 6) | (raw[i + 1] & 0x3F))
            i += 2
        elif b & 0xF0 == 0xE0:
            if (i + 2 >= n or raw[i + 1] & 0xC0 != 0x80
                    or raw[i + 2] & 0xC0 != 0x80):
                raise ValueError("invalid modified UTF-8 three-byte sequence")
            units.append(((b & 0x0F) << 12) | ((raw[i + 1] & 0x3F) << 6) | (raw[i + 2] & 0x3F))
            i += 3
        else:
            raise ValueError("invalid modified UTF-8 leading byte")
    out = []
    j, m = 0, len(units)
    while j < m:
        c = units[j]
        if 0xD800 <= c <= 0xDBFF and j + 1 < m and 0xDC00 <= units[j + 1] <= 0xDFFF:
            out.append(0x10000 + ((c - 0xD800) << 10) + (units[j + 1] - 0xDC00))
            j += 2
        else:
            out.append(c)
            j += 1
    return ''.join(chr(c) for c in out)


def _encode_modified_utf8(value: str) -> bytes:
    """Encode Java DataOutput.writeUTF's modified UTF-8 payload."""
    utf16 = value.encode('utf-16-be', errors='surrogatepass')
    result = bytearray()
    for offset in range(0, len(utf16), 2):
        unit = struct.unpack_from('>H', utf16, offset)[0]
        if 0x0001 <= unit <= 0x007F:
            result.append(unit)
        elif unit > 0x07FF:
            result.extend((
                0xE0 | (unit >> 12),
                0x80 | ((unit >> 6) & 0x3F),
                0x80 | (unit & 0x3F),
            ))
        else:
            result.extend((
                0xC0 | (unit >> 6),
                0x80 | (unit & 0x3F),
            ))
    if len(result) > 0xFFFF:
        raise ValueError(
            "string too long for Java writeUTF: %d bytes (max 65535)"
            % len(result))
    return bytes(result)


class _Writer:
    def __init__(self):
        self.parts = []

    def i32(self, value: int):
        self.parts.append(struct.pack('>i', value))

    def i64(self, value: int):
        self.parts.append(struct.pack('>q', value))

    def f32(self, value: float):
        self.parts.append(struct.pack('>f', value))

    def u8(self, value: int):
        self.parts.append(struct.pack('B', value))

    def take(self, value: bytes):
        self.parts.append(value)

    def java_utf(self, value: str):
        encoded = _encode_modified_utf8(value)
        self.parts.append(struct.pack('>H', len(encoded)))
        self.parts.append(encoded)

    def finish(self) -> bytes:
        return b''.join(self.parts)


def serialize_split_v1(split: Split, include_scores: bool = True) -> bytes:
    """Serialize a Python DataSplit/IndexedSplit as SplitSerializer v1.

    ``include_scores=False`` is used by the Rust read bridge because native
    DataSplit models row ranges but not vector scores. Scores stay attached to
    the original Python IndexedSplit and are never needed to select rows.
    """
    writer = _Writer()
    writer.i64(_SPLIT_SER_MAGIC)
    writer.i32(_SPLIT_SER_VERSION)
    if isinstance(split, IndexedSplit):
        data_split = split.data_split()
        if not isinstance(data_split, DataSplit):
            raise TypeError(
                "IndexedSplit must directly wrap a DataSplit for native serialization")
        writer.i32(_TYPE_INDEXED_SPLIT)
        writer.i64(_INDEXED_SPLIT_MAGIC)
        writer.i32(_INDEXED_SPLIT_VERSION)
        _write_datasplit_body(writer, data_split)
        ranges = split.row_ranges()
        writer.i32(len(ranges))
        for row_range in ranges:
            if row_range.from_ > row_range.to:
                raise ValueError(
                    "row range from %d exceeds to %d"
                    % (row_range.from_, row_range.to))
            writer.i64(row_range.from_)
            writer.i64(row_range.to)
        scores = split.scores()
        if include_scores and scores is not None:
            writer.u8(1)
            writer.i32(len(scores))
            for score in scores:
                writer.f32(score)
        else:
            writer.u8(0)
    elif isinstance(split, DataSplit):
        writer.i32(_TYPE_DATA_SPLIT)
        _write_datasplit_body(writer, split)
    else:
        raise TypeError(
            "SplitSerializer v1 supports DataSplit and IndexedSplit, got %s"
            % type(split).__name__)
    return writer.finish()


def _serialized_row(row) -> bytes:
    if row is None:
        row = GenericRow([], [])
    return GenericRowSerializer.to_bytes(row)


def _write_datasplit_body(writer: _Writer, split: DataSplit):
    files = list(split.files)
    bucket_path = (
        split.bucket_path
        if split.bucket_path is not None else _common_bucket_path(files))
    writer.i64(_SPLIT_MAGIC)
    writer.i32(_SPLIT_VERSION)
    writer.i64(-1 if split.snapshot_id is None else split.snapshot_id)
    partition = _serialized_row(split.partition)
    writer.i32(len(partition))
    writer.take(partition)
    writer.i32(split.bucket)
    writer.java_utf(bucket_path)
    if split.total_buckets is None:
        writer.u8(0)
    else:
        writer.u8(1)
        writer.i32(split.total_buckets)
    writer.i32(0)  # deprecated beforeFiles
    writer.u8(0)  # beforeDeletionFiles = null
    writer.i32(len(files))
    for data_file in files:
        row = _serialize_data_file_meta(data_file, bucket_path)
        writer.i32(len(row))
        writer.take(row)
    _write_deletion_list(writer, split.data_deletion_files, len(files))
    writer.u8(1 if split.is_streaming else 0)
    writer.u8(1 if split.raw_convertible else 0)


def _common_bucket_path(files: List[DataFileMeta]) -> str:
    """Find a shared path only when every file path is unambiguous.

    When Python planning has produced files from different directories, each
    exact path is written as DataFileMeta.external_path instead.
    """
    if not files:
        return ''
    parents = []
    for data_file in files:
        if data_file.external_path:
            return ''
        file_path = data_file.file_path
        suffix = '/' + data_file.file_name
        if not file_path or not file_path.endswith(suffix):
            return ''
        parents.append(file_path[:-len(suffix)])
    first = parents[0]
    return first if all(path == first for path in parents) else ''


def _effective_external_path(data_file: DataFileMeta, bucket_path: str):
    if data_file.external_path is not None:
        return data_file.external_path
    file_path = data_file.file_path
    if file_path is None:
        return None
    expected = '%s/%s' % (bucket_path.rstrip('/'), data_file.file_name)
    if bucket_path and file_path == expected:
        return None
    return file_path


def _serialize_data_file_meta(data_file: DataFileMeta, bucket_path: str) -> bytes:
    values = [
        data_file.file_name,
        data_file.file_size,
        data_file.row_count,
        _serialized_row(data_file.min_key),
        _serialized_row(data_file.max_key),
        _serialize_simple_stats(data_file.key_stats),
        _serialize_simple_stats(data_file.value_stats),
        data_file.min_sequence_number,
        data_file.max_sequence_number,
        data_file.schema_id,
        data_file.level,
        _encode_str_array(data_file.extra_files),
        (data_file.creation_time.get_millisecond()
         if data_file.creation_time is not None else None),
        data_file.delete_row_count,
        data_file.embedded_index,
        data_file.file_source,
        (_encode_str_array(data_file.value_stats_cols)
         if data_file.value_stats_cols is not None else None),
        _effective_external_path(data_file, bucket_path),
        data_file.first_row_id,
        (_encode_str_array(data_file.write_cols)
         if data_file.write_cols is not None else None),
        (_encode_long_array(data_file.column_max_sequence_numbers)
         if data_file.column_max_sequence_numbers is not None else None),
    ]
    serialized = GenericRowSerializer.to_bytes(GenericRow(values, _DFM_FIELDS))
    return serialized[4:]  # DataSplit carries a raw BinaryRow without arity.


def _serialize_simple_stats(stats: Optional[SimpleStats]) -> bytes:
    if stats is None:
        stats = SimpleStats.empty_stats()
    values = [
        _serialized_row(stats.min_values),
        _serialized_row(stats.max_values),
        _encode_long_array(stats.null_counts or []),
    ]
    return GenericRowSerializer.to_bytes(
        GenericRow(values, _SIMPLE_STATS_FIELDS))[4:]


def _write_deletion_list(writer: _Writer, deletion_files, file_count: int):
    if deletion_files is None:
        writer.u8(0)
        return
    if len(deletion_files) != file_count:
        raise ValueError(
            "deletion-file count %d does not match data-file count %d"
            % (len(deletion_files), file_count))
    writer.u8(1)
    writer.i32(len(deletion_files))
    for deletion_file in deletion_files:
        if deletion_file is None:
            writer.u8(0)
            continue
        writer.u8(1)
        writer.java_utf(deletion_file.dv_index_path)
        writer.i64(deletion_file.offset)
        writer.i64(deletion_file.length)
        writer.i64(
            -1 if deletion_file.cardinality is None
            else deletion_file.cardinality)


class _Reader:
    def __init__(self, data: bytes):
        self.d = data
        self.p = 0

    def i32(self) -> int:
        self._require(4)
        v = struct.unpack_from('>i', self.d, self.p)[0]
        self.p += 4
        return v

    def i64(self) -> int:
        self._require(8)
        v = struct.unpack_from('>q', self.d, self.p)[0]
        self.p += 8
        return v

    def f32(self) -> float:
        self._require(4)
        v = struct.unpack_from('>f', self.d, self.p)[0]
        self.p += 4
        return v

    def u8(self) -> int:
        self._require(1)
        v = self.d[self.p]
        self.p += 1
        return v

    def take(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("negative byte length %d" % n)
        self._require(n)
        v = self.d[self.p:self.p + n]
        self.p += n
        return v

    def java_utf(self) -> str:
        self._require(2)
        n = struct.unpack_from('>H', self.d, self.p)[0]
        self.p += 2
        return _decode_modified_utf8(self.take(n))

    def _require(self, size: int):
        available = len(self.d) - self.p
        if available < size:
            raise ValueError(
                "split buffer underrun: need %d bytes, have %d"
                % (size, available))

    def finish(self):
        remaining = len(self.d) - self.p
        if remaining:
            raise ValueError(
                "%d trailing bytes after SplitSerializer v1 frame" % remaining)


def deserialize_split_v1(data: bytes, partition_fields: List[DataField],
                         key_fields: Optional[List[DataField]] = None) -> Split:
    """Rebuild a pypaimon ``DataSplit`` (or ``IndexedSplit``) from ``Split.serialize()`` bytes.

    ``key_fields`` (trimmed primary keys) decode per-file min/max keys for PK
    merge-on-read; None for append tables.
    """
    r = _Reader(data)
    magic = r.i64()
    if magic != _SPLIT_SER_MAGIC:
        raise ValueError("bad SplitSerializer magic %d" % magic)
    version = r.i32()
    if version != _SPLIT_SER_VERSION:
        raise ValueError(
            "unsupported SplitSerializer version %d (expected %d)" % (version, _SPLIT_SER_VERSION))
    type_id = r.i32()
    if type_id == _TYPE_DATA_SPLIT:
        split = _read_datasplit_body(r, partition_fields, key_fields)
    elif type_id == _TYPE_INDEXED_SPLIT:
        imagic = r.i64()
        if imagic != _INDEXED_SPLIT_MAGIC:
            raise ValueError("bad IndexedSplit magic %d" % imagic)
        iversion = r.i32()
        if iversion != _INDEXED_SPLIT_VERSION:
            raise ValueError(
                "unsupported IndexedSplit version %d (expected %d)" % (iversion, _INDEXED_SPLIT_VERSION))
        data_split = _read_datasplit_body(r, partition_fields, key_fields)
        # row_ranges select which global row ids to read -- must be preserved,
        # else the reader scans the whole file instead of the ANN/row-id result.
        range_count = r.i32()
        if range_count < 0:
            raise ValueError("negative IndexedSplit row-range count")
        row_ranges = []
        for _ in range(range_count):
            from_, to = r.i64(), r.i64()
            if from_ > to:
                raise ValueError(
                    "row range from %d exceeds to %d" % (from_, to))
            row_ranges.append(Range(from_, to))
        scores = _read_scores(r)
        split = IndexedSplit(data_split, row_ranges, scores)
    else:
        raise ValueError("unsupported split type id %d" % type_id)
    r.finish()
    return split


def _read_scores(r: '_Reader') -> Optional[List[float]]:
    if r.u8() == 0:
        return None
    count = r.i32()
    if count < 0:
        raise ValueError("negative IndexedSplit score count")
    return [r.f32() for _ in range(count)]


def _read_datasplit_body(r: _Reader, partition_fields: List[DataField],
                         key_fields: Optional[List[DataField]] = None) -> DataSplit:
    if r.i64() != _SPLIT_MAGIC:
        raise ValueError("bad DataSplit magic")
    version = r.i32()
    if version not in _DFM_ARITY_BY_VERSION:
        raise ValueError(
            "unsupported DataSplit version %d (expected 8 or 9)" % version)
    snapshot_id = r.i64()   # scanned snapshot; row-id conflict detection needs it
    partition = GenericRowDeserializer.from_bytes(r.take(r.i32()), partition_fields)
    bucket = r.i32()
    bucket_path = r.java_utf()
    total_buckets = r.i32() if r.u8() != 0 else None
    if r.i32() != 0:                      # deprecated beforeFiles; Java rejects non-empty
        raise ValueError("cannot deserialize a split with before files")
    if r.u8() != 0:                       # beforeDeletionFiles must be null
        raise ValueError("cannot deserialize a split with before deletion files")
    file_count = r.i32()
    if file_count < 0:
        raise ValueError("negative DataSplit file count")
    arity = _DFM_ARITY_BY_VERSION[version]
    files = [_datafilemeta_from_row(r.take(r.i32()), bucket_path, arity, key_fields)
             for _ in range(file_count)]
    data_deletion_files = _read_deletion_list(r)
    is_streaming = r.u8() != 0
    raw_convertible = r.u8() != 0
    return DataSplit(
        files=files,
        partition=partition,
        bucket=bucket,
        raw_convertible=raw_convertible,
        is_streaming=is_streaming,
        data_deletion_files=data_deletion_files,
        snapshot_id=snapshot_id,
        bucket_path=bucket_path,
        total_buckets=total_buckets,
    )


def _decode_key(b: Optional[bytes], key_fields: Optional[List[DataField]]) -> GenericRow:
    """Decode a min/max key (serialized BinaryRow); empty for append tables."""
    if not key_fields or not b:
        return GenericRow([], [])
    return GenericRowDeserializer.from_bytes(b, key_fields)


def _datafilemeta_from_row(row_bytes: bytes, bucket_path: str, arity: int,
                           key_fields: Optional[List[DataField]] = None) -> DataFileMeta:
    row = BinaryRow(struct.pack('>i', arity) + row_bytes, _DFM_FIELDS[:arity])
    g = row.get_field
    file_name = g(0)
    external_path = g(17)
    ct = g(12)
    ct = Timestamp(int(ct)) if ct is not None else None
    key_stats = _decode_simple_stats(g(5))
    value_stats = _decode_simple_stats(g(6))
    meta = DataFileMeta(
        file_name=file_name,
        file_size=g(1),
        row_count=g(2),
        min_key=_decode_key(g(3), key_fields),
        max_key=_decode_key(g(4), key_fields),
        key_stats=key_stats,
        value_stats=value_stats,
        min_sequence_number=g(7),
        max_sequence_number=g(8),
        schema_id=g(9),
        level=g(10),
        extra_files=_decode_str_array(g(11)) or [],
        creation_time=ct,
        delete_row_count=g(13),
        embedded_index=g(14),
        file_source=g(15),
        value_stats_cols=_decode_str_array(g(16)),
        external_path=external_path,
        first_row_id=g(18),
        write_cols=_decode_str_array(g(19)),
        column_max_sequence_numbers=(
            _decode_non_null_long_array(g(20)) if arity >= 21 else None),
    )
    meta.file_path = external_path if external_path else "%s/%s" % (bucket_path.rstrip('/'), file_name)
    return meta


def _decode_simple_stats(data: bytes) -> SimpleStats:
    row = BinaryRow(struct.pack('>i', 3) + data, _SIMPLE_STATS_FIELDS)
    null_counts = _decode_long_array(row.get_field(2))
    return SimpleStats(
        BinaryRow(row.get_field(0), []),
        BinaryRow(row.get_field(1), []),
        null_counts,
    )


def _decode_non_null_long_array(data: Optional[bytes]) -> Optional[List[int]]:
    values = _decode_long_array(data)
    if values is None:
        return None
    if any(value is None for value in values):
        raise ValueError("array<bigint not null> contains a null element")
    return values


def _read_deletion_list(r: _Reader) -> Optional[List[Optional[DeletionFile]]]:
    if r.u8() == 0:
        return None
    result: List[Optional[DeletionFile]] = []
    count = r.i32()
    if count < 0:
        raise ValueError("negative deletion-file count")
    for _ in range(count):
        if r.u8() == 0:
            result.append(None)
            continue
        path = r.java_utf()
        offset, length, cardinality = r.i64(), r.i64(), r.i64()
        result.append(DeletionFile(
            dv_index_path=path,
            offset=offset,
            length=length,
            cardinality=None if cardinality == -1 else cardinality,
        ))
    return result
