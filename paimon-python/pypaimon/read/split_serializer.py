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

"""Deserialize the cross-language ``SplitSerializer`` v1 binary into a pypaimon
:class:`DataSplit`.

Mirror of the Java ``DataSplit#serialize`` (VERSION 8) frame wrapped in the
``SplitSerializer`` v1 header, as produced by ``pypaimon_rust``'s
``Split.serialize()``. Extracts the fields the reader needs, plus per-file
min/max keys for PK merge-on-read; key/value stats (planning-only) stay empty.
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
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer
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
_SPLIT_VERSION = 8

_DFM_ARITY = 20


def _f(idx, name, dtype):
    return DataField(idx, name, dtype)


# DataFileMeta 20-field layout (order/types mirror DataFileMetaSerializer#toRow).
# Fields 3/4 (min/max key) are decoded for PK tables; 5/6 (stats) stay unread.
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
]

# Arity is fixed by DataSplit VERSION 8; keep the field list and arity in lockstep.
assert len(_DFM_FIELDS) == _DFM_ARITY


def _decode_str_array(b: Optional[bytes]) -> Optional[List[str]]:
    """Decode a Paimon ``BinaryArray<string>`` (non-null elements)."""
    if not b:
        return [] if b == b'' else None
    n = struct.unpack_from('<i', b, 0)[0]
    header = 4 + ((n + 31) // 32) * 4  # count + null bitset
    out = []
    for k in range(n):
        eo = header + k * 8
        if b[eo + 7] & 0x80:  # inline: value in first 7 bytes, len in low 7 bits of byte 7
            length = b[eo + 7] & 0x7F
            out.append(b[eo:eo + length].decode('utf-8'))
        else:  # pointer: (var_off << 32) | len
            slot = struct.unpack_from('<q', b, eo)[0]
            var_off = (slot >> 32) & 0xFFFFFFFF
            length = slot & 0xFFFFFFFF
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
            units.append(((b & 0x1F) << 6) | (raw[i + 1] & 0x3F))
            i += 2
        else:  # 0xE0: 3-byte
            units.append(((b & 0x0F) << 12) | ((raw[i + 1] & 0x3F) << 6) | (raw[i + 2] & 0x3F))
            i += 3
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


class _Reader:
    def __init__(self, data: bytes):
        self.d = data
        self.p = 0

    def i32(self) -> int:
        v = struct.unpack_from('>i', self.d, self.p)[0]
        self.p += 4
        return v

    def i64(self) -> int:
        v = struct.unpack_from('>q', self.d, self.p)[0]
        self.p += 8
        return v

    def f32(self) -> float:
        v = struct.unpack_from('>f', self.d, self.p)[0]
        self.p += 4
        return v

    def u8(self) -> int:
        v = self.d[self.p]
        self.p += 1
        return v

    def take(self, n: int) -> bytes:
        v = self.d[self.p:self.p + n]
        self.p += n
        return v

    def java_utf(self) -> str:
        n = struct.unpack_from('>H', self.d, self.p)[0]
        self.p += 2
        return _decode_modified_utf8(self.take(n))


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
        return _read_datasplit_body(r, partition_fields, key_fields)
    if type_id == _TYPE_INDEXED_SPLIT:
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
        row_ranges = [Range(r.i64(), r.i64()) for _ in range(r.i32())]
        return IndexedSplit(data_split, row_ranges, _read_scores(r))
    raise ValueError("unsupported split type id %d" % type_id)


def _read_scores(r: '_Reader') -> Optional[List[float]]:
    if r.u8() == 0:
        return None
    return [r.f32() for _ in range(r.i32())]


def _read_datasplit_body(r: _Reader, partition_fields: List[DataField],
                         key_fields: Optional[List[DataField]] = None) -> DataSplit:
    if r.i64() != _SPLIT_MAGIC:
        raise ValueError("bad DataSplit magic")
    version = r.i32()
    if version != _SPLIT_VERSION:
        raise ValueError(
            "unsupported DataSplit version %d (expected %d)" % (version, _SPLIT_VERSION))
    snapshot_id = r.i64()   # scanned snapshot; row-id conflict detection needs it
    partition = GenericRowDeserializer.from_bytes(r.take(r.i32()), partition_fields)
    bucket = r.i32()
    bucket_path = r.java_utf()
    if r.u8() == 1:
        r.i32()  # total_buckets
    if r.i32() != 0:                      # deprecated beforeFiles; Java rejects non-empty
        raise ValueError("cannot deserialize a split with before files")
    if r.u8() != 0:                       # beforeDeletionFiles must be null
        raise ValueError("cannot deserialize a split with before deletion files")
    file_count = r.i32()
    files = [_datafilemeta_from_row(r.take(r.i32()), bucket_path, key_fields)
             for _ in range(file_count)]
    data_deletion_files = _read_deletion_list(r)
    r.u8()    # isStreaming
    raw_convertible = r.u8() != 0
    return DataSplit(
        files=files,
        partition=partition,
        bucket=bucket,
        raw_convertible=raw_convertible,
        data_deletion_files=data_deletion_files,
        snapshot_id=snapshot_id,
    )


def _decode_key(b: Optional[bytes], key_fields: Optional[List[DataField]]) -> GenericRow:
    """Decode a min/max key (serialized BinaryRow); empty for append tables."""
    if not key_fields or not b:
        return GenericRow([], [])
    return GenericRowDeserializer.from_bytes(b, key_fields)


def _datafilemeta_from_row(row_bytes: bytes, bucket_path: str,
                           key_fields: Optional[List[DataField]] = None) -> DataFileMeta:
    row = BinaryRow(struct.pack('>i', _DFM_ARITY) + row_bytes, _DFM_FIELDS)
    g = row.get_field
    file_name = g(0)
    external_path = g(17)
    ct = g(12)
    ct = Timestamp(int(ct)) if ct is not None else None
    # min/max keys drive PK merge-on-read; stats unused, left empty.
    meta = DataFileMeta(
        file_name=file_name,
        file_size=g(1),
        row_count=g(2),
        min_key=_decode_key(g(3), key_fields),
        max_key=_decode_key(g(4), key_fields),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
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
    )
    meta.file_path = external_path if external_path else "%s/%s" % (bucket_path.rstrip('/'), file_name)
    return meta


def _read_deletion_list(r: _Reader) -> Optional[List[Optional[DeletionFile]]]:
    if r.u8() == 0:
        return None
    result: List[Optional[DeletionFile]] = []
    for _ in range(r.i32()):
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
