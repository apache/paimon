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
import math
import struct
from abc import ABC, abstractmethod
from typing import Any, List, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode

_MURMUR_C1 = 0xCC9E2D51
_MURMUR_C2 = 0x1B873593
_DEFAULT_SEED = 42
_HEADER_SIZE_IN_BITS = 8


def _round_number_of_bytes_to_nearest_word(num_bytes: int) -> int:
    remainder = num_bytes & 0x07
    return num_bytes if remainder == 0 else num_bytes + (8 - remainder)


def _calculate_bit_set_width_in_bytes(arity: int) -> int:
    return ((arity + 63 + _HEADER_SIZE_IN_BITS) // 64) * 8


def _get_fixed_length_part_size(arity: int) -> int:
    return _calculate_bit_set_width_in_bytes(arity) + 8 * arity


def _mix_k1(k1: int) -> int:
    k1 = (k1 * _MURMUR_C1) & 0xFFFFFFFF
    k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
    k1 = (k1 * _MURMUR_C2) & 0xFFFFFFFF
    return k1


def _mix_h1(h1: int, k1: int) -> int:
    h1 = (h1 ^ k1) & 0xFFFFFFFF
    h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
    h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF
    return h1


def _fmix(h1: int, length: int) -> int:
    # Finalization mix - force all bits of a hash block to avalanche
    h1 = (h1 ^ length) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    return h1


def _hash_bytes_by_words(data: bytes, seed: int = _DEFAULT_SEED) -> int:
    n = len(data)
    length_aligned = n - (n % 4)
    h1 = seed
    for i in range(0, length_aligned, 4):
        k1 = struct.unpack_from("<I", data, i)[0]
        k1 = _mix_k1(k1)
        h1 = _mix_h1(h1, k1)
    return _fmix(h1, n)


def _bucket_from_hash(hash_unsigned: int, num_buckets: int) -> int:
    if hash_unsigned >= 0x80000000:
        hash_signed = hash_unsigned - 0x100000000
    else:
        hash_signed = hash_unsigned
    rem = hash_signed - math.trunc(hash_signed / num_buckets) * num_buckets
    return abs(rem)


def _type_name_for_bucket(field_type: Any) -> str:
    if hasattr(field_type, "type") and isinstance(getattr(field_type, "type"), str):
        return getattr(field_type, "type").upper()
    s = str(field_type).upper()
    if "BIGINT" in s or "LONG" in s:
        return "BIGINT"
    if "TINYINT" in s:
        return "TINYINT"
    if "SMALLINT" in s:
        return "SMALLINT"
    if "INT" in s or "INTEGER" in s:
        return "INT"
    if "STRING" in s or "VARCHAR" in s or "CHAR" in s:
        return "STRING"
    if "FLOAT" in s:
        return "FLOAT"
    if "DOUBLE" in s:
        return "DOUBLE"
    if "BOOLEAN" in s or "BOOL" in s:
        return "BOOLEAN"
    return s


def _to_binary_row_bytes(type_names: List[str], values: Tuple[Any, ...]) -> bytes:
    arity = len(type_names)
    assert arity == len(values)
    null_size = _calculate_bit_set_width_in_bytes(arity)
    fixed_size = _get_fixed_length_part_size(arity)
    buf = bytearray(fixed_size)
    buf[0] = 0
    var_cursor = fixed_size
    var_parts = []
    for pos in range(arity):
        type_name = type_names[pos]
        value = values[pos]
        field_offset = null_size + 8 * pos
        if value is None:
            byte_idx = (pos + _HEADER_SIZE_IN_BITS) // 8
            bit_idx = (pos + _HEADER_SIZE_IN_BITS) % 8
            buf[byte_idx] |= 1 << bit_idx
            continue
        if type_name in ("STRING", "VARCHAR", "CHAR"):
            b = value.encode("utf-8") if isinstance(value, str) else value
            length = len(b)
            if length <= 7:
                buf[field_offset:field_offset + length] = b
                buf[field_offset + 7] = 0x80 | length
            else:
                rounded = _round_number_of_bytes_to_nearest_word(length)
                var_parts.append((b, length, rounded))
                struct.pack_into("<II", buf, field_offset, length, var_cursor)
                var_cursor += rounded
        elif type_name in ("INT", "INTEGER", "TINYINT", "SMALLINT"):
            if type_name == "TINYINT":
                struct.pack_into("<b", buf, field_offset, int(value))
            elif type_name == "SMALLINT":
                struct.pack_into("<h", buf, field_offset, int(value))
            else:
                struct.pack_into("<i", buf, field_offset, int(value))
        elif type_name == "BIGINT":
            struct.pack_into("<q", buf, field_offset, int(value))
        elif type_name == "FLOAT":
            struct.pack_into("<f", buf, field_offset, float(value))
        elif type_name == "DOUBLE":
            struct.pack_into("<d", buf, field_offset, float(value))
        elif type_name == "BOOLEAN":
            buf[field_offset] = 1 if value else 0
        else:
            b = str(value).encode("utf-8")
            length = len(b)
            if length <= 7:
                buf[field_offset:field_offset + length] = b
                buf[field_offset + 7] = 0x80 | length
            else:
                rounded = _round_number_of_bytes_to_nearest_word(length)
                var_parts.append((b, length, rounded))
                struct.pack_into("<II", buf, field_offset, length, var_cursor)
                var_cursor += rounded
    for b, length, rounded in var_parts:
        buf.extend(b)
        buf.extend(b"\x00" * (rounded - length))
    return bytes(buf)


class RowKeyExtractor(ABC):
    """Base class for extracting partition and bucket information from PyArrow data."""

    def __init__(self, table_schema: TableSchema):
        self.table_schema = table_schema
        self.partition_indices = self._get_field_indices(table_schema.partition_keys)

    def extract_partition_bucket_batch(self, data: pa.RecordBatch) -> Tuple[List[Tuple], List[int]]:
        partitions = self._extract_partitions_batch(data)
        buckets = self._extract_buckets_batch(data)
        return partitions, buckets

    def _get_field_indices(self, field_names: List[str]) -> List[int]:
        if not field_names:
            return []
        field_map = {field.name: i for i, field in enumerate(self.table_schema.fields)}
        return [field_map[name] for name in field_names if name in field_map]

    def _extract_partitions_batch(self, data: pa.RecordBatch) -> List[Tuple]:
        if not self.partition_indices:
            return [() for _ in range(data.num_rows)]

        partition_columns = [data.column(i) for i in self.partition_indices]

        partitions = []
        for row_idx in range(data.num_rows):
            partition_values = tuple(col[row_idx].as_py() for col in partition_columns)
            partitions.append(partition_values)

        return partitions

    @abstractmethod
    def _extract_buckets_batch(self, table: pa.RecordBatch) -> List[int]:
        """Extract bucket numbers for all rows. Must be implemented by subclasses."""


class FixedBucketRowKeyExtractor(RowKeyExtractor):
    """Fixed bucket mode extractor with configurable number of buckets."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        options = CoreOptions.from_dict(table_schema.options)
        self.num_buckets = options.bucket()
        if self.num_buckets <= 0:
            raise ValueError(f"Fixed bucket mode requires bucket > 0, got {self.num_buckets}")

        bucket_key_option = options.bucket_key()
        if bucket_key_option and bucket_key_option.strip():
            self.bucket_keys = [k.strip() for k in bucket_key_option.split(',')]
        else:
            self.bucket_keys = [pk for pk in table_schema.primary_keys
                                if pk not in table_schema.partition_keys]

        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
        field_map = {f.name: f for f in table_schema.fields}
        self._bucket_key_type_names = [
            _type_name_for_bucket(field_map[name].type) for name in self.bucket_keys
            if name in field_map
        ]

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        columns = [data.column(i) for i in self.bucket_key_indices]
        hashes = []
        for row_idx in range(data.num_rows):
            row_values = tuple(col[row_idx].as_py() for col in columns)
            hashes.append(self._binary_row_hash_code(row_values))
        return [_bucket_from_hash(h, self.num_buckets) for h in hashes]

    def _binary_row_hash_code(self, row_values: Tuple[Any, ...]) -> int:
        row_bytes = _to_binary_row_bytes(self._bucket_key_type_names, row_values)
        return _hash_bytes_by_words(row_bytes)


class UnawareBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))

        if num_buckets != -1:
            raise ValueError(f"Unaware bucket mode requires bucket = -1, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [0] * data.num_rows


class DynamicBucketRowKeyExtractor(RowKeyExtractor):
    """
    Row key extractor for dynamic bucket mode
    Ensures bucket configuration is set to -1 and prevents bucket extraction
    """

    def __init__(self, table_schema: 'TableSchema'):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))

        if num_buckets != -1:
            raise ValueError(
                f"Only 'bucket' = '-1' is allowed for 'DynamicBucketRowKeyExtractor', but found: {num_buckets}"
            )

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        raise ValueError("Can't extract bucket from row in dynamic bucket mode")


class PostponeBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -2))
        if num_buckets != BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(f"Postpone bucket mode requires bucket = -2, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [BucketMode.POSTPONE_BUCKET.value] * data.num_rows
