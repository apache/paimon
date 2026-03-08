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
import random
import struct
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
from pypaimon.table.row.internal_row import RowKind

_MURMUR_C1 = 0xCC9E2D51
_MURMUR_C2 = 0x1B873593
_DEFAULT_SEED = 42


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
        self._bucket_key_fields = [
            field_map[name] for name in self.bucket_keys if name in field_map
        ]

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        columns = [data.column(i) for i in self.bucket_key_indices]
        return [
            _bucket_from_hash(
                self._binary_row_hash_code(tuple(col[row_idx].as_py() for col in columns)),
                self.num_buckets,
            )
            for row_idx in range(data.num_rows)
        ]

    def _binary_row_hash_code(self, row_values: Tuple) -> int:
        row = GenericRow(list(row_values), self._bucket_key_fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        return _hash_bytes_by_words(serialized[4:])


class UnawareBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))

        if num_buckets != -1:
            raise ValueError(f"Unaware bucket mode requires bucket = -1, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [0] * data.num_rows


_SHORT_MAX_VALUE = 32767


def _is_my_bucket(bucket: int, num_assigners: int, assign_id: int) -> bool:
    return bucket % num_assigners == assign_id % num_assigners


def _pick_randomly(bucket_list: List[int]) -> int:
    return random.choice(bucket_list)


class _SimplePartitionIndex:
    def __init__(self, num_assigners: int, assign_id: int, max_buckets_num: int) -> None:
        self.hash2bucket: Dict[int, int] = {}
        self.bucket_information: Dict[int, int] = {}
        self.bucket_list: List[int] = []
        self.current_bucket: int = 0
        self._load_new_bucket(max_buckets_num, num_assigners, assign_id)

    def assign(
        self,
        hash_value: int,
        max_bucket_id: int,
        target_bucket_row_number: int,
        max_buckets_num: int,
        num_assigners: int,
        assign_id: int,
    ) -> Tuple[int, int]:
        if hash_value in self.hash2bucket:
            assigned = self.hash2bucket[hash_value]
            return assigned, max(max_bucket_id, assigned)

        if self.current_bucket not in self.bucket_information:
            self.bucket_list.append(self.current_bucket)
            self.bucket_information[self.current_bucket] = 0
        num = self.bucket_information[self.current_bucket]

        if num >= target_bucket_row_number:
            if (
                max_buckets_num == -1
                or not self.bucket_information
                or max_bucket_id < max_buckets_num - 1
            ):
                self._load_new_bucket(max_buckets_num, num_assigners, assign_id)
            else:
                self.current_bucket = _pick_randomly(self.bucket_list)

        self.bucket_information[self.current_bucket] = (
            self.bucket_information.get(self.current_bucket, 0) + 1
        )
        self.hash2bucket[hash_value] = self.current_bucket
        new_max = max(max_bucket_id, self.current_bucket)
        return self.current_bucket, new_max

    def _load_new_bucket(
        self, max_buckets_num: int, num_assigners: int, assign_id: int
    ) -> None:
        for i in range(_SHORT_MAX_VALUE):
            if _is_my_bucket(i, num_assigners, assign_id) and (
                i not in self.bucket_information
            ):
                if max_buckets_num == -1 or i <= max_buckets_num - 1:
                    self.current_bucket = i
                    return
                return
        raise RuntimeError(
            "Can't find a suitable bucket to assign, all the bucket are assigned?"
        )


class SimpleHashBucketAssigner:
    def __init__(self, num_assigners, assign_id, target_bucket_row_number, max_buckets_num):
        self.num_assigners = num_assigners
        self.assign_id = assign_id
        self.target_bucket_row_number = target_bucket_row_number
        self.max_buckets_num = max_buckets_num
        self.max_bucket_id = 0
        self._partition_index: Dict[Tuple, _SimplePartitionIndex] = {}

    def assign(self, partition: Tuple, hash_value: int) -> int:
        if partition not in self._partition_index:
            self._partition_index[partition] = _SimplePartitionIndex(
                self.num_assigners, self.assign_id, self.max_buckets_num)
        index = self._partition_index[partition]

        assigned, self.max_bucket_id = index.assign(
            hash_value,
            self.max_bucket_id,
            self.target_bucket_row_number,
            self.max_buckets_num,
            self.num_assigners,
            self.assign_id,
        )
        return assigned


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
        opts = CoreOptions.from_dict(table_schema.options)
        self._assigner = SimpleHashBucketAssigner(
            num_assigners=1,
            assign_id=0,
            target_bucket_row_number=opts.dynamic_bucket_target_row_num(),
            max_buckets_num=opts.dynamic_bucket_max_buckets(),
        )
        # TODO: extract bucket key init logic to base class (shared with FixedBucketRowKeyExtractor)
        bucket_key_option = opts.bucket_key()
        if bucket_key_option and bucket_key_option.strip():
            self.bucket_keys = [k.strip() for k in bucket_key_option.split(',')]
        else:
            self.bucket_keys = [
                pk for pk in table_schema.primary_keys
                if pk not in table_schema.partition_keys
            ]
        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
        field_map = {f.name: f for f in table_schema.fields}
        self._bucket_key_fields = [
            field_map[name] for name in self.bucket_keys if name in field_map
        ]

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        partitions = self._extract_partitions_batch(data)
        columns = [data.column(i) for i in self.bucket_key_indices]
        buckets = []
        for row_idx in range(data.num_rows):
            key_hash = _hash_bytes_by_words(
                GenericRowSerializer.to_bytes(
                    GenericRow(
                        [columns[j][row_idx].as_py() for j in range(len(columns))],
                        self._bucket_key_fields,
                        RowKind.INSERT,
                    )
                )[4:]
            )
            buckets.append(
                self._assigner.assign(partitions[row_idx], key_hash))
        return buckets


class PostponeBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -2))
        if num_buckets != BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(f"Postpone bucket mode requires bucket = -2, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [BucketMode.POSTPONE_BUCKET.value] * data.num_rows
