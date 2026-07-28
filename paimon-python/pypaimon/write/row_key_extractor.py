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

import math
import random
import struct
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.index.dynamic_bucket import SHORT_MAX_VALUE, is_my_bucket
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

    def extract_partitions_batch(self, data: pa.RecordBatch) -> List[Tuple]:
        """Return partition tuples without calculating bucket hashes."""
        return self._extract_partitions_batch(data)

    def extract_partition_bucket_row(
            self, values_by_name: Dict[str, Any]) -> Tuple[Tuple, int]:
        partition = tuple(
            values_by_name[self.table_schema.fields[i].name]
            for i in self.partition_indices
        )
        bucket = self._extract_bucket_row(values_by_name)
        return partition, bucket

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

    @staticmethod
    def _binary_row_hash_code(values: Tuple, fields: List) -> int:
        return _hash_bytes_by_words(
            GenericRowSerializer.to_bytes(
                GenericRow(list(values), fields, RowKind.INSERT)
            )[4:]
        )

    @abstractmethod
    def _extract_buckets_batch(self, table: pa.RecordBatch) -> List[int]:
        """Extract bucket numbers for all rows. Must be implemented by subclasses."""

    @abstractmethod
    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        """Extract bucket number for a single row."""


class FixedBucketRowKeyExtractor(RowKeyExtractor):
    """Fixed bucket mode extractor with configurable number of buckets."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        options = CoreOptions.from_dict(table_schema.options)
        self.num_buckets = options.bucket()
        if self.num_buckets <= 0:
            raise ValueError(f"Fixed bucket mode requires bucket > 0, got {self.num_buckets}")

        # Bucket-key resolution lives on TableSchema (mirrors Java
        # ``TableSchema.bucketKeys()`` / ``logicalBucketKeyType()``); reuse
        # it so any reader path that walks the same logic stays in sync.
        self.bucket_keys = table_schema.bucket_keys
        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
        self._bucket_key_fields = table_schema.logical_bucket_key_fields

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        columns = [data.column(i) for i in self.bucket_key_indices]
        return [
            _bucket_from_hash(
                self._binary_row_hash_code(
                    tuple(col[row_idx].as_py() for col in columns),
                    self._bucket_key_fields,
                ),
                self.num_buckets,
            )
            for row_idx in range(data.num_rows)
        ]

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        return _bucket_from_hash(
            self._binary_row_hash_code(
                tuple(values_by_name[name] for name in self.bucket_keys),
                self._bucket_key_fields,
            ),
            self.num_buckets,
        )


class UnawareBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))

        if num_buckets != -1:
            raise ValueError(f"Unaware bucket mode requires bucket = -1, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [0] * data.num_rows

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        return 0


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
        for i in range(SHORT_MAX_VALUE):
            if is_my_bucket(i, num_assigners, assign_id) and (
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
    """Extract dynamic buckets and maintain their persistent hash mapping."""

    def __init__(
        self,
        table_schema: 'TableSchema',
        table=None,
        num_channels: int = 1,
        num_assigners: int = 1,
        assign_id: int = 0,
        ignore_existing: bool = False,
        maintain_index: bool = True,
        base_snapshot_id: Optional[int] = None,
    ):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))
        if num_buckets != -1:
            raise ValueError(
                "Only 'bucket' = '-1' is allowed for "
                f"'DynamicBucketRowKeyExtractor', but found: {num_buckets}"
            )

        opts = CoreOptions.from_dict(table_schema.options)
        self._table = table
        self.base_snapshot_id = 0
        target_bucket_row_number = opts.dynamic_bucket_target_row_num()
        max_buckets_num = opts.dynamic_bucket_max_buckets()

        self.bucket_keys = table_schema.bucket_keys
        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
        self._bucket_key_fields = table_schema.logical_bucket_key_fields
        self._partition_fields = [
            table_schema.fields[index] for index in self.partition_indices
        ]

        if table is None:
            self._assigner = SimpleHashBucketAssigner(
                num_assigners=num_assigners,
                assign_id=assign_id,
                target_bucket_row_number=target_bucket_row_number,
                max_buckets_num=max_buckets_num,
            )
            self._index_maintainer = None
        else:
            from pypaimon.index.dynamic_bucket import (
                DynamicBucketIndexMaintainer,
                HashBucketAssigner,
            )

            if base_snapshot_id is None:
                snapshot = table.snapshot_manager().get_latest_snapshot()
            elif base_snapshot_id == 0:
                snapshot = None
            else:
                snapshot = table.snapshot_manager().get_snapshot_by_id(
                    base_snapshot_id
                )
            self.base_snapshot_id = snapshot.id if snapshot is not None else 0
            self._assigner = HashBucketAssigner(
                table=table,
                num_channels=num_channels,
                num_assigners=num_assigners,
                assign_id=assign_id,
                target_bucket_row_number=target_bucket_row_number,
                max_buckets_num=max_buckets_num,
                ignore_existing=ignore_existing,
                snapshot=snapshot,
            )
            self._index_maintainer = (
                DynamicBucketIndexMaintainer(
                    table,
                    ignore_existing=ignore_existing,
                    snapshot=snapshot,
                )
                if maintain_index
                else None
            )

    def extract_hashes_batch(
        self, data: pa.RecordBatch
    ) -> Tuple[List[Tuple], List[int], List[int]]:
        """Return partitions, BinaryRow partition hashes, and key hashes."""
        partitions = self._extract_partitions_batch(data)
        key_hashes = self._extract_key_hashes_batch(data)
        partition_hash_cache = {}
        partition_hashes = []
        for partition in partitions:
            if partition not in partition_hash_cache:
                partition_hash_cache[partition] = self._binary_row_hash_code(
                    partition, self._partition_fields
                )
            partition_hashes.append(partition_hash_cache[partition])
        return partitions, partition_hashes, key_hashes

    def _extract_key_hashes_batch(self, data: pa.RecordBatch) -> List[int]:
        key_columns = [data.column(i) for i in self.bucket_key_indices]
        return [
            self._binary_row_hash_code(
                tuple(column[row_idx].as_py() for column in key_columns),
                self._bucket_key_fields,
            )
            for row_idx in range(data.num_rows)
        ]

    def extract_assigners_batch(
        self, data: pa.RecordBatch, num_channels: int, num_assigners: int
    ) -> List[int]:
        from pypaimon.index.dynamic_bucket import compute_assigner

        _, partition_hashes, key_hashes = self.extract_hashes_batch(data)
        return [
            compute_assigner(
                partition_hash, key_hash, num_channels, num_assigners
            )
            for partition_hash, key_hash in zip(partition_hashes, key_hashes)
        ]

    def extract_partition_bucket_from_hashes_batch(
        self, data: pa.RecordBatch, key_hashes: List[int]
    ) -> Tuple[List[Tuple], List[int]]:
        """Assign buckets using key hashes calculated by an upstream stage."""
        partitions, buckets, _ = (
            self.extract_partition_bucket_status_from_hashes_batch(
                data, key_hashes
            )
        )
        return partitions, buckets

    def extract_partition_bucket_status_from_hashes_batch(
        self, data: pa.RecordBatch, key_hashes: List[int]
    ) -> Tuple[List[Tuple], List[int], List[bool]]:
        """Assign carried hashes and report whether each mapping is new."""
        from pypaimon.index.dynamic_bucket import to_signed_int32

        if len(key_hashes) != data.num_rows:
            raise ValueError(
                "Precomputed key hash count {} does not match row count {}".format(
                    len(key_hashes), data.num_rows
                )
            )
        partitions = self._extract_partitions_batch(data)
        normalized_hashes = [to_signed_int32(value) for value in key_hashes]
        if self._table is None:
            buckets = [
                self._assigner.assign(partition, key_hash)
                for partition, key_hash in zip(partitions, normalized_hashes)
            ]
            return partitions, buckets, [True] * len(buckets)

        partition_hash_cache = {}
        partition_hashes = []
        for partition in partitions:
            partition_hash = partition_hash_cache.get(partition)
            if partition_hash is None:
                partition_hash = self._binary_row_hash_code(
                    partition, self._partition_fields
                )
                partition_hash_cache[partition] = partition_hash
            partition_hashes.append(partition_hash)
        assignments = self._assigner.assign_batch(
            partitions, partition_hashes, normalized_hashes
        )
        buckets = [assignment[0] for assignment in assignments]
        new_mappings = [assignment[1] for assignment in assignments]
        if self._index_maintainer is not None:
            for partition, bucket, key_hash, is_new in zip(
                partitions, buckets, normalized_hashes, new_mappings
            ):
                if is_new:
                    self._index_maintainer.notify_new_record(
                        partition, bucket, key_hash
                    )
        return partitions, buckets, new_mappings

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        if self._table is None:
            partitions = self._extract_partitions_batch(data)
            key_hashes = self._extract_key_hashes_batch(data)
            return [
                self._assigner.assign(partition, key_hash)
                for partition, key_hash in zip(partitions, key_hashes)
            ]

        partitions, partition_hashes, key_hashes = self.extract_hashes_batch(data)
        assignments = self._assigner.assign_batch(
            partitions, partition_hashes, key_hashes
        )
        buckets = [assignment[0] for assignment in assignments]
        if self._index_maintainer is not None:
            for partition, key_hash, (bucket, is_new) in zip(
                partitions, key_hashes, assignments
            ):
                if is_new:
                    self._index_maintainer.notify_new_record(
                        partition, bucket, key_hash
                    )
        return buckets

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        key_hash = self._binary_row_hash_code(
            tuple(values_by_name[name] for name in self.bucket_keys),
            self._bucket_key_fields,
        )
        partition = tuple(
            values_by_name[self.table_schema.fields[i].name]
            for i in self.partition_indices
        )
        if self._table is None:
            return self._assigner.assign(partition, key_hash)
        partition_hash = self._binary_row_hash_code(
            partition, self._partition_fields
        )
        bucket, is_new = self._assigner.assign_with_status(
            partition, partition_hash, key_hash
        )
        if self._index_maintainer is not None and is_new:
            self._index_maintainer.notify_new_record(partition, bucket, key_hash)
        return bucket

    def prepare_commit(self):
        if self._index_maintainer is None:
            return {}
        return self._index_maintainer.prepare_commit()

    def notify_precomputed_bucket_batch(
        self, data: pa.RecordBatch, bucket: int
    ) -> Optional[Tuple]:
        """Maintain HASH indexes for rows assigned by an upstream coordinator."""
        partitions, _, key_hashes = self.extract_hashes_batch(data)
        return self.notify_precomputed_bucket_hashes_batch(
            data, bucket, key_hashes, partitions=partitions
        )

    def notify_precomputed_bucket_hashes_batch(
        self,
        data: pa.RecordBatch,
        bucket: int,
        key_hashes: List[int],
        partitions: Optional[List[Tuple]] = None,
        new_mappings: Optional[List[bool]] = None,
    ) -> Optional[Tuple]:
        """Maintain HASH indexes using hashes carried through the shuffle."""
        from pypaimon.index.dynamic_bucket import to_signed_int32

        if self._index_maintainer is None:
            raise RuntimeError(
                "Precomputed dynamic buckets require a persistent table extractor"
            )
        if len(key_hashes) != data.num_rows:
            raise ValueError(
                "Precomputed key hash count {} does not match row count {}".format(
                    len(key_hashes), data.num_rows
                )
            )
        if partitions is None:
            partitions = self._extract_partitions_batch(data)
        if new_mappings is not None and len(new_mappings) != data.num_rows:
            raise ValueError(
                "Precomputed new-mapping count {} does not match row count {}".format(
                    len(new_mappings), data.num_rows
                )
            )
        if not partitions:
            return None
        partition = tuple(partitions[0])
        if new_mappings is None:
            new_mappings = [True] * data.num_rows
        for actual_partition, key_hash, is_new in zip(
            partitions, key_hashes, new_mappings
        ):
            if tuple(actual_partition) != tuple(partition):
                raise RuntimeError(
                    "A precomputed dynamic-bucket group contained multiple "
                    f"partitions: expected {partition}, got {actual_partition}"
                )
            if is_new:
                self._index_maintainer.notify_new_record(
                    partition, bucket, to_signed_int32(key_hash)
                )
        return partition

    def release_prepared(self) -> None:
        if self._index_maintainer is not None:
            self._index_maintainer.release_prepared()

    def abort(self) -> None:
        if self._index_maintainer is not None:
            self._index_maintainer.abort()


class PostponeBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -2))
        if num_buckets != BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(f"Postpone bucket mode requires bucket = -2, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [BucketMode.POSTPONE_BUCKET.value] * data.num_rows

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        return BucketMode.POSTPONE_BUCKET.value
