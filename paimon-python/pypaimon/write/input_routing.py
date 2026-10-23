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
import struct
from typing import Any, Dict, List, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import AtomicType
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


class PartitionKeyExtractor:
    """Resolve input routing fields and hash their Java binary layout."""

    def __init__(self, table_schema: TableSchema):
        self.table_schema = table_schema
        self.partition_indices = self._get_field_indices(table_schema.partition_keys)

    def extract_partition_bucket_batch(self, data: pa.RecordBatch) -> Tuple[List[Tuple], List[int]]:
        return self._extract_partitions_batch(data), self._extract_buckets_batch(data)

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

        partition_columns = self._input_columns(data, self.partition_indices)

        partitions = []
        for row_idx in range(data.num_rows):
            partition_values = tuple(col[row_idx].as_py() for col in partition_columns)
            partitions.append(partition_values)

        return partitions

    def _input_columns(self, data: pa.RecordBatch, field_indices: List[int]):
        """Resolve routing fields in the actual write type, as Java's converter does."""
        columns = []
        for field_index in field_indices:
            name = self.table_schema.fields[field_index].name
            index = data.schema.get_field_index(name)
            if index < 0:
                raise ValueError(f"Input must contain exactly one routing column '{name}'.")
            columns.append(data.column(index))
        return columns

    @staticmethod
    def _binary_row_hash_code(values: Tuple, fields: List) -> int:
        return _hash_bytes_by_words(
            GenericRowSerializer.to_bytes(
                GenericRow(list(values), fields, RowKind.INSERT)
            )[4:]
        )


class PostponeFixedBucketInputRouter(PartitionKeyExtractor):
    """Group distributed inputs by the same partition and bucket as writers."""

    def __init__(self, table, bucket_plan):
        super().__init__(table.table_schema)
        if table.options.bucket() != BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(
                "Postpone fixed bucket writes require bucket = -2, got {}".format(
                    table.options.bucket()
                )
            )
        bucket_function = str(
            table.table_schema.options.get("bucket-function.type", "default")
        ).strip().lower()
        if bucket_function != "default":
            raise ValueError(
                "Postpone fixed bucket writes only support "
                "bucket-function.type=default, got {}"
                .format(bucket_function)
            )
        self.bucket_keys = table.table_schema.bucket_keys
        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
        self._bucket_key_fields = table.table_schema.logical_bucket_key_fields
        self._bucket_plan = bucket_plan

    def with_bucket_plan(self, bucket_plan) -> None:
        self._bucket_plan = bucket_plan

    def num_buckets(self, partition: Tuple) -> int:
        return self._bucket_plan.num_buckets(partition)

    def route(self, data) -> pa.Int32Array:
        """Return bucket IDs in input order, as the Native router does."""
        return pa.array(self.extract_partition_bucket_batch(data)[1], type=pa.int32())

    def extract_partition_bucket_batch(
        self, data: pa.RecordBatch
    ) -> Tuple[List[Tuple], List[int]]:
        partitions = self._extract_partitions_batch(data)
        columns = self._input_columns(data, self.bucket_key_indices)
        buckets = [
            _bucket_from_hash(
                self._binary_row_hash_code(
                    tuple(col[row_idx].as_py() for col in columns),
                    self._bucket_key_fields,
                ),
                self.num_buckets(partition),
            )
            for row_idx, partition in enumerate(partitions)
        ]
        return partitions, buckets

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return self.extract_partition_bucket_batch(data)[1]

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        partition = tuple(
            values_by_name[self.table_schema.fields[i].name]
            for i in self.partition_indices
        )
        return _bucket_from_hash(
            self._binary_row_hash_code(
                tuple(values_by_name[name] for name in self.bucket_keys),
                self._bucket_key_fields,
            ),
            self.num_buckets(partition),
        )


class FixedBucketInputRouter(PartitionKeyExtractor):
    """Route distributed inputs without constructing a Python file writer."""

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
        bucket_function = table_schema.options.get('bucket-function.type')
        self.bucket_function = 'default' if bucket_function is None else str(bucket_function).lower()
        if self.bucket_function == 'mod':
            if len(self._bucket_key_fields) != 1:
                raise ValueError('bucket key must have exactly one field in mod bucket function')
            key_type = self._bucket_key_fields[0].type
            if not isinstance(key_type, AtomicType) or key_type.type.upper() not in ('INT', 'INTEGER', 'BIGINT'):
                raise ValueError(
                    'bucket key type must be INT or BIGINT in mod bucket function, but got %s' % key_type)
        elif self.bucket_function not in ('default', 'hive'):
            raise ValueError('Unsupported bucket function type: %s' % self.bucket_function)

    def route(self, data) -> pa.Int32Array:
        """Return bucket IDs in input order, as the Native router does."""
        return pa.array(self.extract_partition_bucket_batch(data)[1], type=pa.int32())

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        self._check_automatic_bucket_function()
        columns = self._input_columns(data, self.bucket_key_indices)
        if self.bucket_function == 'mod':
            return [self._mod_bucket(value.as_py()) for value in columns[0]]
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
        self._check_automatic_bucket_function()
        if self.bucket_function == 'mod':
            return self._mod_bucket(values_by_name[self.bucket_keys[0]])
        return _bucket_from_hash(
            self._binary_row_hash_code(
                tuple(values_by_name[name] for name in self.bucket_keys),
                self._bucket_key_fields,
            ),
            self.num_buckets,
        )

    def _check_automatic_bucket_function(self):
        # Explicit buckets do not use this method. A single bucket also keeps
        # the existing zero result without needing the Hive hash algorithm.
        if self.bucket_function == 'hive' and self.num_buckets > 1:
            raise NotImplementedError(
                'Hive bucket functions require the native writer; enable write.native.enabled')

    def _mod_bucket(self, value):
        # Java reads the zeroed fixed-width slot for NULL; Python's modulo
        # with a positive divisor otherwise matches Math.floorMod, even for
        # Long.MIN_VALUE. Avoid floating-point division for BIGINT values.
        return 0 if value is None else value % self.num_buckets


class DynamicBucketInputRouter(PartitionKeyExtractor):
    """Hash dynamic-bucket input without assigning buckets or maintaining indexes."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))
        if num_buckets != -1:
            raise ValueError(
                "Only 'bucket' = '-1' is allowed for "
                f"'DynamicBucketRowKeyExtractor', but found: {num_buckets}"
            )
        if table_schema.options.get(CoreOptions.BUCKET_KEY.key()) is not None:
            raise ValueError("Cannot define 'bucket-key' in dynamic bucket mode")
        self.bucket_keys = table_schema.bucket_keys
        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
        self._bucket_key_fields = table_schema.logical_bucket_key_fields
        self._partition_fields = [
            table_schema.fields[index] for index in self.partition_indices
        ]

    def extract_hashes_batch(
        self, data: pa.RecordBatch
    ) -> Tuple[List[Tuple], List[int], List[int]]:
        """Return partitions, BinaryRow partition hashes, and key hashes."""
        partitions, partition_hashes = self.extract_partition_hashes_batch(data)
        return partitions, partition_hashes, self._extract_key_hashes_batch(data)

    def extract_partition_hashes_batch(self, data: pa.RecordBatch) -> Tuple[List[Tuple], List[int]]:
        """Hash partitions independently when key hashes arrived from upstream."""
        partitions = self._extract_partitions_batch(data)
        partition_hash_cache = {}
        partition_hashes = []
        for partition in partitions:
            if partition not in partition_hash_cache:
                partition_hash_cache[partition] = self._binary_row_hash_code(
                    partition, self._partition_fields
                )
            partition_hashes.append(partition_hash_cache[partition])
        return partitions, partition_hashes

    def _extract_key_hashes_batch(self, data: pa.RecordBatch) -> List[int]:
        key_columns = self._input_columns(data, self.bucket_key_indices)
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
