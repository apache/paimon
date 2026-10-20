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
