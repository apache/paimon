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

import hashlib
import json
from abc import ABC, abstractmethod
from typing import List, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode


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

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        columns = [data.column(i) for i in self.bucket_key_indices]
        hashes = []
        for row_idx in range(data.num_rows):
            row_values = tuple(col[row_idx].as_py() for col in columns)
            hashes.append(self.hash(row_values))
        return [abs(hash_val) % self.num_buckets for hash_val in hashes]

    @staticmethod
    def hash(data) -> int:
        data_json = json.dumps(data)
        return int(hashlib.md5(data_json.encode()).hexdigest(), 16)


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

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> int:
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
