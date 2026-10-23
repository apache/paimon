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

"""Arrow contracts for Python distributed dynamic-bucket execution."""

import pyarrow as pa

from pypaimon.index.dynamic_bucket import (
    HashBucketAssigner, compute_assigner, resolve_bucket_snapshot, to_signed_int32,
)
from pypaimon.write.input_routing import DynamicBucketInputRouter


class PythonDynamicBucketRouter:

    def __init__(self, table, num_channels, num_assigners):
        if num_channels <= 0 or num_assigners <= 0:
            raise ValueError('num_channels and num_assigners must be positive')
        self._router = DynamicBucketInputRouter(table.table_schema)
        self._num_channels = num_channels
        self._num_assigners = num_assigners

    def route(self, data) -> pa.RecordBatch:
        _, partition_hashes, key_hashes = self._router.extract_hashes_batch(data)
        return pa.RecordBatch.from_arrays([
            pa.array([compute_assigner(partition_hash, key_hash, self._num_channels, self._num_assigners)
                      for partition_hash, key_hash in zip(partition_hashes, key_hashes)], type=pa.int32()),
            pa.array([to_signed_int32(key_hash) for key_hash in key_hashes], type=pa.int32()),
        ], names=['assigner', 'key_hash'])


class PythonDynamicBucketAssigner:

    def __init__(self, table, num_channels, num_assigners, assign_id,
                 ignore_existing=False, base_snapshot_id=None):
        self._router = DynamicBucketInputRouter(table.table_schema)
        snapshot = None if ignore_existing else resolve_bucket_snapshot(table, base_snapshot_id)
        self.restore_snapshot_id = snapshot.id if snapshot is not None else 0
        self._assigner = HashBucketAssigner(
            table, num_channels=num_channels, num_assigners=num_assigners, assign_id=assign_id,
            target_bucket_row_number=table.options.dynamic_bucket_target_row_num(),
            max_buckets_num=table.options.dynamic_bucket_max_buckets(),
            ignore_existing=ignore_existing, snapshot=snapshot)

    def assign(self, data, key_hashes=None) -> pa.Int32Array:
        if key_hashes is None:
            partitions, partition_hashes, hashes = self._router.extract_hashes_batch(data)
        else:
            if isinstance(key_hashes, pa.ChunkedArray):
                key_hashes = key_hashes.combine_chunks()
            if not isinstance(key_hashes, pa.Int32Array):
                raise TypeError('Dynamic bucket key hashes must be an Int32Array')
            if len(key_hashes) != data.num_rows or key_hashes.null_count:
                raise ValueError('Dynamic bucket key hashes must be non-null and match the input row count')
            partitions, partition_hashes = self._router.extract_partition_hashes_batch(data)
            hashes = key_hashes.to_pylist()
        return pa.array([bucket for bucket, _ in self._assigner.assign_batch(
            partitions, partition_hashes, hashes)], type=pa.int32())
