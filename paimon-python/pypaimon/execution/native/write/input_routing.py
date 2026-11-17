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

"""Arrow transport for Rust's distributed input bucket routing."""

import pyarrow as pa

from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.write.native_commit import create_native_write_table


def input_batches(data):
    if isinstance(data, pa.RecordBatch):
        return [data]
    if isinstance(data, pa.Table):
        # Binary columns can retain multiple chunks even after combine_chunks.
        # Preserve every batch and its row count, including zero-column input.
        batches = data.to_batches()
        return batches if batches else [pa.RecordBatch.from_arrays(
            [column.combine_chunks() for column in data.columns], schema=data.schema)]
    raise TypeError('Input routing requires a pyarrow Table or RecordBatch')


def create_native_input_router(table, bucket_plan=None):
    if not native_runtime_available():
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    router = NativeInputRouter(table, bucket_plan)
    router._prepare(native_table)
    return router


class NativeInputRouter:
    """Keep serializable configuration while Rust owns every input-row hash."""

    def __init__(self, table, bucket_plan=None):
        self.table = table
        self._bucket_plan = bucket_plan
        self._router = None

    def _prepare(self, native_table=None):
        from pypaimon_rust.datafusion import BucketRouter
        if native_table is None:
            native_table = create_native_write_table(self.table)
        if native_table is None:
            raise RuntimeError('Native input bucket routing is unavailable')
        router = BucketRouter(native_table.schema())
        if self._bucket_plan is not None:
            router = router.with_bucket_plan(self._bucket_plan.to_arrow(self.table))
        self._router = router

    def route(self, data) -> pa.Int32Array:
        if self._router is None:
            self._prepare()
        buckets = [self._router.route(batch) for batch in input_batches(data)]
        return buckets[0] if len(buckets) == 1 else pa.concat_arrays(buckets)

    def __getstate__(self):
        state = dict(self.__dict__)
        state['_router'] = None
        return state


def create_native_dynamic_bucket_assigner_router(table, num_channels, num_assigners):
    if not native_runtime_available():
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    router = NativeDynamicBucketAssignerRouter(table, num_channels, num_assigners)
    router._prepare(native_table)
    return router


class NativeDynamicBucketAssignerRouter:
    """Route input rows to dynamic bucket assigners using Rust's row hashes."""

    def __init__(self, table, num_channels, num_assigners):
        self.table = table
        self._num_channels = num_channels
        self._num_assigners = num_assigners
        self._router = None

    def _prepare(self, native_table=None):
        from pypaimon_rust.datafusion import DynamicBucketAssignerRouter
        if native_table is None:
            native_table = create_native_write_table(self.table)
        if native_table is None:
            raise RuntimeError('Native dynamic bucket assigner routing is unavailable')
        self._router = DynamicBucketAssignerRouter(native_table.schema(), self._num_channels, self._num_assigners)

    def route(self, data) -> pa.RecordBatch:
        if self._router is None:
            self._prepare()
        routed = [self._router.route(batch) for batch in input_batches(data)]
        if len(routed) == 1:
            return routed[0]
        return pa.RecordBatch.from_arrays([
            pa.concat_arrays([batch.column(index) for batch in routed]) for index in range(2)
        ], schema=routed[0].schema)

    def __getstate__(self):
        state = dict(self.__dict__)
        state['_router'] = None
        return state


def create_native_dynamic_bucket_assigner(table, num_channels, num_assigners, assign_id,
                                          ignore_existing=False, base_snapshot_id=None):
    if not native_runtime_available():
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    assigner = NativeDynamicBucketAssigner(
        table, num_channels, num_assigners, assign_id, ignore_existing, base_snapshot_id)
    assigner._prepare(native_table)
    return assigner


class NativeDynamicBucketAssigner:

    def __init__(self, table, num_channels, num_assigners, assign_id,
                 ignore_existing=False, base_snapshot_id=None):
        self.table = table
        self._num_channels = num_channels
        self._num_assigners = num_assigners
        self._assign_id = assign_id
        self._ignore_existing = ignore_existing
        self._base_snapshot_id = base_snapshot_id
        self._assigner = None

    def _prepare(self, native_table=None):
        from pypaimon_rust.datafusion import DynamicBucketAssigner
        if native_table is None:
            native_table = create_native_write_table(self.table)
        if native_table is None:
            raise RuntimeError('Native dynamic bucket assignment is unavailable')
        self._assigner = DynamicBucketAssigner(
            native_table, self._num_channels, self._num_assigners, self._assign_id,
            restore_snapshot_id=self._base_snapshot_id, overwrite=self._ignore_existing)
        # Core captures the restore point once. Final worker writers must
        # restore that same snapshot even if another commit finishes later.
        self._base_snapshot_id = self._assigner.restore_snapshot_id

    @property
    def restore_snapshot_id(self):
        if self._assigner is None:
            self._prepare()
        return self._base_snapshot_id

    def assign(self, data, key_hashes=None) -> pa.Int32Array:
        batches = input_batches(data)
        if isinstance(key_hashes, pa.ChunkedArray):
            key_hashes = key_hashes.combine_chunks()
        if key_hashes is not None:
            if not isinstance(key_hashes, pa.Int32Array):
                raise TypeError('Dynamic bucket key hashes must be an Int32Array')
            # Validate the whole input before the stateful assigner sees any
            # batch, including a null or missing hash in a later chunk.
            if len(key_hashes) != data.num_rows or key_hashes.null_count:
                raise ValueError('Dynamic bucket key hashes must be non-null and match the input row count')
        if self._assigner is None:
            self._prepare()
        buckets = []
        offset = 0
        for batch in batches:
            hashes = None if key_hashes is None else key_hashes.slice(offset, batch.num_rows)
            buckets.append(self._assigner.assign(batch, key_hashes=hashes))
            offset += batch.num_rows
        return buckets[0] if len(buckets) == 1 else pa.concat_arrays(buckets)
