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


def as_record_batch(data):
    if isinstance(data, pa.RecordBatch):
        return data
    if isinstance(data, pa.Table):
        return pa.RecordBatch.from_arrays(
            [column.combine_chunks() for column in data.columns], schema=data.schema)
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
        return self._router.route(as_record_batch(data))

    def __getstate__(self):
        state = dict(self.__dict__)
        state['_router'] = None
        return state
