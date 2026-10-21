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

"""Select an index executor before building files or deletion messages."""


def build_global_index(request, partition_filter, *, execution="local", concurrency=None, ray_remote_args=None):
    if execution == "local" and request._table.options.native_write_enabled():
        from pypaimon.execution.native.write.native_index_build import build_native_global_index
        messages = build_native_global_index(request, partition_filter)
        if messages is not None:
            return messages
    from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
    return PythonGlobalIndexBuilder(request).build(
        partition_filter, execution=execution, concurrency=concurrency,
        ray_remote_args=ray_remote_args)


def build_global_index_drop(request, partition_filter):
    if request._table.options.native_write_enabled():
        from pypaimon.execution.native.write.native_index_drop import build_native_global_index_drop
        messages = build_native_global_index_drop(request, partition_filter)
        if messages is not None:
            return messages
    from pypaimon.execution.python.globalindex.drop_global_index import build_global_index_drop as execute
    return execute(request, partition_filter)
