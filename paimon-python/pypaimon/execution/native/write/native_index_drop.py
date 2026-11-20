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

"""Native global index deletion message preparation."""


def build_native_global_index_drop(builder, partition_filter):
    from pypaimon.execution.native.table import (_native_table)
    from pypaimon.execution.native.read.builder import (_predicate_to_native)
    from pypaimon.execution.native.runtime import (native_runtime_available)
    from pypaimon.execution.native.write.native_commit import _rest_catalog_supported, from_native_commit_messages

    table = builder._table
    if (not table.options.native_write_enabled()
            or table.options.query_auth_enabled
            or not native_runtime_available()
            or not _rest_catalog_supported(table)):
        return None
    from pypaimon_rust.datafusion import GlobalIndexDropBuilder
    if not GlobalIndexDropBuilder.supports_index_type(builder._index_type):
        return None
    native = _native_table(table).new_global_index_drop_builder()
    native.with_index_columns(builder._index_columns).with_index_type(builder._index_type)
    if partition_filter is not None:
        native.with_partition_filter(_predicate_to_native(partition_filter))
    # Preparing deletion messages is read-only. Python retains explicit
    # commit and dry-run ownership; failures must not remove index files.
    return from_native_commit_messages(table, native.build())
