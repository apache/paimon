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

"""Global-index builds delegated to Rust, retaining Python's commit API."""

from pypaimon.execution.native.table import (_native_table, _options_to_native)
from pypaimon.execution.native.read.builder import (_predicate_to_native)
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.execution.native.write.native_commit import _rest_catalog_supported, from_native_commit_messages
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options


def build_native_global_index(builder, partition_filter, *, execution="local", concurrency=None, ray_remote_args=None):
    """Return unpublished messages, or None before building for an ineligible table.

    Rust owns snapshot selection, range planning and index file generation.
    Build errors propagate: a failed native build must not start a second build.
    """
    table = builder._table
    from pypaimon.globalindex.vindex_options import VINDEX_IDENTIFIERS
    if (builder._index_type not in ('btree', 'bitmap', 'full-text') + tuple(VINDEX_IDENTIFIERS)
            or not table.options.native_write_enabled()
            or (table.options.deletion_vectors_enabled() and builder._index_type == 'full-text')
            or table.options.query_auth_enabled
            or table.is_primary_key_table
            or table.options.file_format() != 'parquet'
            or not native_runtime_available()
            or not _rest_catalog_supported(table)):
        return None
    from pypaimon_rust.datafusion import GlobalIndexBuildBuilder
    if not GlobalIndexBuildBuilder.supports_index_type(builder._index_type):
        return None
    # Keep build parameters separate from schema defaults: alias precedence is
    # resolved within each source, then explicit build parameters win.
    options = _build_options_to_native(builder._user_options)
    table_overrides = {key: None for key, value in builder._user_options.items() if value is None}
    parallelism = CoreOptions.GLOBAL_INDEX_BUILD_PARALLELISM.key()
    table_values = table.options.options.to_map()
    if parallelism not in builder._user_options and isinstance(table_values.get(parallelism), (bool, float)):
        table_overrides[parallelism] = _build_options_to_native(
            {parallelism: table_values[parallelism]})[parallelism]
    native_table = table.copy_without_time_travel(table_overrides)
    native = _native_table(native_table).new_global_index_build_builder()
    native.with_index_column(builder._index_columns[0]).with_index_type(builder._index_type)
    native.with_options(options)
    if partition_filter is not None:
        native.with_partition_filter(_predicate_to_native(partition_filter))
    if execution == "ray":
        from pypaimon.ray.vector_index_build import build_native_vector_index
        # Planning may authorize and capture inputs; its errors must not select
        # another engine or restart a build that may already have side effects.
        plan = native.plan()
        return build_native_vector_index(table, plan, concurrency, ray_remote_args)
    return from_native_commit_messages(table, native.build())


def _build_options_to_native(options):
    encoded = _options_to_native(options)
    key = CoreOptions.GLOBAL_INDEX_BUILD_PARALLELISM.key()
    if isinstance(options.get(key), (bool, float)):
        # Retain Python's typed integer conversion before the string-only ABI.
        encoded[key] = str(int(Options(options).get(CoreOptions.GLOBAL_INDEX_BUILD_PARALLELISM)))
    return encoded


def build_native_index_shard(table, shard):
    """Execute captured metadata in Core; workers never replan or publish."""
    native = _native_table(table).new_global_index_build_builder()
    return from_native_commit_messages(table, native.build_shard(shard))
