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

"""Transport an existing local hybrid query to one Rust core operation."""

from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
from pypaimon.globalindex.full_text_options import NativeFullTextIndexOptions
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.native.table import (_native_table)
from pypaimon.execution.native.read.builder import (_predicate_to_native)
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.execution.native.write.native_commit import _rest_catalog_supported


def try_native_hybrid_search(builder, executor=None):
    """Qualify before execution; core owns one snapshot, routes and fusion."""
    table = builder._table
    if type(table) is not FileStoreTable:
        return None
    reject_search_under_query_auth(table)
    if (not table.options.native_read_enabled()
            or not table.options.row_tracking_enabled()
            or table.is_primary_key_table
            or table.options.file_format() != 'parquet'
            or not native_runtime_available()
            or not _rest_catalog_supported(table)):
        return None
    builder._validate_search()
    if executor is not None and not table.options.data_evolution_enabled():
        raise ValueError('Distributed search supports only data-evolution tables')
    if any(route.is_full_text() for route in builder._routes):
        # Preserve the same typed analyzer normalization used by native full-text.
        options = {'full-text.' + key: value for key, value in NativeFullTextIndexOptions.from_options(
            table.options.options.to_map()).to_native_options().items()}
        table = table.copy(options)
    native = _native_table(table).new_hybrid_search_builder()
    native.with_limit(builder._limit).with_ranker(builder._ranker)
    for route in builder._routes:
        if route.is_vector():
            native.add_vector_route(route.field_name, route.vector, route.limit, route.weight, route.options)
        else:
            native.add_full_text_route(
                route.field_name, route.full_text_query, route.limit, route.weight, route.options)
    try:
        if builder._partition_filter is not None:
            native.with_partition_filter(_predicate_to_native(builder._partition_filter))
        if builder._filter is not None:
            native.with_filter(_predicate_to_native(builder._filter))
    except (ValueError, NotImplementedError):
        # Qualify strict literal support before executing any route or ranking.
        return None
    # Propagate execution failures: retrying Python can select a newer snapshot.
    if executor is None:
        result = native.execute()
    else:
        from pypaimon.execution.native.read.search_execution import native_task_executor
        callback = native_task_executor(table, executor)
        try:
            result = native._execute_with_executor(callback)
        finally:
            callback.cancel()
            callback.raise_if_failed()
    return DictBasedScoredIndexResult(result.row_ids())
