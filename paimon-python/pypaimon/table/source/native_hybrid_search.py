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
from pypaimon.common.json_util import JSON
from pypaimon.globalindex.full_text.native_full_text_global_index_reader import NativeFullTextIndexOptions
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.read.native_plan import _native_table, _predicate_to_native, native_method_available
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.native_commit import _rest_catalog_supported


def try_native_hybrid_search(builder):
    """Qualify before execution; core owns one snapshot, routes and fusion."""
    table = builder._table
    if type(table) is not FileStoreTable:
        return None
    reject_search_under_query_auth(table)
    if (not table.options.native_read_enabled()
            or not table.options.data_evolution_enabled()
            or not table.options.row_tracking_enabled()
            or table.is_primary_key_table
            or table.options.file_format() != 'parquet'
            or not native_method_available('Table', 'new_hybrid_search_builder')
            or not _rest_catalog_supported(table)):
        return None
    builder._validate_search()
    if any(route.is_full_text() for route in builder._routes):
        # Preserve the same typed analyzer normalization used by native full-text.
        options = {'full-text.' + key: value for key, value in NativeFullTextIndexOptions.from_options(
            table.options.options.to_map()).to_native_options().items()}
        table = table.copy(options)
    native = _native_table(table).new_hybrid_search_builder()
    if hasattr(table, '_read_snapshot'):
        snapshot = table._read_snapshot
        native.with_snapshot(JSON.to_json(snapshot) if snapshot is not None else None)
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
    return DictBasedScoredIndexResult(native.execute_local().row_ids())
