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

"""Local REST full-text search delegated to Rust core."""

from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
from pypaimon.globalindex.full_text.native_full_text_global_index_reader import NativeFullTextIndexOptions
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.read.native_plan import _native_table, _predicate_to_native, native_method_available
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.native_commit import _rest_catalog_supported


def try_native_full_text_search(builder):
    """Qualify the route before execution; Rust owns scan, filtering and ranking."""
    table = builder._table
    reject_search_under_query_auth(table)
    if (type(table) is not FileStoreTable
            or not table.options.native_read_enabled()
            or not table.options.data_evolution_enabled()
            or not table.options.row_tracking_enabled()
            or table.is_primary_key_table
            or table.options.file_format() != 'parquet'
            or not native_method_available('Table', 'new_full_text_search_builder')
            or not _rest_catalog_supported(table)):
        return None
    if builder._limit is None or builder._limit <= 0:
        raise ValueError('Limit must be positive, set via with_limit()')
    # The full-text adapter accepts typed lists and booleans. Reuse its mapping
    # before the generic table bridge stringifies options for Rust.
    options = {'full-text.' + key: value for key, value in NativeFullTextIndexOptions.from_options(
        table.options.options.to_map()).to_native_options().items()}
    native = _native_table(table.copy(options)).new_full_text_search_builder()
    if builder._field_name is not None and builder._query is not None:
        native.with_query(builder._field_name, builder._query)
    native.with_limit(builder._limit)
    try:
        if builder._partition_filter is not None:
            native.with_partition_filter(_predicate_to_native(builder._partition_filter))
        if builder._filter is not None:
            native.with_filter(_predicate_to_native(builder._filter))
    except ValueError:
        # Qualify literal support before searching. Some Python literals cannot
        # be represented by the strict typed predicate FFI.
        return None
    # Once execution starts, propagate errors instead of running a second search.
    return DictBasedScoredIndexResult(native.execute_local().row_ids())
