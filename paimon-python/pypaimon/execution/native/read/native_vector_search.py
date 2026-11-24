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

"""Local vector search through the Rust core Scan -> Plan -> Read path."""

from importlib import import_module

from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
from pypaimon.globalindex.vindex_options import is_multi_vector_type
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.native.table import (_catalog_metastore, _filesystem_vector_table_supported, _native_table)
from pypaimon.execution.native.read.builder import (_predicate_to_native)
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.execution.native.read.native_search_plan import _native_prefilter_options, _result


def _has_vector_layout_api():
    sdk = import_module('pypaimon_rust.datafusion')
    # Check optional exports without invoking module-level attribute resolution.
    return hasattr(vars(sdk).get('VectorSearchBuilder'), 'with_multi_vector')


def _filesystem_vector_search_supported(table):
    return (_filesystem_vector_table_supported(table) and table.options.data_evolution_enabled()
            and native_runtime_available() and _has_vector_layout_api())


def _multi_vector_requested(builder):
    return (getattr(builder, '_multi_vector', None) is not None
            or is_multi_vector_type(getattr(builder._vector_column, 'type', None)))


def _filesystem_vector_search_view(table, partition_filter):
    """Capture the search snapshot and its index entries in the selected partitions."""
    from pypaimon.index.index_file_handler import IndexFileHandler
    from pypaimon.snapshot.time_travel_util import TimeTravelUtil

    snapshot = TimeTravelUtil.resolve_snapshot(table)
    view = table if hasattr(table, '_read_snapshot') else table._copy_with_snapshot(snapshot)
    if snapshot is None or snapshot.index_manifest is None:
        return view, []
    predicate = None if partition_filter is None else lambda entry: partition_filter.test(entry.partition)
    return view, IndexFileHandler(view).scan(snapshot, predicate)


def _has_centroid_indexes(entries, fields):
    field_ids = {field.id for field in fields}
    return any(meta is not None and meta.index_field_id in field_ids and meta.is_centroid_index()
               for meta in (entry.index_file.global_index_meta for entry in entries))


def _native_vector_builder(builder, batch=False):
    """Configure a native builder, or return None before execution.

    Search, scalar filtering, refinement and physical-position selection all
    belong to Rust core. Python only translates options and typed predicates.
    """
    table = builder._table
    reject_search_under_query_auth(table)
    native_enabled = getattr(getattr(table, 'options', None), 'native_read_enabled', None)
    if not callable(native_enabled) or not native_enabled():
        return None
    loader = getattr(getattr(table, 'catalog_environment', None), 'catalog_loader', None)
    catalog = _catalog_metastore(loader)
    if catalog != 'rest' and not _filesystem_vector_search_supported(table):
        return None
    # Python's batch reader currently supports only global row-ID results.
    if batch and builder._is_primary_key_vector_search():
        return None
    if catalog == 'rest' and (not native_runtime_available()
                              or (_multi_vector_requested(builder) and not _has_vector_layout_api())):
        return None
    native_table = _native_table(table)
    native = (native_table.new_batch_vector_search_builder() if batch else
              native_table.new_vector_search_builder())
    if builder._vector_column is not None:
        native.with_vector_column(builder._vector_column.name)
    native.with_limit(builder._limit).with_options(_native_prefilter_options(table, builder._options))
    try:
        if builder._filter is not None:
            native.with_filter(_predicate_to_native(builder._filter))
        if builder._partition_filter is not None:
            native.with_partition_filter(_predicate_to_native(builder._partition_filter))
    except ValueError:
        # Qualify unsupported typed literals before searching.
        return None
    return native


def _set_native_query(native, builder, queries, batch):
    if batch:
        native.with_query_vectors(queries)
    elif getattr(builder, '_multi_vector', None) is not None:
        native.with_multi_vector(builder._multi_vector)
    else:
        native.with_query_vector(queries)


def try_native_vector_search(builder, queries, batch=False):
    native = _native_vector_builder(builder, batch)
    if native is None:
        return None
    table = builder._table
    # Once the Native route is chosen, errors must not trigger another search
    # against a potentially newer snapshot.
    _set_native_query(native, builder, queries, batch)
    result = native.execute()
    if batch:
        return [DictBasedScoredIndexResult(item.row_ids()) for item in result]
    return _result(table, result, builder._is_primary_key_vector_search())
