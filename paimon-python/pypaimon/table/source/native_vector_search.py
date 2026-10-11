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

from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.read.native_plan import (
    _catalog_metastore, _native_table, _partition_fields, _predicate_to_native,
    native_method_available)
from pypaimon.read.split_serializer import deserialize_split_v1
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition)


def try_native_vector_search(builder, queries, batch=False):
    """Return existing Python result types, or None to use the Python reader.

    Search, scalar filtering, refinement and physical-position selection all
    belong to Rust core. Python only converts the scored result metadata.
    """
    table = builder._table
    reject_search_under_query_auth(table)
    native_enabled = getattr(table.options, 'native_read_enabled', None)
    if not callable(native_enabled) or not native_enabled():
        return None
    loader = getattr(getattr(table, 'catalog_environment', None), 'catalog_loader', None)
    if loader is None or _catalog_metastore(loader) != 'rest':
        return None
    context_fn = getattr(loader, 'context', None)
    if not callable(context_fn):
        return None
    context = context_fn()
    if any(getattr(context, attr, None) is not None for attr in (
            'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader')):
        return None
    # Python's batch reader currently supports only global row-ID results.
    if batch and builder._is_primary_key_vector_search():
        return None
    method = 'new_batch_vector_search_builder' if batch else 'new_vector_search_builder'
    if not native_method_available('Table', method):
        return None
    native = getattr(_native_table(table), method)()
    if builder._vector_column is not None:
        native.with_vector_column(builder._vector_column.name)
    native.with_limit(builder._limit).with_options(builder._options)
    try:
        if builder._filter is not None:
            native.with_filter(_predicate_to_native(builder._filter))
        if builder._partition_filter is not None:
            native.with_partition_filter(_predicate_to_native(builder._partition_filter))
    except ValueError:
        # Qualify unsupported typed literals before searching.
        return None
    # Once the Native route is chosen, errors must not trigger another search
    # against a potentially newer snapshot.
    if batch:
        native.with_query_vectors(queries)
        results = native.execute_batch_local()
        return [DictBasedScoredIndexResult(result.row_ids()) for result in results]
    native.with_query_vector(queries)
    result = native.execute_local()
    if builder._is_primary_key_vector_search():
        return _NativePrimaryKeyScoredResult(table, result)
    return DictBasedScoredIndexResult(result.row_ids())


class _NativePrimaryKeyScoredResult(PrimaryKeyScoredResult):
    """Metadata view of the physical selections already built by Rust core."""

    def __init__(self, table, result):
        self._snapshot_id = result.snapshot_id() or 0
        self._positions = tuple(PrimaryKeySearchPosition(*position) for position in result.positions())
        self._splits = tuple(deserialize_split_v1(
            split, _partition_fields(table), table.trimmed_primary_keys_fields)
            for split in result.splits())
