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

"""Transport hybrid queries, captured routes and weighted results to Rust Core."""

from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
from pypaimon.globalindex.full_text_options import NativeFullTextIndexOptions
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.native.table import (_native_table)
from pypaimon.execution.native.read.builder import (_predicate_to_native)
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.execution.native.write.native_commit import _rest_catalog_supported


def _native_builder(builder, executor=None):
    """Qualify the complete request before executing or capturing any route."""
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
    return table, native


def try_native_hybrid_search(builder, executor=None):
    """Core owns one snapshot, routes and fusion for the complete query."""
    configured = _native_builder(builder, executor)
    if configured is None:
        return None
    table, native = configured
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


def try_native_hybrid_routes(builder):
    configured = _native_builder(builder)
    if configured is None:
        return None
    _, native = configured
    # Core resolves the read view once. Retain ordinary public per-route
    # builders so callers can still configure them and create scans and reads.
    # Reconstructing these configurations must not resolve another snapshot.
    from copy import copy
    from pypaimon.common.json_util import JSON
    from pypaimon.snapshot.snapshot import Snapshot
    from pypaimon.table.source.hybrid_search_builder import HybridSearchRouteBuilder

    captured = native.route_builders()
    routes = []
    for route, native_route in zip(builder._routes, captured):
        snapshot = native_route.snapshot()
        snapshot = None if snapshot is None else JSON.from_json(snapshot._to_json(), Snapshot)
        execution = copy(builder)
        execution._table = builder._table._copy_with_snapshot(snapshot)
        search = (execution._new_vector_search_builder(route) if route.is_vector() else
                  execution._new_full_text_search_builder(route))
        routes.append(HybridSearchRouteBuilder(route, search))
    return routes


def rank_hybrid_results(builder, route_results):
    """Fuse supplied scores without table access or validation of unused queries."""
    from pypaimon_rust.datafusion import (
        GlobalIndexResult, HybridSearchRanker, HybridSearchWeightedResult,
    )
    from pypaimon.table.source.hybrid_search_builder import MRR_RANKER, RRF_RANKER, WEIGHTED_SCORE_RANKER

    ranker = {
        RRF_RANKER: HybridSearchRanker.RRF,
        MRR_RANKER: HybridSearchRanker.MRR,
        WEIGHTED_SCORE_RANKER: HybridSearchRanker.WEIGHTED_SCORE,
    }[builder._ranker]
    weighted = []
    for route_result in route_results:
        result = route_result.result
        rows = result.results()
        if rows.is_empty():
            scores = {}
        else:
            score = result.score_getter()
            scores = {row_id: score(row_id) or 0.0 for row_id in rows}
        weighted.append(HybridSearchWeightedResult(
            GlobalIndexResult.from_scored_row_ids(scores), route_result.route.weight))
    return DictBasedScoredIndexResult(ranker.rank(weighted, builder._limit).row_ids())
