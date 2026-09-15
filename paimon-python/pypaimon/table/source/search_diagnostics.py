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

"""Opt-in diagnostics for the existing local search execution paths."""

import time
from collections import defaultdict
from concurrent.futures import Future
from dataclasses import dataclass, field
from functools import wraps
from threading import Lock
from typing import Any, Dict, List, Optional

from pypaimon.utils.range import Range


@dataclass
class SearchRoutePlan:
    """Planned work, not a count of live rows or ANN candidates.

    Row counts are unions of inclusive row-id ranges. Indexed and raw ranges
    may overlap when scalar-index coverage requires a fallback. Source-backed
    primary-key plans have no comparable global row-id coverage (None).
    """

    kind: str
    column: str
    limit: int
    query_count: int
    snapshot_id: Optional[int]
    search_mode: str
    index_split_count: int = 0
    index_file_count: int = 0
    index_bytes: int = 0
    index_types: List[str] = field(default_factory=list)
    indexed_range_rows: Optional[int] = 0
    raw_split_count: int = 0
    raw_range_rows: Optional[int] = 0
    overlapping_range_rows: Optional[int] = 0
    scalar_index_file_count: int = 0
    has_pre_filter: bool = False
    has_partition_filter: bool = False
    weight: float = 1.0
    notes: List[str] = field(default_factory=list)


@dataclass
class SearchExplainResult:
    """Search plans in route order; producing this object does not run a search."""

    kind: str
    limit: int
    routes: List[SearchRoutePlan]
    ranker: Optional[str] = None
    route_parallelism: int = 1
    projection: Optional[List[str]] = None
    has_post_filter: bool = False

    def __str__(self):
        lines = ["== PyPaimon Search Plan ==",
                 "Search: {}  limit={}".format(self.kind, self.limit)]
        if self.ranker is not None:
            lines.append("Ranker: {}  route parallelism={}".format(
                self.ranker, self.route_parallelism))
        if self.projection is not None:
            lines.append("Projection: {}".format(self.projection))
        if self.has_post_filter:
            lines.append("Post-filter: applied during result lookup, after search top-k")
        for i, route in enumerate(self.routes):
            lines.append("Route {}: {} column={} limit={} queries={} snapshot={} mode={} weight={}".format(
                i, route.kind, route.column, route.limit, route.query_count,
                route.snapshot_id, route.search_mode, route.weight))
            lines.append("  Index: splits={} files={} bytes={} types={}".format(
                route.index_split_count, route.index_file_count, route.index_bytes,
                ",".join(route.index_types) or "<none>"))
            lines.append("  Raw splits: {}".format(route.raw_split_count))
            lines.append("  Range rows (not live counts): indexed={} raw={} overlap={}".format(
                route.indexed_range_rows, route.raw_range_rows, route.overlapping_range_rows))
            lines.append("  Filters: scalar={} partition={} scalar-index-files={}".format(
                route.has_pre_filter, route.has_partition_filter, route.scalar_index_file_count))
            lines.extend("  Note: " + note for note in route.notes)
        return "\n".join(lines)


@dataclass
class SearchProfileResult:
    """One execution and its result; times are milliseconds.

    Builder results are the normal scored results. Multimodal query results
    are Arrow tables (a list for batch searches), including their lookup cost.
    Stage times are inclusive, and route work can overlap; do not sum them to
    obtain elapsed time. Missing stages were not measured, not necessarily free.
    """

    plan: SearchExplainResult
    result: Any
    elapsed_ms: float
    route_metrics: List[Dict[str, Any]]
    fusion_ms: Optional[float] = None
    lookup_ms: Optional[float] = None
    output_rows: Optional[int] = None
    lookup_snapshot_ids: Optional[List[Optional[int]]] = None

    def __str__(self):
        lines = [str(self.plan), "== Search Profile (ms; stages may overlap) ==",
                 "Elapsed: {:.3f}".format(self.elapsed_ms)]
        for i, metrics in enumerate(self.route_metrics):
            lines.append("Route {}: timings={} counters={}".format(
                i, {k: round(v, 3) for k, v in metrics["timings_ms"].items()},
                metrics["counters"]))
        if self.fusion_ms is not None:
            lines.append("Fusion: {:.3f}".format(self.fusion_ms))
        if self.lookup_ms is not None:
            lines.append("Lookup: {:.3f}  output rows={}".format(self.lookup_ms, self.output_rows))
            lines.append("Lookup snapshots: {}".format(self.lookup_snapshot_ids))
        return "\n".join(lines)


class SearchMetrics:
    """One reader's counters, shared only by its search workers."""

    def __init__(self):
        self.timings_ms = defaultdict(float)
        self.counters = defaultdict(int)
        self._active = 0
        self._lock = Lock()

    def add_time(self, stage, start):
        elapsed = (time.perf_counter() - start) * 1000
        with self._lock:
            self.timings_ms[stage] += elapsed

    def add(self, name, count):
        with self._lock:
            self.counters[name] += count

    def begin_index(self, rows, include):
        included = rows if include is None else include.cardinality()
        with self._lock:
            self._active += 1
            self.counters["peak_index_searches"] = max(self._active, self.counters["peak_index_searches"])
            self.counters["index_searches"] += 1
            self.counters["index_rows_before_filter"] += rows
            self.counters["index_rows_after_filter"] += included

    def end_index(self, start):
        elapsed = (time.perf_counter() - start) * 1000
        with self._lock:
            self.timings_ms["index_search"] += elapsed
            self._active -= 1

    def snapshot(self):
        with self._lock:
            return {"timings_ms": dict(self.timings_ms), "counters": dict(self.counters)}


def search_stage(name):
    """Record synchronous reader work only when profiling is requested."""
    def decorate(method):
        @wraps(method)
        def measured(self, *args, **kwargs):
            metrics = getattr(self, "_search_metrics", None)
            if metrics is None:
                return method(self, *args, **kwargs)
            start = time.perf_counter()
            try:
                return method(self, *args, **kwargs)
            finally:
                metrics.add_time(name, start)
        return measured
    return decorate


def record_count(owner, name, count):
    metrics = getattr(owner, "_search_metrics", None)
    if metrics is not None:
        metrics.add(name, count)


def run_index_search(owner, search, request, row_count, close):
    """Close the reader on completion; profiling finishes before publishing results."""
    metrics = getattr(owner, "_search_metrics", None)
    if metrics is not None:
        metrics.begin_index(row_count, request.include_row_ids)
        start = time.perf_counter()
    try:
        future = search(request)
    except BaseException:
        if metrics is not None:
            metrics.end_index(start)
        close()
        raise
    if metrics is None:
        future.add_done_callback(lambda _: close())
        return future

    observed = Future()
    # Cancelling this wrapper cannot stop native work and must not close its reader.
    observed.set_running_or_notify_cancel()

    def completed(done):
        metrics.end_index(start)
        try:
            try:
                result = done.result()
                metrics.add("index_candidates", result_count(result))
            finally:
                close()
        except BaseException as error:
            observed.set_exception(error)
        else:
            observed.set_result(result)

    future.add_done_callback(completed)
    return observed


def result_count(result):
    if result is None:
        return 0
    if isinstance(result, list):
        return sum(result_count(item) for item in result)
    if hasattr(result, "positions"):
        return len(result.positions)
    return result.results().cardinality()


class SearchDiagnostics:
    """Shared public diagnostics for built-in local search builders."""

    def explain(self):
        """Plan without opening vector/text readers or executing the search.

        Planning can evaluate primary-key scalar predicates, so it is not
        guaranteed to be metadata-only. Coverage is planned range coverage.
        """
        if self._diagnostic_kind == "hybrid":
            from pypaimon.table.source.hybrid_search_builder import _MAX_ROUTE_WORKERS
            routes = self.route_builders()
            plans = [route.search_builder.explain().routes[0] for route in routes]
            for route, plan in zip(routes, plans):
                plan.weight = route.route.weight
            return SearchExplainResult(
                "hybrid", self._limit, plans,
                self._ranker, min(len(routes), _MAX_ROUTE_WORKERS))
        _, _, route = _plan_leaf(self)
        return SearchExplainResult(self._diagnostic_kind, self._limit, [route])

    def profile(self):
        """Execute once and return the normal result with an opt-in profile."""
        start = time.perf_counter()
        if self._diagnostic_kind == "hybrid":
            from pypaimon.table.source.hybrid_search_builder import (
                _MAX_ROUTE_WORKERS, _execute_routes)
            routes = self.route_builders()
            profiles = _execute_routes(routes, lambda route: route.search_builder.profile())
            for route, profile in zip(routes, profiles):
                profile.plan.routes[0].weight = route.route.weight
            fusion_start = time.perf_counter()
            result = self.rank([
                self.to_route_result(route, profile.result)
                for route, profile in zip(routes, profiles)])
            fusion_ms = (time.perf_counter() - fusion_start) * 1000
            plan = SearchExplainResult(
                "hybrid", self._limit, [profile.plan.routes[0] for profile in profiles],
                self._ranker, min(len(routes), _MAX_ROUTE_WORKERS))
            return SearchProfileResult(
                plan, result, (time.perf_counter() - start) * 1000,
                [profile.route_metrics[0] for profile in profiles], fusion_ms=fusion_ms)
        metrics = SearchMetrics()
        reader, scan_plan, route = _plan_leaf(self)
        metrics.add_time("planning", start)
        reader._search_metrics = metrics
        search_start = time.perf_counter()
        try:
            if self._diagnostic_kind == "batch_vector":
                result = reader.read_batch_plan(scan_plan)
            else:
                result = reader.read_plan(scan_plan)
        finally:
            metrics.add_time("search", search_start)
            reader._search_metrics = None
        metrics.add("result_rows", result_count(result))
        return SearchProfileResult(
            SearchExplainResult(self._diagnostic_kind, self._limit, [route]),
            result, (time.perf_counter() - start) * 1000, [metrics.snapshot()])


def _plan_leaf(builder):
    kind = builder._diagnostic_kind
    if kind == "full_text":
        reader = builder.new_full_text_read()
        plan = builder.new_full_text_scan().scan()
        column = builder._field_name
    else:
        reader = (builder.new_batch_vector_search_read() if kind == "batch_vector"
                  else builder.new_vector_search_read())
        plan = builder.new_vector_search_scan().scan()
        column = builder._vector_column.name
    return reader, plan, _summarize(builder, plan, column)


def _summarize(builder, plan, column):
    from pypaimon.common.options.core_options import CoreOptions
    from pypaimon.common.options.options import Options
    from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit, RawFullTextSearchSplit
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit

    kind = builder._diagnostic_kind
    core = CoreOptions(Options(dict(builder._table.table_schema.options)))
    mode = (core.full_text_index_search_mode() if kind == "full_text"
            else core.vector_index_search_mode())
    snapshot = plan.snapshot() if callable(getattr(plan, "snapshot", None)) else None
    snapshot_id = getattr(snapshot, "id", getattr(plan, "snapshot_id", None))
    route = SearchRoutePlan(
        kind, column, builder._limit,
        len(builder._query_vectors) if kind == "batch_vector" else 1,
        snapshot_id, mode.value,
        has_pre_filter=getattr(builder, "_filter", None) is not None,
        has_partition_filter=builder._partition_filter is not None)
    if hasattr(plan, "snapshot_id"):
        route.indexed_range_rows = route.raw_range_rows = route.overlapping_range_rows = None
        route.notes.extend([
            "Primary-key source files: global row-id coverage is unavailable.",
            "Planning may read data to evaluate scalar predicates."])
    files, scalar_files, indexed_ranges, raw_ranges = {}, set(), [], []
    for split in plan.splits():
        if isinstance(split, (IndexVectorSearchSplit, IndexFullTextSearchSplit)):
            route.index_split_count += 1
            indexed_ranges.append(Range(split.row_range_start, split.row_range_end))
            payloads = (split.vector_index_files if isinstance(split, IndexVectorSearchSplit)
                        else split.full_text_index_files)
            for scalar in getattr(split, "scalar_index_files", ()):
                scalar_files.add((scalar.external_path, scalar.file_name))
        elif isinstance(split, (RawVectorSearchSplit, RawFullTextSearchSplit)):
            route.raw_split_count += 1
            raw_ranges.extend(split.row_ranges)
            for scalar in getattr(split, "scalar_index_files", ()):
                scalar_files.add((scalar.external_path, scalar.file_name))
            continue
        elif hasattr(split, "payloads"):
            route.index_split_count += len(split.payloads)
            payloads = split.payloads
            if mode.value != "fast" and split.uncovered_data_files:
                route.raw_split_count += 1
        else:
            raise TypeError("Unsupported search split: %s" % type(split).__name__)
        for index_file in payloads:
            files[(index_file.external_path, index_file.file_name)] = index_file
    route.index_file_count = len(files)
    route.index_bytes = sum(item.file_size for item in files.values())
    route.index_types = sorted({item.index_type for item in files.values()})
    route.scalar_index_file_count = len(scalar_files)
    if route.indexed_range_rows is not None:
        indexed_ranges = Range.sort_and_merge_overlap(indexed_ranges, True)
        raw_ranges = Range.sort_and_merge_overlap(raw_ranges, True)
        route.indexed_range_rows = sum(r.count() for r in indexed_ranges)
        route.raw_range_rows = sum(r.count() for r in raw_ranges)
        route.overlapping_range_rows = sum(r.count() for r in Range.and_(indexed_ranges, raw_ranges))
    if snapshot_id is None:
        route.notes.append("No snapshot ID was supplied by the search planner.")
    return route
