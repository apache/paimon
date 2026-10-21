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

"""Build public explain results from one selected backend's plan and counters."""

from typing import List, Optional

from pypaimon.read.explain import ExplainResult, ExplainSplitInfo, PruningStat
from pypaimon.read.explain_render import render_predicate
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.split import Split
from pypaimon.read.table_scan import TableScan


def explain(builder, verbose: bool = False) -> ExplainResult:
    """Produce a structured scan plan for this builder.

    Runs one planning pass (manifest list + manifest reads, no data
    files) and returns an :class:`ExplainResult` summarising the
    target snapshot, the pushed-down predicate / projection / limit,
    the partition / bucket / file-stats pruning funnel, and split-
    level execution signals (raw-convertible ratio, deletion-vector
    ratio, level histogram, files-per-split and split-size
    distribution). With ``verbose=True``, every split is listed.

    Cost: ``explain()`` reads manifest list + manifests but never
    opens data files. To produce accurate before/after counters it
    suppresses early bucket and sidecar pruning and counts candidates
    before applying LIMIT, so it can be measurably
    heavier than a regular ``new_scan().plan()`` on tables where the
    early filter usually prunes aggressively (e.g. very wide
    HASH_FIXED tables with a tight predicate).
    """
    scan = builder.new_scan()
    plan, stats = scan.scan_with_stats()
    return _build_explain_result(
        table=builder.table,
        scan=scan,
        plan=plan,
        stats=stats,
        predicate=builder._predicate,
        projection=([alias for alias, _ in builder._output_projection.columns]
                    if builder._output_projection else None),
        limit=builder._limit,
        verbose=verbose,
    )


def _build_explain_result(table, scan: TableScan, plan, stats: Optional[ScanStats],
                          predicate, projection, limit, verbose: bool) -> ExplainResult:
    """Translate one (Plan, ScanStats) pair into an ExplainResult."""
    splits: List[Split] = plan.splits()

    table_schema = table.table_schema
    bucket_mode_str = _safe_bucket_mode(table)

    native_planned = stats is None or stats.native_planned
    if stats is None or native_planned:
        partition_pruning = bucket_pruning = file_skipping = None
    else:
        partition_pruning = _partition_pruning(stats, scan)
        bucket_pruning = _bucket_pruning(stats, scan)
        file_skipping = _file_skipping(stats, scan)

    files_per_split = [len(getattr(s, 'files', []) or []) for s in splits]
    sizes = [int(getattr(s, 'file_size', 0) or 0) for s in splits]

    rows_total = sum(int(getattr(s, 'row_count', 0) or 0) for s in splits)
    merged_per_split = [s.merged_row_count() for s in splits]
    if splits and all(v is not None for v in merged_per_split):
        merged_total: Optional[int] = sum(merged_per_split)
    else:
        merged_total = None

    file_count = sum(files_per_split)
    total_size = sum(sizes)
    level_hist: dict = {}
    deletion_file_total = 0
    splits_raw_convertible = 0
    splits_with_dv = 0
    splits_all_above_l0 = 0
    split_infos: List[ExplainSplitInfo] = []

    plan_has_auth = any(isinstance(s, QueryAuthSplit) for s in splits)

    for split in splits:
        files = getattr(split, 'files', []) or []
        per_split_levels: dict = {}
        for f in files:
            lv = getattr(f, 'level', 0) or 0
            level_hist[lv] = level_hist.get(lv, 0) + 1
            per_split_levels[lv] = per_split_levels.get(lv, 0) + 1
        dvs = getattr(split, 'data_deletion_files', None) or []
        dv_count_here = sum(1 for d in dvs if d is not None)
        deletion_file_total += dv_count_here
        has_dv = dv_count_here > 0
        raw = bool(getattr(split, 'raw_convertible', False))
        if raw:
            splits_raw_convertible += 1
        if has_dv:
            splits_with_dv += 1
        if files and all((getattr(f, 'level', 0) or 0) > 0 for f in files):
            splits_all_above_l0 += 1

        if verbose:
            split_infos.append(ExplainSplitInfo(
                partition=_format_partition(split, table),
                bucket=int(getattr(split, 'bucket', -1)),
                file_count=len(files),
                row_count=int(getattr(split, 'row_count', 0) or 0),
                merged_row_count=split.merged_row_count(),
                file_size=int(getattr(split, 'file_size', 0) or 0),
                raw_convertible=raw,
                has_deletion_vectors=has_dv,
                level_histogram=per_split_levels,
                deletion_file_count=dv_count_here,
                file_paths=list(getattr(split, 'file_paths', []) or []),
                data_files=list(files),
            ))

    fps_min, fps_max, fps_avg = _min_max_avg(files_per_split)
    sz_min, sz_max, sz_avg = _min_max_avg(sizes)
    sz_p50 = _percentile(sizes, 50)
    sz_p95 = _percentile(sizes, 95)

    return ExplainResult(
        table_identifier=str(table.identifier.get_full_name()),
        is_primary_key_table=bool(table.is_primary_key_table),
        bucket_mode=bucket_mode_str,
        deletion_vectors_enabled=bool(table.options.deletion_vectors_enabled()),
        data_evolution_enabled=bool(table.options.data_evolution_enabled()),
        snapshot_id=plan.snapshot_id,
        schema_id=table_schema.id if plan.snapshot_id is not None else None,
        predicate=render_predicate(predicate) if predicate is not None else None,
        projection=list(projection) if projection else None,
        limit=limit,
        partition_pruning=partition_pruning,
        bucket_pruning=bucket_pruning,
        file_skipping=file_skipping,
        file_count=file_count,
        total_file_size=total_size,
        estimated_row_count=rows_total,
        estimated_merged_row_count=merged_total,
        deletion_file_count=deletion_file_total,
        level_histogram=level_hist,
        split_count=len(splits),
        splits_raw_convertible=splits_raw_convertible,
        splits_with_deletion_vectors=splits_with_dv,
        splits_all_above_l0=splits_all_above_l0,
        files_per_split_min=fps_min,
        files_per_split_max=fps_max,
        files_per_split_avg=fps_avg,
        split_size_min=sz_min,
        split_size_max=sz_max,
        split_size_avg=sz_avg,
        split_size_p50=sz_p50,
        split_size_p95=sz_p95,
        has_auth=plan_has_auth,
        native_planned=native_planned,
        planning_counters=stats.planning_counters if stats is not None else None,
        splits=split_infos if verbose else None,
    )


def _partition_pruning(stats: ScanStats, scan: TableScan) -> Optional[PruningStat]:
    if scan.predicate is None:
        return None
    table_partition_keys = scan.table.partition_keys or []
    if not table_partition_keys:
        return None
    # ``entries_potential_total`` is the count from manifest-file metadata
    # (manifest-level pruning has not been applied yet). The "after" side
    # is everything that survived both manifest-stats and per-entry
    # partition filters.
    return PruningStat(
        before=stats.entries_potential_total,
        after=stats.entries_after_partition,
    )


def _bucket_pruning(stats: ScanStats, scan: TableScan) -> Optional[PruningStat]:
    # Visible whenever the scan applies any bucket-level filtering — the
    # HASH_FIXED predicate-driven selector OR the POSTPONE_BUCKET
    # synthetic-bucket skip. Tables with neither (e.g. BUCKET_UNAWARE
    # append) leave this counter as ``None``.
    fs = scan.file_scanner
    if fs._bucket_selector is None and not fs.only_read_real_buckets:
        return None
    return PruningStat(before=stats.entries_after_partition, after=stats.entries_after_bucket)


def _file_skipping(stats: ScanStats, scan: TableScan) -> Optional[PruningStat]:
    # Captures the funnel between bucket-stage survivors and the entries
    # that actually feed the split generator. The drop here includes both
    # predicate-driven file-stats pruning AND structural skips that fire
    # in ``_filter_manifest_entry`` once a file is fully decoded (most
    # notably the "do not read level-0 file" rule for DV-enabled PK
    # tables, which is an LSM-shape decision rather than a predicate
    # test).
    if scan.predicate is None:
        return None
    return PruningStat(before=stats.entries_after_bucket, after=stats.entries_after_stats)


def _safe_bucket_mode(table) -> str:
    try:
        return table.bucket_mode().name
    except Exception:
        return "UNKNOWN"


def _format_partition(split, table) -> dict:
    keys = list(table.partition_keys or [])
    partition = getattr(split, 'partition', None)
    if partition is None or not keys:
        return {}
    values = getattr(partition, 'values', None) or []
    return {k: v for k, v in zip(keys, values)}


def _min_max_avg(values):
    if not values:
        return 0, 0, 0.0
    return min(values), max(values), sum(values) / float(len(values))


def _percentile(values, pct: int) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = int(round((pct / 100.0) * (len(ordered) - 1)))
    return int(ordered[idx])
