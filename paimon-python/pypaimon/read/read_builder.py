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

from typing import List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.explain import ExplainResult, ExplainSplitInfo, PruningStat
from pypaimon.read.explain_render import render_predicate
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.projection import Projection, is_row_type


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        # ``_projection`` stores the user-facing name list from
        # :meth:`with_projection`. When dotted names are present,
        # ``_nested_paths`` is also populated and takes precedence
        # in ``read_type()`` and downstream consumers.
        self._projection: Optional[List[str]] = None
        self._nested_paths: Optional[List[List[int]]] = None
        self._limit: Optional[int] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """Project to the given column names.

        Names containing a dot (e.g. ``"struct.subfield"``) walk into ROW
        children and are translated into a nested projection. Top-level-
        only callers see the same observable behaviour as before — the
        dotted form is opt-in. Unknown names are silently skipped to
        preserve the pre-existing contract.

        Precedence: if a dotted name matches an actual top-level field, the
        top-level match wins and the name is not walked as a struct path.
        """
        self._projection = projection
        if projection and any('.' in name for name in projection):
            self._nested_paths = self._resolve_dotted_paths(projection)
        else:
            self._nested_paths = None
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def new_scan(self) -> TableScan:
        return TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit
        )

    def new_read(self) -> TableRead:
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            nested_name_paths=self._nested_name_paths(),
            limit=self._limit,
        )

    def _nested_name_paths(self) -> Optional[List[List[str]]]:
        """Resolve the current nested-projection state into a parallel list
        of name paths against the underlying table schema. Returns ``None``
        if the user only requested top-level projection (or no projection).
        """
        if not self._nested_paths:
            return None
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        return Projection.of(self._nested_paths).to_name_paths(table_fields)

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def explain(self, verbose: bool = False) -> ExplainResult:
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
        suppresses the manifest-reader's early bucket filter and forces
        single-threaded manifest decoding, so it can be measurably
        heavier than a regular ``new_scan().plan()`` on tables where the
        early filter usually prunes aggressively (e.g. very wide
        HASH_FIXED tables with a tight predicate).
        """
        scan = self.new_scan()
        plan, stats = scan.scan_with_stats()
        return _build_explain_result(
            table=self.table,
            scan=scan,
            plan=plan,
            stats=stats,
            predicate=self._predicate,
            projection=self._projection,
            limit=self._limit,
            verbose=verbose,
        )

    def read_type(self) -> List[DataField]:
        table_fields = self.table.fields

        if not self._projection and not self._nested_paths:
            return table_fields

        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)

        if self._nested_paths:
            return Projection.of(self._nested_paths).project(table_fields)

        field_map = {field.name: field for field in table_fields}
        return [field_map[name] for name in self._projection if name in field_map]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_dotted_paths(self, names: List[str]) -> List[List[int]]:
        """Translate dotted-name projection entries into integer paths
        against the current table schema. Names without dots produce
        length-1 paths.
        """
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        top_index = {f.name: i for i, f in enumerate(table_fields)}

        paths: List[List[int]] = []
        for name in names:
            # Dot can be part of a top-level field name, not only a struct path
            # separator. Top-level match takes precedence over struct walk.
            if name in top_index:
                paths.append([top_index[name]])
                continue
            if '.' not in name:
                continue
            parts = name.split('.')
            top = parts[0]
            if top not in top_index:
                continue
            path = [top_index[top]]
            current_field = table_fields[path[0]]
            ok = True
            for part in parts[1:]:
                if not is_row_type(current_field.type):
                    ok = False
                    break
                child_fields = current_field.type.fields
                child_idx = next(
                    (i for i, f in enumerate(child_fields) if f.name == part),
                    -1)
                if child_idx < 0:
                    ok = False
                    break
                path.append(child_idx)
                current_field = child_fields[child_idx]
            if ok:
                paths.append(path)
        return paths


def _build_explain_result(table, scan: TableScan, plan, stats: ScanStats,
                          predicate, projection, limit, verbose: bool) -> ExplainResult:
    """Translate one (Plan, ScanStats) pair into an ExplainResult."""
    splits: List[Split] = plan.splits()

    table_schema = table.table_schema
    bucket_mode_str = _safe_bucket_mode(table)

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
