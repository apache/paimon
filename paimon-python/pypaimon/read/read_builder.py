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

import ast
from typing import Dict, List, Optional, Sequence, Union

import pyarrow

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.explain import ExplainResult, ExplainSplitInfo, PruningStat
from pypaimon.read.explain_render import render_predicate
from pypaimon.read.push_down_utils import predicate_field_names
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.split import Split
from pypaimon.read.table_read import ROW_KIND_COLUMN, TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.read.read_type import OutputProjection, output_fields, project_read_type, reader_adapter
from pypaimon.read.variant_read_type import with_variant_extractions
from pypaimon.schema.data_types import AtomicType, DataField, MapType
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.projection import MapKey, Projection, is_row_type


ProjectionPath = Sequence[Union[int, MapKey]]


def _string_literal(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Str):
        return node.s
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _normalize_sql_literals(expression: str) -> str:
    """Decode SQL doubled quotes before validating the expression with AST."""
    normalized = []
    index = 0
    previous_was_literal = False
    while index < len(expression):
        quote = expression[index]
        if quote not in ("'", '"'):
            normalized.append(quote)
            if not quote.isspace():
                previous_was_literal = False
            index += 1
            continue
        if previous_was_literal:
            raise ValueError("Adjacent string literals are not supported")
        index += 1
        literal = []
        while index < len(expression):
            char = expression[index]
            if char == quote:
                if index + 1 < len(expression) and expression[index + 1] == quote:
                    literal.append(quote)
                    index += 2
                else:
                    index += 1
                    break
            else:
                literal.append(char)
                index += 1
        else:
            raise ValueError("Unterminated string literal in projection expression")
        normalized.append(repr(''.join(literal)))
        previous_was_literal = True
    return ''.join(normalized)


class _ReadPredicateBuilder(PredicateBuilder):

    def __init__(self, fields, unsupported_fields):
        super().__init__(fields)
        self._unsupported_fields = unsupported_fields

    def _get_field_index(self, field: str) -> int:
        if field in self._unsupported_fields:
            raise NotImplementedError(
                "Filtering projected MAP keys is not supported: {}".format(
                    field))
        return super()._get_field_index(field)


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        self._read_type: Optional[List[DataField]] = None
        self._output_projection: Optional[OutputProjection] = None
        self._partition_filter: Optional[Predicate] = None
        self._limit: Optional[int] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_partition_filter(self, partition_filter: Predicate) -> 'ReadBuilder':
        self._partition_filter = partition_filter
        return self

    def with_projection(
        self,
        projection: Union[List[str], Dict[str, str]],
    ) -> 'ReadBuilder':
        """Project columns, nested ROW fields, MAP values or VARIANT expressions.

        Lists retain column, nested ROW and MAP-key projection semantics.
        A mapping assigns output names to source columns or float32
        ``variant_get`` / ``try_variant_get`` expressions. Variant
        expressions require native reading.
        """
        if isinstance(projection, dict):
            name_paths, variants, outputs = self._parse_expression_projection(projection)
            output = OutputProjection(outputs, True)
        else:
            fields = self._table_read_fields()
            indexes = self._resolve_projection_paths(projection) if projection else []
            if projection is None:
                return self.with_read_type(self.table.fields)
            name_paths = Projection.of(indexes).to_name_paths(fields)
            output_indexes = (indexes if any(len(path) > 1 for path in indexes)
                              else [path[0] for path in indexes])
            flat_fields = Projection.of(output_indexes).project(fields)
            output = OutputProjection([(field.name, path) for field, path in zip(flat_fields, name_paths)])
            variants = None
        read_type = project_read_type(self._table_read_fields(), name_paths)
        self.with_read_type(with_variant_extractions(read_type, variants))
        self._output_projection = output
        return self

    def with_read_type(self, read_type: List[DataField]) -> 'ReadBuilder':
        """Set one complete reader request, resetting the result projection."""
        self._read_type = read_type
        self._output_projection = None
        return self

    def _table_read_fields(self):
        fields = self.table.fields
        return (SpecialFields.row_type_with_row_tracking(fields)
                if self.table.options.row_tracking_enabled() else fields)

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def new_scan(self) -> TableScan:
        self._validate_map_key_filter()
        scan = TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit,
            partition_predicate=self._partition_filter,
        )
        scan._read_type = self.read_type()
        return scan

    def new_read(self) -> TableRead:
        self._validate_map_key_filter()
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            output_projection=self._output_projection,
            limit=self._limit,
        )

    def _nested_name_paths(self):
        """Derive format-adapter paths; the read type remains authoritative."""
        _, paths = reader_adapter(self.read_type(), self._table_read_fields())
        return paths if any(len(path) > 1 for path in paths) else None

    def _output_name_paths(self):
        paths = ([path for _, path in self._output_projection.columns]
                 if self._output_projection is not None else [[field.name] for field in self.read_type()])
        return paths if any(len(path) > 1 for path in paths) else None

    def new_predicate_builder(self) -> PredicateBuilder:
        # List projections expose flat leaf names to the Python predicate
        # evaluator. Derive that view without changing the reader request.
        fields = self.read_type()
        if self._output_projection is not None and not self._output_projection.named:
            fields = reader_adapter(fields, self._table_read_fields())[0]
        return _ReadPredicateBuilder(
            fields, self._map_key_output_names())

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
            projection=([alias for alias, _ in self._output_projection.columns]
                        if self._output_projection else None),
            limit=self._limit,
            verbose=verbose,
        )

    def read_type(self) -> List[DataField]:
        """Return the structured reader request, preserving source field IDs.

        Nested ROWs are pruned in place. Selected MAP keys and VARIANT
        extractions use field-description metadata, as in Java. Result aliases
        and flat list-projection outputs are applied separately by TableRead.
        """
        return self._read_type if self._read_type is not None else self.table.fields

    def _output_fields(self):
        return output_fields(self.read_type(), self._output_projection)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_expression_projection(self, expressions: Dict[str, str]):
        if not expressions:
            raise ValueError("Projection expression mapping must not be empty")
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        projection = []
        variants = {}
        outputs = []
        direct_columns = set()
        for alias, expression in expressions.items():
            if not isinstance(alias, str) or not alias:
                raise TypeError("Projection output names must be non-empty strings")
            if alias == ROW_KIND_COLUMN:
                raise ValueError("Projection output name %r is reserved" % alias)
            if not isinstance(expression, str) or not expression:
                raise TypeError("Projection expressions must be non-empty strings")
            paths = self._resolve_projection_paths([expression])
            if paths:
                path = Projection.of(paths).to_name_paths(table_fields)[0]
                source, child = path[0], None
                direct_columns.add(tuple(path))
            else:
                try:
                    call = ast.parse(
                        _normalize_sql_literals(expression), mode='eval').body
                except SyntaxError as error:
                    raise ValueError(
                        "Unsupported projection expression %r" % expression
                    ) from error
                if (not isinstance(call, ast.Call)
                        or not isinstance(call.func, ast.Name)
                        or call.func.id.lower() not in ('variant_get', 'try_variant_get')
                        or len(call.args) != 3 or call.keywords
                        or not (isinstance(call.args[0], ast.Name)
                                or isinstance(call.args[0], ast.Attribute)
                                or _string_literal(call.args[0]) is not None)
                        or any(_string_literal(arg) is None
                               for arg in call.args[1:])):
                    raise ValueError(
                        "Unsupported projection expression %r" % expression)
                source = _source_expression(call.args[0])
                source_indexes = self._resolve_projection_paths([source]) if source else []
                source_paths = Projection.of(source_indexes).to_name_paths(table_fields)
                reader_path = source_paths[0] if source_paths else []
                field = _field_at_path(table_fields, reader_path)
                if (field is None or not isinstance(field.type, AtomicType)
                        or field.type.type.upper() != 'VARIANT'):
                    raise ValueError("Variant extraction requires a VARIANT column: %r" % source)
                path, target_type = (_string_literal(arg) for arg in call.args[1:])
                if ';' in path:
                    raise ValueError(
                        "Variant extraction path must not contain ';': %s"
                        % path)
                if target_type.lower() != 'float':
                    raise ValueError(
                        "Only float32 Variant extractions are supported")
                options = variants.setdefault(tuple(reader_path), {
                    'paths': [], 'target_type': pyarrow.float32(),
                    'fail_on_error': [],
                })
                child = len(options['paths'])
                options['paths'].append(path)
                options['fail_on_error'].append(
                    call.func.id.lower() == 'variant_get')
                path = reader_path + [str(child)]
            if child is None:
                reader_path = path
            if reader_path not in projection:
                projection.append(reader_path)
            outputs.append((alias, path))
        if any(variant[:len(direct)] == direct for direct in direct_columns for variant in variants):
            raise ValueError(
                "A VARIANT column cannot be read both whole and extracted")
        return projection, variants or None, outputs

    def _resolve_projection_paths(self, names: List[str]) -> List[ProjectionPath]:
        """Translate ROW paths and MAP-key selectors into internal paths."""
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        top_index = {f.name: i for i, f in enumerate(table_fields)}

        paths: List[ProjectionPath] = []
        for name in names:
            # Dot can be part of a top-level field name, not only a struct path
            # separator. Top-level match takes precedence over struct walk.
            if name in top_index:
                paths.append([top_index[name]])
                continue

            map_selector = _map_key_selector(name, table_fields)
            if map_selector is not None:
                map_path, key = map_selector
                paths.append(map_path + [MapKey(key)])
                continue

            if "." not in name:
                continue

            # Preserve the original ROW-path semantics before considering a
            # dotted top-level field name as the path prefix.
            parts = name.split(".")
            top = parts[0]
            if top in top_index:
                path = _resolve_row_path(table_fields, top_index[top], parts[1:])
                if path is not None:
                    paths.append(path)
                    continue

            candidates = [
                field_name
                for field_name in top_index
                if name.startswith(field_name + ".")
                and is_row_type(table_fields[top_index[field_name]].type)
            ]
            if not candidates:
                continue
            top = max(candidates, key=len)
            prefix_length = len(top) + 1
            parts = name[prefix_length:].split(".")
            path = _resolve_row_path(table_fields, top_index[top], parts)
            if path is not None:
                paths.append(path)
        return paths

    def _validate_map_key_filter(self):
        if self._predicate is None:
            return
        unsupported = predicate_field_names(self._predicate) & self._map_key_output_names()
        if unsupported:
            raise NotImplementedError("Filtering projected MAP keys is not supported: {}".format(sorted(unsupported)))

    def _map_key_output_names(self):
        if self._output_projection is None:
            return set()
        fields = {field.name: field for field in self._table_read_fields()}
        return {alias for alias, path in self._output_projection.columns
                if any(isinstance(_field_at_path(list(fields.values()), path[:i]).type, MapType)
                       for i in range(1, len(path)))}


def _resolve_row_path(
    table_fields: List[DataField], top_index: int, parts: List[str]
) -> Optional[List[int]]:
    """Walk ROW children from a top-level field; return None for invalid paths."""
    path = [top_index]
    current_field = table_fields[top_index]
    for part in parts:
        if not is_row_type(current_field.type):
            return None
        child_fields = current_field.type.fields
        child_idx = next((i for i, f in enumerate(child_fields) if f.name == part), -1)
        if child_idx < 0:
            return None
        path.append(child_idx)
        current_field = child_fields[child_idx]
    return path


def _field_at_path(fields, path):
    field = None
    for name in path:
        field = next((field for field in fields if field.name == name), None)
        if field is None:
            return None
        fields = field.type.fields if is_row_type(field.type) else []
    return field


def _source_expression(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _source_expression(node.value)
        return parent + '.' + node.attr if parent else None
    return _string_literal(node)


def _map_key_selector(name, table_fields):
    if not name.endswith("]"):
        return None
    candidates = []

    def visit(fields, prefix, indexes):
        for index, field in enumerate(fields):
            full = prefix + field.name
            path = indexes + [index]
            if _is_string_key_map(field.type) and name.startswith(full + "["):
                candidates.append((full, path))
            elif is_row_type(field.type):
                visit(field.type.fields, full + '.', path)
    visit(table_fields, '', [])
    for full, path in sorted(candidates, key=lambda pair: len(pair[0]), reverse=True):
        try:
            key = ast.literal_eval(name[len(full) + 1:-1])
        except (SyntaxError, ValueError):
            continue
        if isinstance(key, str):
            return path, key
    return None


def _is_string_key_map(data_type) -> bool:
    return (
        isinstance(data_type, MapType)
        and isinstance(data_type.key, AtomicType)
        and data_type.key.type.upper() == 'STRING'
    )


def _build_explain_result(table, scan: TableScan, plan, stats: Optional[ScanStats],
                          predicate, projection, limit, verbose: bool) -> ExplainResult:
    """Translate one (Plan, ScanStats) pair into an ExplainResult."""
    splits: List[Split] = plan.splits()

    table_schema = table.table_schema
    bucket_mode_str = _safe_bucket_mode(table)

    # Native plans expose split metadata without Python pruning counters.
    native_planned = stats is None
    if native_planned:
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
