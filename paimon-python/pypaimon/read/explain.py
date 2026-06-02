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

import io
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PruningStat:
    """Before / after counters for one pruning stage.

    ``before`` is the input size to the stage, ``after`` is the size that
    survived. Either may be ``None`` when the stage did not run (for
    example, ``bucket_pruning`` is ``None`` for tables that are not
    HASH_FIXED with all bucket keys pinned).
    """

    before: Optional[int]
    after: Optional[int]

    @property
    def pruned(self) -> Optional[int]:
        if self.before is None or self.after is None:
            return None
        return self.before - self.after

    def format(self) -> str:
        if self.before is None and self.after is None:
            return "n/a"
        before = "?" if self.before is None else str(self.before)
        after = "?" if self.after is None else str(self.after)
        pruned = self.pruned
        suffix = "" if pruned is None else "  (pruned {})".format(pruned)
        return "{} -> {}{}".format(before, after, suffix)


@dataclass
class ExplainSplitInfo:
    """Per-split detail surfaced when ``explain(verbose=True)`` is used."""

    partition: Dict[str, Any]
    bucket: int
    file_count: int
    row_count: int
    merged_row_count: Optional[int]
    file_size: int
    raw_convertible: bool
    has_deletion_vectors: bool
    level_histogram: Dict[int, int]
    deletion_file_count: int
    file_paths: List[str]


@dataclass
class ExplainResult:
    """Structured scan plan returned by ``ReadBuilder.explain()``.

    The compact ``__str__`` shows enough signal to reason about cost: the
    snapshot, the pushed-down predicate / projection / limit, the three
    pruning before-after counters, split-level shape (raw-convertible
    ratio, DV ratio, all-above-L0 ratio, files/split, size/split
    distribution), and the file-level totals. ``verbose=True`` adds a
    block listing each split.
    """

    # Identity
    table_identifier: str
    is_primary_key_table: bool
    bucket_mode: str
    deletion_vectors_enabled: bool
    data_evolution_enabled: bool

    # Snapshot
    snapshot_id: Optional[int]
    schema_id: Optional[int]

    # Pushdown
    predicate: Optional[str] = None
    projection: Optional[List[str]] = None
    limit: Optional[int] = None

    # Pruning (None when not applicable)
    partition_pruning: Optional[PruningStat] = None
    bucket_pruning: Optional[PruningStat] = None
    file_skipping: Optional[PruningStat] = None

    # File-level aggregates over final splits
    file_count: int = 0
    total_file_size: int = 0
    estimated_row_count: int = 0
    estimated_merged_row_count: Optional[int] = None
    deletion_file_count: int = 0
    level_histogram: Dict[int, int] = field(default_factory=dict)

    # Split-level aggregates (shown in compact mode too)
    split_count: int = 0
    splits_raw_convertible: int = 0
    splits_with_deletion_vectors: int = 0
    splits_all_above_l0: int = 0
    files_per_split_min: int = 0
    files_per_split_max: int = 0
    files_per_split_avg: float = 0.0
    split_size_min: int = 0
    split_size_max: int = 0
    split_size_avg: float = 0.0
    split_size_p50: int = 0
    split_size_p95: int = 0

    # Verbose-only
    splits: Optional[List[ExplainSplitInfo]] = None

    def __str__(self) -> str:
        return render_explain(self)


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

def render_explain(result: ExplainResult) -> str:
    out = io.StringIO()
    out.write("== PyPaimon Scan Plan ==\n")

    flags = []
    flags.append("PK" if result.is_primary_key_table else "Append")
    flags.append(result.bucket_mode)
    if result.deletion_vectors_enabled:
        flags.append("dv=on")
    if result.data_evolution_enabled:
        flags.append("data-evolution=on")
    _line(out, "Table", "{} ({})".format(result.table_identifier, ", ".join(flags)))

    if result.snapshot_id is None:
        _line(out, "Snapshot", "<none>  (table is empty or has no snapshot)")
    else:
        schema_part = "" if result.schema_id is None else "  (schema {})".format(result.schema_id)
        _line(out, "Snapshot", "{}{}".format(result.snapshot_id, schema_part))

    _line(out, "Predicate", result.predicate if result.predicate else "<none>")
    _line(out, "Projection",
          "[{}]".format(", ".join(result.projection)) if result.projection else "<all columns>")
    _line(out, "Limit", str(result.limit) if result.limit is not None else "<none>")

    out.write("\n")
    _line(out, "Partition pruning",
          result.partition_pruning.format() if result.partition_pruning else "n/a")
    _line(out, "Bucket pruning",
          result.bucket_pruning.format() if result.bucket_pruning else "n/a")
    _line(out, "File skipping",
          result.file_skipping.format() if result.file_skipping else "n/a")

    out.write("\n")
    _line(out, "Splits", str(result.split_count))
    if result.split_count > 0:
        out.write("  raw-convertible:  {} / {}\n".format(
            result.splits_raw_convertible, result.split_count))
        out.write("  with DV:          {} / {}\n".format(
            result.splits_with_deletion_vectors, result.split_count))
        out.write("  all-above-L0:     {} / {}\n".format(
            result.splits_all_above_l0, result.split_count))
        out.write("  files/split:      min={}  max={}  avg={:.2f}\n".format(
            result.files_per_split_min,
            result.files_per_split_max,
            result.files_per_split_avg))
        out.write("  size/split:       min={}  p50={}  p95={}  max={}\n".format(
            _format_size(result.split_size_min),
            _format_size(result.split_size_p50),
            _format_size(result.split_size_p95),
            _format_size(result.split_size_max)))

    out.write("\n")
    _line(out, "Files", str(result.file_count))
    _line(out, "Total size", _format_size(result.total_file_size))

    rows = "{:,}".format(result.estimated_row_count)
    if result.estimated_merged_row_count is not None:
        rows += "   (merged: {:,})".format(result.estimated_merged_row_count)
    _line(out, "Estimated rows", rows)

    if result.level_histogram:
        levels = sorted(result.level_histogram.items())
        levels_str = "  ".join("L{}={}".format(lv, cnt) for lv, cnt in levels)
        _line(out, "Level histogram", levels_str)
    _line(out, "Deletion files", str(result.deletion_file_count))

    if result.splits:
        out.write("\nSplits[]\n")
        for idx, split in enumerate(result.splits):
            out.write("  [{}] partition={} bucket={} files={} size={} rows={} raw={} dv={}\n".format(
                idx,
                split.partition,
                split.bucket,
                split.file_count,
                _format_size(split.file_size),
                split.row_count,
                split.raw_convertible,
                split.has_deletion_vectors,
            ))
            if split.level_histogram:
                levels = sorted(split.level_histogram.items())
                out.write("      levels: {}\n".format(
                    "  ".join("L{}={}".format(lv, cnt) for lv, cnt in levels)))
            for path in split.file_paths:
                out.write("      file: {}\n".format(path))

    return out.getvalue().rstrip("\n")


def _line(out: io.StringIO, label: str, value: str) -> None:
    out.write("{:<19} {}\n".format(label + ":", value))


_SIZE_UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")


def _format_size(num_bytes: int) -> str:
    if num_bytes is None:
        return "?"
    size = float(num_bytes)
    for unit in _SIZE_UNITS:
        if size < 1024.0 or unit == _SIZE_UNITS[-1]:
            if unit == "B":
                return "{:d} {}".format(int(size), unit)
            return "{:.1f} {}".format(size, unit)
        size /= 1024.0
    return "{:.1f} {}".format(size, _SIZE_UNITS[-1])
