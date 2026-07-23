#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Range-aligned join on Ray for two Paimon tables sorted/clustered by the join key.

The driver cuts the key space into ranges from each file's parquet-footer min/max (the
manifest value stats are empty for pypaimon-written tables); each range is read and
joined in its own Ray task, with no global shuffle. Only beneficial when both sides are
clustered by the first join key: a poorly clustered (wide) split overlaps many ranges and
is read in each, so on unclustered input this can cost more than a shuffle.

Correctness never depends on stats: a split whose min/max is missing (or is wide) joins
every range it overlaps and is clipped in memory. The range count is reduced until that
total re-read stays within a couple of full scans (all-unknown collapses to one range).
"""

from typing import Any, Dict, List, Optional

from pypaimon.ray.join_common import (
    OnSpec,
    get_table,
    key_type,
    norm_on,
    pin_latest_snapshot,
    read_splits,
)

__all__ = ["range_join"]

_MAX_RANGES = 512
# Cap total re-read to this many full scans (see _bounded_ranges).
_REREAD_BUDGET = 2


def _parquet_col_range(metadata, col):
    """Min/max of ``col`` across a parquet file's row groups; None when a row group
    lacks usable stats for ``col``."""
    lo, hi = None, None
    for i in range(metadata.num_row_groups):
        rg = metadata.row_group(i)
        stats = None
        for j in range(rg.num_columns):
            if rg.column(j).path_in_schema == col:
                stats = rg.column(j).statistics
                break
        if stats is None or not stats.has_min_max:
            return None
        lo = stats.min if lo is None else min(lo, stats.min)
        hi = stats.max if hi is None else max(hi, stats.max)
    return None if lo is None else (lo, hi)


def _footer_col_type(metadata, col):
    """The arrow type ``col`` is stored as in this parquet file; None if unavailable."""
    try:
        return metadata.schema.to_arrow_schema().field(col).type
    except Exception:
        return None


def _split_key_range(split, col, key_type, file_io):
    """Min/max of ``col`` over the split's files, read from their parquet footers;
    (None, None) when any file isn't parquet, lacks usable stats, or stored the key in a
    different type than the current one (the split then joins every range). A different
    type means schema evolution, and its footer bounds order differently (e.g. INT '10'
    < '2' as a string), so they can't be trusted -- only same-type stats are used.
    Manifest value stats are empty for pypaimon-written tables, so the footer is the
    source of truth."""
    import pyarrow.parquet as pq
    lo, hi = None, None
    for f in split.files:
        path = f.external_path if f.external_path else f.file_path
        if path is None or not path.endswith(".parquet"):
            return None, None
        stream = file_io.new_input_stream(path)
        try:
            metadata = pq.read_metadata(stream)
        except Exception:
            return None, None
        finally:
            stream.close()
        if _footer_col_type(metadata, col) != key_type:
            return None, None
        rng = _parquet_col_range(metadata, col)
        if rng is None:
            return None, None
        lo = rng[0] if lo is None else min(lo, rng[0])
        hi = rng[1] if hi is None else max(hi, rng[1])
    if lo is None:
        return None, None
    return lo, hi


def _plan_ranged_splits(table_id, catalog_options, projection, range_col, partitions=None):
    """Plan driver-side; returns ``(ranged_splits, schema_id)`` where ranged_splits is
    a list of (split, lo, hi). Ranges come from each file's parquet footer (read in
    parallel); the splits sent to workers carry no stats. ``partitions`` (a {column:
    value} dict on partition columns) prunes to those partitions before planning."""
    import os
    from concurrent.futures import ThreadPoolExecutor
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.schema.data_types import PyarrowFieldParser
    table = get_table(table_id, catalog_options, None, "range_join")
    schema_id = table.table_schema.id
    if pin_latest_snapshot(table) is None:
        return [], schema_id
    file_io = table.file_io
    key_type = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields).field(range_col).type
    rb = table.new_read_builder()
    if partitions:
        # Build the partition predicate before projection, so its field list still has
        # the partition columns.
        pb = rb.new_predicate_builder()
        rb = rb.with_partition_filter(
            PredicateBuilder.and_predicates([pb.equal(c, v) for c, v in partitions.items()]))
    if projection is not None:
        rb = rb.with_projection(projection)
    splits = list(rb.new_scan().plan().splits())
    workers = min(16, (os.cpu_count() or 4) * 4, len(splits) or 1)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        bounds = pool.map(
            lambda s: _split_key_range(s, range_col, key_type, file_io), splits)
    return [(s, lo, hi) for s, (lo, hi) in zip(splits, bounds)], schema_id


def _cut_points(ranged_sides, num_ranges):
    """Pick ``num_ranges - 1`` cut values from row-count-weighted file boundaries."""
    points = []
    for ranged in ranged_sides:
        for split, lo, hi in ranged:
            if lo is None:
                continue
            rows = sum(f.row_count for f in split.files)
            points.append((lo, rows / 2.0))
            points.append((hi, rows / 2.0))
    if not points:
        return []
    points.sort(key=lambda p: p[0])
    total = sum(w for _, w in points)
    cuts, acc, k = [], 0.0, 1
    for value, weight in points:
        acc += weight
        if k >= num_ranges:
            break
        if acc >= total * k / num_ranges:
            if not cuts or value > cuts[-1]:  # strictly increasing
                cuts.append(value)
                k += 1
    return cuts


def _split_rows(split):
    return sum(f.row_count for f in split.files)


def _total_reads(l_ranged, r_ranged, ranges):
    """Rows physically read = each split's rows times the ranges it overlaps (each range
    reads the whole split and clips). Counts unknown-stats and wide known splits alike."""
    reads = 0
    for ranged in (l_ranged, r_ranged):
        for split, lo, hi in ranged:
            spans = sum(1 for r_lo, r_hi in ranges if _overlaps(lo, hi, r_lo, r_hi))
            reads += _split_rows(split) * spans
    return reads


def _bounded_ranges(l_ranged, r_ranged, num_ranges):
    """Cut into ``num_ranges`` ranges, halving until total re-read <= _REREAD_BUDGET full
    scans, so poorly clustered input can't cost far more than one scan."""
    total_rows = sum(_split_rows(s)
                     for ranged in (l_ranged, r_ranged) for s, _, _ in ranged)
    budget = _REREAD_BUDGET * max(1, total_rows)
    while True:
        ranges = _ranges_from_cuts(_cut_points((l_ranged, r_ranged), num_ranges))
        if num_ranges <= 1 or _total_reads(l_ranged, r_ranged, ranges) <= budget:
            return ranges
        num_ranges = max(1, num_ranges // 2)


def _ranges_from_cuts(cuts):
    # Half-open [lo, hi); None = unbounded end.
    bounds = [None] + cuts + [None]
    return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]


def _overlaps(lo, hi, r_lo, r_hi):
    if lo is None:  # unknown split range: belongs to every range
        return True
    return (r_lo is None or hi >= r_lo) and (r_hi is None or lo < r_hi)


def _restrict_to_range(arrow_table, col, lo, hi):
    """Keep rows with ``lo <= col < hi``. Null keys are always dropped (an inner
    join never matches them), which also keeps the result independent of num_ranges."""
    import pyarrow.compute as pc
    mask = pc.is_valid(arrow_table[col])
    if lo is not None:
        mask = pc.and_(mask, pc.greater_equal(arrow_table[col], lo))
    if hi is not None:
        mask = pc.and_(mask, pc.less(arrow_table[col], hi))
    return arrow_table.filter(mask)


def range_join(
    left: str,
    right: str,
    catalog_options: Dict[str, str],
    *,
    on: Optional[OnSpec] = None,
    left_on: Optional[OnSpec] = None,
    right_on: Optional[OnSpec] = None,
    num_ranges: Optional[int] = None,
    left_projection: Optional[List[str]] = None,
    right_projection: Optional[List[str]] = None,
    left_partitions: Optional[Dict[str, Any]] = None,
    right_partitions: Optional[Dict[str, Any]] = None,
    join_type: str = "inner",
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> "ray.data.Dataset":
    """Join two tables clustered by the first join key with no global shuffle.

    ``on`` when both sides use the same column names, or ``left_on``/``right_on``
    when they differ (positionally paired). The first pair is the range key used to
    cut the key space. ``left_partitions``/``right_partitions`` ({column: value} dicts
    on partition columns) prune each side to those partitions first. Sides must not
    share column names other than ``on`` keys. Returns a ``ray.data.Dataset``.
    """
    import ray

    if not hasattr(ray.data, "from_arrow_refs"):
        raise RuntimeError(
            "range_join needs a Ray version with ray.data.from_arrow_refs; "
            f"installed ray is {ray.__version__}.")

    if (on is None) == (left_on is None and right_on is None):
        raise ValueError("range_join requires exactly one of on= or left_on=/right_on=.")
    if on is not None:
        lkeys = rkeys = norm_on(on)
    else:
        if left_on is None or right_on is None:
            raise ValueError("range_join requires both left_on= and right_on=.")
        lkeys, rkeys = norm_on(left_on), norm_on(right_on)
    if len(lkeys) != len(rkeys) or not lkeys:
        raise ValueError(
            f"range_join join keys must pair up non-empty; got left_on={lkeys}, right_on={rkeys}.")
    if join_type != "inner":
        # Outer joins would need every unmatched row emitted exactly once across
        # ranges plus null-key handling; only inner is supported for now.
        raise ValueError(f"range_join currently supports only join_type='inner'; got {join_type!r}.")

    ltable = get_table(left, catalog_options, None, "range_join")
    rtable = get_table(right, catalog_options, None, "range_join")

    missing = [c for c in lkeys if c not in ltable.field_dict] \
        + [c for c in rkeys if c not in rtable.field_dict]
    if missing:
        raise ValueError(f"range_join keys not found in table schema: {missing}.")
    type_mismatch = [
        (lc, rc, key_type(ltable, lc), key_type(rtable, rc))
        for lc, rc in zip(lkeys, rkeys)
        if key_type(ltable, lc) != key_type(rtable, rc)
    ]
    if type_mismatch:
        raise ValueError(
            "range_join key columns must have the same type on both sides; "
            f"mismatched (left, right, left type, right type): {type_mismatch}.")

    # The range key is the first pair; reject types that can't be range-partitioned safely.
    range_key_type = key_type(ltable, lkeys[0]).upper()
    if range_key_type.startswith("FLOAT") or range_key_type.startswith("DOUBLE"):
        # NaN compares false to every bound, so it would fall out of every range while
        # pyarrow's hash join still matches NaN == NaN -> silently dropped matches.
        raise ValueError(
            f"range_join range key {lkeys[0]!r} must not be FLOAT/DOUBLE (NaN can't be "
            "range-partitioned); use an integer/string/date key.")
    if "LOCAL TIME ZONE" in range_key_type:
        # Manifest stats decode to naive datetimes; a tz-aware Arrow column can't be
        # compared against them. Not supported yet.
        raise ValueError(
            f"range_join range key {lkeys[0]!r} of type TIMESTAMP WITH LOCAL TIME ZONE "
            "is not supported yet.")

    # The join keys must survive projection, or the local join has no key.
    if left_projection is not None and not set(lkeys) <= set(left_projection):
        raise ValueError(
            f"left_projection must include the join keys {lkeys}; got {left_projection}.")
    if right_projection is not None and not set(rkeys) <= set(right_projection):
        raise ValueError(
            f"right_projection must include the join keys {rkeys}; got {right_projection}.")
    # pyarrow drops the right keys (coalesced into the left), so the output keeps the LEFT
    # key names. A right non-key column sharing a left column name collides -> reject it.
    lcols = left_projection if left_projection is not None else ltable.field_names
    rcols = right_projection if right_projection is not None else rtable.field_names
    collisions = sorted(set(lcols) & (set(rcols) - set(rkeys)))
    if collisions:
        raise ValueError(
            f"range_join output columns collide: {collisions}. The output keeps the left "
            "key names and the right non-key columns; project or rename the overlap away.")

    l_range_col, r_range_col = lkeys[0], rkeys[0]
    l_ranged, l_schema_id = _plan_ranged_splits(
        left, catalog_options, left_projection, l_range_col, left_partitions)
    r_ranged, r_schema_id = _plan_ranged_splits(
        right, catalog_options, right_projection, r_range_col, right_partitions)

    def _empty():
        empty = read_splits(
            left, catalog_options, left_projection, [], l_schema_id, "range_join").join(
            read_splits(right, catalog_options, right_projection, [], r_schema_id, "range_join"),
            keys=lkeys, right_keys=rkeys, join_type=join_type)
        return ray.data.from_arrow(empty)

    if not l_ranged or not r_ranged:  # inner join: one empty side, empty result
        return _empty()

    if num_ranges is None:
        num_ranges = max(1, min(_MAX_RANGES, max(len(l_ranged), len(r_ranged))))
    elif num_ranges < 1:
        raise ValueError(f"num_ranges must be >= 1; got {num_ranges}.")
    # Reduce ranges until the total re-read (wide known splits + unknown-stats splits,
    # both read in every range they overlap) stays bounded -- see _bounded_ranges.
    ranges = _bounded_ranges(l_ranged, r_ranged, num_ranges)

    def _join_range(left_splits, right_splits, lo, hi):
        # No predicate pushdown: the range key may be schema-evolved (e.g. a file stored
        # as INT read as STRING), which the reader can't compare against a new-type bound.
        # The in-memory clip below does the exact, evolution-safe filtering.
        lt = _restrict_to_range(
            read_splits(left, catalog_options, left_projection, left_splits,
                        l_schema_id, "range_join"),
            l_range_col, lo, hi)
        rt = _restrict_to_range(
            read_splits(right, catalog_options, right_projection, right_splits,
                        r_schema_id, "range_join"),
            r_range_col, lo, hi)
        return lt.join(rt, keys=lkeys, right_keys=rkeys, join_type=join_type)

    # ``@ray.remote()`` (empty parens) is rejected by Ray, so wrap conditionally.
    remote_fn = ray.remote(**ray_remote_args)(_join_range) if ray_remote_args else ray.remote(_join_range)
    refs = []
    for r_lo, r_hi in ranges:
        ls = [s for s, lo, hi in l_ranged if _overlaps(lo, hi, r_lo, r_hi)]
        rs = [s for s, lo, hi in r_ranged if _overlaps(lo, hi, r_lo, r_hi)]
        if not ls or not rs:  # inner join: a one-sided range can't match
            continue
        refs.append(remote_fn.remote(ls, rs, r_lo, r_hi))
    if not refs:
        return _empty()
    # Keep each range's result as a distributed object ref -- never pulled into the driver.
    return ray.data.from_arrow_refs(refs)
