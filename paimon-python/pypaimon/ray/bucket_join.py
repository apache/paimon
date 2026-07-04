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

"""Bucket-aligned join on Ray for two co-bucketed Paimon tables.

Same key -> same bucket on both sides, so each bucket is read and joined in its own
Ray task with no global shuffle -- the no-shuffle alternative to ``ray.data.join``.
"""

import threading
from typing import Any, Dict, List, Optional, Sequence, Union

__all__ = ["bucket_join"]

OnSpec = Union[str, Sequence[str]]


def _norm(on: OnSpec) -> List[str]:
    return [on] if isinstance(on, str) else list(on)


def _key_type(table, col):
    # Logical type without nullability -- a present key hashes the same either way.
    return str(table.field_dict[col].type).replace(" NOT NULL", "")


def _bucketing(table):
    # Resolved bucket keys (a PK table without an explicit bucket-key buckets by its
    # trimmed primary key) plus the bucket function: same key co-locates only under both.
    return (table.options.bucket(),
            list(table.table_schema.bucket_keys),
            table.table_schema.options.get("bucket-function.type", "default"))


# Per-worker table cache, keyed by schema id (so a schema change invalidates it) and
# lock-guarded against concurrent tasks. Planning always loads a fresh table.
_TABLE_CACHE: Dict = {}
_TABLE_CACHE_LOCK = threading.Lock()


def _get_table(table_id, catalog_options, schema_id=None):
    from pypaimon.catalog.catalog_factory import CatalogFactory
    if schema_id is None:  # planning: always load the latest schema
        return CatalogFactory.create(catalog_options).get_table(table_id)
    key = (table_id, tuple(sorted(catalog_options.items())), schema_id)
    with _TABLE_CACHE_LOCK:
        table = _TABLE_CACHE.get(key)
        if table is None:
            table = CatalogFactory.create(catalog_options).get_table(table_id)
            if table.table_schema.id != schema_id:
                # get_table loads the latest schema; a mismatch means the schema moved
                # after the driver planned, so the split plan is stale -- fail fast.
                raise ValueError(
                    f"{table_id} schema changed during bucket_join (planned {schema_id}, "
                    f"now {table.table_schema.id}); retry.")
            _TABLE_CACHE[key] = table
        return table


def _read_builder(table_id, catalog_options, projection, schema_id=None):
    rb = _get_table(table_id, catalog_options, schema_id).new_read_builder()
    return rb.with_projection(projection) if projection is not None else rb


def _plan_splits_by_bucket(table_id, catalog_options, projection, expected_total_buckets):
    """Plan the manifest and group splits by bucket (driver-side).

    Returns ``(by_bucket, schema_id)``: the schema id of the table instance that
    built this plan, so workers validate against the schema the plan was made with
    (not a possibly-newer one loaded earlier by the caller).
    """
    from pypaimon.common.options.core_options import CoreOptions
    table = _get_table(table_id, catalog_options)  # fresh, latest schema
    schema_id = table.table_schema.id
    snapshot = table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        return {}, schema_id
    # Pin the guard and the split plan to one snapshot, else a commit between the two
    # manifest reads could slip stale-bucket files past the guard. Drop any existing
    # scan.mode / point-in-time options first so snapshot-id doesn't clash with them.
    opts = table.options.options
    for key in (CoreOptions.SCAN_MODE, CoreOptions.SCAN_SNAPSHOT_ID,
                CoreOptions.SCAN_TAG_NAME, CoreOptions.SCAN_WATERMARK,
                CoreOptions.SCAN_TIMESTAMP, CoreOptions.SCAN_TIMESTAMP_MILLIS,
                CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
                CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                CoreOptions.SCAN_CREATION_TIME_MILLIS):
        opts.data.pop(key.key(), None)
    opts.set(CoreOptions.SCAN_SNAPSHOT_ID, snapshot.id)
    rb = table.new_read_builder()
    scan = (rb.with_projection(projection) if projection is not None else rb).new_scan()
    # Guard against a rescaled table (old files under a different total_buckets, which
    # splits -- bucket only -- can't tell apart); read the entries once for it.
    fs = scan.file_scanner
    entries = fs.plan_files()
    stale = {e.total_buckets for e in entries if e.total_buckets != expected_total_buckets}
    if stale:
        raise ValueError(
            f"bucket_join needs {table_id} fully in bucket count {expected_total_buckets}, "
            f"but files exist under {sorted(stale)} (rescale in progress); rewrite first.")
    # Reuse those entries: scan.plan() re-reads plan_files() (append/pk) otherwise.
    fs.plan_files = lambda: entries
    by_bucket = {}
    for s in scan.plan().splits():
        by_bucket.setdefault(s.bucket, []).append(s)
    return by_bucket, schema_id


def _read_splits(table_id, catalog_options, projection, splits, schema_id):
    # Snapshot-independent but schema-dependent -> cache by schema id (in _get_table).
    return _read_builder(
        table_id, catalog_options, projection, schema_id).new_read().to_arrow(splits)


def bucket_join(
    left: str,
    right: str,
    catalog_options: Dict[str, str],
    *,
    on: OnSpec,
    left_projection: Optional[List[str]] = None,
    right_projection: Optional[List[str]] = None,
    join_type: str = "inner",
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> "ray.data.Dataset":
    """Join two co-bucketed tables (same bucket count + bucket-key, joined on the
    bucket-key) with no global shuffle. ``on`` must equal the bucket-key. The two
    sides must not share column names other than the join key (pyarrow ``join``
    would otherwise collide). Returns a ``ray.data.Dataset``."""
    import ray
    from pypaimon.catalog.catalog_factory import CatalogFactory

    if not hasattr(ray.data, "from_arrow_refs"):
        raise RuntimeError(
            "bucket_join needs a Ray version with ray.data.from_arrow_refs; "
            f"installed ray is {ray.__version__}.")

    on_cols = _norm(on)
    cat = CatalogFactory.create(catalog_options)
    ltable, rtable = cat.get_table(left), cat.get_table(right)
    lcount, lkey, lfunc = _bucketing(ltable)
    rcount, rkey, rfunc = _bucketing(rtable)

    if ltable.partition_keys or rtable.partition_keys:
        # Bucket numbers are per-partition, so the same bucket id lives in every
        # partition -- grouping splits by bucket alone would join across partitions.
        # Supporting this needs grouping by (partition, bucket); not done yet.
        raise ValueError(
            "bucket_join does not support partitioned tables yet; got partition keys "
            f"{left}={ltable.partition_keys}, {right}={rtable.partition_keys}.")
    if not lcount or lcount <= 0 or not rcount or rcount <= 0:
        raise ValueError(
            "bucket_join requires both tables to be fixed-bucket (bucket > 0); "
            f"got {left}={lcount}, {right}={rcount}.")
    if lcount != rcount:
        raise ValueError(
            f"bucket_join requires the same bucket count; {left}={lcount}, {right}={rcount}.")
    if lkey != rkey:
        raise ValueError(
            f"bucket_join requires the same bucket-key; {left}={lkey}, {right}={rkey}.")
    if lfunc != rfunc:
        # Different bucket functions hash the same key to different buckets.
        raise ValueError(
            f"bucket_join requires the same bucket-function.type; {left}={lfunc}, {right}={rfunc}.")
    if on_cols != lkey:
        raise ValueError(
            f"bucket_join requires the join key to be the bucket-key {lkey}; got on={on_cols}. "
            "Equal keys only co-locate by bucket when joining on the bucket-key "
            "(the comparison is order-sensitive for composite keys).")
    # Same name isn't enough: differing key types (INT vs BIGINT) can hash apart and
    # silently drop matches (types compared without nullability).
    key_type_mismatch = [
        (c, _key_type(ltable, c), _key_type(rtable, c))
        for c in on_cols
        if _key_type(ltable, c) != _key_type(rtable, c)
    ]
    if key_type_mismatch:
        raise ValueError(
            "bucket_join requires the bucket-key columns to have the same type on both "
            f"sides; mismatched (column, left, right): {key_type_mismatch}.")
    if join_type != "inner":
        # Outer joins would need the union of buckets (a bucket missing on one side
        # still emits rows); only inner is correct with the per-bucket intersection.
        raise ValueError(f"bucket_join currently supports only join_type='inner'; got {join_type!r}.")

    # The join key must survive projection on both sides, or ``Table.join`` has no key.
    if left_projection is not None and not set(on_cols) <= set(left_projection):
        raise ValueError(
            f"left_projection must include the join key {on_cols}; got {left_projection}.")
    if right_projection is not None and not set(on_cols) <= set(right_projection):
        raise ValueError(
            f"right_projection must include the join key {on_cols}; got {right_projection}.")
    # The two sides must not share non-key columns, or pyarrow's join collides on them.
    # Check up front (against the projected columns) instead of failing inside a task.
    lcols = left_projection if left_projection is not None else ltable.field_names
    rcols = right_projection if right_projection is not None else rtable.field_names
    collisions = sorted((set(lcols) & set(rcols)) - set(on_cols))
    if collisions:
        raise ValueError(
            f"bucket_join sides must not share columns other than the join key {on_cols}; "
            f"both have {collisions}. Project or rename them away.")

    # Plan each side's manifest once (driver-side, split metadata only -- the join
    # results stay distributed below), then dispatch per-bucket splits to the tasks.
    left_by_bucket, l_schema_id = _plan_splits_by_bucket(
        left, catalog_options, left_projection, lcount)
    right_by_bucket, r_schema_id = _plan_splits_by_bucket(
        right, catalog_options, right_projection, rcount)

    def _join_bucket(left_splits, right_splits):
        left_t = _read_splits(left, catalog_options, left_projection, left_splits, l_schema_id)
        right_t = _read_splits(right, catalog_options, right_projection, right_splits, r_schema_id)
        return left_t.join(right_t, keys=on_cols, join_type=join_type)

    # ``@ray.remote()`` (empty parens) is rejected by Ray, so wrap conditionally.
    remote_fn = ray.remote(**ray_remote_args)(_join_bucket) if ray_remote_args else ray.remote(_join_bucket)
    # Inner join: only buckets present on both sides can match.
    buckets = sorted(set(left_by_bucket) & set(right_by_bucket))
    if not buckets:
        # No shared bucket: empty result, but keep the join schema (join two empties).
        empty = _read_splits(left, catalog_options, left_projection, [], l_schema_id).join(
            _read_splits(right, catalog_options, right_projection, [], r_schema_id),
            keys=on_cols, join_type=join_type)
        return ray.data.from_arrow(empty)
    # Keep each bucket's result as a distributed object ref -- never pulled into the driver.
    refs = [remote_fn.remote(left_by_bucket[b], right_by_bucket[b]) for b in buckets]
    return ray.data.from_arrow_refs(refs)
