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

from typing import Any, Dict, List, Optional

from pypaimon.ray.join_common import (
    OnSpec,
    get_table as _shared_get_table,
    key_type as _key_type,
    norm_on as _norm,
    pin_latest_snapshot,
    read_splits,
)
# The table cache now lives in join_common; keep the old name for callers/tests.
from pypaimon.ray.join_common import _TABLE_CACHE  # noqa: F401

__all__ = ["bucket_join"]


def _bucketing(table):
    # Resolved bucket keys (a PK table without an explicit bucket-key buckets by its
    # trimmed primary key) plus the bucket function: same key co-locates only under both.
    return (table.options.bucket(),
            list(table.table_schema.bucket_keys),
            table.table_schema.options.get("bucket-function.type", "default"))


def _get_table(table_id, catalog_options, schema_id=None):
    return _shared_get_table(table_id, catalog_options, schema_id, "bucket_join")


def _plan_splits_by_bucket(table_id, catalog_options, projection, expected_total_buckets):
    """Plan the manifest and group splits by bucket (driver-side).

    Returns ``(by_bucket, schema_id)``: the schema id of the table instance that
    built this plan, so workers validate against the schema the plan was made with
    (not a possibly-newer one loaded earlier by the caller).
    """
    table = _get_table(table_id, catalog_options)  # fresh, latest schema
    schema_id = table.table_schema.id
    # Pin the guard and the split plan to one snapshot.
    if pin_latest_snapshot(table) is None:
        return {}, schema_id
    return _plan_table_splits_by_bucket(
        table, projection, expected_total_buckets)


def _plan_table_splits_by_bucket(
        table, projection, expected_total_buckets):
    """Plan an already snapshot-pinned table by bucket."""
    schema_id = table.table_schema.id
    rb = table.new_read_builder()
    scan = (
        rb.with_projection(projection)
        if projection is not None else rb
    ).new_scan()
    # Guard against a rescaled table (old files under a different total_buckets, which
    # splits -- bucket only -- can't tell apart); read the entries once for it.
    fs = scan.file_scanner
    entries = fs.plan_files()
    stale = {
        e.total_buckets
        for e in entries
        if e.total_buckets != expected_total_buckets
    }
    if stale:
        raise ValueError(
            f"bucket_join needs {table.identifier} fully in bucket count "
            f"{expected_total_buckets}, but files exist under "
            f"{sorted(stale)} (rescale in progress); rewrite first.")
    # Reuse those entries: scan.plan() re-reads plan_files() (append/pk) otherwise.
    fs.plan_files = lambda: entries
    by_bucket = {}
    for s in scan.plan().splits():
        by_bucket.setdefault(s.bucket, []).append(s)
    return by_bucket, schema_id


def _validate_bucket_join(
        left,
        right,
        ltable,
        rtable,
        on,
        left_projection,
        right_projection,
        join_type):
    on_cols = _norm(on)
    lcount, lkey, lfunc = _bucketing(ltable)
    rcount, rkey, rfunc = _bucketing(rtable)

    if ltable.partition_keys or rtable.partition_keys:
        raise ValueError(
            "bucket_join does not support partitioned tables yet; got "
            "partition keys {}={}, {}={}.".format(
                left, ltable.partition_keys, right, rtable.partition_keys))
    if not lcount or lcount <= 0 or not rcount or rcount <= 0:
        raise ValueError(
            "bucket_join requires both tables to be fixed-bucket "
            "(bucket > 0); got {}={}, {}={}.".format(
                left, lcount, right, rcount))
    if lcount != rcount:
        raise ValueError(
            "bucket_join requires the same bucket count; "
            "{}={}, {}={}.".format(left, lcount, right, rcount))
    if lkey != rkey:
        raise ValueError(
            "bucket_join requires the same bucket-key; "
            "{}={}, {}={}.".format(left, lkey, right, rkey))
    if lfunc != rfunc:
        raise ValueError(
            "bucket_join requires the same bucket-function.type; "
            "{}={}, {}={}.".format(left, lfunc, right, rfunc))
    if on_cols != lkey:
        raise ValueError(
            "bucket_join requires the join key to be the bucket-key {}; "
            "got on={}. Equal keys only co-locate by bucket when joining "
            "on the bucket-key (the comparison is order-sensitive for "
            "composite keys).".format(lkey, on_cols))

    key_type_mismatch = [
        (c, _key_type(ltable, c), _key_type(rtable, c))
        for c in on_cols
        if _key_type(ltable, c) != _key_type(rtable, c)
    ]
    if key_type_mismatch:
        raise ValueError(
            "bucket_join requires the bucket-key columns to have the same "
            "type on both sides; mismatched (column, left, right): {}."
            .format(key_type_mismatch))
    if join_type != "inner":
        raise ValueError(
            "bucket_join currently supports only join_type='inner'; got "
            "{!r}.".format(join_type))

    if (left_projection is not None
            and not set(on_cols) <= set(left_projection)):
        raise ValueError(
            "left_projection must include the join key {}; got {}."
            .format(on_cols, left_projection))
    if (right_projection is not None
            and not set(on_cols) <= set(right_projection)):
        raise ValueError(
            "right_projection must include the join key {}; got {}."
            .format(on_cols, right_projection))
    lcols = (
        left_projection
        if left_projection is not None else ltable.field_names)
    rcols = (
        right_projection
        if right_projection is not None else rtable.field_names)
    collisions = sorted((set(lcols) & set(rcols)) - set(on_cols))
    if collisions:
        raise ValueError(
            "bucket_join sides must not share columns other than the join "
            "key {}; both have {}. Project or rename them away."
            .format(on_cols, collisions))
    return on_cols, lcount


def _bucket_join_from_split_plans(
        left,
        right,
        catalog_options,
        on_cols,
        left_projection,
        right_projection,
        join_type,
        left_by_bucket,
        right_by_bucket,
        left_schema_id,
        right_schema_id,
        ray_remote_args=None):
    import ray

    def _join_bucket(left_splits, right_splits):
        left_t = _read_splits(
            left,
            catalog_options,
            left_projection,
            left_splits,
            left_schema_id,
        )
        right_t = _read_splits(
            right,
            catalog_options,
            right_projection,
            right_splits,
            right_schema_id,
        )
        return left_t.join(right_t, keys=on_cols, join_type=join_type)

    remote_fn = (
        ray.remote(**ray_remote_args)(_join_bucket)
        if ray_remote_args else ray.remote(_join_bucket)
    )
    buckets = sorted(set(left_by_bucket) & set(right_by_bucket))
    if not buckets:
        empty = _read_splits(
            left,
            catalog_options,
            left_projection,
            [],
            left_schema_id,
        ).join(
            _read_splits(
                right,
                catalog_options,
                right_projection,
                [],
                right_schema_id,
            ),
            keys=on_cols,
            join_type=join_type,
        )
        return ray.data.from_arrow(empty)
    refs = [
        remote_fn.remote(left_by_bucket[b], right_by_bucket[b])
        for b in buckets
    ]
    return ray.data.from_arrow_refs(refs)


def _read_splits(table_id, catalog_options, projection, splits, schema_id):
    return read_splits(table_id, catalog_options, projection, splits, schema_id, "bucket_join")


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

    cat = CatalogFactory.create(catalog_options)
    ltable, rtable = cat.get_table(left), cat.get_table(right)
    on_cols, bucket_count = _validate_bucket_join(
        left,
        right,
        ltable,
        rtable,
        on,
        left_projection,
        right_projection,
        join_type,
    )

    # Plan each side's manifest once (driver-side, split metadata only -- the join
    # results stay distributed below), then dispatch per-bucket splits to the tasks.
    left_by_bucket, l_schema_id = _plan_splits_by_bucket(
        left, catalog_options, left_projection, bucket_count)
    right_by_bucket, r_schema_id = _plan_splits_by_bucket(
        right, catalog_options, right_projection, bucket_count)
    return _bucket_join_from_split_plans(
        left,
        right,
        catalog_options,
        on_cols,
        left_projection,
        right_projection,
        join_type,
        left_by_bucket,
        right_by_bucket,
        l_schema_id,
        r_schema_id,
        ray_remote_args,
    )
