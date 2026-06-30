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

from typing import Any, Dict, List, Optional, Sequence, Union

__all__ = ["bucket_join"]

OnSpec = Union[str, Sequence[str]]


def _norm(on: OnSpec) -> List[str]:
    return [on] if isinstance(on, str) else list(on)


def _bucketing(table):
    count = table.options.bucket()
    key = table.options.bucket_key()
    return count, ([k.strip() for k in key.split(",")] if key else [])


def _read_builder(table_id, catalog_options, projection):
    from pypaimon.catalog.catalog_factory import CatalogFactory
    rb = CatalogFactory.create(catalog_options).get_table(table_id).new_read_builder()
    return rb.with_projection(projection) if projection is not None else rb


def _plan_splits_by_bucket(table_id, catalog_options, projection):
    """Plan the manifest once and group splits by bucket (driver-side)."""
    by_bucket = {}
    for s in _read_builder(table_id, catalog_options, projection).new_scan().plan().splits():
        by_bucket.setdefault(s.bucket, []).append(s)
    return by_bucket


def _read_splits(table_id, catalog_options, projection, splits):
    return _read_builder(table_id, catalog_options, projection).new_read().to_arrow(splits)


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
    bucket-key) with no global shuffle. ``on`` must equal the bucket-key. Returns a
    ``ray.data.Dataset``."""
    import ray
    from pypaimon.catalog.catalog_factory import CatalogFactory

    on_cols = _norm(on)
    cat = CatalogFactory.create(catalog_options)
    lcount, lkey = _bucketing(cat.get_table(left))
    rcount, rkey = _bucketing(cat.get_table(right))

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
    if on_cols != lkey:
        raise ValueError(
            f"bucket_join requires the join key to be the bucket-key {lkey}; got on={on_cols}. "
            "Equal keys only co-locate by bucket when joining on the bucket-key.")

    # Plan each side's manifest once, then dispatch per-bucket splits to the tasks.
    left_by_bucket = _plan_splits_by_bucket(left, catalog_options, left_projection)
    right_by_bucket = _plan_splits_by_bucket(right, catalog_options, right_projection)

    def _join_bucket(left_splits, right_splits):
        left_t = _read_splits(left, catalog_options, left_projection, left_splits)
        right_t = _read_splits(right, catalog_options, right_projection, right_splits)
        if left_t.num_rows == 0 or right_t.num_rows == 0:
            return None
        return left_t.join(right_t, keys=on_cols, join_type=join_type)

    # ``@ray.remote()`` (empty parens) is rejected by Ray, so wrap conditionally.
    remote_fn = ray.remote(**ray_remote_args)(_join_bucket) if ray_remote_args else ray.remote(_join_bucket)
    # Inner join: only buckets present on both sides can contribute matches.
    buckets = sorted(set(left_by_bucket) & set(right_by_bucket))
    refs = [remote_fn.remote(left_by_bucket[b], right_by_bucket[b]) for b in buckets]
    parts = [p for p in ray.get(refs) if p is not None and p.num_rows > 0]
    if not parts:
        return ray.data.from_items([])
    return ray.data.from_arrow(parts)
