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

"""Distributed row-id read on Ray for data-evolution tables.

The read-side mirror of ``update_by_row_id``: read columns (including blob) for a set
of ``_ROW_ID``s by routing each to its owning data file -- no full-target read, no
shuffle join. Pairs with ``bucket_join``, which produces the row ids.
"""

from typing import Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.ray.data_evolution_merge_into import (
    _normalize_source,
    _reraise_inner,
    _require_ray_join,
    _resolve_num_partitions,
)
from pypaimon.ray.data_evolution_merge_join import (
    _read_output_schema,
    distributed_read_by_row_id,
)

__all__ = ["read_by_row_id"]


def _empty_result(table: "FileStoreTable", read_cols: List[str]):
    """An empty ``ray.data.Dataset`` with the projected read schema (empty source
    or target). Uses the same schema builder as the read path so they can't drift."""
    import ray

    return ray.data.from_arrow(_read_output_schema(table, read_cols).empty_table())


def read_by_row_id(
    target: str,
    row_ids: Any,
    catalog_options: Dict[str, str],
    *,
    projection: List[str],
    row_id_col: Optional[str] = None,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
):
    """Read ``projection`` columns of a data-evolution table by ``_ROW_ID``.

    ``row_ids`` (a ``ray.data.Dataset`` / ``pyarrow.Table`` / ``pandas.DataFrame``)
    must carry the target row ids in column ``row_id_col`` (default ``_ROW_ID``; set
    e.g. ``row_id_col="row_id"`` for a ``bucket_join`` locator). Each row id is routed
    to the data file owning it and only those files -- and only the matched rows --
    are read, so the target is never fully scanned and there is no join against it.
    ``projection`` lists top-level columns; blob columns are resolved to their payloads.
    Requires ``ray >= 2.50`` and a target with ``data-evolution.enabled`` +
    ``row-tracking.enabled``.

    Lookup/set semantics, like SQL ``... WHERE _ROW_ID IN (...)``: the result has one
    row per *distinct* matched row id -- duplicate row ids are deduplicated, source
    columns other than ``row_id_col`` are dropped, and the input row order is not
    preserved (rows come out grouped by owning file). An empty source yields an empty
    but correctly-typed Dataset.

    Returns a ``ray.data.Dataset`` of ``(*projection, _ROW_ID)``.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.table.special_fields import SpecialFields

    _require_ray_join()
    if not projection:
        raise ValueError("projection must be non-empty.")
    projection = list(dict.fromkeys(projection))
    num_partitions = _resolve_num_partitions(num_partitions)

    table = CatalogFactory.create(catalog_options).get_table(target)
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"read_by_row_id requires 'data-evolution.enabled'='true' on '{target}'.")
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"read_by_row_id requires 'row-tracking.enabled'='true' on '{target}'.")
    if table.options.deletion_vectors_enabled():
        # A DV-deleted row still lives in its file, so slicing would surface it.
        raise ValueError(
            f"read_by_row_id does not support deletion-vectors-enabled tables yet: "
            f"'{target}'.")

    rid = SpecialFields.ROW_ID.name
    src_rid_col = row_id_col or rid
    for col in projection:
        if col != rid and col not in table.field_names:
            raise ValueError(f"projection column {col!r} is not in target '{target}'.")

    if isinstance(row_ids, str):
        # A source table's _ROW_ID is its own, not the target's; require in-memory ids.
        raise ValueError(
            "read_by_row_id does not accept a table-name source; pass a ray.data."
            "Dataset / pyarrow.Table / pandas.DataFrame carrying the target row ids.")
    source_ds = _normalize_source(row_ids, catalog_options)
    if src_rid_col not in set(source_ds.schema().names):
        raise ValueError(f"row_ids source is missing the {src_rid_col!r} column.")

    def _project_rid(batch: pa.Table) -> pa.Table:
        return pa.table({rid: batch.column(src_rid_col).cast(pa.int64())})

    rid_ds = source_ds.map_batches(_project_rid, batch_format="pyarrow")
    read_cols = list(projection) + ([rid] if rid not in projection else [])

    # Empty source -> typed empty Dataset (a zero-row groupby has no schema).
    source_empty = rid_ds.limit(1).count() == 0

    base = table.snapshot_manager().get_latest_snapshot()
    # No DV (rejected above) -> total_record_count is the live row count; 0 = empty.
    if base is None or base.total_record_count == 0:
        if not source_empty:
            raise ValueError(
                f"target '{target}' has no rows; every _ROW_ID in the source is foreign.")
        return _empty_result(table, read_cols)
    if source_empty:
        return _empty_result(table, read_cols)
    try:
        result = distributed_read_by_row_id(
            rid_ds, table, projection,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            base_snapshot_id=base.id,
        )
    except Exception as e:
        _reraise_inner(e)
        raise  # _reraise_inner always raises
    if result is None:
        return _empty_result(table, read_cols)
    return result
