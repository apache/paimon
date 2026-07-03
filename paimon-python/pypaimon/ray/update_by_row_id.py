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

"""Distributed row-id update on Ray for data-evolution tables.

Update columns of a data-evolution table straight from a Ray Dataset that already
carries ``_ROW_ID`` and the new values -- no full-target read and no big-table
shuffle join (unlike ``merge_into(on=["_ROW_ID"])``, which reads and joins the whole
target). Pairs with ``bucket_join``, which produces the row ids.
"""

from typing import Any, Dict, List, Optional

import pyarrow as pa

from pypaimon.ray.data_evolution_merge_into import (
    _normalize_source,
    _reraise_inner,
    _require_ray_join,
    _resolve_num_partitions,
)
from pypaimon.ray.data_evolution_merge_join import distributed_update_apply
from pypaimon.ray.data_evolution_merge_transform import build_update_schema

__all__ = ["update_by_row_id"]


def _blob_col_names(table) -> set:
    return {f.name for f in table.table_schema.fields
            if getattr(f.type, "type", None) == "BLOB"}


def update_by_row_id(
    target: str,
    source: Any,
    catalog_options: Dict[str, str],
    *,
    update_cols: List[str],
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Update ``update_cols`` of a data-evolution table by ``_ROW_ID``.

    ``source`` (a ``ray.data.Dataset`` / ``pyarrow.Table`` / ``pandas.DataFrame`` /
    table-name str) must already carry ``_ROW_ID`` and the new values. Each row is
    routed to the data file owning its row id and only those files are rewritten --
    the target is never fully read and there is no join against it. Requires
    ``ray >= 2.50`` and a target with ``data-evolution.enabled`` + ``row-tracking.enabled``.

    Returns ``{"num_updated": <rows>}``.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    _require_ray_join()
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    update_cols = list(update_cols)
    num_partitions = _resolve_num_partitions(num_partitions)

    table = CatalogFactory.create(catalog_options).get_table(target)
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"update_by_row_id requires 'data-evolution.enabled'='true' on '{target}'.")
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"update_by_row_id requires 'row-tracking.enabled'='true' on '{target}'.")

    rid = SpecialFields.ROW_ID.name
    blob_cols = _blob_col_names(table)
    partition_keys = set(table.partition_keys or [])
    for col in update_cols:
        if col not in table.field_names:
            raise ValueError(f"update column {col!r} is not in target '{target}'.")
        if col in blob_cols:
            # Update writes plain data files; blob deltas are a separate path.
            raise ValueError(f"update_by_row_id cannot update blob column {col!r}.")
        if col in partition_keys:
            # In-place rewrite can't move a row across partitions.
            raise ValueError(
                f"update_by_row_id cannot update partition column {col!r}; "
                "cross-partition row movement is not supported.")

    source_ds = _normalize_source(source, catalog_options)
    src_cols = set(source_ds.schema().names)
    missing = [c for c in [rid] + update_cols if c not in src_cols]
    if missing:
        raise ValueError(
            f"source is missing columns {missing}; it must carry {rid} and {update_cols}.")

    # Cast to the on-disk schema (int64 _ROW_ID + target column types) so the writer
    # gets exactly the target types regardless of the source's arrow types.
    target_pa = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    update_schema = build_update_schema(target_pa, update_cols, rid)

    def _project_cast(batch: pa.Table) -> pa.Table:
        return batch.select([rid] + update_cols).cast(update_schema)

    update_ds = source_ds.map_batches(_project_cast, batch_format="pyarrow")

    base = table.snapshot_manager().get_latest_snapshot()
    if base is None:
        # No files -> every source row id is foreign; don't silently no-op non-empty input.
        if update_ds.limit(1).count() > 0:
            raise ValueError(
                f"target '{target}' has no rows; every _ROW_ID in the source is foreign.")
        return {"num_updated": 0}
    try:
        msgs, num_updated, _ = distributed_update_apply(
            update_ds, table, update_cols,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            base_snapshot_id=base.id if base is not None else None,
        )
    except Exception as e:
        _reraise_inner(e)

    if msgs:
        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.commit(msgs)
        tc.close()
    return {"num_updated": num_updated}
