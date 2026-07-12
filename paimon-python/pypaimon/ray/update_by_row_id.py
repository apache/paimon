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

import logging
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

logger = logging.getLogger(__name__)


def _blob_col_names(table: "FileStoreTable") -> set:
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

    ``source`` (a ``ray.data.Dataset`` / ``pyarrow.Table`` / ``pandas.DataFrame``)
    must already carry the target ``_ROW_ID`` and the new values. Each row is
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
    update_cols = list(dict.fromkeys(update_cols))  # de-dup, keep order
    num_partitions = _resolve_num_partitions(num_partitions)

    table = CatalogFactory.create(catalog_options).get_table(target)
    if not table.options.data_evolution_enabled():
        raise ValueError(
            f"update_by_row_id requires 'data-evolution.enabled'='true' on '{target}'.")
    if not table.options.row_tracking_enabled():
        raise ValueError(
            f"update_by_row_id requires 'row-tracking.enabled'='true' on '{target}'.")
    if table.options.deletion_vectors_enabled():
        # A DV-deleted row still lives in its data file, so row-id ranges can't tell it
        # apart without reading the target; refuse rather than update a deleted row.
        raise ValueError(
            f"update_by_row_id does not support deletion-vectors-enabled tables yet: "
            f"'{target}'.")

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

    if isinstance(source, str):
        # A table's system _ROW_ID is its own, independent of the target's, so a
        # table-name source can't address target rows. Require in-memory data that
        # already carries the target row ids (e.g. produced by bucket_join).
        raise ValueError(
            "update_by_row_id does not accept a table-name source; pass a ray.data."
            f"Dataset / pyarrow.Table / pandas.DataFrame carrying the target {rid}.")
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
    # Without deletion vectors (rejected above), total_record_count is the live row
    # count, so 0 means the target is empty (never written, or emptied by overwrite).
    if base is None or base.total_record_count == 0:
        # Every source row id is foreign; don't silently no-op non-empty input.
        if update_ds.limit(1).count() > 0:
            raise ValueError(
                f"target '{target}' has no rows; every _ROW_ID in the source is foreign.")
        return {"num_updated": 0}
    try:
        msgs, num_updated, _ = distributed_update_apply(
            update_ds, table, update_cols,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            base_snapshot_id=base.id,
        )
    except Exception as e:
        _reraise_inner(e)
        raise  # _reraise_inner always raises; keeps msgs/num_updated defined for linters

    if msgs:
        _commit_update_messages(table, msgs)
    return {"num_updated": num_updated}


def _commit_update_messages(table, commit_messages) -> None:
    pending_msgs: list = list(commit_messages)
    commit_started = False

    try:
        table_commit = None
        try:
            table_commit = table.new_batch_write_builder().new_commit()
            commit_started = True
            table_commit.commit(pending_msgs)
        finally:
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as close_error:
                    logger.warning(
                        "Failed to close update_by_row_id commit: %s",
                        close_error,
                        exc_info=close_error,
                    )
    except Exception as e:
        if not commit_started:
            _abort_pending_update_messages(table, pending_msgs)
        _reraise_inner(e)


def _abort_pending_update_messages(table, commit_messages) -> None:
    if not commit_messages:
        return

    table_commit = None
    try:
        table_commit = table.new_batch_write_builder().new_commit()
        table_commit.abort(commit_messages)
    except Exception as abort_error:
        logger.warning(
            "Failed to abort pending update_by_row_id commit messages: %s",
            abort_error,
            exc_info=abort_error,
        )
    finally:
        if table_commit is not None:
            try:
                table_commit.close()
            except Exception as close_error:
                logger.warning(
                    "Failed to close update_by_row_id abort commit: %s",
                    close_error,
                    exc_info=close_error,
                )
