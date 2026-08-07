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

"""File-group-aligned row-id range planning on Ray."""

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from pypaimon.ray.data_evolution_merge_into import (
    _normalize_source,
    _reraise_inner,
    _require_ray_join,
    _resolve_num_partitions,
)
from pypaimon.ray.data_evolution_merge_join import (
    _map_kwargs,
    _read_output_schema,
    distributed_update_apply,
)
from pypaimon.ray.data_evolution_merge_transform import build_update_schema
from pypaimon.ray.update_by_row_id import (
    _abort_pending_update_messages,
    _blob_col_names,
    _commit_update_messages,
)

__all__ = [
    "process_row_id_ranges",
]

logger = logging.getLogger(__name__)


def _positive_int(name, value):
    if (isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0):
        raise ValueError("{} must be a positive integer.".format(name))


def _partitions(value):
    if value is not None:
        _positive_int("num_partitions", value)
    return _resolve_num_partitions(value)


def _validate_table(table, target):
    if table.is_primary_key_table:
        raise ValueError(
            "row-id range planning requires a non-primary-key table.")
    if not table.options.data_evolution_enabled():
        raise ValueError(
            "row-id range planning requires "
            "'data-evolution.enabled'='true' on '{}'.".format(target))
    if not table.options.row_tracking_enabled():
        raise ValueError(
            "row-id range planning requires "
            "'row-tracking.enabled'='true' on '{}'.".format(target))
    if table.options.deletion_vectors_enabled():
        raise ValueError(
            "row-id range planning does not support "
            "deletion-vectors-enabled tables yet: '{}'.".format(target))


def _file_group_range(files_info, first_row_id):
    from pypaimon.utils.range import Range

    files = files_info.first_row_id_index[first_row_id][1]
    ends = [
        data_file.row_id_range().to
        for data_file in files
        if (data_file.first_row_id == first_row_id
            and data_file.row_id_range() is not None)
    ]
    if not ends:
        raise RuntimeError(
            "Cannot determine row count for file group {}.".format(
                first_row_id))
    return Range(first_row_id, max(ends))


@dataclass
class _PlanState:
    schema_id: Optional[int]
    closed: bool = False


@dataclass(frozen=True)
class _RangeToken:
    target: str
    catalog_options: Dict[str, str]
    table: Any
    read_table: Any
    files_info: Any
    state: _PlanState


@dataclass(frozen=True)
class _RowIdRange:
    """Immutable metadata for one complete set of logical file groups."""

    snapshot_id: int
    range_start: int
    range_end: int
    estimated_rows: int
    sequence_number: int
    _token: _RangeToken = field(repr=False, compare=False)


def _pack_ranges(
    files_info,
    target_rows,
    target,
    catalog_options,
    table,
    read_table,
    state,
):
    from pypaimon.utils.range import Range
    from pypaimon.write.table_update_by_row_id import _FilesInfo

    ranges = []
    first_row_ids = []
    group_ranges = []
    estimated_rows = 0

    def append_range():
        if not first_row_ids:
            return
        subset = _FilesInfo(
            snapshot_id=files_info.snapshot_id,
            first_row_ids=list(first_row_ids),
            first_row_id_index={
                first_row_id: files_info.first_row_id_index[first_row_id]
                for first_row_id in first_row_ids
            },
            valid_row_id_ranges=Range.sort_and_merge_overlap(
                list(group_ranges), True, True),
        )
        ranges.append(_RowIdRange(
            snapshot_id=files_info.snapshot_id,
            range_start=first_row_ids[0],
            range_end=group_ranges[-1].to,
            estimated_rows=estimated_rows,
            sequence_number=len(ranges),
            _token=_RangeToken(
                target=target,
                catalog_options=dict(catalog_options),
                table=table,
                read_table=read_table,
                files_info=subset,
                state=state,
            ),
        ))

    for first_row_id in files_info.first_row_ids:
        group_range = _file_group_range(files_info, first_row_id)
        first_row_ids.append(first_row_id)
        group_ranges.append(group_range)
        estimated_rows += group_range.to - group_range.from_ + 1
        if estimated_rows >= target_rows:
            append_range()
            first_row_ids = []
            group_ranges = []
            estimated_rows = 0
    append_range()
    return ranges


def _route_blocks(first_row_ids, num_partitions):
    count = len(first_row_ids)
    blocks = max(1, min(count, num_partitions))
    size = (count + blocks - 1) // blocks
    return [
        pa.table({"_FIRST_ROW_ID": first_row_ids[start:start + size]})
        for start in range(0, count, size)
    ]


class _RowIdRangePlan:

    def __init__(
            self, snapshot_id, ranges, table=None, tag_name=None, state=None):
        self.snapshot_id = snapshot_id
        self._ranges = ranges
        self._table = table
        self._tag_name = tag_name
        self._state = state or _PlanState(None)

    def __enter__(self):
        _ensure_open(self._state)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self) -> Iterator[_RowIdRange]:
        _ensure_open(self._state)
        return iter(self._ranges)

    def __len__(self):
        return len(self._ranges)

    def close(self):
        if self._state.closed:
            return
        self._state.closed = True
        if self._tag_name is not None:
            try:
                self._table.delete_tag(self._tag_name)
            except Exception:
                logger.warning(
                    "Failed to delete row-id range tag %s.",
                    self._tag_name,
                    exc_info=True,
                )


def _ensure_open(state):
    if state.closed:
        raise RuntimeError("row-id range plan is closed.")


def _ensure_schema_unchanged(token):
    from pypaimon.write.commit.conflict_detection import CommitConflictError

    latest = token.table.schema_manager.latest()
    if latest is None or latest.id != token.state.schema_id:
        raise CommitConflictError(
            "Target schema changed after row-id ranges were planned.")


def _plan_row_id_ranges(
    target: str,
    catalog_options: Dict[str, str],
    *,
    target_rows_per_range: int,
):
    """Plan immutable, file-group-aligned ranges from the current snapshot."""
    from dataclasses import replace

    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.options.core_options import CoreOptions
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    _require_ray_join()
    _positive_int("target_rows_per_range", target_rows_per_range)
    table = CatalogFactory.create(catalog_options).get_table(target)
    _validate_table(table, target)
    base = table.snapshot_manager().get_latest_snapshot()
    if base is None or base.total_record_count == 0:
        return _RowIdRangePlan(None, [])

    read_table = table.copy_without_time_travel({
        CoreOptions.SCAN_SNAPSHOT_ID.key(): str(base.id),
    })
    planner = TableUpdateByRowId(
        read_table,
        "_row_id_range_planner_" + uuid.uuid4().hex[:8],
        BATCH_COMMIT_IDENTIFIER,
    )
    files_info = replace(
        planner._snapshot_files_info(), snapshot_id=base.id)
    state = _PlanState(table.table_schema.id)
    ranges = _pack_ranges(
        files_info,
        target_rows_per_range,
        target,
        catalog_options,
        table,
        read_table,
        state,
    )
    tag_name = "pypaimon-row-id-range-{}".format(uuid.uuid4().hex)
    table.create_tag(tag_name, snapshot_id=base.id, time_retained="30d")
    return _RowIdRangePlan(base.id, ranges, table, tag_name, state)


def _read_row_id_range(
    row_range: _RowIdRange,
    projection: List[str],
    filter: Optional[Union[str, Callable]] = None,
    *,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
):
    """Read one planned range as a Ray Dataset, including ``_ROW_ID``."""
    from pypaimon.common.where_parser import (
        parse_where_clause,
    )
    from pypaimon.table.special_fields import SpecialFields

    if not isinstance(row_range, _RowIdRange):
        raise ValueError("invalid row-id range.")
    if not projection:
        raise ValueError("projection must be non-empty.")
    projection = list(dict.fromkeys(projection))
    token = row_range._token
    _ensure_open(token.state)
    table = token.table
    unknown = [col for col in projection if col not in table.field_names]
    if unknown:
        raise ValueError(
            "read column {!r} is not in target '{}'.".format(
                unknown[0], token.target))
    if (filter is not None
            and not isinstance(filter, str)
            and not callable(filter)):
        raise ValueError("filter must be a SQL expression or callable.")

    predicate = (
        parse_where_clause(filter, table.table_schema.fields)
        if isinstance(filter, str) else None
    )
    row_id = SpecialFields.ROW_ID.name
    read_columns = projection + ([row_id] if row_id not in projection else [])
    empty = _read_output_schema(table, read_columns).empty_table()
    num_partitions = _partitions(num_partitions)

    import ray

    info_ref = ray.put(token.files_info)
    read_table = token.read_table
    state = token.state
    callable_filter = filter if callable(filter) else None

    def read_groups(routes):
        _ensure_open(state)
        info = ray.get(info_ref)
        results = []
        for first_row_id in routes.column("_FIRST_ROW_ID").to_pylist():
            owning_split, target_files = info.first_row_id_index[first_row_id]
            target_names = {file.file_name for file in target_files}
            split = owning_split.filter_file(
                lambda file: file.file_name in target_names,
            )
            if split is None or len(split.files) != len(target_files):
                raise RuntimeError(
                    "Cannot rebuild file group {} from its planned split."
                    .format(first_row_id))
            builder = read_table.new_read_builder()
            if predicate is not None:
                builder = builder.with_filter(predicate)
            result = (
                builder.with_projection(read_columns)
                .new_read().to_arrow([split])
            )
            if callable_filter is not None and result.num_rows:
                mask = callable_filter(result.select(projection))
                try:
                    mask = (mask if isinstance(
                        mask, (pa.Array, pa.ChunkedArray)) else pa.array(mask))
                except Exception as error:
                    raise ValueError(
                        "callable filter must return a boolean mask.") from error
                if (not pa.types.is_boolean(mask.type)
                        or len(mask) != result.num_rows):
                    raise ValueError(
                        "callable filter must return one boolean per row.")
                if mask.null_count:
                    import pyarrow.compute as pc
                    mask = pc.fill_null(mask, False)
                result = result.filter(mask)
            if result.num_rows:
                results.append(result)
        return pa.concat_tables(results) if results else empty

    routes = ray.data.from_arrow(_route_blocks(
        token.files_info.first_row_ids, num_partitions))
    result = routes.map_batches(
        read_groups, **_map_kwargs(ray_remote_args))
    return result.union(ray.data.from_arrow(empty))


def _update_by_row_id_from_plan(
    row_range: _RowIdRange,
    updates: Any,
    update_cols: List[str],
    *,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Update one planned range using values carrying its ``_ROW_ID``."""
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    if not isinstance(row_range, _RowIdRange):
        raise ValueError("invalid row-id range.")
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    update_cols = list(dict.fromkeys(update_cols))
    token = row_range._token
    _ensure_open(token.state)
    _ensure_schema_unchanged(token)
    table = token.table
    blob_columns = _blob_col_names(table)
    partition_keys = set(table.partition_keys or [])
    for column in update_cols:
        if column not in table.field_names:
            raise ValueError(
                "update column {!r} is not in target '{}'.".format(
                    column, token.target))
        if column in blob_columns:
            raise ValueError(
                "row-id ranges cannot update blob column {!r}.".format(
                    column))
        if column in partition_keys:
            raise ValueError(
                "row-id ranges cannot update partition column {!r}.".format(
                    column))

    row_id = SpecialFields.ROW_ID.name
    if isinstance(updates, str):
        raise ValueError(
            "updates must carry row ids from the range input; "
            "a table-name source is not accepted.")
    source = _normalize_source(updates, token.catalog_options)
    source_schema = source.schema(fetch_if_missing=False)
    if source_schema is not None:
        missing = [
            column for column in [row_id] + update_cols
            if column not in set(source_schema.names)
        ]
        if missing:
            raise ValueError(
                "updates are missing columns {}.".format(missing))

    target_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields)
    update_schema = build_update_schema(
        target_schema, update_cols, row_id)
    allowed_ranges = token.files_info.valid_row_id_ranges
    import numpy as np
    range_starts = np.asarray(
        [range_.from_ for range_ in allowed_ranges], dtype=np.int64)
    range_ends = np.asarray(
        [range_.to for range_ in allowed_ranges], dtype=np.int64)

    def project_and_validate(batch):
        missing = [
            column for column in [row_id] + update_cols
            if column not in set(batch.column_names)
        ]
        if missing:
            raise ValueError(
                "updates are missing columns {}.".format(missing))
        projected = batch.select([row_id] + update_cols).cast(update_schema)
        row_ids = projected.column(row_id)
        if row_ids.null_count:
            raise ValueError("updates contain a null _ROW_ID.")
        values = row_ids.to_numpy(zero_copy_only=False)
        indexes = np.searchsorted(
            range_starts, values, side="right") - 1
        safe_indexes = np.maximum(indexes, 0)
        valid = (indexes >= 0) & (values <= range_ends[safe_indexes])
        if not np.all(valid):
            raise ValueError(
                "updates contain _ROW_ID {} outside [{}, {}].".format(
                    values[np.flatnonzero(~valid)[0]],
                    row_range.range_start,
                    row_range.range_end,
                ))
        return projected

    source = source.map_batches(
        project_and_validate, batch_format="pyarrow")
    num_partitions = _partitions(num_partitions)
    try:
        messages, num_updated, _ = distributed_update_apply(
            source,
            table,
            update_cols,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            base_snapshot_id=row_range.snapshot_id,
            precomputed_files_info=token.files_info,
        )
    except Exception as error:
        _reraise_inner(error)
        raise
    if messages:
        try:
            _ensure_open(token.state)
            _ensure_schema_unchanged(token)
        except Exception:
            _abort_pending_update_messages(table, messages)
            raise
        _commit_update_messages(table, messages)
    return {"num_updated": num_updated}


def process_row_id_ranges(
    target: str,
    catalog_options: Dict[str, str],
    *,
    target_rows_per_range: int,
    read_projection: List[str],
    update_cols: List[str],
    processor: Callable[[Any], Any],
    filter: Optional[Union[str, Callable]] = None,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Read, process, update, and commit file-group-aligned ranges."""
    if not callable(processor):
        raise ValueError("processor must be callable.")
    if not read_projection:
        raise ValueError("read_projection must be non-empty.")
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    processed = 0
    updated = 0
    with _plan_row_id_ranges(
        target,
        catalog_options,
        target_rows_per_range=target_rows_per_range,
    ) as plan:
        for row_range in plan:
            source = _read_row_id_range(
                row_range,
                read_projection,
                filter,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
            )
            updates = processor(source)
            if updates is not None:
                result = _update_by_row_id_from_plan(
                    row_range,
                    updates,
                    update_cols,
                    num_partitions=num_partitions,
                    ray_remote_args=ray_remote_args,
                )
                updated += result["num_updated"]
            processed += 1
    return {"num_ranges": processed, "num_updated": updated}
