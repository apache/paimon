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

"""File-group-aligned row-id range processing on Ray."""

import logging
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
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
from pypaimon.ray.update_by_row_id import _blob_col_names

__all__ = [
    "RowIdRangeContext",
    "plan_row_id_ranges",
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
            "row-id range processing requires a non-primary-key table.")
    if not table.options.data_evolution_enabled():
        raise ValueError(
            "row-id range processing requires "
            "'data-evolution.enabled'='true' on '{}'.".format(target))
    if not table.options.row_tracking_enabled():
        raise ValueError(
            "row-id range processing requires "
            "'row-tracking.enabled'='true' on '{}'.".format(target))
    if table.options.deletion_vectors_enabled():
        raise ValueError(
            "row-id range processing does not support "
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


@dataclass(frozen=True)
class _RangeSpec:
    sequence_number: int
    range_start: int
    range_end: int
    estimated_rows: int
    files_info: Any


def _pack_ranges(files_info, target_rows):
    from pypaimon.utils.range import Range
    from pypaimon.write.table_update_by_row_id import _FilesInfo

    specs = []
    first_row_ids = []
    group_ranges = []
    estimated_rows = 0

    def append_range():
        if not first_row_ids:
            return
        valid_ranges = Range.sort_and_merge_overlap(
            list(group_ranges), True, True)
        subset = _FilesInfo(
            snapshot_id=files_info.snapshot_id,
            first_row_ids=list(first_row_ids),
            first_row_id_index={
                first_row_id: files_info.first_row_id_index[first_row_id]
                for first_row_id in first_row_ids
            },
            valid_row_id_ranges=valid_ranges,
        )
        specs.append(_RangeSpec(
            sequence_number=len(specs),
            range_start=first_row_ids[0],
            range_end=group_ranges[-1].to,
            estimated_rows=estimated_rows,
            files_info=subset,
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
    return specs


def _route_blocks(first_row_ids, num_partitions):
    count = len(first_row_ids)
    blocks = max(1, min(count, num_partitions))
    size = (count + blocks - 1) // blocks
    return [
        pa.table({"_FIRST_ROW_ID": first_row_ids[start:start + size]})
        for start in range(0, count, size)
    ]


class RowIdRangeContext:
    """One planned row-id range aligned to complete file groups."""

    def __init__(self, owner, spec):
        self._owner = owner
        self._spec = spec
        self._read_columns = []
        self._updated = False
        self.snapshot_id = owner.snapshot_id
        self.range_start = spec.range_start
        self.range_end = spec.range_end
        self.estimated_rows = spec.estimated_rows
        self.sequence_number = spec.sequence_number

    def read(
        self,
        projection: List[str],
        filter: Optional[Union[str, Callable]] = None,
        *,
        num_partitions: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Read this range as a Ray Dataset, including ``_ROW_ID``."""
        from pypaimon.common.where_parser import (
            extract_fields_from_where,
            parse_where_clause,
        )
        from pypaimon.table.special_fields import SpecialFields

        self._owner._check_open()
        if not projection:
            raise ValueError("projection must be non-empty.")
        projection = list(dict.fromkeys(projection))
        table = self._owner.table
        unknown = [col for col in projection if col not in table.field_names]
        if unknown:
            raise ValueError(
                "read column {!r} is not in target '{}'.".format(
                    unknown[0], self._owner.target))
        if (filter is not None
                and not isinstance(filter, str)
                and not callable(filter)):
            raise ValueError("filter must be a SQL expression or callable.")

        predicate = (
            parse_where_clause(filter, table.table_schema.fields)
            if isinstance(filter, str) else None
        )
        filter_columns = (
            extract_fields_from_where(filter, set(table.field_names))
            if isinstance(filter, str) else set()
        )
        self._read_columns = list(dict.fromkeys(
            self._read_columns + projection + [
                col for col in table.field_names if col in filter_columns
            ]
        ))

        row_id = SpecialFields.ROW_ID.name
        read_columns = projection + ([row_id] if row_id not in projection else [])
        empty = _read_output_schema(table, read_columns).empty_table()
        num_partitions = _partitions(num_partitions)

        import ray

        info_ref = ray.put(self._spec.files_info)
        read_table = self._owner.read_table
        callable_filter = filter if callable(filter) else None

        def read_groups(routes):
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
            self._spec.files_info.first_row_ids, num_partitions))
        result = routes.map_batches(
            read_groups, **_map_kwargs(ray_remote_args))
        return result.union(ray.data.from_arrow(empty))

    def update_by_row_id(
        self,
        updates: Any,
        update_cols: List[str],
        *,
        num_partitions: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        """Update this range from values carrying the ``_ROW_ID`` read by it."""
        if self._updated:
            raise RuntimeError("a row-id range can be updated only once.")
        with self._owner._update_guard(self.sequence_number):
            return self._update_by_row_id(
                updates, update_cols, num_partitions, ray_remote_args)

    def _update_by_row_id(
        self, updates, update_cols, num_partitions, ray_remote_args,
    ):
        from pypaimon.schema.data_types import PyarrowFieldParser
        from pypaimon.table.special_fields import SpecialFields

        self._owner._check_open()
        self._owner._check_schema()
        if not update_cols:
            raise ValueError("update_cols must be non-empty.")
        update_cols = list(dict.fromkeys(update_cols))
        table = self._owner.table
        blob_columns = _blob_col_names(table)
        partition_keys = set(table.partition_keys or [])
        for column in update_cols:
            if column not in table.field_names:
                raise ValueError(
                    "update column {!r} is not in target '{}'.".format(
                        column, self._owner.target))
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
                "updates must carry row ids returned by this range; "
                "a table-name source is not accepted.")
        source = _normalize_source(updates, self._owner.catalog_options)
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
        allowed_ranges = self._spec.files_info.valid_row_id_ranges
        import numpy as np
        range_starts = np.asarray(
            [range_.from_ for range_ in allowed_ranges], dtype=np.int64)
        range_ends = np.asarray(
            [range_.to for range_ in allowed_ranges], dtype=np.int64)
        range_start = self.range_start
        range_end = self.range_end

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
                raise ValueError(
                    "updates contain a _ROW_ID outside [{}, {}].".format(
                        range_start, range_end))
            values = row_ids.to_numpy(zero_copy_only=False)
            indexes = np.searchsorted(
                range_starts, values, side="right") - 1
            safe_indexes = np.maximum(indexes, 0)
            valid = (indexes >= 0) & (values <= range_ends[safe_indexes])
            if not np.all(valid):
                raise ValueError(
                    "updates contain _ROW_ID {} outside [{}, {}].".format(
                        values[np.flatnonzero(~valid)[0]],
                        range_start,
                        range_end,
                    ))
            return projected

        source = source.map_batches(
            project_and_validate, batch_format="pyarrow")
        num_partitions = _partitions(num_partitions)
        self._owner._check_schema()
        try:
            messages, num_updated, _ = distributed_update_apply(
                source,
                table,
                update_cols,
                num_partitions=num_partitions,
                ray_remote_args=ray_remote_args,
                base_snapshot_id=self.snapshot_id,
                precomputed_files_info=self._spec.files_info,
            )
        except Exception as error:
            _reraise_inner(error)
            raise

        if messages:
            self._owner._commit(messages, self._read_columns)
        self._owner.num_updated += num_updated
        self._updated = True
        return {"num_updated": num_updated}


class _RowIdRanges:

    def __init__(self, target, catalog_options, table, base, specs, tag_name):
        from pypaimon.common.options.core_options import CoreOptions

        self.target = target
        self.catalog_options = catalog_options
        self.table = table
        self.snapshot_id = base.id
        self._planned_schema_id = table.table_schema.id
        self._checkpoint = base
        self._specs = specs
        self._tag_name = tag_name
        self._closed = False
        self._table_commit = None
        self._commit_user = None
        self._next_identifier = 1
        self._next_update_sequence = 0
        self._update_lock = threading.Lock()
        self.num_updated = 0
        self.read_table = table.copy_without_time_travel({
            CoreOptions.SCAN_SNAPSHOT_ID.key(): str(base.id),
        })

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self) -> Iterator[RowIdRangeContext]:
        self._check_open()
        return (RowIdRangeContext(self, spec) for spec in self._specs)

    def __len__(self):
        return len(self._specs)

    def _check_open(self):
        if self._closed:
            raise RuntimeError("row-id range plan is closed.")

    @contextmanager
    def _update_guard(self, sequence_number):
        if not self._update_lock.acquire(blocking=False):
            raise RuntimeError(
                "row-id ranges from one plan cannot be updated concurrently.")
        try:
            if sequence_number != self._next_update_sequence:
                raise RuntimeError(
                    "row-id ranges must be updated in sequence order; "
                    "expected {}, got {}.".format(
                        self._next_update_sequence, sequence_number))
            yield
            self._next_update_sequence += 1
        finally:
            self._update_lock.release()

    def _check_schema(self):
        latest = self.table.schema_manager.latest()
        if latest is None or latest.id != self._planned_schema_id:
            from pypaimon.write.commit.conflict_detection import (
                CommitConflictError,
            )
            raise CommitConflictError(
                "Target schema changed during row-id range processing.")

    def _commit(self, messages, protected_columns):
        if self._table_commit is None:
            builder = self.table.new_stream_write_builder()
            self._commit_user = builder.commit_user
            self._table_commit = builder.new_commit()
        self._table_commit.protect_from_external_rewrites(
            self._checkpoint,
            self._commit_user,
            self._planned_schema_id,
            protected_columns,
        )
        committed = self._table_commit.commit(
            messages, self._next_identifier)
        if committed is None:
            raise RuntimeError("Committed row-id range snapshot is missing.")
        self._check_schema()
        if committed.schema_id != self._planned_schema_id:
            from pypaimon.write.commit.conflict_detection import (
                CommitConflictError,
            )
            raise CommitConflictError(
                "Target schema changed during row-id range processing.")
        self._checkpoint = committed
        self._next_identifier += 1

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._table_commit is not None:
            try:
                self._table_commit.close()
            except Exception:
                logger.warning(
                    "Failed to close row-id range commit.", exc_info=True)
        if self._tag_name is not None:
            try:
                self.table.delete_tag(self._tag_name)
            except Exception:
                logger.warning(
                    "Failed to delete row-id range tag %s.",
                    self._tag_name,
                    exc_info=True,
                )


def plan_row_id_ranges(
    target: str,
    catalog_options: Dict[str, str],
    *,
    target_rows_per_range: int,
):
    """Plan stable, file-group-aligned ranges from the current snapshot."""
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
        return _EmptyRowIdRanges()

    scan_table = table.copy_without_time_travel({
        CoreOptions.SCAN_SNAPSHOT_ID.key(): str(base.id),
    })
    planner = TableUpdateByRowId(
        scan_table,
        "_row_id_range_planner_" + uuid.uuid4().hex[:8],
        BATCH_COMMIT_IDENTIFIER,
    )
    files_info = replace(
        planner._snapshot_files_info(), snapshot_id=base.id)
    specs = _pack_ranges(files_info, target_rows_per_range)
    tag_name = "pypaimon-row-id-range-{}".format(uuid.uuid4().hex)
    table.create_tag(tag_name, snapshot_id=base.id, time_retained="30d")
    return _RowIdRanges(
        target, catalog_options, table, base, specs, tag_name)


class _EmptyRowIdRanges:
    snapshot_id = None
    num_updated = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __iter__(self):
        return iter(())

    def __len__(self):
        return 0

    def close(self):
        pass


def process_row_id_ranges(
    target: str,
    catalog_options: Dict[str, str],
    *,
    target_rows_per_range: int,
    processor: Callable[[RowIdRangeContext], Any],
) -> Dict[str, int]:
    """Run ``processor`` sequentially for each planned range."""
    if not callable(processor):
        raise ValueError("processor must be callable.")
    processed = 0
    with plan_row_id_ranges(
        target,
        catalog_options,
        target_rows_per_range=target_rows_per_range,
    ) as ranges:
        for context in ranges:
            processor(context)
            processed += 1
        return {
            "num_ranges": processed,
            "num_updated": ranges.num_updated,
        }
