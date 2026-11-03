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

"""Local column enrichment over Data Evolution tables."""

from dataclasses import dataclass
from typing import Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.multimodal.table import (
    _cast_merge_on_columns, _normalize_merge_on, _target_schema,
    _to_arrow_table, _validate_merge_on,
)
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.time_travel_util import SCAN_KEYS
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.update_validation import _authorize_update


@dataclass(frozen=True)
class ColumnUpdateResult:
    """Schema and data versions produced by a successful column operation."""

    source_snapshot_id: Optional[int]
    snapshot_id: Optional[int]
    schema_id: int
    updated_rows: int
    unmatched_rows: int
    added_columns: Tuple[str, ...]


class ColumnUpdateError(RuntimeError):
    """A failed enrichment, with its last known publication state.

    ``schema_changed=None`` means the schema request had an unknown outcome.
    ``commit_outcome`` is ``not_started``, ``unknown``, or ``committed``. Never
    retry blindly after an unknown outcome; inspect the table first. The
    original failure is retained as ``__cause__``.
    """

    def __init__(self, operation):
        self.phase = operation.phase
        self.schema_changed = operation.schema_changed
        self.schema_id = operation.raw.table_schema.id
        self.source_snapshot_id = operation.source_snapshot_id
        self.commit_outcome = operation.commit_outcome
        super().__init__(
            "Column update failed during %s (schema_changed=%s, schema_id=%s, "
            "source_snapshot_id=%s, commit_outcome=%s)."
            % (self.phase, self.schema_changed, self.schema_id,
               self.source_snapshot_id, self.commit_outcome))


def _validate_update(table):
    selectors = SCAN_KEYS + CoreOptions.INCREMENTAL_SCAN_KEYS + [
        CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
        CoreOptions.SCAN_CREATION_TIME_MILLIS.key(),
    ]
    if (hasattr(table, "_read_snapshot")
            or any(table.table_schema.options.get(key) is not None for key in selectors)
            or table.table_schema.options.get("scan.mode", "default")
            not in ("default", "latest", "latest-full")):
        raise ValueError("Column updates require a current full-table view.")
    _authorize_update(table)


class _ColumnUpdate:
    def __init__(self, owner, raw):
        self.owner = owner
        self.raw = raw
        self.snapshot = raw.snapshot_manager().get_latest_snapshot()
        self.source_snapshot_id = None if self.snapshot is None else self.snapshot.id
        self.added_columns = ()
        self.schema_changed = False
        self.commit_outcome = "not_started"
        self.phase = "schema"
        self.resources = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, error, traceback):
        for resource in reversed(self.resources):
            try:
                resource.close()
            except Exception as close_error:
                if error is None:
                    self.phase = "close"
                    error = close_error
        if error is not None and isinstance(error, Exception):
            raise ColumnUpdateError(self) from error
        return False

    def keep(self, resource):
        self.resources.append(resource)
        return resource

    def check_current(self):
        # This is a stale-work check, not a lock. These local enrichment APIs
        # require a single writer; ordinary commit conflict checks still apply.
        latest = self.raw.snapshot_manager().get_latest_snapshot()
        latest_id = None if latest is None else latest.id
        schema_id = self.owner.catalog.get_table(self.owner.identifier).table_schema.id
        if latest_id != self.source_snapshot_id or schema_id != self.raw.table_schema.id:
            raise RuntimeError("Table changed during column enrichment; recompute from the current table.")

    def add_schema(self, fields):
        if not fields:
            return
        self.check_current()
        changes = [SchemaChange.add_column(
            field.name, PyarrowFieldParser.to_paimon_type(field.type, True),
            comment=(field.metadata or {}).get(b"description", b"").decode("utf-8") or None,
        ) for field in fields]
        self.schema_changed = None
        self.owner.catalog.alter_table(self.owner.identifier, changes)
        self.schema_changed = True
        # Only this operation's schema addition needs a new table definition.
        # Ordinary calls keep the caller's existing handle, like add/update/merge.
        raw = self.owner.catalog.get_table(self.owner.identifier).copy_without_time_travel(
            getattr(self.raw, "_applied_dynamic_options", {}))
        _authorize_update(raw)
        self.raw = self.owner.raw_table = self.owner.table = raw
        self.added_columns = tuple(field.name for field in fields)

    def finish(self, updated_rows, unmatched_rows=0, commit=None, messages=()):
        self.phase = "validate"
        self.check_current()
        if messages:
            self.phase = "commit"
            self.commit_outcome = "unknown"
            commit.commit(messages)
            self.commit_outcome = "committed"
        self.phase = "result"
        snapshot = self.raw.snapshot_manager().get_latest_snapshot()
        return ColumnUpdateResult(
            self.source_snapshot_id, None if snapshot is None else snapshot.id,
            self.raw.table_schema.id, updated_rows, unmatched_rows, self.added_columns)


def _validate_names(names, label):
    if (not names or any(not isinstance(name, str) or not name for name in names)
            or len(names) != len(set(names))):
        raise ValueError("%s must contain nonempty, unique column names." % label)
    reserved = set(names).intersection(SpecialFields.SYSTEM_FIELD_NAMES)
    if reserved:
        raise ValueError("%s cannot contain system columns: %s" % (label, sorted(reserved)))


def _canonical_field(field):
    data_type = PyarrowFieldParser.to_paimon_type(field.type, field.nullable)
    return field.with_type(PyarrowFieldParser.from_paimon_type(data_type))


def _normalize_output(data, schema):
    if not isinstance(data, (pa.Table, pa.RecordBatch)):
        raise ValueError("Column function must return a pyarrow.Table or RecordBatch.")
    if (len(data.schema.names) != len(schema.names)
            or set(data.schema.names) != set(schema.names)):
        raise ValueError("Output must contain exactly the declared columns: %s" % schema.names)
    arrays = []
    for field in schema:
        array = data[field.name]
        if not pa.types.is_null(array.type):
            source_type = _canonical_field(pa.field(field.name, array.type)).type
            if source_type != field.type:
                raise ValueError("Incompatible type for column %s: %s != %s"
                                 % (field.name, array.type, field.type))
        if not field.nullable and array.null_count:
            raise ValueError("Cannot write null to non-null column %s." % field.name)
        arrays.append(array.cast(field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _match_rows(raw, snapshot, data, target_keys, source_keys):
    aliases = ["key_%d" % i for i in range(len(target_keys))]
    source = data.select(source_keys).rename_columns(aliases)
    for column in source.columns:
        if column.null_count:
            raise ValueError("Source match keys cannot contain null values.")
        if pa.types.is_floating(column.type) and pc.any(pc.is_nan(column)).as_py():
            raise ValueError("Source match keys cannot contain NaN values.")
    source = _normalize_float_keys(source, aliases)
    source = source.append_column("source_index", pa.array(range(data.num_rows), type=pa.int64()))
    if data.num_rows == 0:
        return pa.table({"source_index": pa.array([], type=pa.int64()),
                         "row_id": pa.array([], type=pa.int64())}), 0
    counts = source.group_by(aliases).aggregate([("source_index", "count")])
    if pc.any(pc.greater(counts["source_index_count"], 1)).as_py():
        raise ValueError("Source contains duplicate match keys.")

    builder = raw._copy_with_snapshot(snapshot).new_read_builder().with_projection(
        target_keys + [SpecialFields.ROW_ID.name])
    target = builder.new_read().to_arrow(builder.new_scan().plan_all().splits())
    target = target.rename_columns(aliases + ["row_id"])
    target = _normalize_float_keys(target, aliases)
    matched = source.join(target, keys=aliases, join_type="left outer")
    counts = matched.group_by("source_index").aggregate([("row_id", "count")])
    if pc.any(pc.greater(counts["row_id_count"], 1)).as_py():
        raise ValueError("Source key matches multiple target rows.")
    unmatched = matched["row_id"].null_count
    return matched.filter(pc.is_valid(matched["row_id"])).select(["source_index", "row_id"]), unmatched


def _normalize_float_keys(table, names):
    # Arrow hashes IEEE signed zero by representation; numeric key equality
    # requires both representations to match and share uniqueness checks.
    for name in names:
        index = table.schema.get_field_index(name)
        column = table.column(index)
        if pa.types.is_floating(column.type):
            column = pc.if_else(pc.equal(column, 0), pa.scalar(0, type=column.type), column)
            table = table.set_column(index, table.schema.field(index), column)
    return table


def _cast_match_keys(data, target_schema, target_keys, source_keys):
    casted = _cast_merge_on_columns(data, target_schema, target_keys, source_keys)
    for name in source_keys:
        original, converted = data[name], casted[name]
        if original.type == converted.type:
            continue
        try:
            restored = converted.cast(original.type)
        except pa.ArrowException as error:
            raise ValueError("Match key %s cannot be converted without loss." % name) from error
        if not original.equals(restored):
            raise ValueError("Match key %s cannot be converted without loss." % name)
    return casted


def merge_columns(owner, data, *, on, unmatched):
    if unmatched not in ("error", "ignore"):
        raise ValueError("unmatched must be 'error' or 'ignore'.")
    raw = owner.raw_table
    _validate_update(raw)
    data = _to_arrow_table(data)
    _validate_names(data.column_names, "Source")
    target_keys, source_keys = _normalize_merge_on(on, "merge_columns")
    target_schema = _target_schema(raw)
    _validate_merge_on(target_schema, data, target_keys, source_keys, "merge_columns")
    data = _cast_match_keys(data, target_schema, target_keys, source_keys)
    output_names = [name for name in data.column_names if name not in source_keys]
    _validate_names(output_names, "Output")
    if set(output_names).intersection(raw.partition_keys + target_keys):
        raise ValueError("merge_columns cannot update partition or match keys.")
    if set(output_names).intersection(raw.options.video_frame_fields()):
        raise ValueError("Use replace_video() to update video-frame columns.")
    fields = [target_schema.field(name) if name in target_schema.names
              else _canonical_field(data.schema.field(name)).with_nullable(True)
              for name in output_names]
    output = _normalize_output(data.select(output_names), pa.schema(fields))
    operation = _ColumnUpdate(owner, raw)
    matches, unmatched_rows = _match_rows(raw, operation.snapshot, data, target_keys, source_keys)
    if unmatched_rows and unmatched == "error":
        raise ValueError("Source contains %d unmatched rows." % unmatched_rows)
    updates = output.take(matches["source_index"]).append_column(
        SpecialFields.ROW_ID.name, matches["row_id"])
    with operation:
        operation.add_schema([field for field in fields if field.name not in target_schema.names])
        if updates.num_rows == 0:
            return operation.finish(0, unmatched_rows)
        operation.phase = "write"
        operation.check_current()
        builder = operation.raw._copy_with_snapshot(operation.snapshot).new_batch_write_builder()
        commit = operation.keep(builder.new_commit())
        messages = builder.new_update().with_update_type(output_names).update_by_arrow_with_row_id(updates)
        return operation.finish(updates.num_rows, unmatched_rows, commit, messages)


def add_columns(owner, schema, *, inputs, fn, batch_size):
    if not isinstance(schema, pa.Schema):
        raise ValueError("schema must be a pyarrow.Schema.")
    _validate_names(schema.names, "Schema")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if any(not field.nullable for field in schema):
        raise ValueError("New columns must be nullable until backfill is complete.")
    fields = [_canonical_field(field) for field in schema]
    raw = owner.raw_table
    _validate_update(raw)
    existing = set(schema.names).intersection(raw.field_names)
    if existing:
        raise ValueError("Column already exists: %s" % sorted(existing))
    if fn is None:
        if inputs is not None:
            raise ValueError("inputs requires fn.")
    else:
        if not callable(fn) or inputs is None:
            raise ValueError("A callable fn and explicit inputs are required for backfill.")
        inputs = [inputs] if isinstance(inputs, str) else list(inputs)
        _validate_names(inputs, "inputs")
        missing = set(inputs).difference(raw.field_names)
        if missing:
            raise ValueError("Input columns do not exist: %s" % sorted(missing))
    with _ColumnUpdate(owner, raw) as operation:
        operation.add_schema(fields)
        if fn is None:
            return operation.finish(0)
        operation.phase = "compute"
        table = operation.raw.copy_without_time_travel({
            "blob-as-descriptor": "false", "read.batch-size": str(batch_size),
        })._copy_with_snapshot(operation.snapshot)
        builder = table.new_batch_write_builder()
        commit = operation.keep(builder.new_commit())
        update = builder.new_update().with_update_type(schema.names)
        update.with_read_projection(inputs)
        shard = operation.keep(update.new_shard_updator(0, 1))
        updated_rows = 0
        for batch in shard.arrow_reader():
            for start in range(0, batch.num_rows, batch_size):
                operation.phase = "compute"
                piece = batch.slice(start, batch_size)
                output = _normalize_output(fn(piece), pa.schema(fields))
                if output.num_rows != piece.num_rows:
                    raise ValueError("Column function must return one output row per input row, in input order.")
                operation.phase = "write"
                for output_batch in output.to_batches(max_chunksize=batch_size):
                    shard.update_by_arrow_batch(output_batch)
                updated_rows += output.num_rows
        operation.phase = "write"
        messages = shard.prepare_commit()
        return operation.finish(updated_rows, commit=commit, messages=messages)
