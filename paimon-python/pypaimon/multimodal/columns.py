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
    _to_arrow_table, _validate_merge_on, _validate_row_id_update_schema,
)
from pypaimon.schema.arrow_schema import normalize_arrow_write_nullability
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.time_travel_util import SCAN_KEYS
from pypaimon.table.data_evolution_merge_into import WhenMatched, source_col
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.row_input import normalize_row_ids
from pypaimon.write.update_validation import _authorize_update


@dataclass(frozen=True)
class ColumnUpdateResult:
    """Schema and data versions produced by a successful column operation.

    ``updated_rows`` is available for ``add_columns``, ``compute_columns``,
    and ``update_columns_by_row_id``
    and is ``None`` for ``merge_columns``, whose MERGE executor does not return
    logical row counts.
    """

    source_snapshot_id: Optional[int]
    snapshot_id: Optional[int]
    schema_id: int
    updated_rows: Optional[int]
    added_columns: Tuple[str, ...]


class ColumnUpdateError(RuntimeError):
    """A failed enrichment, with its last known publication state.

    ``schema_changed=None`` means the schema request had an unknown outcome.
    ``commit_outcome`` is ``not_started``, ``unknown``, or ``committed``. Never
    retry blindly after an unknown outcome; inspect the table first. The
    underlying error is retained as ``__cause__``. Ray preserves a worker
    exception's type only if it can serialize and deserialize the exception.
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

    def add_schema(self, fields):
        if not fields:
            return
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

    def finish(self, updated_rows, commit=None, messages=()):
        if messages:
            self.phase = "commit"
            self.commit_outcome = "unknown"
            commit.commit(messages)
            self.commit_outcome = "committed"
        self.phase = "result"
        snapshot = self.raw.snapshot_manager().get_latest_snapshot()
        return ColumnUpdateResult(
            self.source_snapshot_id, None if snapshot is None else snapshot.id,
            self.raw.table_schema.id, updated_rows, self.added_columns)


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


def _validate_batch_size(batch_size):
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")


def _validate_inputs(raw, inputs, fn):
    if not callable(fn) or inputs is None:
        raise ValueError("A callable fn and explicit inputs are required for column computation.")
    inputs = [inputs] if isinstance(inputs, str) else list(inputs)
    _validate_names(inputs, "inputs")
    missing = set(inputs).difference(raw.field_names)
    if missing:
        raise ValueError("Input columns do not exist: %s" % sorted(missing))
    return inputs


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
            # Same-type casts can reject child NULLs hidden by a NULL parent.
            if array.type != source_type:
                array = array.cast(source_type)
            # Read schemas relax nested nullability; restore it before comparing types.
            source = pa.Table.from_arrays([array], names=[field.name])
            array = normalize_arrow_write_nullability(source, pa.schema([field]))[field.name]
            if array.type != field.type:
                raise ValueError("Incompatible type for column %s: %s != %s"
                                 % (field.name, array.type, field.type))
        if not field.nullable and array.null_count:
            raise ValueError("Cannot write null to non-null column %s." % field.name)
        arrays.append(array if array.type == field.type else array.cast(field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def merge_columns(owner, data, *, on):
    raw = owner.raw_table
    _validate_update(raw)
    data = _to_arrow_table(data)
    _validate_names(data.column_names, "Source")
    target_keys, source_keys = _normalize_merge_on(on, "merge_columns")
    target_schema = _target_schema(raw)
    _validate_merge_on(target_schema, data, target_keys, source_keys, "merge_columns")
    data = _cast_merge_on_columns(data, target_schema, target_keys, source_keys)
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
    source = data
    for field, column in zip(output.schema, output.columns):
        source = source.set_column(source.schema.get_field_index(field.name), field, column)
    with _ColumnUpdate(owner, raw) as operation:
        operation.add_schema([field for field in fields if field.name not in target_schema.names])
        if source.num_rows == 0:
            return operation.finish(None)
        operation.phase = "write"
        builder = operation.raw._copy_with_snapshot(operation.snapshot).new_batch_write_builder()
        commit = operation.keep(builder.new_commit())
        messages = builder.new_update().merge_into(
            source, on=dict(zip(target_keys, source_keys)),
            when_matched=[WhenMatched.update({name: source_col(name) for name in output_names})])
        return operation.finish(None, commit=commit, messages=messages)


def update_columns_by_row_id(owner, data, *, columns):
    if not isinstance(data, pa.Table):
        raise ValueError("data must be a pyarrow.Table.")
    raw = owner.raw_table
    _validate_update(raw)
    if not raw.options.data_evolution_enabled() or not raw.options.row_tracking_enabled():
        raise ValueError("Row ID column updates require a Data Evolution table with row tracking.")
    _validate_row_id_update_schema(owner)
    row_id = SpecialFields.ROW_ID.name
    if data.column_names.count(row_id) != 1:
        raise ValueError("Input must contain exactly one _ROW_ID column.")
    columns = [columns] if isinstance(columns, str) else list(columns)
    _validate_names(columns, "Output")
    missing = set(columns).difference(raw.field_names)
    if missing:
        raise ValueError("Output columns do not exist: %s" % sorted(missing))
    if set(columns).intersection(raw.partition_keys):
        raise ValueError("update_columns_by_row_id cannot update partition columns.")
    if set(columns).intersection(raw.options.video_frame_fields()):
        raise ValueError("Use replace_video() to update video-frame columns.")
    for name in columns:
        if data.column_names.count(name) != 1:
            raise ValueError("Input must contain exactly one %s column." % name)
    row_ids = normalize_row_ids(data[row_id])
    if len(pc.unique(row_ids)) != data.num_rows:
        raise ValueError("Input data contains duplicate _ROW_ID values.")
    if data.num_rows and pc.min(row_ids).as_py() < 0:
        raise ValueError("_ROW_ID values must be non-negative.")
    target_schema = _target_schema(raw)
    schema = pa.schema([target_schema.field(name) for name in columns])
    source = _normalize_output(data.select(columns), schema).append_column(row_id, row_ids)
    with _ColumnUpdate(owner, raw) as operation:
        if source.num_rows == 0:
            return operation.finish(0)
        operation.phase = "validate"
        table = raw._copy_with_snapshot(operation.snapshot)
        read = table.new_read_builder().with_projection([row_id])
        # Read only the requested row IDs, retaining deletion-vector filtering.
        plan = read.new_scan().with_row_ranges(Range.to_ranges(row_ids.to_pylist())).plan_all()
        visible = read.new_read().to_arrow(plan.splits())[row_id]
        missing = pc.is_null(pc.index_in(row_ids, value_set=visible))
        if pc.any(missing).as_py():
            raise ValueError("Row IDs not found or deleted: %s" % row_ids.filter(missing).to_pylist())
        operation.phase = "write"
        builder = table.new_batch_write_builder()
        commit = operation.keep(builder.new_commit())
        messages = builder.new_update().with_update_type(columns).update_by_arrow_with_row_id(source)
        return operation.finish(source.num_rows, commit=commit, messages=messages)


def add_columns(owner, schema, *, inputs, fn, batch_size):
    if not isinstance(schema, pa.Schema):
        raise ValueError("schema must be a pyarrow.Schema.")
    _validate_names(schema.names, "Schema")
    _validate_batch_size(batch_size)
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
        inputs = _validate_inputs(raw, inputs, fn)
    with _ColumnUpdate(owner, raw) as operation:
        operation.add_schema(fields)
        if fn is None:
            return operation.finish(0)
        return _compute_columns(operation, pa.schema(fields), inputs, fn, batch_size)


def compute_columns(owner, columns, *, inputs, fn, batch_size,
                    execution="local", concurrency=None, ray_remote_args=None):
    if execution not in ("local", "ray"):
        raise ValueError("execution must be 'local' or 'ray'.")
    if execution == "local" and (concurrency is not None or ray_remote_args is not None):
        raise ValueError("Ray options require execution='ray'.")
    columns = [columns] if isinstance(columns, str) else list(columns)
    _validate_names(columns, "Output")
    _validate_batch_size(batch_size)
    raw = owner.raw_table
    _validate_update(raw)
    missing = set(columns).difference(raw.field_names)
    if missing:
        raise ValueError("Output columns do not exist: %s" % sorted(missing))
    if set(columns).intersection(raw.partition_keys):
        raise ValueError("compute_columns cannot update partition columns.")
    if set(columns).intersection(raw.options.video_frame_fields()):
        raise ValueError("Use replace_video() to update video-frame columns.")
    inputs = _validate_inputs(raw, inputs, fn)
    target_schema = _target_schema(raw)
    schema = pa.schema([target_schema.field(name) for name in columns])
    if execution == "ray":
        from pypaimon.ray.compute_columns import validate_ray_options
        concurrency, ray_remote_args = validate_ray_options(concurrency, ray_remote_args)
    with _ColumnUpdate(owner, raw) as operation:
        return _compute_columns(operation, schema, inputs, fn, batch_size, execution=execution,
                                concurrency=concurrency, ray_remote_args=ray_remote_args)


def _compute_columns(operation, schema, inputs, fn, batch_size, *,
                     execution="local", concurrency=None, ray_remote_args=None):
    operation.phase = "compute"
    options = {
        "blob-as-descriptor": "false", "read.batch-size": str(batch_size),
    }
    if execution == "ray":
        options[CoreOptions.SCALAR_INDEX_SEARCH_MODE.key()] = "full"
    table = operation.raw.copy_without_time_travel(options)._copy_with_snapshot(operation.snapshot)
    builder = table.new_batch_write_builder()
    commit = operation.keep(builder.new_commit())
    if execution == "local":
        update = builder.new_update().with_update_type(schema.names)
        update.with_read_projection(inputs)
        shard = operation.keep(update.new_shard_updator(0, 1))
        messages, updated_rows = _prepare_column_shard(shard, schema, fn, batch_size, operation)
    else:
        from pypaimon.ray.compute_columns import prepare_ray_column_update
        messages, updated_rows = prepare_ray_column_update(
            operation, table, builder.commit_user, inputs, schema, fn, batch_size, concurrency, ray_remote_args)
    return operation.finish(updated_rows, commit=commit, messages=messages)


def _prepare_column_shard(shard, schema, fn, batch_size, progress):
    updated_rows = 0
    for batch in shard.arrow_reader():
        for start in range(0, batch.num_rows, batch_size):
            progress.phase = "compute"
            piece = batch.slice(start, batch_size)
            output = _normalize_output(fn(piece), schema)
            if output.num_rows != piece.num_rows:
                raise ValueError("Column function must return one output row per input row, in input order.")
            progress.phase = "write"
            for output_batch in output.to_batches(max_chunksize=batch_size):
                shard.update_by_arrow_batch(output_batch)
            updated_rows += output.num_rows
    progress.phase = "write"
    return shard.prepare_commit(), updated_rows
