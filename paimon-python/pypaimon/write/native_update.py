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

"""Optional native row-ID updates for batch data-evolution tables."""

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.time_travel_util import SCAN_KEYS
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.native_commit import (
    create_native_write_table, from_native_commit_messages,
)
from pypaimon.write.native_write import native_write_available
from pypaimon.write.table_update_by_row_id import _RowIdUpdateFileWriter


def _native_row_id_table(table, builder_method):
    """Resolve an eligible table before creating a native row-ID writer."""
    if (type(table) is not FileStoreTable
            or not table.options.native_write_enabled()
            or not table.options.data_evolution_enabled()
            or not table.options.row_tracking_enabled()
            or not _RowIdUpdateFileWriter.supports_table(table)
            or any(table.options.options.contains_key(key) for key in SCAN_KEYS)
            or not native_write_available()):
        return None
    from pypaimon_rust.datafusion import BatchWriteBuilder
    if not hasattr(BatchWriteBuilder, builder_method):
        return None
    return create_native_write_table(table)


def create_native_update(table, commit_user, columns):
    """Use the public core updater for direct and grouped row-ID updates."""
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    selected = schema.names if columns is None else columns
    if any(pa.types.is_nested(schema.field(name).type)
           for name in selected if name in schema.names):
        return None
    native_table = _native_row_id_table(table, 'new_update')
    if native_table is None:
        return None
    from pypaimon_rust.datafusion import BatchTableUpdate
    if not hasattr(BatchTableUpdate, 'update_by_arrow_batches_with_row_id'):
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update())
    if columns is not None:
        writer.with_update_type(columns)
    return NativeBatchTableUpdate(table, writer, columns)


def _create_native_matched_update(table, commit_user, columns):
    """Select Rust only before writing and only for its plain-Parquet path."""
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    # PyPaimon accepts Arrow's inferred list-of-pairs representation for MAP;
    # the Rust writer requires the exact nested Arrow type at this boundary.
    if any(pa.types.is_nested(schema.field(name).type)
           for name in columns if name in schema.names):
        return None
    native_table = _native_row_id_table(table, '_new_matched_update')
    if native_table is None:
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              ._new_matched_update(columns))
    try:
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if snapshot is not None:
            writer.pin_read_snapshot(snapshot.id)
    except Exception:
        writer.close()
        raise
    return _NativeMatchedUpdate(table, writer)


def create_native_upsert(table, commit_user, data, keys, columns):
    """Select the core Rust upsert for full Arrow rows on plain Parquet."""
    if table.partition_keys:
        return None
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    if (len(data.column_names) != len(schema.names)
            or set(data.column_names) != set(schema.names)
            or any(data.schema.field(name).type != schema.field(name).type
                   for name in schema.names)
            or any(pa.types.is_nested(schema.field(name).type)
                   for name in columns)):
        return None
    native_table = _native_row_id_table(table, 'new_update')
    if native_table is None:
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update()
              .with_update_type(columns))
    if not hasattr(writer, 'upsert_by_arrow_with_key'):
        return None
    return NativeTableUpsert(table, writer, keys)


def create_native_predicate_update(table, scan_table, commit_user, columns,
                                   predicate, projection):
    """Prepare Rust predicate reading and assignment writing before callbacks."""
    native = _create_native_matched_update(table, commit_user, columns)
    if native is None or not hasattr(native.writer, 'add_assigned_table'):
        if native is not None:
            native.writer.close()
        return None
    from pypaimon.read.native_plan import (
        _prepare_native_read, native_split_bridge_available,
        native_split_from_python,
    )
    if not native_split_bridge_available():
        native.writer.close()
        return None
    try:
        reader = _prepare_native_read(
            scan_table, predicate=predicate, projection=projection
        )
    except Exception:
        native.writer.close()
        raise
    return NativePredicateTableUpdate(native, reader, native_split_from_python)


def native_predicate_row_ids(scan_table, predicate, splits):
    """Match a batch delete predicate in Rust and return its row IDs."""
    from pypaimon.read.native_plan import (
        _prepare_native_read, native_split_bridge_available,
        native_split_from_python,
    )
    if not native_split_bridge_available():
        return None
    reader = _prepare_native_read(
        scan_table, predicate=predicate, projection=['_ROW_ID']
    )
    row_ids = []
    for split in splits:
        for batch in reader([native_split_from_python(split)]):
            row_ids.extend(batch.column('_ROW_ID').to_pylist())
    return row_ids


def create_native_delete(table, commit_user):
    """Select Rust's deletion-vector writer for supported batch deletes."""
    if not table.options.deletion_vectors_enabled(False):
        return None
    native_table = _native_row_id_table(table, 'new_update')
    if native_table is None:
        return None
    from pypaimon_rust.datafusion import BatchTableUpdate
    if not hasattr(BatchTableUpdate, 'delete_by_row_id'):
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update())
    return NativeBatchTableUpdate(table, writer)


def _raise_native_row_id_error(error):
    detail = str(error)
    if 'duplicate UPDATE operations' in detail:
        raise ValueError('duplicate _ROW_ID: ' + detail) from error
    if 'No file found for _ROW_ID' in detail:
        raise ValueError(
            detail + ' does not belong to any valid range') from error
    raise error


class _NativeMatchedUpdate:
    """Private predicate bridge until scan orchestration moves into core."""

    def __init__(self, table, writer):
        self.table = table
        self.writer = writer


class NativeBatchTableUpdate:
    """Wrap public core operations and decode their commit messages."""

    def __init__(self, table, writer, columns=None):
        self.table = table
        self.writer = writer
        self.columns = columns
        self.row_id_updater = None

    def pin_read_snapshot(self, snapshot_id):
        self.row_id_updater = self.writer.new_update_by_row_id()
        self.row_id_updater._pin_read_snapshot(snapshot_id)

    def update_by_arrow_with_row_id(self, data: pa.Table):
        try:
            if self.row_id_updater is None:
                messages = self.writer.update_by_arrow_with_row_id(data)
            else:
                messages = self.row_id_updater.update_columns(data, self.columns)
        except ValueError as error:
            _raise_native_row_id_error(error)
        return from_native_commit_messages(self.table, messages)

    def update_by_arrow_batches_with_row_id(self, tables):
        try:
            messages = self.writer.update_by_arrow_batches_with_row_id(tables)
        except ValueError as error:
            _raise_native_row_id_error(error)
        return from_native_commit_messages(self.table, messages)

    def delete_by_row_id(self, row_ids):
        ids = []
        for row_id in row_ids:
            if row_id is None:
                raise ValueError('_ROW_ID value must not be null.')
            ids.append(int(row_id))
        try:
            messages = self.writer.delete_by_row_id(ids)
        except ValueError as error:
            _raise_native_row_id_error(error)
        return from_native_commit_messages(self.table, messages)


class NativeTableUpsert:
    """Submit full Arrow rows to the core Rust upsert writer."""

    def __init__(self, table, writer, keys):
        self.table = table
        self.writer = writer
        self.keys = keys

    def upsert(self, data: pa.Table):
        return from_native_commit_messages(
            self.table,
            self.writer.upsert_by_arrow_with_key(data, self.keys))


class NativePredicateTableUpdate:
    """Rust reads predicate matches and evaluates per-group assignments."""

    def __init__(self, native, reader, convert_split):
        self.native = native
        self.reader = reader
        self.convert_split = convert_split

    def update(self, groups, assignments, schema, combine_all=False):
        writer = self.native.writer
        try:
            native_groups = [self.convert_split(split) for split in groups]
            if combine_all:
                batches = []
                for split in native_groups:
                    batches.extend(self.reader([split]))
                if batches:
                    writer.add_assigned_table(
                        pa.Table.from_batches(batches), assignments, schema
                    )
            else:
                for split in native_groups:
                    batches = list(self.reader([split]))
                    if batches:
                        writer.add_assigned_table(
                            pa.Table.from_batches(batches), assignments, schema
                        )
            return from_native_commit_messages(
                self.native.table, writer.prepare_commit()
            )
        finally:
            writer.close()
