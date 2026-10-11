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

from pypaimon.schema.data_types import MapType, PyarrowFieldParser, is_blob_file_field
from pypaimon.common.options.core_options import ChangelogProducer, CoreOptions
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.snapshot.time_travel_util import SCAN_KEYS
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.native_commit import (
    create_native_write_table, from_native_commit_messages,
)
from pypaimon.write.native_write import native_write_available, _native_partition_types_supported
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.row_utils import _contains_blob_value, value_for_arrow


def _native_row_id_table(table):
    """Resolve an eligible table before creating a native row-ID writer."""
    if (type(table) is not FileStoreTable
            or not table.options.native_write_enabled()
            or not table.options.data_evolution_enabled()
            or not table.options.row_tracking_enabled()
            or table.is_primary_key_table
            or table.options.file_format() != 'parquet'
            or table.options.with_vector_format()
            or table.options.changelog_producer() != ChangelogProducer.NONE
            or any(table.options.options.contains_key(key) for key in SCAN_KEYS)
            or not native_write_available()):
        return None
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    if not _native_partition_types_supported(
            schema, table.partition_keys, table.options.options.get(CoreOptions.PARTITION_GENERATE_LEGACY_NAME)):
        return None
    return create_native_write_table(table)


def create_native_update(table, commit_user, columns):
    """Use the public core updater for direct and grouped row-ID updates."""
    if not _native_update_columns_supported(table, columns):
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update())
    if columns is not None:
        writer.with_update_type(columns)
    return NativeBatchTableUpdate(table, writer)


def create_native_update_by_row_id(table, commit_user, commit_identifier):
    """Create a core updater sharing one snapshot across incremental calls."""
    # Columns are selected per call; one core updater pins the snapshot and
    # tracks overlaps across Parquet and dedicated Blob updates.
    if not _native_update_columns_supported(table, None):
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    if commit_identifier == BATCH_COMMIT_IDENTIFIER:
        writer = (native_table.new_batch_write_builder()._with_commit_user(commit_user)
                  .new_update().new_update_by_row_id())
    else:
        writer = (native_table.new_stream_write_builder().with_commit_user(commit_user)
                  .new_update().new_update_by_row_id(commit_identifier))
    return NativeTableUpdateByRowId(
        table, commit_user, commit_identifier, writer)


def create_native_shard_updator(table, commit_user, projection, columns, shard, shard_count, stream=False):
    """Core owns whole-file sharding, the captured reader and positional updates."""
    from pypaimon.read.native_plan import native_method_available
    if not native_method_available('BatchTableUpdate', 'new_shard_updator'):
        return None
    if not _native_update_columns_supported(table, columns):
        return None
    # Nested output aliases are interpreted by Python's read builder.
    if projection is not None and any(
            name not in table.field_names and name != '_ROW_ID' for name in projection):
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    builder = (native_table.new_stream_write_builder().with_commit_user(commit_user) if stream else
               native_table.new_batch_write_builder()._with_commit_user(commit_user))
    update = builder.new_update()
    if projection is not None:
        update.with_read_projection(projection)
    if columns is not None:
        update.with_update_type(columns)
    return NativeShardTableUpdator(table, projection, update.new_shard_updator(shard, shard_count))


class NativeShardTableUpdator:
    """Transport Arrow batches and commit messages for a core shard updater."""

    def __init__(self, table, projection, writer):
        self.table = table
        self.writer = writer
        self.projection = projection

    def arrow_reader(self):
        from pypaimon.read.table_read import _ClosableArrowBatchReader
        builder = self.table.new_read_builder()
        if self.projection is not None:
            builder.with_projection(self.projection)
        read = builder.new_read()
        schema = PyarrowFieldParser.from_paimon_schema(builder.read_type())
        batches = read._convert_native_batches(self.writer.arrow_reader(), schema)
        return _ClosableArrowBatchReader(pa.ipc.RecordBatchReader.from_batches(schema, batches), batches)

    def update_by_arrow_batch(self, data):
        self.writer.update_by_arrow_batch(data)

    def prepare_commit(self):
        return from_native_commit_messages(self.table, self.writer.prepare_commit())

    def close(self):
        self.writer.close()


def _supported_upsert_key_type(data_type):
    return any(check(data_type) for check in (
        pa.types.is_boolean, pa.types.is_integer, pa.types.is_string,
        pa.types.is_large_string, pa.types.is_binary, pa.types.is_large_binary,
        pa.types.is_fixed_size_binary, pa.types.is_date, pa.types.is_decimal,
        pa.types.is_time, pa.types.is_timestamp,
        pa.types.is_floating,
    ))


def _native_update_columns_supported(table, columns):
    """Core writes Parquet columns and Java-compatible sparse Blob deltas."""
    if type(table) is not FileStoreTable:
        return False
    names = set(table.field_names if columns is None else columns)
    return not names.intersection(table.options.video_frame_fields())


def _upsert_row_batches(rows, fields, schema, table=None, blob_rows=None):
    """Encode consecutive row shapes without padding absent fields with NULL.

    Keep source order, including duplicates across shapes. Key matching,
    last-write-wins and matched/append column validation belong to Rust core.
    """
    batches = []
    start = 0
    while start < len(rows):
        names = set(rows[start])
        if not names <= set(schema.names):
            raise ValueError('upsert row fields must be in the table schema')
        end = start + 1
        while end < len(rows) and set(rows[end]) == names:
            end += 1
        selected = [field for field in fields if field.name in names]
        batch_schema = pa.schema([schema.field(field.name) for field in selected])
        if blob_rows is not None:
            batches.append(blob_rows.to_batch(table, rows[start:end], batch_schema.names))
        else:
            batches.append(pa.RecordBatch.from_pydict({
                field.name: [value_for_arrow(row[field.name], field) for row in rows[start:end]]
                for field in selected
            }, schema=batch_schema))
        start = end
    return batches


def create_native_upsert(table, commit_user, data, keys, columns):
    """Prepare one core upsert from Arrow columns or named row values."""
    if table.options.video_frame_fields() or not _native_update_columns_supported(table, columns):
        # An upsert can append unmatched rows; packed video writing is still
        # provided by the Python writer.
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    fields = table.table_schema.fields
    schema = PyarrowFieldParser.from_paimon_schema(fields)
    blob_rows = None
    if any(not _supported_upsert_key_type(schema.field(key).type) for key in set(keys + table.partition_keys)):
        return None
    if not isinstance(data, pa.Table):
        if not columns or not data:
            return None
        if any(_contains_blob_value(value) for row in data for value in row.values()) or any(
                is_blob_file_field(field) and isinstance(field.type, MapType) and field.name in row
                for field in fields for row in data):
            from pypaimon.write.native_blob_rows import NativeBlobRows
            blob_rows = NativeBlobRows()
        data = _upsert_row_batches(data, fields, schema, table, blob_rows)
    elif (len(data.column_names) != len(set(data.column_names))
            or not set(data.column_names) <= set(schema.names)
            or any(data.schema.field(name).type != schema.field(name).type
                   for name in data.column_names)):
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update()
              .with_update_type(columns))
    if blob_rows is not None:
        writer._with_blob_uri_reader_factory(blob_rows)
    return NativeTableUpsert(table, writer, keys, data, blob_rows)


def create_native_predicate_update(table, commit_user, predicate, columns=None):
    """Prepare a public core operation before any assignment can run."""
    if not _native_update_columns_supported(table, columns):
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    from pypaimon.read.native_plan import _predicate_to_native
    native_predicate = None if predicate is None else _predicate_to_native(predicate)
    if native_predicate is not None:
        # Check predicate translation while fallback is still safe. Planning,
        # reading and callback execution belong to the core operation below.
        native_table.new_read_builder().with_filter(native_predicate)
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user).new_update())
    return NativePredicateTableUpdate(table, writer, native_predicate)


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
    native_table = _native_row_id_table(table)
    if native_table is None:
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


class NativeBatchTableUpdate:
    """Wrap public core operations and decode their commit messages."""

    def __init__(self, table, writer):
        self.table = table
        self.writer = writer

    def update_by_arrow_with_row_id(self, data: pa.Table):
        try:
            messages = self.writer.update_by_arrow_with_row_id(data)
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


class NativeTableUpdateByRowId(TableUpdateByRowId):
    """Adapt Python row inputs around the core incremental Arrow updater."""

    def __init__(self, table, commit_user, commit_identifier, writer):
        self.table = table
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier
        self.writer = writer

    @property
    def commit_messages(self):
        return from_native_commit_messages(self.table, self.writer.commit_messages)

    def update_columns(self, data, column_names):
        try:
            messages = self.writer.update_columns(data, column_names)
        except ValueError as error:
            _raise_native_row_id_error(error)
        return from_native_commit_messages(self.table, messages)

    def _write_row_columns(self, data, column_names, blob_object_columns):
        # Row input normalization happens in the public Python API; physical
        # field layouts and Blob references are handled by the core updater.
        if not blob_object_columns:
            return self.update_columns(data, column_names)
        from pypaimon.write.native_blob_rows import NativeBlobRows
        blobs = NativeBlobRows()
        for name, values in blob_object_columns.items():
            field = self.table.field_dict[name]
            arrow_field = PyarrowFieldParser.from_paimon_field(field)
            if isinstance(field.type, MapType):
                # Arrow Map disallows NULL keys; Java's Blob map records allow
                # one. Core accepts row entries as a list of nullable-key structs.
                entries = pa.struct([pa.field('key', arrow_field.type.key_type),
                                     pa.field('value', arrow_field.type.item_type)])
                arrow_field = pa.field(name, pa.list_(entries), nullable=arrow_field.nullable)
            data = data.append_column(arrow_field, pa.array(
                [blobs.encode(value, field.type) for value in values], type=arrow_field.type))
        self.writer._with_blob_uri_reader_factory(blobs)
        try:
            return self.update_columns(data, column_names)
        finally:
            self.writer._with_blob_uri_reader_factory(None)


class NativeTableUpsert:
    """Submit Arrow tables or row-shape batches to the core Rust upsert writer."""

    def __init__(self, table, writer, keys, data, blob_rows=None):
        self.table = table
        self.writer = writer
        self.keys = keys
        self.data = data
        self.blob_rows = blob_rows

    def upsert(self):
        try:
            return from_native_commit_messages(
                self.table,
                self.writer.upsert_by_arrow_with_key(self.data, self.keys))
        finally:
            if self.blob_rows is not None:
                self.blob_rows.readers.clear()


class NativePredicateTableUpdate:
    """Convert operation inputs and commit messages around the core updater."""

    def __init__(self, table, writer, predicate):
        self.table = table
        self.writer = writer
        self.predicate = predicate

    def update(self, assignments, read_columns):
        return from_native_commit_messages(
            self.table,
            self.writer.update_by_predicate(
                self.predicate, dict(assignments), list(read_columns or ())))
