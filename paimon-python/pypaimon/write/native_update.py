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
from pypaimon.write.native_write import native_write_available, _native_partition_types_supported
from pypaimon.write.table_update_by_row_id import _RowIdUpdateFileWriter
from pypaimon.write.row_utils import value_for_arrow


def _native_row_id_table(table, builder_method):
    """Resolve an eligible table before creating a native row-ID writer."""
    if (type(table) is not FileStoreTable
            or not table.options.native_write_enabled()
            or not table.options.data_evolution_enabled()
            or not table.options.row_tracking_enabled()
            or table.options.data_file_path_directory() is not None
            or not _RowIdUpdateFileWriter.supports_table(table)
            or any(table.options.options.contains_key(key) for key in SCAN_KEYS)
            or not native_write_available()):
        return None
    from pypaimon_rust.datafusion import BatchWriteBuilder
    if not hasattr(BatchWriteBuilder, builder_method):
        return None
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    if not _native_partition_types_supported(schema, table.partition_keys):
        return None
    if not _native_update_paths_supported(table):
        return None
    return create_native_write_table(table)


def _native_update_paths_supported(table):
    """Keep legacy Python partition directories on the path-aware Python updater."""
    if not table.partition_keys:
        return True
    factory = table.path_factory()
    # Core update scans for itself, unlike native reads which receive splits
    # with repaired file paths. Decide before callbacks or staging can start.
    splits = table.new_read_builder().new_scan().plan_for_write().splits()
    bucket_files = {}
    for split in splits:
        partition = tuple(split.partition.values)
        bucket_path = factory.bucket_path(partition, split.bucket)
        if bucket_path == factory.bucket_path(partition, split.bucket, canonical_partition=True):
            continue
        candidates = [file for file in split.files if not file.external_path]
        if not candidates:
            continue
        if bucket_path not in bucket_files:
            bucket_files[bucket_path] = {
                status.base_name for status in table.file_io.list_status(bucket_path)
            }
        # Python reads prefer an existing legacy path even if a canonical copy
        # also exists. Canonical-only files and explicit external paths are safe.
        if any(file.file_name in bucket_files[bucket_path] for file in candidates):
            return False
    return True


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
    return NativeBatchTableUpdate(table, writer)


def _supported_upsert_key_type(data_type):
    return any(check(data_type) for check in (
        pa.types.is_boolean, pa.types.is_integer, pa.types.is_string,
        pa.types.is_large_string, pa.types.is_binary, pa.types.is_large_binary,
        pa.types.is_fixed_size_binary, pa.types.is_date, pa.types.is_decimal,
    ))


def create_native_upsert(table, commit_user, data, keys, columns):
    """Prepare one core upsert from full Arrow rows or named row values."""
    if table.partition_keys:
        return None
    native_table = _native_row_id_table(table, 'new_update')
    if native_table is None:
        return None
    fields = table.table_schema.fields
    schema = PyarrowFieldParser.from_paimon_schema(fields)
    if (any(not _supported_upsert_key_type(schema.field(key).type) for key in keys)
            or any(pa.types.is_nested(schema.field(name).type) for name in columns)):
        return None
    if not isinstance(data, pa.Table):
        # Missing fields retain their row-object semantics on the fallback
        # path; converting them to Arrow NULLs would change update behavior.
        if not columns or any(set(values) != set(schema.names) for values in data):
            return None
        data = pa.Table.from_pydict({
            field.name: [value_for_arrow(values[field.name], field) for values in data]
            for field in fields
        }, schema=schema)
    if (len(data.column_names) != len(schema.names)
            or set(data.column_names) != set(schema.names)
            or any(data.schema.field(name).type != schema.field(name).type
                   for name in schema.names)):
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update()
              .with_update_type(columns))
    if not hasattr(writer, 'upsert_by_arrow_with_key'):
        return None
    return NativeTableUpsert(table, writer, keys, data)


def create_native_predicate_update(table, commit_user, columns, predicate):
    """Prepare a public core operation before any assignment can run."""
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    if any(pa.types.is_nested(schema.field(name).type)
           for name in columns if name in schema.names):
        return None
    native_table = _native_row_id_table(table, 'new_update')
    if native_table is None:
        return None
    from pypaimon_rust.datafusion import BatchTableUpdate
    if not hasattr(BatchTableUpdate, 'update_by_predicate'):
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


class NativeTableUpsert:
    """Submit full Arrow rows to the core Rust upsert writer."""

    def __init__(self, table, writer, keys, data):
        self.table = table
        self.writer = writer
        self.keys = keys
        self.data = data

    def upsert(self):
        return from_native_commit_messages(
            self.table,
            self.writer.upsert_by_arrow_with_key(self.data, self.keys))


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
