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
    """Select Rust only before writing and only for its plain-Parquet path."""
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    # PyPaimon accepts Arrow's inferred list-of-pairs representation for MAP;
    # the Rust writer requires the exact nested Arrow type at this boundary.
    if any(pa.types.is_nested(schema.field(name).type)
           for name in columns if name in schema.names):
        return None
    native_table = _native_row_id_table(table, 'new_update')
    if native_table is None:
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_update(columns))
    return NativeBatchTableUpdate(table, writer)


def create_native_delete(table, commit_user):
    """Select Rust's deletion-vector writer for supported batch deletes."""
    if not table.options.deletion_vectors_enabled(False):
        return None
    native_table = _native_row_id_table(table, 'new_delete')
    if native_table is None:
        return None
    writer = (native_table.new_batch_write_builder()
              ._with_commit_user(commit_user)
              .new_delete())
    return NativeBatchTableDelete(table, writer)


def _raise_native_row_id_error(error):
    detail = str(error)
    if 'duplicate UPDATE operations' in detail:
        raise ValueError('duplicate _ROW_ID: ' + detail) from error
    if 'No file found for _ROW_ID' in detail:
        raise ValueError(
            detail + ' does not belong to any valid range') from error
    raise error


class NativeBatchTableUpdate:
    """Submit matched Arrow batches to Rust and decode its commit messages."""

    def __init__(self, table, writer):
        self.table = table
        self.writer = writer

    def update_by_arrow_with_row_id(self, data: pa.Table):
        try:
            for batch in data.to_batches():
                self.writer.add_matched_batch(batch)
            try:
                messages = self.writer.prepare_commit()
            except ValueError as error:
                # Preserve the public PyPaimon error contract while retaining
                # the native cause for diagnostics.
                _raise_native_row_id_error(error)
            return from_native_commit_messages(self.table, messages)
        finally:
            self.writer.close()


class NativeBatchTableDelete:
    """Submit row IDs to Rust's deletion-vector writer and decode messages."""

    def __init__(self, table, writer):
        self.table = table
        self.writer = writer

    def delete_by_row_id(self, row_ids):
        try:
            ids = []
            for row_id in row_ids:
                if row_id is None:
                    raise ValueError('_ROW_ID value must not be null.')
                ids.append(int(row_id))
            self.writer.add_row_ids(ids)
            try:
                messages = self.writer.prepare_commit()
            except ValueError as error:
                _raise_native_row_id_error(error)
            return from_native_commit_messages(self.table, messages)
        finally:
            self.writer.close()
