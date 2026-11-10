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

"""Field identity validation and structured row-ID conflicts shared by both backends."""

from pypaimon.schema.data_types import RowType, collect_field_ids
from pypaimon.table.special_fields import SpecialFields


def resolve_write_fields(schema, write_cols):
    """Resolve file write paths in their historical schema, matching Java ROW paths."""
    if write_cols is None:
        fields = schema.data_file_fields(None) if hasattr(schema, 'data_file_fields') else schema.fields
        return [field for field in fields if not SpecialFields.is_system_field(field.name)]
    top_fields = {field.name: field for field in schema.fields}
    result = []
    for name in write_cols:
        if SpecialFields.is_system_field(name):
            continue
        # Exact names containing dots take precedence at every ROW level.
        fields, path = top_fields, name
        while True:
            field = fields.get(path)
            if field is not None:
                break
            head, separator, path = path.partition('.')
            parent = fields.get(head)
            if not separator or parent is None or not isinstance(parent.type, RowType):
                break
            fields = {child.name: child for child in parent.type.fields}
        if field is None:
            raise RuntimeError("Column '{}' not found in schema {}".format(name, schema.id))
        result.append(field)
    return result


def validate_row_id_update_fields(current_schema, schema_manager, files):
    """Reject staged updates whose written field IDs are absent from this schema."""
    current_ids = set()
    for field in current_schema.fields:
        current_ids.add(field.id)
        collect_field_ids(field.type, current_ids)
    checked = set()
    for file in files:
        if file.first_row_id is None:
            continue
        key = (file.schema_id, None if file.write_cols is None else tuple(file.write_cols))
        if key in checked:
            continue
        schema = current_schema if file.schema_id == current_schema.id else schema_manager.get_schema(file.schema_id)
        if schema is None:
            raise RuntimeError('Schema {} not found'.format(file.schema_id))
        written_ids = set()
        for field in resolve_write_fields(schema, file.write_cols):
            written_ids.add(field.id)
            collect_field_ids(field.type, written_ids)
        if not written_ids.issubset(current_ids):
            raise RuntimeError('Row-id update cannot restore a dropped field')
        checked.add(key)


class RowIdExistenceConflict(RuntimeError):
    """A staged row-id file no longer matches the current base-file layout."""

    def __init__(self, entry):
        self.entry = entry
        self._initialize(entry.file.file_name, entry.file.first_row_id,
                         entry.file.row_count, entry.bucket)

    @classmethod
    def _from_fields(cls, file_name, first_row_id, row_count, bucket,
                     current_first_row_id=None, current_row_count=None):
        error = cls.__new__(cls)
        error.entry = None
        error._initialize(file_name, first_row_id, row_count, bucket,
                          current_first_row_id, current_row_count)
        return error

    @classmethod
    def _from_delete(cls, deleted, current):
        error = cls._from_fields(
            deleted.file.file_name, deleted.file.first_row_id,
            deleted.file.row_count, deleted.bucket,
            current.file.first_row_id, current.file.row_count)
        error.entry = deleted
        return error

    def _initialize(self, file_name, first_row_id, row_count, bucket,
                    current_first_row_id=None, current_row_count=None):
        self.file_name = file_name
        self.first_row_id = first_row_id
        self.row_count = row_count
        self.bucket = bucket
        self.current_first_row_id = current_first_row_id
        self.current_row_count = current_row_count
        first_id_text = 'null' if first_row_id is None else first_row_id
        if current_row_count is None:
            message = (
                "Row ID existence conflict: file '{}' references "
                "firstRowId={}, rowCount={} in bucket {}, "
                "but no matching file exists in the current snapshot. "
                "The referenced file may have been rewritten by a "
                "concurrent compaction or removed by an overwrite.".format(
                    file_name, first_id_text, row_count, bucket))
        else:
            message = (
                "Row ID existence conflict: DELETE for file '{}' references "
                "firstRowId={}, rowCount={} in bucket {}, but the current file "
                "has firstRowId={}, rowCount={}. The file may have been "
                "reassigned by a concurrent commit.".format(
                    file_name, first_id_text, row_count, bucket,
                    'null' if current_first_row_id is None else current_first_row_id, current_row_count))
        super().__init__(message)

    def __reduce__(self):
        # Ray transports errors between worker processes. RuntimeError's
        # default reduction would pass the formatted message as a file entry.
        return (_restore_row_id_conflict, (
            self.entry, self.file_name, self.first_row_id, self.row_count,
            self.bucket, self.current_first_row_id, self.current_row_count))


def _restore_row_id_conflict(entry, file_name, first_row_id, row_count, bucket,
                             current_first_row_id, current_row_count):
    error = RowIdExistenceConflict._from_fields(
        file_name, first_row_id, row_count, bucket,
        current_first_row_id, current_row_count)
    error.entry = entry
    return error


def check_row_id_conflicts(table, messages, latest_snapshot_id, check_from_snapshot):
    """Check logical writes before a rebase, without opening a publisher."""
    from pypaimon.write.update_validation import _authorize_update

    if (any(not isinstance(value, int) or isinstance(value, bool)
            or value <= 0 or value > (1 << 63) - 1
            for value in (latest_snapshot_id, check_from_snapshot))
            or check_from_snapshot > latest_snapshot_id):
        raise ValueError('Row-ID conflict checks require 0 < check-from snapshot <= latest snapshot')
    baselines = [message.check_from_snapshot for message in messages if message.check_from_snapshot is not None]
    invalid_baseline = any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > (1 << 63) - 1
        for value in baselines)
    missing_baseline = any(
        message.check_from_snapshot is None and any(
            file.first_row_id is not None for file in message.new_files + message.deleted_files)
        for message in messages)
    if (not baselines or invalid_baseline or min(baselines) != check_from_snapshot or missing_baseline):
        raise ValueError('Row-ID conflict check baseline must match the commit messages')
    _authorize_update(table)
    from pypaimon.write.backend_dispatch import execute_row_id_conflict_check
    execute_row_id_conflict_check(table, messages, latest_snapshot_id, check_from_snapshot)
