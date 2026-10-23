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

"""Structured row-ID conflicts shared by both execution backends."""


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
    if table.options.native_commit_enabled():
        from pypaimon.execution.native.write.native_commit import prepare_native_row_id_conflict_check
        # Unsupported configurations return None explicitly. Permission or
        # transport failures must never restart the check in another backend.
        native = prepare_native_row_id_conflict_check(table, messages)
        if native is not None:
            commit, native_messages = native
            failure = None
            try:
                commit.check_row_id_conflicts(native_messages, latest_snapshot_id, check_from_snapshot)
            except BaseException as error:
                failure = error
                raise
            finally:
                try:
                    commit.close()
                except BaseException:
                    if failure is None:
                        raise
            return
    from pypaimon.execution.python.write.commit.row_id_conflict import check_row_id_conflicts as check
    check(table, messages, latest_snapshot_id, check_from_snapshot)
