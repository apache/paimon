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

"""
Conflict detection for commit operations.
"""

import bisect

from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.file_entry import FileEntry
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper
from pypaimon.write.commit.commit_scanner import CommitScanner


class RowIdColumnConflictChecker:
    """Checks for row ID × column conflicts between delta files and committed files.

    Built from the current commit's delta files. For each committed file,
    checks whether it overlaps with the delta files on BOTH dimensions:
    row-id range AND write columns.
    """

    def __init__(self, write_ranges, schema_manager):
        self._write_ranges = write_ranges
        self._schema_manager = schema_manager
        self._field_id_cache = {}

    @classmethod
    def from_data_files(cls, schema_manager, delta_files):
        files_with_row_id = [f for f in delta_files if f.first_row_id is not None]
        if not files_with_row_id:
            return None

        range_helper = RangeHelper(lambda f: f.row_id_range())
        groups = range_helper.merge_overlapping_ranges(files_with_row_id)

        write_ranges = []
        for group in groups:
            merged_from = min(f.first_row_id for f in group)
            merged_to = max(f.first_row_id + f.row_count - 1 for f in group)
            merged_range = Range(merged_from, merged_to)

            field_ids = set()
            for f in group:
                cls._add_write_field_ids(field_ids, f, schema_manager)

            write_ranges.append(_WriteRange(merged_range, field_ids))

        write_ranges.sort(key=lambda wr: (wr.range.from_, wr.range.to))
        return cls(write_ranges, schema_manager)

    def is_empty(self):
        return len(self._write_ranges) == 0

    def conflicts_with(self, file):
        if file.first_row_id is None:
            return False

        file_range = Range(file.first_row_id, file.first_row_id + file.row_count - 1)
        index = self._first_possible_range(file_range)

        while index < len(self._write_ranges):
            wr = self._write_ranges[index]
            if wr.range.from_ > file_range.to:
                return False
            if wr.range.overlaps(file_range) and self._contains_any_write_field(wr.field_ids, file):
                return True
            index += 1

        return False

    def _first_possible_range(self, target):
        keys = [wr.range.to for wr in self._write_ranges]
        return bisect.bisect_left(keys, target.from_)

    def _contains_any_write_field(self, field_ids, file):
        if file.write_cols is None:
            return True
        for col_name in file.write_cols:
            fid = self._field_id(file, col_name)
            if fid is not None and fid in field_ids:
                return True
        return False

    def _field_id(self, file, col_name):
        if SpecialFields.is_system_field(col_name):
            return None
        name_to_id = self._field_id_by_name(file.schema_id)
        fid = name_to_id.get(col_name)
        if fid is None:
            raise RuntimeError(
                f"Column '{col_name}' not found in schema {file.schema_id}")
        return fid

    def _field_id_by_name(self, schema_id):
        if schema_id not in self._field_id_cache:
            schema = self._schema_manager.get_schema(schema_id)
            if schema is None:
                raise RuntimeError(f"Schema {schema_id} not found")
            self._field_id_cache[schema_id] = {
                field.name: field.id for field in schema.fields
            }
        return self._field_id_cache[schema_id]

    @classmethod
    def _add_write_field_ids(cls, field_ids, file, schema_manager):
        if file.write_cols is None:
            schema = schema_manager.get_schema(file.schema_id)
            if schema is not None:
                for field in schema.fields:
                    if not SpecialFields.is_system_field(field.name):
                        field_ids.add(field.id)
        else:
            name_to_id = {}
            schema = schema_manager.get_schema(file.schema_id)
            if schema is not None:
                name_to_id = {field.name: field.id for field in schema.fields}
            for col_name in file.write_cols:
                if SpecialFields.is_system_field(col_name):
                    continue
                fid = name_to_id.get(col_name)
                if fid is not None:
                    field_ids.add(fid)


class _WriteRange:

    def __init__(self, range_, field_ids):
        self.range = range_
        self.field_ids = field_ids


class ConflictDetection:
    """Detects conflicts between base and delta files during commit."""

    def __init__(self, data_evolution_enabled, snapshot_manager,
                 manifest_list_manager: ManifestListManager, table, commit_scanner: CommitScanner):
        self.data_evolution_enabled = data_evolution_enabled
        self.snapshot_manager = snapshot_manager
        self.manifest_list_manager = manifest_list_manager
        self.table = table
        self._row_id_check_from_snapshot = None
        self.commit_scanner = commit_scanner

    def should_be_overwrite_commit(self):
        return False

    def has_row_id_check_from_snapshot(self):
        return self._row_id_check_from_snapshot is not None

    def check_conflicts(self, latest_snapshot, base_entries, delta_entries, commit_kind):
        all_entries = list(base_entries) + list(delta_entries)

        try:
            merged_entries = FileEntry.merge_entries(all_entries)
        except Exception as e:
            return RuntimeError(
                "File deletion conflicts detected! Give up committing. " + str(e))

        if commit_kind != "COMPACT":
            next_row_id = latest_snapshot.next_row_id if latest_snapshot else None
            conflict = self.check_row_id_existence(
                base_entries, delta_entries, next_row_id)
            if conflict is not None:
                return conflict

        conflict = self.check_row_id_range_conflicts(commit_kind, merged_entries)
        if conflict is not None:
            return conflict

        return self.check_row_id_from_snapshot(latest_snapshot, delta_entries)

    def check_row_id_existence(self, base_entries, delta_entries, next_row_id=None):
        if not self.data_evolution_enabled:
            return None

        if next_row_id is None:
            return None

        files_to_check = [
            entry for entry in delta_entries
            if entry.kind == 0
            and entry.file.first_row_id is not None
            and entry.file.first_row_id < next_row_id
        ]

        if not files_to_check:
            return None

        existing_index = set()
        for base in base_entries:
            if base.file.first_row_id is not None:
                existing_index.add((
                    base.partition, base.bucket,
                    base.file.first_row_id, base.file.row_count))

        for entry in files_to_check:
            key = (entry.partition, entry.bucket,
                   entry.file.first_row_id, entry.file.row_count)
            if key not in existing_index:
                return RuntimeError(
                    "Row ID existence conflict: file '{}' references "
                    "firstRowId={}, rowCount={} in bucket {}, "
                    "but no matching file exists in the current snapshot. "
                    "The referenced file may have been rewritten by a "
                    "concurrent compaction or removed by an overwrite.".format(
                        entry.file.file_name,
                        entry.file.first_row_id,
                        entry.file.row_count,
                        entry.bucket))

        return None

    def check_row_id_range_conflicts(self, commit_kind, commit_entries):
        if not self.data_evolution_enabled:
            return None
        if self._row_id_check_from_snapshot is None and commit_kind != "COMPACT":
            return None

        entries_with_row_id = [
            entry for entry in commit_entries
            if entry.file.first_row_id is not None
        ]

        if not entries_with_row_id:
            return None

        range_helper = RangeHelper(lambda entry: entry.file.row_id_range())
        merged_groups = range_helper.merge_overlapping_ranges(entries_with_row_id)

        for group in merged_groups:
            data_files = [
                entry for entry in group
                if not DataFileMeta.is_blob_file(entry.file.file_name)
            ]
            if not range_helper.are_all_ranges_same(data_files):
                file_descriptions = [
                    "{name}(rowId={row_id}, count={count})".format(
                        name=entry.file.file_name,
                        row_id=entry.file.first_row_id,
                        count=entry.file.row_count,
                    )
                    for entry in data_files
                ]
                return RuntimeError(
                    "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' "
                    "operations have encountered conflicts, data files: "
                    + str(file_descriptions))

        return None

    def check_row_id_from_snapshot(self, latest_snapshot, commit_entries):
        if not self.data_evolution_enabled:
            return None
        if self._row_id_check_from_snapshot is None:
            return None

        delta_files = [entry.file for entry in commit_entries]
        column_checker = RowIdColumnConflictChecker.from_data_files(
            self.table.schema_manager, delta_files)
        if column_checker is None or column_checker.is_empty():
            return None

        check_snapshot = self.snapshot_manager.get_snapshot_by_id(
            self._row_id_check_from_snapshot)
        if check_snapshot is None or check_snapshot.next_row_id is None:
            raise RuntimeError(
                "Next row id cannot be null for snapshot "
                "{snapshot}.".format(snapshot=self._row_id_check_from_snapshot))
        check_next_row_id = check_snapshot.next_row_id

        for snapshot_id in range(
                self._row_id_check_from_snapshot + 1,
                latest_snapshot.id + 1):
            snapshot = self.snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                continue
            if snapshot.commit_kind == "COMPACT":
                continue

            incremental_entries = self.commit_scanner.read_incremental_entries_from_changed_partitions(
                snapshot, commit_entries)
            for entry in incremental_entries:
                file_range = entry.file.row_id_range()
                if file_range is None:
                    continue
                if file_range.from_ < check_next_row_id:
                    if column_checker.conflicts_with(entry.file):
                        return RuntimeError(
                            "For Data Evolution table, multiple 'MERGE INTO' "
                            "operations have encountered conflicts, updating "
                            "the same file, which can render some updates "
                            "ineffective.")

        return None
