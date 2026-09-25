# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass
from typing import List, Optional

import pyarrow as pa

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.split import DataSplit
from pypaimon.read.table_read import TableRead
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


@dataclass
class RowIdRewriteResult:
    commit_entries: List[ManifestEntry]
    rewritten_file_count: int


class RowIdConflictRewriter:
    """Rebase stale partial-column files onto current row-id file ranges."""

    def __init__(
            self,
            table,
            commit_user: str,
            commit_identifier: int,
            max_rewrite_size: int):
        self.table = table
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier
        self.max_rewrite_size = max_rewrite_size

    def rewrite(
            self,
            latest_snapshot,
            base_entries: List[ManifestEntry],
            delta_entries: List[ManifestEntry],
    ) -> Optional[RowIdRewriteResult]:
        if self.max_rewrite_size <= 0:
            return None
        if self.table.options.deletion_vectors_enabled(False):
            return None

        current_exact_ranges = {
            self._range_key(entry)
            for entry in base_entries
            if self._is_normal_row_id_file(entry.file)
        }
        candidates = [
            entry
            for entry in delta_entries
            if self._is_rewrite_candidate(
                entry, current_exact_ranges, latest_snapshot.next_row_id)
        ]
        if not candidates:
            return None

        candidate_ids = {id(entry) for entry in candidates}
        if any(
                self._is_dedicated_file(entry.file)
                and entry.file.first_row_id is not None
                and entry.file.first_row_id < latest_snapshot.next_row_id
                for entry in delta_entries
        ):
            return None

        if not self._ranges_are_still_covered(base_entries, candidates):
            return None

        affected_files = self._affected_current_files(base_entries, candidates)
        affected_size = sum(file.file_size for file in affected_files)
        if affected_size > self.max_rewrite_size:
            raise RuntimeError(
                "Automatic row-id conflict rewrite requires reading "
                "{} bytes across {} current data file(s), exceeding "
                "'data-evolution.row-id-conflict-rewrite.max-size'={} bytes."
                .format(
                    affected_size,
                    len(affected_files),
                    self.max_rewrite_size,
                )
            )

        new_messages = []
        try:
            files_info = TableUpdateByRowId._files_info_from_entries(
                self.table,
                latest_snapshot.id,
                base_entries,
            )
            groups = self._group_by_write_columns(candidates)
            for column_names, entries in groups:
                update_tables = [
                    self._read_staged_update(entry, column_names)
                    for entry in entries
                ]
                update_data = (
                    update_tables[0]
                    if len(update_tables) == 1
                    else pa.concat_tables(update_tables)
                )
                row_ids = update_data[SpecialFields.ROW_ID.name].to_pylist()
                if len(row_ids) != len(set(row_ids)):
                    raise RuntimeError(
                        "Automatic row-id conflict rewrite cannot merge "
                        "overlapping staged files for columns {}.".format(
                            column_names)
                    )
                updater = TableUpdateByRowId(
                    self.table,
                    self.commit_user,
                    self.commit_identifier,
                    _precomputed_files_info=files_info,
                )
                try:
                    new_messages.extend(
                        updater.update_columns(update_data, column_names)
                    )
                except Exception:
                    self._abort(updater.commit_messages)
                    raise
        except Exception:
            self._abort(new_messages)
            raise

        rewritten_entries = [
            entry for entry in delta_entries if id(entry) not in candidate_ids
        ]
        rewritten_entries.extend(self._to_manifest_entries(new_messages))
        return RowIdRewriteResult(
            commit_entries=rewritten_entries,
            rewritten_file_count=len(candidates),
        )

    def _is_rewrite_candidate(
            self, entry, current_exact_ranges, next_row_id):
        file = entry.file
        write_cols = self._partial_file_write_cols(file)
        return (
            self._is_normal_row_id_file(file)
            and file.first_row_id < next_row_id
            and bool(write_cols)
            and not any(
                SpecialFields.is_system_field(name)
                for name in write_cols
            )
            and self._range_key(entry) not in current_exact_ranges
        )

    def _ranges_are_still_covered(self, base_entries, candidates):
        current_ranges = {}
        for entry in base_entries:
            if not self._is_normal_row_id_file(entry.file):
                continue
            key = (tuple(entry.partition.values), entry.bucket)
            current_ranges.setdefault(key, []).append(
                entry.file.row_id_range())
        current_ranges = {
            key: Range.sort_and_merge_overlap(ranges, True, True)
            for key, ranges in current_ranges.items()
        }

        for entry in candidates:
            key = (tuple(entry.partition.values), entry.bucket)
            if entry.file.row_id_range().exclude(
                    current_ranges.get(key, [])):
                return False
        return True

    def _affected_current_files(self, base_entries, candidates):
        affected = {}
        for base in base_entries:
            if not self._is_normal_row_id_file(base.file):
                continue
            base_key = (tuple(base.partition.values), base.bucket)
            for candidate in candidates:
                candidate_key = (
                    tuple(candidate.partition.values), candidate.bucket)
                if (
                    base_key == candidate_key
                    and base.file.row_id_range().overlaps(
                        candidate.file.row_id_range())
                ):
                    key = (
                        base_key,
                        base.file.external_path or base.file.file_path
                        or base.file.file_name,
                    )
                    affected[key] = base.file
                    break
        return list(affected.values())

    def _group_by_write_columns(self, candidates):
        groups = {}
        for entry in candidates:
            key = tuple(self._partial_file_write_cols(entry.file))
            groups.setdefault(key, []).append(entry)
        return [
            (list(columns), entries)
            for columns, entries in groups.items()
        ]

    def _partial_file_write_cols(self, file):
        schema = (
            self.table.table_schema
            if file.schema_id == self.table.table_schema.id
            else self.table.schema_manager.get_schema(file.schema_id)
        )
        if schema is None:
            raise RuntimeError(f"Schema {file.schema_id} not found")
        return schema.partial_file_write_cols(file.write_cols)

    def _read_staged_update(self, entry, column_names):
        read_fields = [self.table.field_dict[name] for name in column_names]
        read_fields.append(SpecialFields.ROW_ID)
        split = DataSplit(
            files=[entry.file],
            partition=entry.partition,
            bucket=entry.bucket,
            raw_convertible=True,
        )
        result = TableRead(
            self.table,
            predicate=None,
            read_type=read_fields,
        ).to_arrow([split])
        if result.num_rows != entry.file.row_count:
            raise RuntimeError(
                "Automatic row-id conflict rewrite read {} rows from staged "
                "file '{}', expected {}.".format(
                    result.num_rows,
                    entry.file.file_name,
                    entry.file.row_count,
                )
            )
        return result

    def _to_manifest_entries(self, messages: List[CommitMessage]):
        entries = []
        for message in messages:
            partition = GenericRow(
                list(message.partition),
                self.table.partition_keys_fields,
            )
            for file in message.new_files:
                entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=message.bucket,
                    total_buckets=self.table.total_buckets,
                    file=file,
                ))
        return entries

    def _abort(self, messages):
        for message in messages:
            for file in message.new_files:
                path = file.external_path or file.file_path
                if path:
                    self.table.file_io.delete_quietly(path)

    @staticmethod
    def _range_key(entry):
        return (
            tuple(entry.partition.values),
            entry.bucket,
            entry.file.first_row_id,
            entry.file.row_count,
        )

    @staticmethod
    def _is_normal_row_id_file(file):
        return (
            file.first_row_id is not None
            and not RowIdConflictRewriter._is_dedicated_file(file)
        )

    @staticmethod
    def _is_dedicated_file(file):
        return (
            DataFileMeta.is_blob_file(file.file_name)
            or DataFileMeta.is_vector_file(file.file_name)
        )
