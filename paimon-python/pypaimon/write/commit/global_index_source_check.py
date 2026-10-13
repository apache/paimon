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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Revalidate unpublished indexes against the data scanned to build them."""

from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta
from pypaimon.utils.range import Range


def check_global_index_sources(detection, latest_snapshot, merged_entries, delta_entries, index_entries):
    if not detection.data_evolution_enabled:
        return None
    sources = []
    for entry in detection.global_index_file_additions(index_entries):
        meta = entry.index_file.global_index_meta
        if not DataEvolutionIndexSourceMeta.is_data_evolution_meta(meta.source_meta):
            continue
        source_id = DataEvolutionIndexSourceMeta.deserialize(meta.source_meta).scan_snapshot_id
        if latest_snapshot is None or source_id > latest_snapshot.id:
            return RuntimeError('Global index source conflict: source snapshot is not available.')
        if detection.snapshot_manager.get_snapshot_by_id(source_id) is None:
            return RuntimeError('Global index source conflict: source snapshot {} is missing.'.format(source_id))
        sources.append((source_id, entry))
    if not sources:
        return None
    conflict = detection.check_global_index_row_id_existence(merged_entries, [entry for _, entry in sources])
    if conflict is not None:
        return conflict
    conflict = _check_column_changes(detection, sources, delta_entries)
    if conflict is not None:
        return conflict
    earliest = min(source_id for source_id, _ in sources)
    for snapshot_id in range(earliest + 1, latest_snapshot.id + 1):
        snapshot = detection.snapshot_manager.get_snapshot_by_id(snapshot_id)
        if snapshot is None:
            return RuntimeError('Global index source conflict: snapshot {} is missing.'.format(snapshot_id))
        if snapshot.commit_kind == 'COMPACT':
            continue
        changes = detection.commit_scanner.read_incremental_raw_entries_from_changed_partitions(
            snapshot, [], index_entries=[entry for _, entry in sources])
        conflict = _check_column_changes(detection, sources, changes, snapshot_id)
        if conflict is not None:
            return conflict
    return None


def _check_column_changes(detection, sources, changes, snapshot_id=None):
    from pypaimon.write.commit.conflict_detection import RowIdColumnConflictChecker

    field_checker = RowIdColumnConflictChecker([], detection.table.schema_manager)
    for change in changes:
        row_range = change.file.row_id_range()
        if row_range is None:
            continue
        for source_id, index in sources:
            if snapshot_id is not None and snapshot_id <= source_id:
                continue
            if tuple(index.partition.values) != tuple(change.partition.values) or index.bucket != change.bucket:
                continue
            meta = index.index_file.global_index_meta
            if not row_range.overlaps(Range(meta.row_range_start, meta.row_range_end)):
                continue
            fields = {meta.index_field_id}
            fields.update(meta.extra_field_ids or [])
            if field_checker._contains_any_write_field(fields, change.file):
                return RuntimeError(
                    "Global index source conflict: indexed values changed after building "
                    "index file '{}'.".format(index.index_file.file_name))
    return None
