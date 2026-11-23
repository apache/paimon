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

from collections import defaultdict

from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta
from pypaimon.utils.range import Range


def check_global_index_sources(detection, latest_snapshot, merged_entries, delta_entries, index_entries):
    if not detection.data_evolution_enabled:
        return None
    snapshots = {latest_snapshot.id: latest_snapshot} if latest_snapshot is not None else {}
    sources = []
    for entry in detection.global_index_file_additions(index_entries):
        meta = entry.index_file.global_index_meta
        if meta.is_centroid_index() and not meta.is_centroid_router():
            # The manifest validates common source metadata and row ranges for the group.
            continue
        if not DataEvolutionIndexSourceMeta.is_data_evolution_meta(meta.source_meta):
            continue
        source_id = DataEvolutionIndexSourceMeta.deserialize(meta.source_meta).scan_snapshot_id
        if latest_snapshot is None or source_id > latest_snapshot.id:
            return RuntimeError('Global index source conflict: source snapshot is not available.')
        if source_id not in snapshots:
            snapshot = detection.snapshot_manager.get_snapshot_by_id(source_id)
            if snapshot is None:
                return RuntimeError('Global index source conflict: source snapshot {} is missing.'.format(source_id))
            snapshots[source_id] = snapshot
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
    centroid_sources = [(source_id, entry) for source_id, entry in sources
                        if entry.index_file.global_index_meta.is_centroid_router()]
    for snapshot_id in range(earliest + 1, latest_snapshot.id + 1):
        snapshot = snapshots.get(snapshot_id) or detection.snapshot_manager.get_snapshot_by_id(snapshot_id)
        if snapshot is None:
            return RuntimeError('Global index source conflict: snapshot {} is missing.'.format(snapshot_id))
        compact = snapshot.commit_kind == 'COMPACT'
        active = [(source_id, entry) for source_id, entry in (centroid_sources if compact else sources)
                  if source_id < snapshot_id]
        if not active:
            continue
        # Java's DELTA scan includes removed files. A deleted partial-column
        # file can expose an older value for a row still covered by other files.
        changes = detection.commit_scanner.read_incremental_raw_entries_from_changed_partitions(
            snapshot, [], index_entries=[entry for _, entry in active])
        conflict = _check_column_changes(detection, active, changes, snapshot_id)
        if conflict is not None:
            return conflict
    return None


def check_retained_centroid_indexes(detection, snapshot, existing_row_changes, index_entries):
    from pypaimon.index.index_file_handler import IndexFileHandler

    updates = [entry for entry in existing_row_changes if entry.kind == 0]
    deletes = [entry for entry in existing_row_changes if entry.kind == 1]
    removed = {entry.index_file.file_name for entry in index_entries if entry.kind == 1}

    def retained(entry):
        meta = entry.index_file.global_index_meta
        return (meta is not None and meta.is_centroid_router() and entry.index_file.file_name not in removed
                and (deletes or meta.has_centroid_bitmap_fields()))

    sources = [(snapshot.id, entry) for entry in IndexFileHandler(detection.table).scan(snapshot, retained)]
    if deletes and sources and _check_column_changes(detection, sources, deletes) is not None:
        raise RuntimeError("Delete affects an active centroid index. Prepare the delete again or drop the index.")
    bitmap_sources = [(source_id, entry) for source_id, entry in sources
                      if entry.index_file.global_index_meta.has_centroid_bitmap_fields()]
    if updates and bitmap_sources and _check_column_changes(detection, bitmap_sources, updates) is not None:
        raise RuntimeError("Update affects an active centroid index with Bitmap fields. "
                           "Drop the index or use DROP_PARTITION_INDEX before updating.")


def _check_column_changes(detection, sources, changes, snapshot_id=None):
    from pypaimon.execution.python.write.commit.conflict_detection import RowIdColumnConflictChecker

    field_checker = RowIdColumnConflictChecker([], detection.table.schema_manager)
    by_partition = defaultdict(list)
    for source_id, index in sources:
        by_partition[tuple(index.partition.values), index.bucket].append((source_id, index))
    for change in changes:
        row_range = change.file.row_id_range()
        if row_range is None:
            continue
        partition = (tuple(change.partition.values), change.bucket)
        for source_id, index in by_partition.get(partition, ()):
            if snapshot_id is not None and snapshot_id <= source_id:
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
