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

"""Live-row filtering shared by global-index based search readers."""

from typing import Optional

from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import DataSplit
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def live_rows(table, partition_filter=None, snapshot=None, row_ranges=None) -> Optional[RoaringBitmap64]:
    """Return live global row ids at ``snapshot`` for deletion-vector tables.

    ``None`` means no live-row filter is needed. This keeps tables without
    deletion vectors on the old zero-overhead path.
    ``row_ranges`` restricts both file planning and the returned live-row bitmap.
    """
    options = getattr(table, "options", None)
    deletion_vectors_enabled = getattr(options, "deletion_vectors_enabled", None)
    if (table is None
            or not callable(deletion_vectors_enabled)
            or not deletion_vectors_enabled(False)):
        return None

    read_table = table_at_snapshot(table, snapshot)
    read_builder = read_table.new_read_builder()
    if partition_filter is not None:
        read_builder = read_builder.with_partition_filter(partition_filter)

    scan = read_builder.new_scan()
    if row_ranges is not None:
        scan = scan.with_row_ranges(row_ranges)
    data_splits = []
    for split in scan.plan().splits():
        while isinstance(split, (QueryAuthSplit, IndexedSplit)):
            split = split.split if isinstance(split, QueryAuthSplit) else split.data_split()
        if isinstance(split, DataSplit):
            data_splits.append(split)

    rows = RoaringBitmap64()
    # Phase 1: union every file's row-id range.
    for split in data_splits:
        _add_row_ranges(rows, split, row_ranges)
    # Phase 2: subtract each DV. Ranges are all unioned first (no re-add), and
    # peak memory stays at one DV at a time.
    for split in data_splits:
        _subtract_deleted_rows(read_table, rows, split)
    return rows


def table_at_snapshot(table, snapshot):
    """Return a table pinned to ``snapshot``, clearing conflicting time travel."""
    if snapshot is None:
        return table

    return table._copy_with_snapshot(snapshot)


def for_range(live_row_ids: Optional[RoaringBitmap64],
              from_: int, to: int) -> Optional[RoaringBitmap64]:
    if live_row_ids is None:
        return None

    row_range = Range(from_, to)
    include = RoaringBitmap64()
    include.add_range(row_range.from_, row_range.to)
    include = RoaringBitmap64.and_(include, live_row_ids)
    return None if include.cardinality() == row_range.count() else include


def _add_row_ranges(rows: RoaringBitmap64, split: DataSplit, row_ranges=None) -> None:
    for data_file in split.files:
        row_id_range = data_file.row_id_range()
        if row_id_range is not None:
            ranges = [row_id_range] if row_ranges is None else Range.and_([row_id_range], row_ranges)
            for r in ranges:
                rows.add_range(r.from_, r.to)


def _subtract_deleted_rows(table, rows: RoaringBitmap64, split: DataSplit) -> None:
    deletion_files = split.data_deletion_files or []
    for i, data_file in enumerate(split.files):
        if data_file.row_id_range() is None:
            continue

        deletion_file = deletion_files[i] if i < len(deletion_files) else None
        if deletion_file is None or deletion_file.cardinality == 0:
            continue

        deletion_vector = DeletionVector.read(table.file_io, deletion_file)
        if deletion_vector.is_empty():
            continue

        deleted = RoaringBitmap64()
        first_row_id = data_file.first_row_id
        for position in deletion_vector.bit_map():
            deleted.add(first_row_id + position)
        rows.remove_all_inplace(deleted)
