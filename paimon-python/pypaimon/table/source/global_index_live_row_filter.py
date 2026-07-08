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
from pypaimon.read.split import DataSplit
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def live_rows(table, partition_filter=None) -> Optional[RoaringBitmap64]:
    """Return current live global row ids for deletion-vector tables.

    ``None`` means no live-row filter is needed. This keeps tables without
    deletion vectors on the old zero-overhead path.
    """
    options = getattr(table, "options", None)
    deletion_vectors_enabled = getattr(options, "deletion_vectors_enabled", None)
    if (table is None
            or not callable(deletion_vectors_enabled)
            or not deletion_vectors_enabled(False)):
        return None

    read_builder = table.new_read_builder()
    if partition_filter is not None:
        read_builder = read_builder.with_partition_filter(partition_filter)

    rows = RoaringBitmap64()
    scan = read_builder.new_scan()
    scan._query_auth_fn = None
    for split in scan.plan().splits():
        if isinstance(split, DataSplit):
            rows = _add_live_rows(table, rows, split)
    return rows


def for_range(live_row_ids: Optional[RoaringBitmap64],
              from_: int, to: int) -> Optional[RoaringBitmap64]:
    if live_row_ids is None:
        return None

    row_range = Range(from_, to)
    include = RoaringBitmap64()
    include.add_range(row_range.from_, row_range.to)
    include = RoaringBitmap64.and_(include, live_row_ids)
    return None if include.cardinality() == row_range.count() else include


def _add_live_rows(table, rows: RoaringBitmap64, split: DataSplit) -> RoaringBitmap64:
    deletion_files = split.data_deletion_files or []
    for i, data_file in enumerate(split.files):
        row_id_range = data_file.row_id_range()
        if row_id_range is None:
            continue

        rows.add_range(row_id_range.from_, row_id_range.to)
        deletion_file = deletion_files[i] if i < len(deletion_files) else None
        if deletion_file is None or deletion_file.cardinality == 0:
            continue

        deletion_vector = DeletionVector.read(table.file_io, deletion_file)
        if deletion_vector.is_empty():
            continue

        deleted_rows = RoaringBitmap64()
        first_row_id = data_file.first_row_id
        for position in deletion_vector.bit_map():
            deleted_rows.add(first_row_id + position)
        rows = RoaringBitmap64.remove_all(rows, deleted_rows)
    return rows
