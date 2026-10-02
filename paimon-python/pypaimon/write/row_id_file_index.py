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

"""Snapshot file index used to route row-id updates."""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.data_evolution_split_generator import (
    DataEvolutionSplitGenerator,
)
from pypaimon.read.split import DataSplit
from pypaimon.utils.range import Range


@dataclass(frozen=True)
class RowIdFileIndex:
    """Snapshot files keyed by the first row ID of each data file group."""

    snapshot_id: int
    first_row_ids: List[int]
    first_row_id_index: Dict[int, Tuple[DataSplit, List[DataFileMeta]]] = (
        field(default_factory=dict)
    )
    valid_row_id_ranges: List[Range] = field(default_factory=list)

    @classmethod
    def from_table(cls, table) -> "RowIdFileIndex":
        """Plan the table's write snapshot and index its splits."""
        scan = table.new_read_builder().new_scan()
        plan = scan.plan_for_write()
        snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        return cls.from_splits(snapshot_id, plan.splits())

    @classmethod
    def from_entries(
            cls, table, snapshot_id: int, entries: List[ManifestEntry]
    ) -> "RowIdFileIndex":
        """Index an already resolved snapshot entry set."""
        splits = DataEvolutionSplitGenerator(
            table,
            table.options.source_split_target_size(),
            table.options.source_split_open_file_cost(),
        ).create_splits(entries)
        return cls.from_splits(snapshot_id, splits)

    @classmethod
    def from_splits(
            cls, snapshot_id: int, splits: List[DataSplit]
    ) -> "RowIdFileIndex":
        index: Dict[int, Tuple[DataSplit, List[DataFileMeta]]] = {}
        row_id_ranges: List[Range] = []
        for split in splits:
            files_with_row_id = [
                file for file in split.files if file.first_row_id is not None
            ]
            data_files = [
                file for file in files_with_row_id
                if not DataFileMeta.is_blob_file(file.file_name)
            ]
            for file in split.files:
                if (
                        file.first_row_id is None
                        or DataFileMeta.is_blob_file(file.file_name)
                ):
                    continue
                row_id_ranges.append(file.row_id_range())
            for file in data_files:
                target_files = [
                    target_file
                    for target_file in files_with_row_id
                    if cls._overlaps(
                        file.row_id_range(), target_file.row_id_range()
                    )
                ]

                entry = index.get(file.first_row_id)
                if entry is None:
                    index[file.first_row_id] = (split, target_files)
                else:
                    existing_files = entry[1]
                    existing_names = {
                        existing.file_name for existing in existing_files
                    }
                    existing_files.extend(
                        target_file
                        for target_file in target_files
                        if target_file.file_name not in existing_names
                    )

        if row_id_ranges:
            merged = Range.sort_and_merge_overlap(row_id_ranges, True, True)
        else:
            merged = []

        return cls(
            snapshot_id=snapshot_id,
            first_row_ids=sorted(index.keys()),
            first_row_id_index=index,
            valid_row_id_ranges=merged,
        )

    @staticmethod
    def _overlaps(left: Range, right: Range) -> bool:
        return left.from_ <= right.to and right.from_ <= left.to
