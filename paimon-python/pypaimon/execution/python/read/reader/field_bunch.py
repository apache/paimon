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
FieldBunch classes for organizing files by field in data evolution.

These classes help organize DataFileMeta objects into groups based on their field content,
supporting both regular data files and blob files.
"""
from abc import ABC
from typing import AbstractSet, List, Optional

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.utils.range import Range


class FieldBunch(ABC):
    """Interface for files organized by field."""

    def row_count(self) -> int:
        """Return the total row count for this bunch."""
        ...

    def files(self) -> List[DataFileMeta]:
        """Return the list of files in this bunch."""
        ...


class DataBunch(FieldBunch):
    """Files for a single data file."""

    def __init__(self, data_file: DataFileMeta):
        self.data_file = data_file

    def row_count(self) -> int:
        return self.data_file.row_count

    def files(self) -> List[DataFileMeta]:
        return [self.data_file]


class BlobBunch(FieldBunch):
    """Files for partial field (blob files)."""

    def __init__(self, expected_row_count: int, row_id_push_down: bool = False,
                 expected_row_range: Optional[Range] = None):
        self._files: List[DataFileMeta] = []
        self.expected_row_count = expected_row_count
        self.row_id_push_down = row_id_push_down
        self._row_count = 0
        # The normal file is the logical anchor. A Blob column added later may
        # physically cover only part of its row-id range.
        self.expected_row_range = expected_row_range
        self._merged_ranges: List[Range] = []
        self._finished = False

    def add(self, file: DataFileMeta) -> None:
        if self._finished:
            raise ValueError("Cannot add a blob file to a finished blob bunch.")
        if not DataFileMeta.is_blob_file(file.file_name):
            raise ValueError("Only blob file can be added to a blob bunch.")
        if self._files and file.write_cols != self._files[0].write_cols:
            raise ValueError("All files in a blob bunch should have the same write columns.")

        self._files.append(file)

    def finish(self) -> None:
        if self._finished:
            return

        merged = self._merged_physical_ranges()
        physical_row_count = sum(row_range.count() for row_range in merged)
        if self.expected_row_range is not None:
            for row_range in merged:
                if (row_range.from_ < self.expected_row_range.from_
                        or row_range.to > self.expected_row_range.to):
                    raise ValueError(
                        f"Blob file range {row_range} should be within normal "
                        f"file range {self.expected_row_range}."
                    )
            self._row_count = self.expected_row_range.count()
        else:
            self._row_count = physical_row_count
            if self.expected_row_count >= 0 and self._row_count > self.expected_row_count:
                raise ValueError(
                    f"Blob files row count exceed the expect {self.expected_row_count}"
                )

        if not self.row_id_push_down and self.expected_row_range is None:
            if len(merged) != 1:
                raise ValueError("Blob file bunch should always contain a contiguous row range.")
            if self.expected_row_count >= 0 and self._row_count != self.expected_row_count:
                raise ValueError(
                    "The merged row count of blob file bunch should be aligned "
                    f"with normal files, expect {self.expected_row_count}, got {self._row_count}."
                )
        self._merged_ranges = merged
        self._finished = True

    def row_count(self) -> int:
        self.finish()
        return self._row_count

    def files(self) -> List[DataFileMeta]:
        return self._files

    def logical_range(self) -> Range:
        if self.expected_row_range is not None:
            return self.expected_row_range
        merged = self._merged_physical_ranges()
        if not merged:
            raise ValueError("Blob bunch should not be empty.")
        return Range(merged[0].from_, merged[-1].to)

    def sequential_read_optimize(self) -> bool:
        if not self._files:
            raise ValueError("Blob bunch should not be empty.")
        max_sequence_number = self._files[0].max_sequence_number
        same_sequence = all(
            file.max_sequence_number == max_sequence_number
            for file in self._files
        )
        merged = self._merged_physical_ranges()
        return (same_sequence and len(merged) == 1
                and merged[0] == self.logical_range())

    def _merged_physical_ranges(self) -> List[Range]:
        if self._finished:
            return self._merged_ranges
        return Range.sort_and_merge_overlap(
            [blob_file.row_id_range() for blob_file in self._files], True, True)


class VectorBunch(FieldBunch):
    """Sequential files that provide the same projected vector fields."""

    def __init__(self, files: List[DataFileMeta], field_ids: AbstractSet[int]):
        self._files = files
        self.field_ids = field_ids

    def row_count(self) -> int:
        return sum(file.row_count for file in self._files)

    def files(self) -> List[DataFileMeta]:
        return self._files
