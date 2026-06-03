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
from typing import List

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


class _SpecialFieldBunch(FieldBunch):
    """Base class for partial field files (blob/vector)."""

    def __init__(self, expected_row_count: int, row_id_push_down: bool = False):
        self._files: List[DataFileMeta] = []
        self.expected_row_count = expected_row_count
        self.row_id_push_down = row_id_push_down
        self.latest_first_row_id = -1
        self.expected_next_first_row_id = -1
        self.latest_max_sequence_number = -1
        self._row_count = 0

    def _is_special_file(self, file_name: str) -> bool:
        raise NotImplementedError

    def _file_type_label(self) -> str:
        raise NotImplementedError

    def add(self, file: DataFileMeta) -> None:
        if not self._is_special_file(file.file_name):
            raise ValueError(
                f"Only {self._file_type_label()} file can be added to "
                f"a {self._file_type_label()} bunch.")

        if self._files and file.write_cols != self._files[0].write_cols:
            raise ValueError(
                f"All files in a {self._file_type_label()} bunch should "
                f"have the same write columns."
            )

        if file.first_row_id == self.latest_first_row_id:
            if file.max_sequence_number >= self.latest_max_sequence_number:
                raise ValueError(
                    f"{self._file_type_label().capitalize()} file with same first row id "
                    f"should have decreasing sequence number."
                )
            return

        if self._files:
            first_row_id = file.first_row_id
            if self.row_id_push_down:
                if first_row_id < self.expected_next_first_row_id:
                    if file.max_sequence_number > self.latest_max_sequence_number:
                        last_file = self._files.pop()
                        self._row_count -= last_file.row_count
                    else:
                        return
            else:
                if first_row_id < self.expected_next_first_row_id:
                    if file.max_sequence_number >= self.latest_max_sequence_number:
                        raise ValueError(
                            f"{self._file_type_label().capitalize()} file with overlapping "
                            f"row id should have decreasing sequence number."
                        )
                    return
                elif first_row_id > self.expected_next_first_row_id:
                    raise ValueError(
                        f"{self._file_type_label().capitalize()} file first row id should "
                        f"be continuous, expect {self.expected_next_first_row_id} "
                        f"but got {first_row_id}"
                    )

            if self._files:
                if file.write_cols != self._files[0].write_cols:
                    raise ValueError(
                        f"All files in a {self._file_type_label()} bunch should "
                        f"have the same write columns."
                    )

        self._files.append(file)
        self._row_count += file.row_count
        if self._row_count > self.expected_row_count:
            raise ValueError(
                f"{self._file_type_label().capitalize()} files row count exceed "
                f"the expect {self.expected_row_count}"
            )

        self.latest_max_sequence_number = file.max_sequence_number
        self.latest_first_row_id = file.first_row_id
        self.expected_next_first_row_id = self.latest_first_row_id + file.row_count

    def row_count(self) -> int:
        return self._row_count

    def files(self) -> List[DataFileMeta]:
        return self._files


class BlobBunch(_SpecialFieldBunch):
    """Files for partial field (blob files)."""

    def add(self, file: DataFileMeta) -> None:
        if not self._is_special_file(file.file_name):
            raise ValueError("Only blob file can be added to a blob bunch.")
        if self._files and file.write_cols != self._files[0].write_cols:
            raise ValueError("All files in a blob bunch should have the same write columns.")

        self._files.append(file)
        merged = Range.sort_and_merge_overlap(
            [blob_file.row_id_range() for blob_file in self._files],
            True,
            True,
        )
        self._row_count = sum(row_range.count() for row_range in merged)
        if self.expected_row_count >= 0 and self._row_count > self.expected_row_count:
            raise ValueError(
                f"Blob files row count exceed the expect {self.expected_row_count}"
            )

    def row_count(self) -> int:
        merged = Range.sort_and_merge_overlap(
            [blob_file.row_id_range() for blob_file in self._files],
            True,
            True,
        )
        row_count = sum(row_range.count() for row_range in merged)
        if not self.row_id_push_down:
            if len(merged) != 1:
                raise ValueError("Blob file bunch should always contain a contiguous row range.")
            if self.expected_row_count >= 0 and row_count != self.expected_row_count:
                raise ValueError(
                    "The merged row count of blob file bunch should be aligned "
                    f"with normal files, expect {self.expected_row_count}, got {row_count}."
                )
        return row_count

    def sequential_read_optimize(self) -> bool:
        if not self._files:
            raise ValueError("Blob bunch should not be empty.")
        max_sequence_number = self._files[0].max_sequence_number
        return all(
            file.max_sequence_number == max_sequence_number
            for file in self._files
        )

    def _is_special_file(self, file_name: str) -> bool:
        return DataFileMeta.is_blob_file(file_name)

    def _file_type_label(self) -> str:
        return "blob"


class VectorBunch(_SpecialFieldBunch):
    """Files for partial field (vector files)."""

    def _is_special_file(self, file_name: str) -> bool:
        return DataFileMeta.is_vector_file(file_name)

    def _file_type_label(self) -> str:
        return "vector"
