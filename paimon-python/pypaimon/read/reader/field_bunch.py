"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
FieldBunch classes for organizing files by field in data evolution.

These classes help organize DataFileMeta objects into groups based on their field content,
supporting both regular data files and blob files.
"""
from abc import ABC
from typing import List
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


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

    def __init__(self, expected_row_count: int):
        self._files: List[DataFileMeta] = []
        self.expected_row_count = expected_row_count
        self.latest_first_row_id = -1
        self.expected_next_first_row_id = -1
        self.latest_max_sequence_number = -1
        self._row_count = 0

    def add(self, file: DataFileMeta) -> None:
        """Add a blob file to this bunch."""
        if not DataFileMeta.is_blob_file(file.file_name):
            raise ValueError("Only blob file can be added to a blob bunch.")

        if file.first_row_id == self.latest_first_row_id:
            if file.max_sequence_number >= self.latest_max_sequence_number:
                raise ValueError(
                    "Blob file with same first row id should have decreasing sequence number."
                )
            return

        if self._files:
            first_row_id = file.first_row_id
            if first_row_id < self.expected_next_first_row_id:
                if file.max_sequence_number >= self.latest_max_sequence_number:
                    raise ValueError(
                        "Blob file with overlapping row id should have decreasing sequence number."
                    )
                return
            elif first_row_id > self.expected_next_first_row_id:
                raise ValueError(
                    f"Blob file first row id should be continuous, expect "
                    f"{self.expected_next_first_row_id} but got {first_row_id}"
                )

            if file.schema_id != self._files[0].schema_id:
                raise ValueError(
                    "All files in a blob bunch should have the same schema id."
                )
            if file.write_cols != self._files[0].write_cols:
                raise ValueError(
                    "All files in a blob bunch should have the same write columns."
                )

        self._files.append(file)
        self._row_count += file.row_count
        if self._row_count > self.expected_row_count:
            raise ValueError(
                f"Blob files row count exceed the expect {self.expected_row_count}"
            )

        self.latest_max_sequence_number = file.max_sequence_number
        self.latest_first_row_id = file.first_row_id
        self.expected_next_first_row_id = self.latest_first_row_id + file.row_count

    def row_count(self) -> int:
        return self._row_count

    def files(self) -> List[DataFileMeta]:
        return self._files
