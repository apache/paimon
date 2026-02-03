################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

from typing import Optional, List

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.globalindex.range import Range


class RowRangeFilterRecordReader(RecordReader):
    """
    A RecordReader that filters records based on row ID ranges.
    """

    def __init__(self, reader: RecordReader, first_row_id: int, row_id_ranges: List[Range]):
        self.reader = reader
        self.current_row_id = first_row_id
        self.row_id_ranges = row_id_ranges

    def read_batch(self) -> Optional[RecordIterator]:
        iterator = self.reader.read_batch()
        if iterator is None:
            return None
        return RowRangeFilterIterator(iterator, self.current_row_id, self.row_id_ranges, self)

    def close(self) -> None:
        self.reader.close()

    def get_current_row_id(self) -> int:
        return self.current_row_id

    def update_row_id(self, row_id: int):
        self.current_row_id = row_id


class RowRangeFilterIterator(RecordIterator):
    """
    A RecordIterator that filters records based on row ID ranges.
    """

    def __init__(
            self,
            iterator: RecordIterator,
            row_id_ranges: List[Range],
            reader: RowRangeFilterRecordReader):
        self.iterator = iterator
        self.current_row_id = reader.get_current_row_id()
        self.row_id_ranges = row_id_ranges
        self.reader = reader

    def next(self) -> Optional[object]:
        while True:
            record = self.iterator.next()
            if record is None:
                return None

            self.current_row_id = self.current_row_id + 1
            self.reader.update_row_id(self.current_row_id)

            if self._is_row_in_range(self.current_row_id - 1):
                return record

    def _is_row_in_range(self, row_id: int) -> bool:
        """Check if the given row ID is within any of the allowed ranges."""
        for range_obj in self.row_id_ranges:
            if range_obj.contains(row_id):
                return True
        return False
