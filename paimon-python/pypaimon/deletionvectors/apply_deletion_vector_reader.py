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

from typing import List, Optional

import pyarrow
from pyarrow import RecordBatch

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.read.reader.iface.record_reader import RecordReader


class PositionMappedDeletionVector:
    """
    Adapts a deletion vector to the returned positions of the current reader.
    """

    def __init__(
        self,
        deletion_vector: DeletionVector,
        file_offset: int = 0,
        row_positions: Optional[List[int]] = None,
    ):
        self._deletion_vector = deletion_vector
        self._file_offset = file_offset
        self._row_positions = row_positions

    def is_deleted(self, position: int) -> bool:
        return self._deletion_vector.is_deleted(self._mapped_position(position))

    def _mapped_position(self, position: int) -> int:
        if self._row_positions is None:
            return self._file_offset + position
        if position >= len(self._row_positions):
            raise ValueError(
                "Deletion vector row positions are fewer than returned rows."
            )
        return self._file_offset + self._row_positions[position]


class ApplyDeletionVectorReader(RecordBatchReader):
    """
    A RecordReader which applies DeletionVector to filter records.
    """

    def __init__(
        self,
        reader: RecordReader,
        deletion_vector,
    ):
        """
        Initialize an ApplyDeletionVectorReader.

        Args:
            reader: The underlying record reader.
            deletion_vector: The deletion vector to apply. It should already
                be mapped to the returned positions of the underlying reader.
        """
        self._reader = reader
        self._deletion_vector = deletion_vector
        self._returned_position = 0

    def reader(self) -> RecordReader:
        return self._reader

    def deletion_vector(self):
        return self._deletion_vector

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        self._reader: RecordBatchReader
        # Skip physical batches that become empty after DV filtering. Some merge readers
        # treat a zero-row batch as EOF and would otherwise stop before later live rows.
        while True:
            arrow_batch = self._reader.read_arrow_batch()
            if arrow_batch is None:
                return None

            start = self._returned_position
            end = start + arrow_batch.num_rows
            self._returned_position = end
            keep_indices = [
                i for i, position in enumerate(range(start, end))
                if not self._deletion_vector.is_deleted(position)
            ]
            if keep_indices:
                return arrow_batch.take(pyarrow.array(keep_indices, type=pyarrow.int32()))

    def read_batch(self) -> Optional[RecordIterator]:
        """
        Reads one batch with deletion vector applied.

        Returns:
            A RecordIterator with deletion filtering, or None if no more data.
        """
        batch = self._reader.read_batch()

        if batch is None:
            return None

        return ApplyDeletionRecordIterator(batch, self._deletion_vector)

    def close(self):
        self._reader.close()


class ApplyDeletionRecordIterator(RecordIterator):
    """
    A RecordIterator that wraps another RecordIterator and applies a DeletionVector
    to filter out deleted records.
    """

    def __init__(
        self,
        iterator: RecordIterator,
        deletion_vector,
    ):
        """
        Initialize an ApplyDeletionRecordIterator.

        Args:
            iterator: The underlying record iterator.
            deletion_vector: The deletion vector to apply for filtering.
        """
        self._iterator = iterator
        self._deletion_vector = deletion_vector
        self._returned_position = -1

    def iterator(self) -> RecordIterator:
        return self._iterator

    def deletion_vector(self):
        return self._deletion_vector

    def returned_position(self) -> int:
        return self._returned_position

    def next(self) -> Optional[object]:
        """
        Gets the next non-deleted record from the iterator.

        This method skips over any records that are marked as deleted in the
        deletion vector, returning only non-deleted records.

        Returns:
            The next non-deleted record, or None if no more records exist.
        """
        while True:
            record = self._iterator.next()

            if record is None:
                return None

            self._returned_position += 1
            position = self._returned_position
            if not self._deletion_vector.is_deleted(position):
                return record
