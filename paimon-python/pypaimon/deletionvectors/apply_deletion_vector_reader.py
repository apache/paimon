#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Optional

import pyarrow
from pyarrow import RecordBatch

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.deletionvectors.deletion_vector import DeletionVector

from pyroaring import BitMap

from pypaimon.read.reader.iface.record_reader import RecordReader


class ApplyDeletionVectorReader(RecordBatchReader):
    """
    A RecordReader which applies DeletionVector to filter records.
    """

    def __init__(self, reader: RecordReader, deletion_vector: DeletionVector):
        """
        Initialize an ApplyDeletionVectorReader.

        Args:
            reader: The underlying record reader.
            deletion_vector: The deletion vector to apply.
        """
        self._reader = reader
        self._deletion_vector = deletion_vector

    def reader(self) -> RecordReader:
        return self._reader

    def deletion_vector(self) -> DeletionVector:
        return self._deletion_vector

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        self._reader: RecordBatchReader
        arrow_batch = self._reader.read_arrow_batch()
        if arrow_batch is None:
            return None
        # Remove the deleted rows from the batch
        range_bitmap = BitMap(
            range(self._reader.return_batch_pos() - arrow_batch.num_rows, self._reader.return_batch_pos()))
        intersection_bitmap = range_bitmap - self._deletion_vector.bit_map()
        added_row_list = [x - (self._reader.return_batch_pos() - arrow_batch.num_rows) for x in
                          list(intersection_bitmap)]
        return arrow_batch.take(pyarrow.array(added_row_list, type=pyarrow.int32()))

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

    def __init__(self, iterator: RecordIterator, deletion_vector: DeletionVector):
        """
        Initialize an ApplyDeletionRecordIterator.

        Args:
            iterator: The underlying record iterator.
            deletion_vector: The deletion vector to apply for filtering.
        """
        self._iterator = iterator
        self._deletion_vector = deletion_vector

    def iterator(self) -> RecordIterator:
        return self._iterator

    def deletion_vector(self) -> DeletionVector:
        return self._deletion_vector

    def returned_position(self) -> int:
        return self._iterator.return_pos()

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

            # Check if the current position is deleted
            if not self._deletion_vector.is_deleted(self._iterator.return_pos()):
                return record
