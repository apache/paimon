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
# limitations under the License.
################################################################################

from abc import abstractmethod
from typing import Iterator, Optional, TypeVar

import polars
from pyarrow import RecordBatch

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.offset_row import OffsetRow

T = TypeVar('T')


class RecordBatchReader(RecordReader):
    """
    The reader that reads the pyarrow batches of records.
    """

    @abstractmethod
    def read_arrow_batch(self) -> Optional[RecordBatch]:
        """
        Reads one batch. The method should return null when reaching the end of the input.
        """

    def return_batch_pos(self) -> int:
        """
        Returns the current batch position in the file.
        """

    def read_next_df(self) -> Optional[polars.DataFrame]:
        arrow_batch = self.read_arrow_batch()
        if arrow_batch is None:
            return None
        return polars.from_arrow(arrow_batch)

    def tuple_iterator(self) -> Optional[Iterator[tuple]]:
        df = self.read_next_df()
        if df is None:
            return None
        return df.iter_rows()

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        df = self.read_next_df()
        if df is None:
            return None
        return InternalRowWrapperIterator(df.iter_rows(), df.width)


class InternalRowWrapperIterator(RecordIterator[InternalRow]):
    def __init__(self, iterator: Iterator[tuple], width: int):
        self._iterator = iterator
        self._reused_row = OffsetRow(None, 0, width)

    def next(self) -> Optional[InternalRow]:
        row_tuple = next(self._iterator, None)
        if row_tuple is None:
            return None
        self._reused_row.replace(row_tuple)
        return self._reused_row


class RowPositionReader(RecordBatchReader):

    def __init__(self, data_reader: RecordBatchReader):
        self._data_reader = data_reader
        self._row_iterator = RowPositionRecordIterator()
        self.batch_pos = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = self._data_reader.read_arrow_batch()
        if batch is None:
            return None
        self.batch_pos += batch.num_rows
        return batch

    def return_batch_pos(self) -> int:
        return self.batch_pos

    def tuple_iterator(self) -> Optional[RecordIterator]:
        return self._row_iterator.replace_iterator(self._data_reader.tuple_iterator())

    def close(self):
        self._data_reader.close()


class RowPositionRecordIterator(RecordIterator[tuple]):

    def __init__(self):
        self.reused_iterator: Optional[Iterator[tuple]] = None
        self.pos = -1

    def next(self) -> Optional[tuple]:
        row_tuple = next(self.reused_iterator, None)
        if row_tuple is None:
            return None
        self.pos += 1
        return row_tuple

    def return_pos(self) -> int:
        return self.pos

    def replace_iterator(self, iterator: Iterator[tuple]) -> Optional[RecordIterator[tuple]]:
        self.reused_iterator = iterator
        if self.reused_iterator is None:
            return None
        return self

    def __next__(self):
        return self.next()
