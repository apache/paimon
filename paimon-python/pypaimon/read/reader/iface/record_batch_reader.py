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

    def _read_next_df(self) -> Optional[polars.DataFrame]:
        arrow_batch = self.read_arrow_batch()
        if arrow_batch is None:
            return None
        # Convert RecordBatch to Table for Polars compatibility with PyArrow 5.0.0
        import pyarrow as pa
        if hasattr(arrow_batch, 'num_rows'):
            # This is a RecordBatch, convert to Table first
            table = pa.Table.from_batches([arrow_batch])
            # Check if table is empty to avoid Polars "empty table" error
            if table.num_rows == 0:
                return None
            return polars.from_arrow(table)
        else:
            # Check if arrow_batch is empty to avoid Polars "empty table" error
            if hasattr(arrow_batch, 'num_rows') and arrow_batch.num_rows == 0:
                return None
            return polars.from_arrow(arrow_batch)

    def tuple_iterator(self) -> Optional[Iterator[tuple]]:
        df = self._read_next_df()
        if df is None:
            return None
        # Polars 0.9.12 compatibility - iter_rows doesn't exist, use alternative
        if hasattr(df, 'iter_rows'):
            return df.iter_rows()
        else:
            # Fallback for Polars 0.9.12: convert to pandas and iterate
            pandas_df = df.to_pandas()
            return (tuple(row) for _, row in pandas_df.iterrows())

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        df = self._read_next_df()
        if df is None:
            return None
        # Polars 0.9.12 compatibility - iter_rows doesn't exist, use alternative
        if hasattr(df, 'iter_rows'):
            return InternalRowWrapperIterator(df.iter_rows(), df.width)
        else:
            # Fallback for Polars 0.9.12: convert to pandas and iterate
            pandas_df = df.to_pandas()
            row_iterator = (tuple(row) for _, row in pandas_df.iterrows())
            return InternalRowWrapperIterator(row_iterator, df.width)


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
