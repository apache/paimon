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

import collections
from typing import Callable, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class ConcatBatchReader(RecordBatchReader):

    def __init__(self, reader_suppliers: List[Callable]):
        self.queue = collections.deque(reader_suppliers)
        self.current_reader: Optional[RecordBatchReader] = None

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        while True:
            if self.current_reader is not None:
                batch = self.current_reader.read_arrow_batch()
                if batch is not None:
                    return batch
                self.current_reader.close()
                self.current_reader = None
            elif self.queue:
                supplier = self.queue.popleft()
                self.current_reader = supplier()
            else:
                return None

    def close(self) -> None:
        if self.current_reader:
            self.current_reader.close()
            self.current_reader = None
        self.queue.clear()


class ShardBatchReader(ConcatBatchReader):

    def __init__(self, readers, split_start_row, split_end_row):
        super().__init__(readers)
        self.split_start_row = split_start_row
        self.split_end_row = split_end_row
        self.cur_end = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = super().read_arrow_batch()
        if batch is None:
            return None
        if self.split_start_row is not None or self.split_end_row is not None:
            cur_begin = self.cur_end  # begin idx of current batch based on the split
            self.cur_end += batch.num_rows
            # shard the first batch and the last batch
            if self.split_start_row <= cur_begin < self.cur_end <= self.split_end_row:
                return batch
            elif cur_begin <= self.split_start_row < self.cur_end:
                return batch.slice(self.split_start_row - cur_begin,
                                   min(self.split_end_row, self.cur_end) - self.split_start_row)
            elif cur_begin < self.split_end_row <= self.cur_end:
                return batch.slice(0, self.split_end_row - cur_begin)
            else:
                # return empty RecordBatch if the batch size has not reached split_start_row
                return pa.RecordBatch.from_arrays([], [])
        else:
            return batch


class MergeAllBatchReader(RecordBatchReader):
    """
    A reader that accepts multiple reader suppliers and concatenates all their arrow batches
    into one big batch. This is useful when you want to merge all data from multiple sources
    into a single batch for processing.
    """

    def __init__(self, reader_suppliers: List[Callable], batch_size: int = 4096):
        self.reader_suppliers = reader_suppliers
        self.merged_batch: Optional[RecordBatch] = None
        self.reader = None
        self._batch_size = batch_size

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        if self.reader:
            return self.reader.read_next_batch()

        all_batches = []

        # Read all batches from all reader suppliers
        for supplier in self.reader_suppliers:
            reader = supplier()
            try:
                while True:
                    batch = reader.read_arrow_batch()
                    if batch is None:
                        break
                    all_batches.append(batch)
            finally:
                reader.close()

        # Concatenate all batches into one big batch
        if all_batches:
            # For PyArrow < 17.0.0, use Table.concat_tables approach
            # Convert batches to tables and concatenate
            tables = [pa.Table.from_batches([batch]) for batch in all_batches]
            if len(tables) == 1:
                # Single table, just get the first batch
                self.merged_batch = tables[0].to_batches()[0]
            else:
                # Multiple tables, concatenate them
                concatenated_table = pa.concat_tables(tables)
                # Convert back to a single batch by taking all batches and combining
                all_concatenated_batches = concatenated_table.to_batches()
                if len(all_concatenated_batches) == 1:
                    self.merged_batch = all_concatenated_batches[0]
                else:
                    # If still multiple batches, we need to manually combine them
                    # This shouldn't happen with concat_tables, but just in case
                    combined_arrays = []
                    for i in range(len(all_concatenated_batches[0].columns)):
                        column_arrays = [batch.column(i) for batch in all_concatenated_batches]
                        combined_arrays.append(pa.concat_arrays(column_arrays))
                    self.merged_batch = pa.RecordBatch.from_arrays(
                        combined_arrays,
                        names=all_concatenated_batches[0].schema.names
                    )
        else:
            self.merged_batch = None
        dataset = ds.InMemoryDataset(self.merged_batch)
        self.reader = dataset.scanner(batch_size=self._batch_size).to_reader()
        return self.reader.read_next_batch()

    def close(self) -> None:
        self.merged_batch = None
        self.reader = None
