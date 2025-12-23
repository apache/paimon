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
        self.queue: collections.deque[Callable] = collections.deque(reader_suppliers)
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
            try:
                return self.reader.read_next_batch()
            except StopIteration:
                return None

        all_batches = []

        # Read all batches from all reader suppliers
        for supplier in self.reader_suppliers:
            reader = supplier()
            if reader is None:
                continue
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


class DataEvolutionMergeReader(RecordBatchReader):
    """
    This is a union reader which contains multiple inner readers, Each reader is responsible for reading one file.

    This reader, assembling multiple reader into one big and great reader, will merge the batches from all readers.

    For example, if rowOffsets is {0, 2, 0, 1, 2, 1} and fieldOffsets is {0, 0, 1, 1, 1, 0}, it means:
     - The first field comes from batch0, and it is at offset 0 in batch0.
     - The second field comes from batch2, and it is at offset 0 in batch2.
     - The third field comes from batch0, and it is at offset 1 in batch0.
     - The fourth field comes from batch1, and it is at offset 1 in batch1.
     - The fifth field comes from batch2, and it is at offset 1 in batch2.
     - The sixth field comes from batch1, and it is at offset 0 in batch1.
    """

    def __init__(self, row_offsets: List[int], field_offsets: List[int], readers: List[Optional[RecordBatchReader]]):
        if row_offsets is None:
            raise ValueError("Row offsets must not be null")
        if field_offsets is None:
            raise ValueError("Field offsets must not be null")
        if len(row_offsets) != len(field_offsets):
            raise ValueError("Row offsets and field offsets must have the same length")
        if not row_offsets:
            raise ValueError("Row offsets must not be empty")
        if not readers or len(readers) < 1:
            raise ValueError("Readers should be more than 0")
        self.row_offsets = row_offsets
        self.field_offsets = field_offsets
        self.readers = readers

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batches: List[Optional[RecordBatch]] = [None] * len(self.readers)
        for i, reader in enumerate(self.readers):
            if reader is not None:
                batch = reader.read_arrow_batch()
                if batch is None:
                    # all readers are aligned, as long as one returns null, the others will also have no data
                    return None
                batches[i] = batch
        # Assemble record batches from batches based on row_offsets and field_offsets
        columns = []
        names = []
        for i in range(len(self.row_offsets)):
            batch_index = self.row_offsets[i]
            field_index = self.field_offsets[i]
            if batches[batch_index] is not None:
                column = batches[batch_index].column(field_index)
                columns.append(column)
                names.append(batches[batch_index].schema.names[field_index])
        if columns:
            return pa.RecordBatch.from_arrays(columns, names)
        return None

    def close(self) -> None:
        try:
            for reader in self.readers:
                if reader is not None:
                    reader.close()
        except Exception as e:
            raise IOError("Failed to close inner readers") from e
