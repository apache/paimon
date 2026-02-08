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

_MIN_BATCH_SIZE_TO_REFILL = 1024


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

    def __init__(self, reader_suppliers: List[Callable], batch_size: int = 1024):
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
                        schema=all_concatenated_batches[0].schema
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

    When row_offsets[i] == -1 (no file provides that field), output a column of nulls using schema.
    """

    def __init__(
        self,
        row_offsets: List[int],
        field_offsets: List[int],
        readers: List[Optional[RecordBatchReader]],
        schema: pa.Schema,
    ):
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
        self.schema = schema
        self._buffers: List[Optional[RecordBatch]] = [None] * len(readers)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batches: List[Optional[RecordBatch]] = [None] * len(self.readers)
        for i, reader in enumerate(self.readers):
            if reader is not None:
<<<<<<< HEAD
                if self._buffers[i] is not None:
                    remainder = self._buffers[i]
                    self._buffers[i] = None
                    if remainder.num_rows >= _MIN_BATCH_SIZE_TO_REFILL:
                        batches[i] = remainder
                    else:
                        new_batch = reader.read_arrow_batch()
                        if new_batch is not None and new_batch.num_rows > 0:
                            combined_arrays = [
                                pa.concat_arrays([remainder.column(j), new_batch.column(j)])
                                for j in range(remainder.num_columns)
                            ]
                            batches[i] = pa.RecordBatch.from_arrays(
                                combined_arrays, schema=remainder.schema
                            )
                        else:
                            batches[i] = remainder
                else:
                    batch = reader.read_arrow_batch()
                    if batch is None:
                        batches[i] = None
                    else:
                        batches[i] = batch
            else:
                batches[i] = None

        if not any(b is not None for b in batches):
            return None

        min_rows = min(b.num_rows for b in batches if b is not None)
        if min_rows == 0:
            return None

=======
                batch = reader.read_arrow_batch()
                if batch is None:
                    # all readers are aligned, as long as one returns null, the others will also have no data
                    return None
                batches[i] = batch
        # All readers may be None (e.g. all bunches had empty read_fields_per_bunch)
        if not any(b is not None for b in batches):
            return None
>>>>>>> c00cafaf0 (support data evolution read)
        columns = []
        for i in range(len(self.row_offsets)):
            batch_index = self.row_offsets[i]
            field_index = self.field_offsets[i]
<<<<<<< HEAD
            if batch_index >= 0 and batches[batch_index] is not None:
                columns.append(batches[batch_index].column(field_index).slice(0, min_rows))
            else:
                columns.append(pa.nulls(min_rows, type=self.schema.field(i).type))

        for i in range(len(self.readers)):
            if batches[i] is not None and batches[i].num_rows > min_rows:
                self._buffers[i] = batches[i].slice(min_rows, batches[i].num_rows - min_rows)

        return pa.RecordBatch.from_arrays(columns, schema=self.schema)
=======
            field_name = self.schema.field(i).name if self.schema else None
            column = None
            out_name = None

            if batch_index >= 0 and batches[batch_index] is not None:
                src_batch = batches[batch_index]
                if field_name is not None and field_name in src_batch.schema.names:
                    column = src_batch.column(src_batch.schema.get_field_index(field_name))
                    out_name = (
                        self.schema.field(i).name
                        if self.schema is not None and i < len(self.schema)
                        else field_name
                    )
                elif field_index < src_batch.num_columns:
                    column = src_batch.column(field_index)
                    out_name = (
                        self.schema.field(i).name
                        if self.schema is not None and i < len(self.schema)
                        else src_batch.schema.names[field_index]
                    )

            if column is None and field_name is not None:
                for b in batches:
                    if b is not None and field_name in b.schema.names:
                        column = b.column(b.schema.get_field_index(field_name))
                        out_name = (
                            self.schema.field(i).name
                            if self.schema is not None and i < len(self.schema)
                            else field_name
                        )
                        break

            if column is not None and out_name is not None:
                columns.append(column)
            elif self.schema is not None and i < len(self.schema):
                # row_offsets[i] == -1 or no batch has this field: output null column per schema
                num_rows = next(b.num_rows for b in batches if b is not None)
                columns.append(pa.nulls(num_rows, type=self.schema.field(i).type))
        if columns and len(columns) == len(self.schema):
            return pa.RecordBatch.from_arrays(columns, schema=self.schema)
        return None
>>>>>>> c00cafaf0 (support data evolution read)

    def close(self) -> None:
        try:
            self._buffers = [None] * len(self.readers)
            for reader in self.readers:
                if reader is not None:
                    reader.close()
        except Exception as e:
            raise IOError("Failed to close inner readers") from e
