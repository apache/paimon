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

from pyarrow import RecordBatch
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.format_blob_reader import FormatBlobReader


class ShardBatchReader(RecordBatchReader):
    """
    A reader that reads a subset of rows from a data file
    """
    def __init__(self, reader, start_row, end_row):
        self.reader = reader
        self.start_row = start_row
        self.end_row = end_row
        self.current_row = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        # Check if reader is FormatBlobReader (blob type)
        if isinstance(self.reader.format_reader, FormatBlobReader):
            # For blob reader, pass begin_idx and end_idx parameters
            return self.reader.read_arrow_batch(start_idx=self.start_row, end_idx=self.end_row)
        else:
            # For non-blob reader (DataFileBatchReader), use standard read_arrow_batch
            batch = self.reader.read_arrow_batch()

            if batch is None:
                return None

            # Apply row range filtering for non-blob readers
            batch_begin = self.current_row
            self.current_row += batch.num_rows

            # Check if batch is within the desired range
            if self.start_row <= batch_begin < self.current_row <= self.end_row:  # batch is within the desired range
                return batch
            elif batch_begin < self.start_row < self.current_row:  # batch starts before the desired range
                return batch.slice(self.start_row - batch_begin, self.end_row - self.start_row)
            elif batch_begin < self.end_row < self.current_row:  # batch ends after the desired range
                return batch.slice(0, self.end_row - batch_begin)
            else:  # batch is outside the desired range
                return self.read_arrow_batch()

    def close(self):
        self.reader.close()
