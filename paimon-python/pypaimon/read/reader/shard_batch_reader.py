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

from typing import Optional

from pyarrow import RecordBatch
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.format_blob_reader import FormatBlobReader


class ShardBatchReader(RecordBatchReader):
    """
    A reader that reads a subset of rows from a data file
    """
    def __init__(self, reader, start_pos, end_pos):
        self.reader = reader
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.current_pos = 0
        self._adopt_metadata(reader)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        # Check if reader is FormatBlobReader (blob type)
        if isinstance(self.reader.format_reader, FormatBlobReader):
            # For blob reader, pass begin_idx and end_idx parameters
            return self.reader.read_arrow_batch(start_idx=self.start_pos, end_idx=self.end_pos)

        # For non-blob reader (DataFileBatchReader), use standard read_arrow_batch.
        # Loop rather than recurse over skipped batches: a slice/shard whose range
        # sits deep in a file (default parquet batch_size is 1024 rows) skips one
        # batch per step, so recursing here overflows the stack (RecursionError)
        # once the skipped count exceeds the interpreter limit. Mirrors the
        # while-loop skip pattern in ConcatBatchReader / ApplyDeletionVectorReader.
        while True:
            batch = self.reader.read_arrow_batch()

            if batch is None:
                return None

            # Apply row range filtering for non-blob readers
            batch_begin = self.current_pos
            self.current_pos += batch.num_rows

            # Check if batch is within the desired range
            if self.start_pos <= batch_begin < self.current_pos <= self.end_pos:  # batch is within the desired range
                return batch
            elif batch_begin < self.start_pos < self.current_pos:  # batch starts before the desired range
                return batch.slice(self.start_pos - batch_begin, self.end_pos - self.start_pos)
            elif batch_begin < self.end_pos < self.current_pos:  # batch ends after the desired range
                return batch.slice(0, self.end_pos - batch_begin)
            # else: batch is outside the desired range -> read the next one (loop)

    def close(self):
        self.reader.close()
