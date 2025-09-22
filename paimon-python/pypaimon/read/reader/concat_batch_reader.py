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

    def __init__(self, readers, split):
        super().__init__(readers)
        self.split = split
        self.file_idx = 0
        self.file = None
        self.file_start_row = 0
        self.file_end_row = 0
        self.cur_row = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = super().read_arrow_batch()
        if batch is None:
            return None
        # get next file according to the reader in the queue
        if len(self.queue) + self.file_idx < len(self.split.files):
            self.file = self.split.files[self.file_idx]
            self.file_start_row = self.file.file_start_row
            self.file_end_row = self.file.file_end_row
            self.cur_row = 0
            self.file_idx += 1

        if self.file_start_row is not None or self.file_end_row is not None:
            batch_begin_row = self.cur_row
            self.cur_row = self.cur_row + batch.num_rows
            # shard the first batch and the last batch in the file
            if batch_begin_row <= self.file_start_row < self.cur_row:
                return batch.slice(self.file_start_row - batch_begin_row,
                                   min(self.cur_row, self.file_end_row) - self.file_start_row)
            elif batch_begin_row < self.file_end_row <= self.cur_row:
                return batch.slice(0, self.file_end_row - batch_begin_row)
            elif self.file_start_row <= batch_begin_row < self.cur_row <= self.file_end_row:
                return batch
        else:
            return batch
