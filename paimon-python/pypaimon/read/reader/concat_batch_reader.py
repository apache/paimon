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

    def __init__(self, readers, split_start_row, split_end_row):
        super().__init__(readers)
        self.split_start_row = split_start_row
        self.split_end_row = split_end_row
        self.cur_end = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        for batch in iter(super().read_arrow_batch, None):
            if self.split_start_row is not None or self.split_end_row is not None:
                cur_begin = self.cur_end  # begin idx of current batch based on the split
                self.cur_end += batch.num_rows
                # shard the first batch and the last batch
                if cur_begin <= self.split_start_row < self.cur_end:
                    return batch.slice(self.split_start_row - cur_begin,
                                       min(self.split_end_row, self.cur_end) - self.split_start_row)
                elif cur_begin < self.split_end_row <= self.cur_end:
                    return batch.slice(0, self.split_end_row - cur_begin)
                elif self.split_start_row <= cur_begin < self.cur_end <= self.split_end_row:
                    return batch
            else:
                return batch
        return None
