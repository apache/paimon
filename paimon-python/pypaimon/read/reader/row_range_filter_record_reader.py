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
#  limitations under the License.
################################################################################

from typing import Optional, List
import pyarrow

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.utils.range import Range
from pyarrow import RecordBatch


class RowIdFilterRecordBatchReader(RecordBatchReader):
    """
    A RecordBatchReader that filters records based on row ID ranges.
    """

    def __init__(self, reader: RecordBatchReader, first_row_id: int, row_id_ranges: List[Range]):
        self.reader = reader
        self.current_row_id = first_row_id
        self.row_id_ranges = row_id_ranges

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        while True:
            batch = self.reader.read_arrow_batch()
            if batch is None:
                return None
            if batch.num_rows == 0:
                return batch
            import numpy as np
            # Build mask for rows that are in allowed_row_ids
            mask = np.zeros(batch.num_rows, dtype=bool)
            for i in range(batch.num_rows):
                if self._is_row_in_range(self.current_row_id + i):
                    mask[i] = True

            self.current_row_id += batch.num_rows

            # Filter batch
            if np.any(mask):
                filtered_batch = batch.filter(pyarrow.array(mask))
                if filtered_batch.num_rows > 0:
                    return filtered_batch

    def close(self) -> None:
        self.reader.close()

    def _is_row_in_range(self, row_id: int) -> bool:
        """Check if the given row ID is within any of the allowed ranges."""
        for range_obj in self.row_id_ranges:
            if range_obj.contains(row_id):
                return True
        return False
