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

from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class SampleBatchReader(RecordBatchReader):
    """
    A reader that reads a subset of rows from a data file based on specified sample positions.

    This reader wraps another RecordBatchReader and only returns rows at the specified
    sample positions, enabling efficient random sampling of data without reading all rows.

    The reader supports two modes:
    1. For blob readers: Directly reads specific rows by index
    2. For other readers: Reads batches sequentially and extracts only the sampled rows

    Attributes:
        reader: The underlying RecordBatchReader to read data from
        sample_positions: A sorted list of row indices to sample (0-based)
        sample_idx: Current index in the sample_positions list
        current_pos: Current absolute row position in the data file
    """

    def __init__(self, reader, sample_positions):
        """
        Initialize the SampleBatchReader.

        Args:
            reader: The underlying RecordBatchReader to read data from
            sample_positions: A bitmap of row indices to sample (0-based).
                            Must be sorted in ascending order for correct behavior.
        """
        self.reader = reader
        self.sample_positions = sample_positions
        self.sample_idx = 0
        self.current_pos = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        """
        Read the next batch containing sampled rows.

        This method reads data from the underlying reader and returns only the rows
        at the specified sample positions. The behavior differs based on reader type:

        - For FormatBlobReader: Directly reads individual rows by index
        - For other readers: Reads batches sequentially and extracts sampled rows
          using PyArrow's take() method
        """
        if self.sample_idx >= len(self.sample_positions):
            return None
        if isinstance(self.reader.format_reader, FormatBlobReader):
            # For blob reader, pass begin_idx and end_idx parameters
            batch = self.reader.read_arrow_batch(start_idx=self.sample_positions[self.sample_idx],
                                                 end_idx=self.sample_positions[self.sample_idx] + 1)
            self.sample_idx += 1
            return batch
        else:
            while True:
                batch = self.reader.read_arrow_batch()
                if batch is None:
                    return None

                batch_begin = self.current_pos
                self.current_pos += batch.num_rows
                take_idxes = []

                sample_pos = self.sample_positions[self.sample_idx]
                while batch_begin <= sample_pos < self.current_pos:
                    take_idxes.append(sample_pos - batch_begin)
                    self.sample_idx += 1
                    if self.sample_idx >= len(self.sample_positions):
                        break
                    sample_pos = self.sample_positions[self.sample_idx]

                if take_idxes:
                    return batch.take(take_idxes)
                # batch is outside the desired range, continue to next batch

    def close(self):
        self.reader.close()
