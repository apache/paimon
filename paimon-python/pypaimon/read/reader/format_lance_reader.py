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

from typing import List, Optional, Any

import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance_utils import to_lance_specified


class FormatLanceReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Lance file using PyArrow,
    and filters it based on the provided predicate and projection.
    """

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 push_down_predicate: Any, batch_size: int = 4096):
        """Initialize Lance reader."""
        import lance

        file_path_for_lance, storage_options = to_lance_specified(file_io, file_path)

        columns_for_lance = read_fields if read_fields else None
        lance_reader = lance.file.LanceFileReader(
            file_path_for_lance,
            storage_options=storage_options,
            columns=columns_for_lance)
        reader_results = lance_reader.read_all()

        # Convert to PyArrow table
        pa_table = reader_results.to_table()

        if push_down_predicate is not None:
            in_memory_dataset = ds.InMemoryDataset(pa_table)
            scanner = in_memory_dataset.scanner(filter=push_down_predicate, batch_size=batch_size)
            self.reader = scanner.to_reader()
        else:
            self.reader = iter(pa_table.to_batches(max_chunksize=batch_size))

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            # Handle both scanner reader and iterator
            if hasattr(self.reader, 'read_next_batch'):
                return self.reader.read_next_batch()
            else:
                # Iterator of batches
                return next(self.reader)
        except StopIteration:
            return None

    def close(self):
        if self.reader is not None:
            self.reader = None
