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
#  Unless required by applicable law or in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

from typing import Optional

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.table.row.offset_row import OffsetRow


class PredicateFilterRecordBatchReader(RecordBatchReader):
    def __init__(self, reader: RecordBatchReader, predicate: Predicate):
        self.reader = reader
        self.predicate = predicate

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        while True:
            batch = self.reader.read_arrow_batch()
            if batch is None:
                return None
            if batch.num_rows == 0:
                return batch
            ncols = batch.num_columns
            nrows = batch.num_rows
            mask = []
            row_tuple = [None] * ncols
            offset_row = OffsetRow(row_tuple, 0, ncols)
            for i in range(nrows):
                for j in range(ncols):
                    row_tuple[j] = batch.column(j)[i].as_py()
                offset_row.replace(tuple(row_tuple))
                try:
                    mask.append(self.predicate.test(offset_row))
                except IndexError:
                    raise
                except (TypeError, ValueError):
                    mask.append(False)
            if any(mask):
                filtered = batch.filter(pa.array(mask))
                if filtered.num_rows > 0:
                    return filtered
            # no rows passed predicate in this batch, continue to next batch
            continue

    def return_batch_pos(self) -> int:
        return getattr(self.reader, 'return_batch_pos', lambda: 0)()

    def close(self) -> None:
        self.reader.close()
