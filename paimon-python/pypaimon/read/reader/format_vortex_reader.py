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

from typing import List, Optional, Any

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class FormatVortexReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Vortex file,
    and filters it based on the provided predicate and projection.
    """

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 push_down_predicate: Any, batch_size: int = 1024,
                 row_indices: Optional[Any] = None):
        import vortex

        from pypaimon.read.reader.vortex_utils import to_vortex_specified
        file_path_for_vortex, store_kwargs = to_vortex_specified(file_io, file_path)

        if store_kwargs:
            from vortex import store
            vortex_store = store.from_url(file_path_for_vortex, **store_kwargs)
            vortex_file = vortex_store.open()
        else:
            vortex_file = vortex.open(file_path_for_vortex)

        columns_for_vortex = read_fields if read_fields else None

        if row_indices is not None:
            # Use scan with indices for efficient row-level pushdown
            vortex_indices = vortex.array(row_indices)
            scan_iter = vortex_file.scan(
                columns_for_vortex, indices=vortex_indices, batch_size=batch_size)
            pa_table = scan_iter.read_all().to_arrow_table()
            pa_table = self._cast_string_view_columns(pa_table)
            if push_down_predicate is not None:
                in_memory_dataset = ds.InMemoryDataset(pa_table)
                scanner = in_memory_dataset.scanner(filter=push_down_predicate, batch_size=batch_size)
                self.reader = scanner.to_reader()
            else:
                self.reader = iter(pa_table.to_batches(max_chunksize=batch_size))
        else:
            pa_table = vortex_file.to_arrow(columns_for_vortex).read_all()
            # Vortex exports string_view which some PyArrow kernels don't support yet.
            pa_table = self._cast_string_view_columns(pa_table)

            if push_down_predicate is not None:
                in_memory_dataset = ds.InMemoryDataset(pa_table)
                scanner = in_memory_dataset.scanner(filter=push_down_predicate, batch_size=batch_size)
                self.reader = scanner.to_reader()
            else:
                self.reader = iter(pa_table.to_batches(max_chunksize=batch_size))

    @staticmethod
    def _cast_string_view_columns(table: pa.Table) -> pa.Table:
        new_fields = []
        needs_cast = False
        for field in table.schema:
            if field.type == pa.string_view():
                new_fields.append(field.with_type(pa.utf8()))
                needs_cast = True
            elif field.type == pa.binary_view():
                new_fields.append(field.with_type(pa.binary()))
                needs_cast = True
            else:
                new_fields.append(field)
        if not needs_cast:
            return table
        return table.cast(pa.schema(new_fields))

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            if hasattr(self.reader, 'read_next_batch'):
                return self.reader.read_next_batch()
            else:
                return next(self.reader)
        except StopIteration:
            return None

    def close(self):
        if self.reader is not None:
            self.reader = None
