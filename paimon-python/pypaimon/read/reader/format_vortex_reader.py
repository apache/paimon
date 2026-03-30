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

        # Try to convert Arrow predicate to Vortex expr for native push-down
        vortex_expr = None
        if push_down_predicate is not None:
            try:
                from vortex.arrow.expression import arrow_to_vortex
                arrow_schema = vortex_file.dtype.to_arrow_schema()
                vortex_expr = arrow_to_vortex(push_down_predicate, arrow_schema)
            except Exception:
                pass

        indices = None
        if row_indices is not None:
            indices = vortex.array(row_indices)

        self.record_batch_reader = vortex_file.scan(
            columns_for_vortex, expr=vortex_expr, indices=indices, batch_size=batch_size).to_arrow()

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            return next(self.record_batch_reader)
        except StopIteration:
            return None

    def close(self):
        self.scan_iter = None
