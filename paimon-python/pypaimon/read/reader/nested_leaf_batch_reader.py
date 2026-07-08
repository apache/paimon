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

from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import RecordBatch

from pypaimon.read.reader.field_indices import blob_field_indices, vector_field_indices
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


class NestedLeafBatchReader(RecordBatchReader):
    """Extract projected nested leaves from batches of full top-level columns.

    The inner reader yields batches carrying the widened top-level columns,
    already normalized to the latest schema by field id (renames followed,
    missing sub-fields padded NULL, types cast). Each requested name path is
    walked through the struct children (a NULL parent propagates to the
    leaf), producing the user's flat projected schema.
    """

    def __init__(self, inner: RecordBatchReader, name_paths: List[List[str]],
                 output_fields: List[DataField]):
        if len(name_paths) != len(output_fields):
            raise ValueError(
                "name_paths length {} does not match output_fields length {}".format(
                    len(name_paths), len(output_fields)))
        self._inner = inner
        self._paths = name_paths
        self._schema = PyarrowFieldParser.from_paimon_schema(output_fields)
        self.file_io = inner.file_io
        self.blob_field_indices = blob_field_indices(output_fields)
        self.vector_field_indices = vector_field_indices(output_fields)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        arrays = []
        for i, path in enumerate(self._paths):
            column = batch.column(path[0])
            for name in path[1:]:
                column = pc.struct_field(column, name)
            target_type = self._schema.field(i).type
            if column.type != target_type:
                column = column.cast(target_type, safe=False)
            arrays.append(column)
        return pa.RecordBatch.from_arrays(arrays, schema=self._schema)

    def close(self) -> None:
        self._inner.close()
