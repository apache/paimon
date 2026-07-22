# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import List, Optional

import pyarrow as pa
from pyarrow import RecordBatch

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.table.row.blob import Blob


class DeferredBlobResolveReader(RecordBatchReader):
    """Materialize projected BLOB payloads after row filtering.

    This must remain the outermost BLOB materialization layer because adopted
    metadata still identifies the materialized columns as logical BLOB fields.
    """

    def __init__(self, inner: RecordBatchReader, file_io,
                 blob_field_names: List[str], blob_parallelism: int = 1):
        self._inner = inner
        self._file_io = file_io
        self._blob_field_names = blob_field_names
        self._blob_parallelism = max(1, blob_parallelism)
        self._adopt_metadata(inner)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None

        columns = list(batch.columns)
        fields = list(batch.schema)
        changed = False
        for field_name in self._blob_field_names:
            column_index = batch.schema.get_field_index(field_name)
            if column_index < 0:
                continue
            values = batch.column(column_index).to_pylist()
            blobs = [Blob.from_bytes(value, self._file_io) for value in values]
            payloads = self._file_io.read_blobs_concurrent(
                blobs, self._blob_parallelism)
            source_field = batch.schema.field(column_index)
            columns[column_index] = pa.array(payloads, type=pa.large_binary())
            fields[column_index] = pa.field(
                field_name,
                pa.large_binary(),
                nullable=source_field.nullable,
                metadata=source_field.metadata,
            )
            changed = True
        if not changed:
            return batch
        return pa.RecordBatch.from_arrays(
            columns,
            schema=pa.schema(fields, metadata=batch.schema.metadata),
        )

    def close(self) -> None:
        self._inner.close()
