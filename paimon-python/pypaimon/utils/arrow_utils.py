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

import pyarrow as pa


def zero_column_batch(num_rows: int, metadata=None) -> pa.RecordBatch:
    """Build a zero-column batch without losing its logical row count."""
    empty_struct = pa.Array.from_buffers(
        pa.struct([]), num_rows, [None], children=[])
    return pa.RecordBatch.from_struct_array(empty_struct).replace_schema_metadata(metadata)


# Internal provenance: Arrow represents BlobData and BlobRef as the same bytes
# type. Payloads may themselves parse as references, so inspecting their bytes
# cannot distinguish them. Remove this marker before exposing output batches.
_ROW_BLOB_DATA = b'paimon.row-sidecar.blob-data'


def is_blob_data(field):
    return _ROW_BLOB_DATA in (field.metadata or {})


def as_blob_data(field):
    metadata = dict(field.metadata or {})
    metadata[_ROW_BLOB_DATA] = b'true'
    return field.with_metadata(metadata)


def clear_blob_data(batch):
    if not any(is_blob_data(field) for field in batch.schema):
        return batch
    fields = []
    for field in batch.schema:
        metadata = dict(field.metadata or {})
        metadata.pop(_ROW_BLOB_DATA, None)
        fields.append(field.with_metadata(metadata or None))
    return pa.RecordBatch.from_arrays(
        batch.columns, schema=pa.schema(fields, metadata=batch.schema.metadata))
