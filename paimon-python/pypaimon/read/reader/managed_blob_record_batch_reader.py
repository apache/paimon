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

from pypaimon.blob.managed_blob_payload import materialize_managed_blob
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class ManagedBlobRecordBatchReader(RecordBatchReader):
    """Resolve managed payloads before filtering, limiting or projecting rows."""

    def __init__(self, reader, specs, file_io, table_path):
        self.reader = reader
        self.specs = specs
        self.table_path = table_path
        self._adopt_metadata(reader)
        self.file_io = file_io
        # Resolved scalar BLOBs must be exposed as payloads by get_blob().
        resolved = {index for index, _name, _kind in specs}
        self.descriptor_field_indices = (
            set(self.descriptor_field_indices or ()) - resolved)

    def read_arrow_batch(self):
        batch = self.reader.read_arrow_batch()
        if batch is None:
            return None
        self._refresh_blob_view_lookup(self.reader)
        for _index, name, kind in self.specs:
            column_index = batch.schema.get_field_index(name)
            field = batch.schema.field(column_index)
            values = [
                materialize_managed_blob(value, kind, self.file_io, self.table_path)
                for value in batch.column(column_index).to_pylist()
            ]
            batch = batch.set_column(
                column_index, field, pa.array(values, type=field.type))
        return batch

    def close(self):
        self.reader.close()
