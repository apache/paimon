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

from typing import Optional

from pyarrow import RecordBatch

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class BlobDescriptorConvertReader(RecordBatchReader):
    def __init__(self, inner: RecordBatchReader, table):
        self._inner = inner
        self._table = table
        self._descriptor_fields = CoreOptions.blob_descriptor_fields(table.options)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        import pyarrow
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        return self._convert_batch(batch, pyarrow)

    def _convert_batch(self, batch, pyarrow):
        from pypaimon.table.row.blob import Blob, BlobDescriptor

        result = batch
        for field_name in self._descriptor_fields:
            if field_name not in result.schema.names:
                continue
            values = result.column(field_name).to_pylist()
            converted_values = []
            for value in values:
                if value is None:
                    converted_values.append(None)
                    continue
                if hasattr(value, 'as_py'):
                    value = value.as_py()
                if isinstance(value, str):
                    value = value.encode('utf-8')
                if isinstance(value, bytearray):
                    value = bytes(value)
                if not isinstance(value, bytes):
                    converted_values.append(value)
                    continue
                try:
                    descriptor = BlobDescriptor.deserialize(value)
                    if descriptor.serialize() != value:
                        converted_values.append(value)
                        continue
                    uri_reader = self._table.file_io.uri_reader_factory.create(descriptor.uri)
                    converted_values.append(Blob.from_descriptor(uri_reader, descriptor).to_data())
                except Exception:
                    converted_values.append(value)

            column_idx = result.schema.names.index(field_name)
            result = result.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )
        return result

    def close(self):
        self._inner.close()
