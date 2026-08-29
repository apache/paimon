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

import struct

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.schema.data_types import is_blob_type
from pypaimon.table.row.blob import Blob, BlobRef
from pypaimon.write.blob_format_writer import BlobFormatWriter


class SharedBlobFormatWriter(BlobFormatWriter):
    """Writes one physical payload for every exact descriptor in a file."""

    VERSION = 1
    FOOTER_MAGIC_NUMBER = 0x4C424853
    NULL_REFERENCE = -1
    PLACE_HOLDER_REFERENCE = -2

    def __init__(self, output_stream, file_path=None,
                 copy_buffer_size=BlobFormatWriter.BUFFER_SIZE):
        super().__init__(
            output_stream,
            blob_consumer=None,
            file_path=file_path,
            copy_buffer_size=copy_buffer_size,
        )
        self._row_references = []
        self._physical_blobs = {}

    def add_element(self, row) -> None:
        if not hasattr(row, 'values') or len(row.values) != 1:
            raise ValueError("SharedBlobFormatWriter only supports one field")
        if not is_blob_type(row.fields[0].type):
            raise ValueError("SharedBlobFormatWriter only supports one scalar BLOB field")

        blob_value = row.values[0]
        if blob_value is None:
            self._row_references.append(self.NULL_REFERENCE)
            return
        if blob_value is Blob.PLACE_HOLDER:
            self._row_references.append(self.PLACE_HOLDER_REFERENCE)
            return
        if type(blob_value) is not BlobRef:
            raise ValueError(
                "Shared blob fields require an exact BlobRef with a stable descriptor; "
                "inline BlobData and custom Blob implementations are not supported."
            )

        descriptor = blob_value.to_descriptor()
        ordinal = self._physical_blobs.get(descriptor)
        if ordinal is None:
            super().add_blob(row.fields[0].name, blob_value)
            ordinal = len(self.lengths) - 1
            self._physical_blobs[descriptor] = ordinal
        self._row_references.append(ordinal)

    @property
    def physical_blob_count(self) -> int:
        return len(self.lengths)

    def close(self) -> None:
        physical_index = DeltaVarintCompressor.compress(self.lengths)
        row_index = DeltaVarintCompressor.compress(self._row_references)
        self.output_stream.write(physical_index)
        self.output_stream.write(row_index)
        self.output_stream.write(
            struct.pack(
                '<IIIB',
                len(physical_index),
                len(row_index),
                self.FOOTER_MAGIC_NUMBER,
                self.VERSION,
            )
        )
        if hasattr(self.output_stream, 'flush'):
            self.output_stream.flush()
        if hasattr(self.output_stream, 'close'):
            self.output_stream.close()
