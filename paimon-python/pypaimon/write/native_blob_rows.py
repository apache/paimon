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

"""Transport row Blob streams into the core URI copy path."""

from collections.abc import Mapping
from uuid import uuid4

from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.schema.data_types import ArrayType, MapType
from pypaimon.table.row.blob import Blob, BlobDescriptor


class _BlobRowReader:
    def __init__(self, blob):
        self.blob = blob

    def new_input_stream(self, uri):
        return self.blob.new_input_stream()


class NativeBlobRows:
    """Expose each Python Blob lazily, without opening or materializing it.

    The synthetic descriptors only transport live objects for this operation.
    Core copies their payloads into normal .blob records; these URIs must never
    become stored descriptors. A new reader is kept for each Blob object so
    the core cannot reuse an unrelated Python stream.
    """

    def __init__(self, file_io):
        self.readers = {}
        self.fallback = UriReaderFactory.from_file_io(file_io)

    def create(self, uri):
        if uri in self.readers:
            return self.readers[uri]
        return self.fallback.create(uri)

    def encode(self, value, data_type):
        if hasattr(value, 'as_py'):
            value = value.as_py()
        if value is None:
            return None
        if isinstance(data_type, ArrayType):
            if isinstance(value, (bytes, bytearray, str)):
                raise ValueError('ARRAY<BLOB> field value must be a list or tuple.')
            return [self.encode(item, data_type.element) for item in value]
        if isinstance(data_type, MapType):
            entries = value.items() if isinstance(value, Mapping) else value
            return [(key, self.encode(item, data_type.value)) for key, item in entries]
        if isinstance(value, Blob):
            if any(value is placeholder for placeholder in (
                    Blob.PLACE_HOLDER, Blob.ARRAY_PLACE_HOLDER, Blob.MAP_PLACE_HOLDER)):
                raise ValueError('Blob placeholders are reserved for unchanged rows.')
            uri = 'pypaimon-blob-row://' + uuid4().hex
            self.readers[uri] = _BlobRowReader(value)
            return BlobDescriptor(uri, 0, -1).serialize()
        return value
