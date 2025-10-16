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

import io
from abc import ABC, abstractmethod
from typing import Optional, Union
from urllib.parse import urlparse

from pypaimon.common.uri_reader import UriReader, FileUriReader


class BlobDescriptor:
    CURRENT_VERSION = 1

    def __init__(self, uri: str, offset: int, length: int, version: int = CURRENT_VERSION):
        self._version = version
        self._uri = uri
        self._offset = offset
        self._length = length

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def length(self) -> int:
        return self._length

    @property
    def version(self) -> int:
        return self._version

    def serialize(self) -> bytes:
        import struct

        uri_bytes = self._uri.encode('utf-8')
        uri_length = len(uri_bytes)

        # Pack using little endian format
        data = struct.pack('<B', self._version)  # version (1 byte)
        data += struct.pack('<I', uri_length)  # uri length (4 bytes)
        data += uri_bytes  # uri bytes
        data += struct.pack('<q', self._offset)  # offset (8 bytes, signed)
        data += struct.pack('<q', self._length)  # length (8 bytes, signed)

        return data

    @classmethod
    def deserialize(cls, data: bytes) -> 'BlobDescriptor':
        import struct

        if len(data) < 5:  # minimum size: version(1) + uri_length(4)
            raise ValueError("Invalid BlobDescriptor data: too short")

        offset = 0

        # Read version
        version = struct.unpack('<B', data[offset:offset + 1])[0]
        offset += 1

        # For now, we only support version 1, but allow flexibility for future versions
        if version < 1:
            raise ValueError(f"Unsupported BlobDescriptor version: {version}")

        # Read URI length
        uri_length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4

        # Read URI bytes
        if offset + uri_length > len(data):
            raise ValueError("Invalid BlobDescriptor data: URI length exceeds data size")

        uri_bytes = data[offset:offset + uri_length]
        uri = uri_bytes.decode('utf-8')
        offset += uri_length

        # Read offset and length
        if offset + 16 > len(data):
            raise ValueError("Invalid BlobDescriptor data: missing offset/length")

        blob_offset = struct.unpack('<q', data[offset:offset + 8])[0]
        offset += 8

        blob_length = struct.unpack('<q', data[offset:offset + 8])[0]

        return cls(uri, blob_offset, blob_length, version)

    def __eq__(self, other) -> bool:
        """Check equality with another BlobDescriptor."""
        if not isinstance(other, BlobDescriptor):
            return False
        return (self._version == other._version and
                self._uri == other._uri and
                self._offset == other._offset and
                self._length == other._length)

    def __hash__(self) -> int:
        """Calculate hash for the BlobDescriptor."""
        return hash((self._version, self._uri, self._offset, self._length))

    def __str__(self) -> str:
        """String representation of BlobDescriptor."""
        return (f"BlobDescriptor(version={self._version}, uri='{self._uri}', "
                f"offset={self._offset}, length={self._length})")

    def __repr__(self) -> str:
        """Detailed representation of BlobDescriptor."""
        return self.__str__()


class Blob(ABC):

    @abstractmethod
    def to_data(self) -> bytes:
        pass

    @abstractmethod
    def to_descriptor(self) -> BlobDescriptor:
        pass

    @abstractmethod
    def new_input_stream(self) -> io.BytesIO:
        pass

    @staticmethod
    def from_data(data: bytes) -> 'Blob':
        return BlobData(data)

    @staticmethod
    def from_local(file: str) -> 'Blob':
        # Import FileIO locally to avoid circular imports
        from pypaimon.common.file_io import FileIO

        parsed = urlparse(file)
        if parsed.scheme == "file":
            file_uri = file
        else:
            file_uri = f"file://{file}"
        file_io = FileIO(file_uri, {})
        uri_reader = FileUriReader(file_io)
        descriptor = BlobDescriptor(file, 0, -1)
        return Blob.from_descriptor(uri_reader, descriptor)

    @staticmethod
    def from_http(uri: str) -> 'Blob':
        descriptor = BlobDescriptor(uri, 0, -1)
        return BlobRef(UriReader.from_http(), descriptor)

    @staticmethod
    def from_file(file_io, file_path: str, offset: int, length: int) -> 'Blob':
        uri_reader = FileUriReader(file_io)
        descriptor = BlobDescriptor(file_path, offset, length)
        return Blob.from_descriptor(uri_reader, descriptor)

    @staticmethod
    def from_descriptor(uri_reader: UriReader, descriptor: BlobDescriptor) -> 'Blob':
        return BlobRef(uri_reader, descriptor)


class BlobData(Blob):

    def __init__(self, data: Optional[Union[bytes, bytearray]] = None):
        if data is None:
            self._data = b''
        elif isinstance(data, (bytes, bytearray)):
            self._data = bytes(data)
        else:
            raise TypeError(f"BlobData expects bytes, bytearray, or None, got {type(data)}")

    @classmethod
    def from_bytes(cls, data: bytes) -> 'BlobData':
        return cls(data)

    @property
    def data(self) -> bytes:
        return self._data

    def to_data(self) -> bytes:
        return self._data

    def to_descriptor(self) -> 'BlobDescriptor':
        raise RuntimeError("Blob data can not convert to descriptor.")

    def new_input_stream(self) -> io.BytesIO:
        return io.BytesIO(self._data)

    def __eq__(self, other) -> bool:
        if other is None or not isinstance(other, BlobData):
            return False
        return self._data == other._data

    def __hash__(self) -> int:
        return hash(self._data)


class BlobRef(Blob):

    def __init__(self, uri_reader: UriReader, descriptor: BlobDescriptor):
        self._uri_reader = uri_reader
        self._descriptor = descriptor

    def to_data(self) -> bytes:
        try:
            with self.new_input_stream() as stream:
                return stream.read()
        except Exception as e:
            raise IOError(f"Failed to read blob data: {e}")

    def to_descriptor(self) -> BlobDescriptor:
        return self._descriptor

    def new_input_stream(self) -> io.BytesIO:
        uri = self._descriptor.uri
        offset = self._descriptor.offset
        length = self._descriptor.length
        with self._uri_reader.new_input_stream(uri) as input_stream:
            if offset > 0:
                input_stream.seek(offset)
            if length == -1:
                data = input_stream.read()
            else:
                data = input_stream.read(length)
            return io.BytesIO(data)

    def __eq__(self, other) -> bool:
        if not isinstance(other, BlobRef):
            return False
        return self._descriptor == other._descriptor

    def __hash__(self) -> int:
        return hash(self._descriptor)
