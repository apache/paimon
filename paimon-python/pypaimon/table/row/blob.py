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

import io
import struct
from abc import ABC, abstractmethod
from typing import BinaryIO, Callable, Optional, Union
from urllib.parse import urlparse

from pypaimon.common.identifier import Identifier
from pypaimon.common.uri_reader import UriReader, FileUriReader


class BlobDescriptor:
    CURRENT_VERSION = 2
    MAGIC = 0x424C4F4244455343  # "BLOBDESC"

    def __init__(self, uri: str, offset: int, length: int):
        self._version = self.CURRENT_VERSION
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
        uri_bytes = self._uri.encode('utf-8')
        uri_length = len(uri_bytes)
        data = struct.pack('<B', self._version)  # version (1 byte)
        if self._version > 1:
            data += struct.pack('<Q', self.MAGIC)  # magic (8 bytes, unsigned)
        data += struct.pack('<I', uri_length)  # uri length (4 bytes)
        data += uri_bytes  # uri bytes
        data += struct.pack('<q', self._offset)  # offset (8 bytes, signed)
        data += struct.pack('<q', self._length)  # length (8 bytes, signed)
        return data

    @classmethod
    def deserialize(cls, data: bytes) -> 'BlobDescriptor':
        if len(data) < 5:
            raise ValueError("Invalid BlobDescriptor data: too short")

        offset = 0

        # Read version
        version = struct.unpack('<B', data[offset:offset + 1])[0]
        offset += 1

        if version > cls.CURRENT_VERSION:
            raise ValueError(
                f"Expecting BlobDescriptor version to be less than or equal to "
                f"{cls.CURRENT_VERSION}, but found {version}."
            )

        if version > 1:
            if offset + 8 > len(data):
                raise ValueError("Invalid BlobDescriptor data: too short")
            magic = struct.unpack('<Q', data[offset:offset + 8])[0]
            offset += 8
            if magic != cls.MAGIC:
                raise ValueError(
                    f"Invalid BlobDescriptor: missing magic header. Expected magic: "
                    f"{cls.MAGIC}, but found: {magic}"
                )

        # Read URI length
        if offset + 4 > len(data):
            raise ValueError("Invalid BlobDescriptor data: too short")
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

        descriptor = cls(uri, blob_offset, blob_length)
        descriptor._version = version
        return descriptor

    @classmethod
    def is_blob_descriptor(cls, data: bytes) -> bool:
        if not isinstance(data, (bytes, bytearray)):
            return False
        raw = bytes(data)
        if len(raw) < 9:
            return False

        version = raw[0]
        # v1 descriptors remain deserializable for compatibility,
        # but descriptor detection is v2-only.
        if version == 1:
            return False
        if version > cls.CURRENT_VERSION:
            return False

        try:
            magic = struct.unpack('<Q', raw[1:9])[0]
            return magic == cls.MAGIC
        except Exception:
            return False

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


class BlobViewStruct:
    CURRENT_VERSION = 1
    MAGIC = 0x424C4F4256494557  # "BLOBVIEW"

    def __init__(self, identifier: Union[Identifier, str], field_id: int, row_id: int):
        if isinstance(identifier, str):
            identifier = Identifier.from_string(identifier)
        if not isinstance(identifier, Identifier):
            raise TypeError("BlobViewStruct identifier must be Identifier or str.")
        self._identifier = identifier
        self._field_id = field_id
        self._row_id = row_id

    @property
    def identifier(self) -> Identifier:
        return self._identifier

    @property
    def field_id(self) -> int:
        return self._field_id

    @property
    def row_id(self) -> int:
        return self._row_id

    def serialize(self) -> bytes:
        identifier_bytes = self._identifier.get_full_name().encode('utf-8')
        data = struct.pack('<B', self.CURRENT_VERSION)
        data += struct.pack('<Q', self.MAGIC)
        data += struct.pack('<I', len(identifier_bytes))
        data += identifier_bytes
        data += struct.pack('<i', self._field_id)
        data += struct.pack('<q', self._row_id)
        return data

    @classmethod
    def deserialize(cls, data: bytes) -> 'BlobViewStruct':
        if len(data) < 25:
            raise ValueError("Invalid BlobViewStruct data: too short")

        offset = 0
        version = struct.unpack('<B', data[offset:offset + 1])[0]
        offset += 1
        if version != cls.CURRENT_VERSION:
            raise ValueError(
                f"Expecting BlobViewStruct version to be {cls.CURRENT_VERSION}, "
                f"but found {version}."
            )

        magic = struct.unpack('<Q', data[offset:offset + 8])[0]
        offset += 8
        if magic != cls.MAGIC:
            raise ValueError(
                f"Invalid BlobViewStruct: missing magic header. Expected magic: "
                f"{cls.MAGIC}, but found: {magic}"
            )

        identifier_length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        if offset + identifier_length + 12 > len(data):
            raise ValueError("Invalid BlobViewStruct data: identifier length exceeds data size")

        identifier = data[offset:offset + identifier_length].decode('utf-8')
        offset += identifier_length
        field_id = struct.unpack('<i', data[offset:offset + 4])[0]
        offset += 4
        row_id = struct.unpack('<q', data[offset:offset + 8])[0]
        offset += 8
        if offset != len(data):
            raise ValueError("Invalid BlobViewStruct data: trailing bytes")

        return cls(Identifier.from_string(identifier), field_id, row_id)

    @classmethod
    def is_blob_view_struct(cls, data: bytes) -> bool:
        if not isinstance(data, (bytes, bytearray)):
            return False
        raw = bytes(data)
        if len(raw) < 9:
            return False
        version = raw[0]
        if version != cls.CURRENT_VERSION:
            return False
        try:
            magic = struct.unpack('<Q', raw[1:9])[0]
            return magic == cls.MAGIC
        except Exception:
            return False

    def __eq__(self, other) -> bool:
        if not isinstance(other, BlobViewStruct):
            return False
        return (self._identifier == other._identifier
                and self._field_id == other._field_id
                and self._row_id == other._row_id)

    def __hash__(self) -> int:
        return hash((self._identifier.get_full_name(), self._field_id, self._row_id))

    def __str__(self) -> str:
        return (
            f"BlobViewStruct(identifier={self._identifier.get_full_name()}, "
            f"field_id={self._field_id}, row_id={self._row_id})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class OffsetInputStream(io.RawIOBase):

    def __init__(self, wrapped, offset: int, length: int):
        self._wrapped = wrapped
        self._offset = offset
        self._length = length
        if offset != 0:
            wrapped.seek(offset)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def readinto(self, b):
        if self._length != -1:
            remaining = self._length - self.tell()
            if remaining <= 0:
                return 0
            if len(b) > remaining:
                b = memoryview(b)[:remaining]
        n = self._wrapped.readinto(b)
        return n if n is not None else 0

    def read(self, size=-1):
        if size is None:
            size = -1
        if self._length != -1:
            remaining = self._length - self.tell()
            if remaining <= 0:
                return b''
            if size < 0 or size > remaining:
                size = remaining
        if size < 0:
            return self._wrapped.read()
        return self._wrapped.read(size)

    def seek(self, pos, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            if pos < 0:
                raise ValueError(f"Negative seek position: {pos}")
            target = self._offset + pos
        elif whence == io.SEEK_CUR:
            target = self._wrapped.tell() + pos
            target = max(target, self._offset)
        elif whence == io.SEEK_END:
            if self._length != -1:
                target = self._offset + self._length + pos
            else:
                end = self._wrapped.seek(0, io.SEEK_END)
                target = max(end + pos, self._offset)
            target = max(target, self._offset)
        else:
            raise ValueError(f"Invalid whence: {whence}")
        return self._wrapped.seek(target) - self._offset

    def tell(self) -> int:
        return self._wrapped.tell() - self._offset

    def close(self):
        if not self.closed:
            self._wrapped.close()
            super().close()


class Blob(ABC):

    @abstractmethod
    def to_data(self) -> bytes:
        pass

    @abstractmethod
    def to_descriptor(self) -> BlobDescriptor:
        pass

    @abstractmethod
    def new_input_stream(self) -> BinaryIO:
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
        file_io = FileIO.get(file_uri, {})
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

    @staticmethod
    def from_view(view_struct: BlobViewStruct) -> 'BlobView':
        return BlobView(view_struct)

    @staticmethod
    def from_bytes(data: Optional[bytes], file_io=None, allow_blob_data: bool = True) -> Optional['Blob']:
        if data is None:
            return None
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Blob.from_bytes expects bytes, got {type(data)}")
        data = bytes(data)
        if BlobViewStruct.is_blob_view_struct(data):
            return Blob.from_view(BlobViewStruct.deserialize(data))
        is_descriptor = BlobDescriptor.is_blob_descriptor(data)
        if not allow_blob_data and not is_descriptor:
            raise ValueError(
                "Expected BlobDescriptor bytes, got raw bytes (allow_blob_data=False)"
            )
        if is_descriptor:
            if file_io is None:
                raise ValueError("file_io is required to resolve BlobDescriptor bytes")
            descriptor = BlobDescriptor.deserialize(data)
            uri_reader = file_io.uri_reader_factory.create(descriptor.uri)
            return BlobRef(uri_reader, descriptor)
        return BlobData(data)


class _PlaceholderBlob(Blob):

    def to_data(self) -> bytes:
        raise RuntimeError("Should never call this method for placeholder blob.")

    def to_descriptor(self) -> BlobDescriptor:
        raise RuntimeError("Should never call this method for placeholder blob.")

    def new_input_stream(self) -> BinaryIO:
        raise RuntimeError("Should never call this method for placeholder blob.")


Blob.PLACE_HOLDER = _PlaceholderBlob()


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

    def new_input_stream(self) -> BinaryIO:
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

    def new_input_stream(self) -> BinaryIO:
        uri = self._descriptor.uri
        offset = self._descriptor.offset
        length = self._descriptor.length
        stream = self._uri_reader.new_input_stream(uri)
        try:
            return OffsetInputStream(stream, offset, length)
        except Exception:
            stream.close()
            raise

    def __eq__(self, other) -> bool:
        if not isinstance(other, BlobRef):
            return False
        return self._descriptor == other._descriptor

    def __hash__(self) -> int:
        return hash(self._descriptor)


BlobConsumer = Callable[[str, Optional[BlobDescriptor]], bool]


class BlobView(Blob):

    def __init__(self, view_struct: BlobViewStruct):
        self._view_struct: BlobViewStruct = view_struct
        self._resolved_blob: Optional[BlobRef] = None

    @property
    def view_struct(self) -> BlobViewStruct:
        return self._view_struct

    def is_resolved(self) -> bool:
        return self._resolved_blob is not None

    def resolve(self, uri_reader: UriReader, descriptor: BlobDescriptor):
        self._resolved_blob = BlobRef(uri_reader, descriptor)

    def to_data(self) -> bytes:
        return self._resolved().to_data()

    def to_descriptor(self) -> BlobDescriptor:
        return self._resolved().to_descriptor()

    def new_input_stream(self) -> BinaryIO:
        return self._resolved().new_input_stream()

    def _resolved(self) -> BlobRef:
        if self._resolved_blob is None:
            raise RuntimeError("BlobView is not resolved.")
        return self._resolved_blob

    def __eq__(self, other) -> bool:
        if not isinstance(other, BlobView):
            return False
        return self._view_struct == other._view_struct

    def __hash__(self) -> int:
        return hash(self._view_struct)
