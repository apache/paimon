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
    # v1 wire: version (1) + uri_length (4) + offset (8) + length (8)
    _V1_MIN_WIRE_SIZE = 1 + 4 + 16

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
        # Always write CURRENT_VERSION with magic, matching Java BlobDescriptor.serialize().
        data = struct.pack('<B', self.CURRENT_VERSION)
        data += struct.pack('<Q', self.MAGIC)
        data += struct.pack('<i', uri_length)
        data += uri_bytes
        data += struct.pack('<q', self._offset)
        data += struct.pack('<q', self._length)
        return data

    @classmethod
    def deserialize(cls, data: bytes) -> 'BlobDescriptor':
        if cls is BlobDescriptor:
            return BlobDescriptorSerde.deserialize(data)
        return cls._deserialize(data)

    @classmethod
    def _deserialize(cls, data: bytes) -> 'BlobDescriptor':
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
        uri_length = struct.unpack('<i', data[offset:offset + 4])[0]
        offset += 4
        if uri_length < 0:
            raise ValueError(
                f"Invalid BlobDescriptor data: negative URI length: {uri_length}"
            )

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
    def parse_if_serialized(cls, data: bytes) -> Optional['BlobDescriptor']:
        """Parse when data is exactly a serialized descriptor (no trailing bytes).

        Dispatches through :class:`BlobDescriptorSerde` so an exact
        :class:`VideoFrameDescriptor` is accepted before the ordinary v1/v2
        BlobDescriptor length check. Unlike :meth:`is_blob_descriptor` (v2
        magic header only), this accepts v1 descriptors without a magic
        prefix. Unlike ordinary :meth:`deserialize`, the encoded length must
        match the buffer exactly. Still heuristic: arbitrary inline blob
        bytes could theoretically match.
        """
        if not isinstance(data, (bytes, bytearray)):
            return None
        return BlobDescriptorSerde.parse_if_serialized(bytes(data))

    @classmethod
    def _parse_ordinary_if_serialized(cls, raw: bytes) -> Optional['BlobDescriptor']:
        if len(raw) < cls._V1_MIN_WIRE_SIZE:
            return None
        try:
            offset = 0
            version = raw[offset]
            offset += 1
            if version < 1 or version > cls.CURRENT_VERSION:
                return None
            if version > 1:
                if offset + 8 > len(raw):
                    return None
                magic = struct.unpack('<Q', raw[offset:offset + 8])[0]
                if magic != cls.MAGIC:
                    return None
                offset += 8
            if offset + 4 > len(raw):
                return None
            uri_length = struct.unpack('<i', raw[offset:offset + 4])[0]
            if uri_length < 0:
                return None
            total = offset + 4 + uri_length + 16
            if total != len(raw):
                return None
            return cls._deserialize(raw)
        except (ValueError, struct.error, UnicodeDecodeError):
            return None

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


class VideoFrameDescriptor(BlobDescriptor):
    """Descriptor for one logical frame in an encoded video payload.

    The URI range identifies the complete encoded video. ``frame_index`` is a
    zero-based presentation-order frame ordinal interpreted by the decoder.
    """

    CURRENT_VERSION = 1
    MAGIC = 0x564944454F46524D  # "VIDEOFRM"
    _FIXED_LENGTH = 1 + 8 + 4 + 8 + 8 + 8

    def __init__(self, uri: str, offset: int, length: int, frame_index: int):
        if isinstance(frame_index, bool) or not isinstance(frame_index, int):
            raise TypeError("Video frame index must be an int.")
        if frame_index < 0:
            raise ValueError(
                "Video frame index must be non-negative, but was %s."
                % frame_index
            )
        super().__init__(uri, offset, length)
        self._frame_index = frame_index

    @property
    def frame_index(self) -> int:
        return self._frame_index

    @property
    def payload_descriptor(self) -> BlobDescriptor:
        """Physical video identity without the logical frame locator."""
        return BlobDescriptor(self.uri, self.offset, self.length)

    def serialize(self) -> bytes:
        uri_bytes = self.uri.encode('utf-8')
        return (
            struct.pack('<BQI', self.CURRENT_VERSION, self.MAGIC, len(uri_bytes))
            + uri_bytes
            + struct.pack(
                '<qqq', self.offset, self.length, self.frame_index
            )
        )

    @classmethod
    def deserialize(cls, data: bytes) -> 'VideoFrameDescriptor':
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(
                "VideoFrameDescriptor.deserialize expects bytes, got %s"
                % type(data)
            )
        raw = bytes(data)
        if len(raw) < cls._FIXED_LENGTH:
            raise ValueError("Invalid VideoFrameDescriptor data: too short")

        version, magic, uri_length = struct.unpack('<BQI', raw[:13])
        if version != cls.CURRENT_VERSION:
            raise ValueError(
                "Expecting VideoFrameDescriptor version to be %s, but found %s."
                % (cls.CURRENT_VERSION, version)
            )
        if magic != cls.MAGIC:
            raise ValueError(
                "Invalid VideoFrameDescriptor data: missing magic header"
            )
        expected_length = cls._FIXED_LENGTH + uri_length
        if len(raw) != expected_length:
            message = (
                "trailing bytes"
                if len(raw) > expected_length
                else "invalid URI length: %s" % uri_length
            )
            raise ValueError("Invalid VideoFrameDescriptor data: " + message)

        uri_end = 13 + uri_length
        uri = raw[13:uri_end].decode('utf-8')
        offset, length, frame_index = struct.unpack(
            '<qqq', raw[uri_end:uri_end + 24]
        )
        try:
            return cls(uri, offset, length, frame_index)
        except ValueError as error:
            raise ValueError(
                "Invalid VideoFrameDescriptor data: negative frame index: %s"
                % frame_index
            ) from error

    @classmethod
    def is_video_frame_descriptor(cls, data: bytes) -> bool:
        if not isinstance(data, (bytes, bytearray)) or len(data) < 9:
            return False
        raw = bytes(data)
        return (
            raw[0] == cls.CURRENT_VERSION
            and struct.unpack('<Q', raw[1:9])[0] == cls.MAGIC
        )

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, VideoFrameDescriptor)
            and self.payload_descriptor == other.payload_descriptor
            and self.frame_index == other.frame_index
        )

    def __hash__(self) -> int:
        return hash((self.payload_descriptor, self.frame_index))

    def __str__(self) -> str:
        return (
            "VideoFrameDescriptor(payload=%s, frame_index=%s)"
            % (self.payload_descriptor, self.frame_index)
        )


class BlobDescriptorSerde:
    """Single dispatch point for persisted BlobDescriptor wire types."""

    @staticmethod
    def is_descriptor(data: bytes) -> bool:
        return (
            VideoFrameDescriptor.is_video_frame_descriptor(data)
            or BlobDescriptor.is_blob_descriptor(data)
        )

    @staticmethod
    def deserialize(data: bytes) -> BlobDescriptor:
        if VideoFrameDescriptor.is_video_frame_descriptor(data):
            return VideoFrameDescriptor.deserialize(data)
        return BlobDescriptor._deserialize(data)

    @staticmethod
    def parse_if_serialized(data: bytes) -> Optional[BlobDescriptor]:
        """Exact-length parse for any persisted BlobDescriptor wire type."""
        if not isinstance(data, (bytes, bytearray)):
            return None
        raw = bytes(data)
        if VideoFrameDescriptor.is_video_frame_descriptor(raw):
            try:
                return VideoFrameDescriptor.deserialize(raw)
            except (ValueError, struct.error, UnicodeDecodeError):
                return None
        return BlobDescriptor._parse_ordinary_if_serialized(raw)


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
    def _blob_ref_from_descriptor(
            descriptor: 'BlobDescriptor', file_io=None, uri_reader_factory=None,
    ) -> 'BlobRef':
        if uri_reader_factory is None:
            if file_io is None:
                raise ValueError("file_io is required to resolve BlobDescriptor bytes")
            uri_reader = UriReader.from_file(file_io)
        else:
            uri_reader = uri_reader_factory.create(descriptor.uri)
        return BlobRef(uri_reader, descriptor)

    @staticmethod
    def from_descriptor_bytes(
            data: Optional[bytes], file_io=None, uri_reader_factory=None,
    ) -> Optional['Blob']:
        """Build a Blob from bytes known to contain a descriptor.

        Version 1 descriptors have no magic header, so they cannot be
        distinguished safely from arbitrary payload bytes. Callers which know
        from schema or storage context that a value is a descriptor must use
        this method instead of the heuristic :meth:`from_bytes` entry point.

        Parsing uses :meth:`BlobDescriptor.deserialize`, matching Java: a
        valid v1/v2 prefix is accepted and trailing bytes after that prefix
        are ignored. This is not a detector; garbage that happens to look
        like a v1 prefix can produce a BlobRef with a nonsense URI.
        Bytes that are not a parseable prefix raise :class:`ValueError`.
        """
        if data is None:
            return None
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(
                f"Blob.from_descriptor_bytes expects bytes, got {type(data)}")

        try:
            descriptor = BlobDescriptor.deserialize(bytes(data))
        except (ValueError, struct.error, UnicodeDecodeError) as exc:
            raise ValueError(
                "Expected BlobDescriptor bytes, got raw bytes") from exc
        return Blob._blob_ref_from_descriptor(
            descriptor, file_io=file_io, uri_reader_factory=uri_reader_factory)

    @staticmethod
    def from_view(view_struct: BlobViewStruct) -> 'BlobView':
        return BlobView(view_struct)

    @staticmethod
    def from_bytes(
            data: Optional[bytes], file_io=None, uri_reader_factory=None, allow_blob_data: bool = True,
    ) -> Optional['Blob']:
        if data is None:
            return None
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Blob.from_bytes expects bytes, got {type(data)}")
        data = bytes(data)
        if BlobViewStruct.is_blob_view_struct(data):
            return Blob.from_view(BlobViewStruct.deserialize(data))
        if BlobDescriptorSerde.is_descriptor(data) or not allow_blob_data:
            try:
                descriptor = BlobDescriptor.deserialize(data)
            except (ValueError, struct.error, UnicodeDecodeError) as exc:
                raise ValueError(
                    "Expected BlobDescriptor bytes, got raw bytes"
                    + ("" if allow_blob_data else " (allow_blob_data=False)")
                ) from exc
            return Blob._blob_ref_from_descriptor(
                descriptor, file_io=file_io, uri_reader_factory=uri_reader_factory)
        return BlobData(data)


class _PlaceholderBlob(Blob):

    def to_data(self) -> bytes:
        raise RuntimeError("Should never call this method for placeholder blob.")

    def to_descriptor(self) -> BlobDescriptor:
        raise RuntimeError("Should never call this method for placeholder blob.")

    def new_input_stream(self) -> BinaryIO:
        raise RuntimeError("Should never call this method for placeholder blob.")


Blob.PLACE_HOLDER = _PlaceholderBlob()


class _PlaceholderBlobArray:

    def __repr__(self) -> str:
        return "Blob.ARRAY_PLACE_HOLDER"


Blob.ARRAY_PLACE_HOLDER = _PlaceholderBlobArray()


class _PlaceholderBlobMap:

    def __repr__(self) -> str:
        return "Blob.MAP_PLACE_HOLDER"


Blob.MAP_PLACE_HOLDER = _PlaceholderBlobMap()


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

    @property
    def uri_reader(self) -> UriReader:
        """UriReader used to fetch this blob's payload."""
        return self._uri_reader

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


def video_payload_descriptor(value) -> Optional[BlobDescriptor]:
    """Return the physical payload identity represented by a video frame value."""
    if hasattr(value, 'as_py'):
        value = value.as_py()
    if value is None or value is Blob.PLACE_HOLDER:
        return None
    if type(value) is BlobRef:
        descriptor = value.to_descriptor()
        return (
            descriptor.payload_descriptor
            if isinstance(descriptor, VideoFrameDescriptor)
            else None
        )
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        if VideoFrameDescriptor.is_video_frame_descriptor(raw):
            return VideoFrameDescriptor.deserialize(raw).payload_descriptor
    return None


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
