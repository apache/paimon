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
import zlib
from typing import List, Optional, Tuple

from pypaimon.index.pk.primary_key_index_source_meta import (
    _decode_modified_utf8,
    _encode_modified_utf8,
)


class ManagedBlobReferenceFile:
    """Versioned metadata listing managed BLOB packs referenced by one data file."""

    MAGIC = 0x50424C52
    VERSION = 1
    MANAGED_BLOB_SUFFIX = ".managed.blob"
    REFERENCE_FILE_SUFFIX = ".blobref"

    @staticmethod
    def from_descriptor_uri(descriptor_uri: str) -> Optional["Reference"]:
        if not descriptor_uri.endswith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX):
            return None
        parent, _, name = descriptor_uri.rpartition("/")
        if not parent or not name:
            return None
        return Reference(parent, name)

    @staticmethod
    def sidecar_path(data_file_path: str) -> str:
        return data_file_path + ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX

    @staticmethod
    def sidecar_name(data_file_name: str) -> str:
        return data_file_name + ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX

    @staticmethod
    def write(file_io, path: str, references: List["Reference"]) -> None:
        normalized = sorted(
            references,
            key=lambda ref: (ref.storage_root_id, ref.relative_path),
        )
        unique: List[Reference] = []
        for reference in normalized:
            if not unique or reference != unique[-1]:
                unique.append(reference)

        payload = io.BytesIO()
        payload.write(struct.pack(">B", ManagedBlobReferenceFile.VERSION))
        payload.write(struct.pack(">i", len(unique)))
        for reference in unique:
            _write_modified_utf(payload, reference.storage_root_id)
            _write_modified_utf(payload, reference.relative_path)

        payload_bytes = payload.getvalue()
        checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF
        checksum_signed = (
            checksum if checksum < 0x80000000 else checksum - 0x100000000
        )
        try:
            with file_io.new_output_stream(path) as out:
                out.write(struct.pack(">i", ManagedBlobReferenceFile.MAGIC))
                out.write(payload_bytes)
                out.write(struct.pack(">i", checksum_signed))
        except Exception:
            file_io.delete_quietly(path)
            raise

    @staticmethod
    def read(file_io, path: str) -> List["Reference"]:
        with file_io.new_input_stream(path) as stream:
            data = stream.read()
        if len(data) < 8:
            raise IOError("Invalid managed BLOB reference file: too short")
        magic = struct.unpack_from(">i", data, 0)[0]
        if magic != ManagedBlobReferenceFile.MAGIC:
            raise IOError("Invalid managed BLOB reference file magic: %s" % magic)

        offset = 4
        version = data[offset]
        offset += 1
        if version != ManagedBlobReferenceFile.VERSION:
            raise IOError("Unsupported managed BLOB reference file version: %s" % version)

        count = struct.unpack_from(">i", data, offset)[0]
        offset += 4
        if count < 0:
            raise IOError("Invalid managed BLOB reference count: %s" % count)

        references: List[Reference] = []
        for _ in range(count):
            root, offset = _read_modified_utf(data, offset)
            rel, offset = _read_modified_utf(data, offset)
            references.append(Reference(root, rel))

        if offset + 4 > len(data):
            raise IOError("Invalid managed BLOB reference file checksum")
        expected_checksum = struct.unpack_from(">i", data, offset)[0] & 0xFFFFFFFF
        actual_checksum = zlib.crc32(data[4:offset]) & 0xFFFFFFFF
        if expected_checksum != actual_checksum:
            raise IOError(
                "Invalid managed BLOB reference file checksum. Expected %s but computed %s."
                % (expected_checksum, actual_checksum))
        if offset + 4 != len(data):
            raise IOError("Unexpected trailing bytes in managed BLOB reference file.")
        return references


class Reference:
    """Exact identity of a managed BLOB payload pack."""

    __slots__ = ("storage_root_id", "relative_path")

    def __init__(self, storage_root_id: str, relative_path: str):
        if not storage_root_id:
            raise ValueError("Managed BLOB storage root must not be empty.")
        if not relative_path or "/" in relative_path or relative_path in (".", ".."):
            raise ValueError(
                "Managed BLOB relative path must be a file name: %s." % relative_path)
        self.storage_root_id = storage_root_id
        self.relative_path = relative_path

    def __eq__(self, other) -> bool:
        if not isinstance(other, Reference):
            return False
        return (
            self.storage_root_id == other.storage_root_id
            and self.relative_path == other.relative_path
        )

    def __repr__(self) -> str:
        return "%s/%s" % (self.storage_root_id, self.relative_path)


def _write_modified_utf(out: io.BytesIO, value: str) -> None:
    encoded = _encode_modified_utf8(value)
    if len(encoded) > 65535:
        raise ValueError("Modified UTF-8 string is too long.")
    out.write(struct.pack(">H", len(encoded)))
    out.write(encoded)


def _read_modified_utf(data: bytes, offset: int) -> Tuple[str, int]:
    if offset + 2 > len(data):
        raise IOError("Truncated modified UTF-8 length.")
    length = struct.unpack_from(">H", data, offset)[0]
    offset += 2
    end = offset + length
    if end > len(data):
        raise IOError("Truncated modified UTF-8 payload.")
    return _decode_modified_utf8(data[offset:end]), end
