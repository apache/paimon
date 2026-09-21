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

import bisect
import struct

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.uri_reader import UriReader
from pypaimon.table.row.blob import Blob, VideoFrameDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind


class VideoFileMeta:
    """Validated embedded index of a ``.video`` file."""

    VERSION = 1
    MAGIC_NUMBER = 0x4F454449
    FOOTER_SIZE = 21
    NULL_REFERENCE = -1
    PLACE_HOLDER_REFERENCE = -2

    def __init__(self, stream, file_size: int):
        if file_size < self.FOOTER_SIZE:
            raise IOError(
                "Corrupt video file: file is smaller than its footer."
            )
        footer_start = file_size - self.FOOTER_SIZE
        stream.seek(footer_start)
        footer = stream.read(self.FOOTER_SIZE)
        if len(footer) != self.FOOTER_SIZE:
            raise IOError("Corrupt video file: cannot read footer.")
        lengths = struct.unpack('<IIIIIB', footer)
        index_lengths = lengths[:4]
        magic, version = lengths[4:]
        if magic != self.MAGIC_NUMBER:
            raise IOError(
                "Corrupt video file: invalid footer magic %s." % magic
            )
        if version != self.VERSION:
            raise IOError("Unsupported video format version: %s" % version)

        total_index_length = sum(index_lengths)
        if total_index_length > footer_start:
            raise IOError("Corrupt video file: indexes exceed the file size.")
        index_start = footer_start - total_index_length
        indexes = []
        offset = index_start
        for name, length in zip(
                ("physical video", "run length", "run reference", "first frame"),
                index_lengths):
            stream.seek(offset)
            raw = stream.read(length)
            if len(raw) != length:
                raise IOError(
                    "Corrupt video file: cannot read %s index." % name
                )
            indexes.append(DeltaVarintCompressor.decompress(raw))
            offset += length

        physical_lengths, run_lengths, references, first_frames = indexes
        physical_offsets = []
        payload_offset = 0
        for ordinal, length in enumerate(physical_lengths):
            if length <= 0 or length > index_start - payload_offset:
                raise IOError(
                    "Corrupt video file: invalid physical video length %s "
                    "at ordinal %s." % (length, ordinal)
                )
            physical_offsets.append(payload_offset)
            payload_offset += length
        if payload_offset != index_start:
            raise IOError(
                "Corrupt video file: indexed videos use %s bytes, but payload "
                "region contains %s bytes." % (payload_offset, index_start)
            )

        if not (len(run_lengths) == len(references) == len(first_frames)):
            raise IOError(
                "Corrupt video file: run indexes have different counts."
            )
        run_ends = []
        row_count = 0
        for run, (length, reference, first_frame) in enumerate(zip(
                run_lengths, references, first_frames)):
            if length <= 0:
                raise IOError(
                    "Corrupt video file: invalid run length %s at run %s."
                    % (length, run)
                )
            if (
                reference not in (
                    self.NULL_REFERENCE, self.PLACE_HOLDER_REFERENCE
                )
                and (reference < 0 or reference >= len(physical_lengths))
            ):
                raise IOError(
                    "Corrupt video file: run %s references physical video %s, "
                    "but physical video count is %s."
                    % (run, reference, len(physical_lengths))
                )
            if reference >= 0 and first_frame < 0:
                raise IOError(
                    "Corrupt video file: run %s has negative first frame %s."
                    % (run, first_frame)
                )
            row_count += length
            run_ends.append(row_count)

        self.physical_lengths = physical_lengths
        self.physical_offsets = physical_offsets
        self.run_ends = run_ends
        self.references = references
        self.first_frames = first_frames
        self.row_count = row_count
        self.selected_positions = None

    @property
    def record_count(self) -> int:
        return (
            self.row_count
            if self.selected_positions is None
            else len(self.selected_positions)
        )

    def select(self, row_indices) -> None:
        selected = []
        for value in row_indices:
            position = int(value)
            if position < 0 or position >= self.row_count:
                raise IndexError(
                    "Video row index %s is out of range, record count: %s."
                    % (position, self.row_count)
                )
            selected.append(position)
        self.selected_positions = selected

    def logical_position(self, returned_row: int) -> int:
        if self.selected_positions is None:
            return returned_row
        return self.selected_positions[returned_row]

    def frame(self, returned_row: int):
        logical = self.logical_position(returned_row)
        run = bisect.bisect_left(self.run_ends, logical + 1)
        reference = self.references[run]
        if reference == self.NULL_REFERENCE:
            return None
        if reference == self.PLACE_HOLDER_REFERENCE:
            return Blob.PLACE_HOLDER
        run_start = 0 if run == 0 else self.run_ends[run - 1]
        return (
            self.physical_offsets[reference],
            self.physical_lengths[reference],
            self.first_frames[run] + logical - run_start,
        )


class VideoFrameRecordIterator:

    def __init__(self, file_io, file_path, meta, field):
        self.file_io = file_io
        self.file_path = file_path
        self.meta = meta
        self.field = field
        self.current_position = 0
        self._uri_reader = UriReader.from_file(file_io)

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_position >= self.meta.record_count:
            raise StopIteration
        value = self.meta.frame(self.current_position)
        if isinstance(value, tuple):
            offset, length, frame_index = value
            descriptor = VideoFrameDescriptor(
                self.file_path, offset, length, frame_index
            )
            value = Blob.from_descriptor(self._uri_reader, descriptor)
        self.current_position += 1
        return GenericRow([value], [self.field], RowKind.INSERT)
