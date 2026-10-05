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

"""Sparse seek metadata for encoded video payloads."""

import operator
import struct
import zlib


class VideoKeyframeIndex:
    """Persist video metadata ranges and presentation-order keyframes."""

    VERSION = 1
    MAGIC = 0x564944454F4B4649  # "VIDEOKFI"
    HEADER = struct.Struct('<BQII')
    METADATA_RANGE = struct.Struct('<qq')
    ENTRY = struct.Struct('<qqq')
    MAX_METADATA_RANGE_COUNT = 64 * 1024
    MAX_KEYFRAME_COUNT = 64 * 1024
    _CHUNK_SIZE = 64 * 1024

    def __init__(self, metadata_ranges, keyframes):
        normalized_metadata_ranges = []
        metadata_range_count = 0
        for offset, length in metadata_ranges:
            if metadata_range_count >= self.MAX_METADATA_RANGE_COUNT:
                raise ValueError(
                    "Video keyframe index exceeds the %s-metadata-range "
                    "limit." % self.MAX_METADATA_RANGE_COUNT
                )
            metadata_range_count += 1
            offset, length = operator.index(offset), operator.index(length)
            if normalized_metadata_ranges:
                previous_offset, previous_length = (
                    normalized_metadata_ranges[-1])
                if previous_offset + previous_length == offset:
                    normalized_metadata_ranges[-1] = (
                        previous_offset, previous_length + length)
                    continue
            normalized_metadata_ranges.append((offset, length))
        self.metadata_ranges = tuple(normalized_metadata_ranges)
        normalized_keyframes = []
        for ordinal, pts, position in keyframes:
            if len(normalized_keyframes) >= self.MAX_KEYFRAME_COUNT:
                raise ValueError(
                    "Video keyframe index exceeds the %s-entry limit."
                    % self.MAX_KEYFRAME_COUNT
                )
            normalized_keyframes.append((
                operator.index(ordinal), operator.index(pts),
                operator.index(position),
            ))
        self.keyframes = tuple(normalized_keyframes)
        previous_end = 0
        for offset, length in self.metadata_ranges:
            if offset < previous_end or length <= 0:
                raise ValueError(
                    "Video metadata ranges must be ordered, non-overlapping, "
                    "and non-empty."
                )
            previous_end = offset + length
        if not self.keyframes or self.keyframes[0][0] != 0:
            raise ValueError("Video keyframe index requires an initial keyframe.")
        previous_ordinal, previous_pts, previous_position = -1, None, -1
        for ordinal, pts, position in self.keyframes:
            if ordinal <= previous_ordinal:
                raise ValueError("Keyframe ordinals must be strictly increasing.")
            if previous_pts is not None and pts <= previous_pts:
                raise ValueError("Keyframe timestamps must be strictly increasing.")
            if position <= previous_position:
                raise ValueError(
                    "Keyframe packet positions must be non-negative and "
                    "strictly increasing."
                )
            previous_ordinal, previous_pts, previous_position = (
                ordinal, pts, position)

    def serialize(self):
        entries = b''.join(self.ENTRY.pack(*entry) for entry in self.keyframes)
        return self.HEADER.pack(
            self.VERSION,
            self.MAGIC,
            len(self.metadata_ranges),
            len(self.keyframes),
        ) + b''.join(
            self.METADATA_RANGE.pack(*value)
            for value in self.metadata_ranges
        ) + zlib.compress(entries)

    @classmethod
    def inspect(cls, source, payload_length):
        """Build an index for the first video stream of an ISO BMFF file."""
        try:
            import av
        except ImportError as error:
            raise ImportError(
                "Video seek-index generation requires PyAV."
            ) from error

        metadata_ranges = cls._iso_bmff_metadata_ranges(
            source, payload_length)
        source.seek(0)
        with av.open(source) as container:
            if not container.streams.video:
                raise ValueError("Video has no video stream.")
            stream = container.streams.video[0]
            packet_positions = {}
            for packet in container.demux(stream):
                if (not packet.is_keyframe or packet.pts is None
                        or packet.pos is None or packet.pos < 0
                        or getattr(packet, "is_discard", False)):
                    continue
                pts = cls._stream_pts(
                    packet.pts,
                    packet.time_base or stream.time_base,
                    stream.time_base,
                )
                if pts in packet_positions:
                    raise ValueError("Video has duplicate keyframe timestamps.")
                if len(packet_positions) >= cls.MAX_KEYFRAME_COUNT:
                    raise ValueError(
                        "Video keyframe index exceeds the %s-entry limit."
                        % cls.MAX_KEYFRAME_COUNT
                    )
                packet_positions[pts] = int(packet.pos)

        source.seek(0)
        with av.open(source) as container:
            stream = container.streams.video[0]
            keyframes = []
            previous_pts = None
            for ordinal, frame in enumerate(container.decode(stream)):
                if frame.pts is None:
                    raise ValueError("Video frame has no presentation timestamp.")
                pts = cls._stream_pts(
                    frame.pts,
                    frame.time_base or stream.time_base,
                    stream.time_base,
                )
                if previous_pts is not None and pts <= previous_pts:
                    raise ValueError(
                        "Video frame timestamps must be strictly increasing."
                    )
                if frame.key_frame:
                    position = packet_positions.get(pts)
                    if position is None:
                        raise ValueError(
                            "Video keyframe has no packet byte position."
                        )
                    if len(keyframes) >= cls.MAX_KEYFRAME_COUNT:
                        raise ValueError(
                            "Video keyframe index exceeds the %s-entry limit."
                            % cls.MAX_KEYFRAME_COUNT
                        )
                    keyframes.append((ordinal, pts, position))
                previous_pts = pts
        return cls(metadata_ranges, keyframes)

    @staticmethod
    def _stream_pts(pts, time_base, stream_time_base):
        value = pts * time_base / stream_time_base
        if value.denominator != 1:
            raise ValueError(
                "Video timestamp does not fit stream time base."
            )
        return int(value)

    @classmethod
    def _iso_bmff_metadata_ranges(cls, source, payload_length):
        ranges = []
        has_moov = False
        offset = 0
        while offset < payload_length:
            source.seek(offset)
            header = source.read(8)
            if len(header) != 8:
                raise ValueError("Invalid ISO BMFF box header.")
            size, box_type = struct.unpack(">I4s", header)
            header_size = 8
            if size == 1:
                extended = source.read(8)
                if len(extended) != 8:
                    raise ValueError("Invalid ISO BMFF extended box header.")
                size = struct.unpack(">Q", extended)[0]
                header_size = 16
            elif size == 0:
                size = payload_length - offset
            if size < header_size or offset + size > payload_length:
                raise ValueError("Invalid ISO BMFF box size.")
            length = (
                header_size if box_type in (b"mdat", b"free", b"skip")
                else size
            )
            if ranges and ranges[-1][0] + ranges[-1][1] == offset:
                ranges[-1] = (ranges[-1][0], ranges[-1][1] + length)
            else:
                if len(ranges) >= cls.MAX_METADATA_RANGE_COUNT:
                    raise ValueError(
                        "Video keyframe index exceeds the "
                        "%s-metadata-range limit."
                        % cls.MAX_METADATA_RANGE_COUNT
                    )
                ranges.append((offset, length))
            has_moov |= box_type == b"moov"
            offset += size
        if offset != payload_length or not has_moov:
            raise ValueError("Video is not a supported ISO BMFF file.")
        return ranges

    @classmethod
    def validate(cls, data, payload_length=None):
        metadata_count, keyframe_count, entries_offset = cls._read_header(data)
        for _ in cls._iter_metadata_ranges(
                data, metadata_count, payload_length):
            pass
        for _ in cls._iter_keyframes(
                data, entries_offset, keyframe_count, payload_length):
            pass

    @classmethod
    def deserialize(cls, data, payload_length=None):
        metadata_count, keyframe_count, entries_offset = cls._read_header(data)
        return cls(
            cls._iter_metadata_ranges(data, metadata_count, payload_length),
            cls._iter_keyframes(
                data, entries_offset, keyframe_count, payload_length),
        )

    @classmethod
    def _read_header(cls, data):
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("VideoKeyframeIndex expects bytes.")
        if len(data) <= cls.HEADER.size:
            raise ValueError("Invalid video keyframe index: too short.")
        version, magic, metadata_count, keyframe_count = (
            cls.HEADER.unpack(data[:cls.HEADER.size]))
        if version != cls.VERSION or magic != cls.MAGIC:
            raise ValueError("Invalid video keyframe index version or magic.")
        if keyframe_count == 0:
            raise ValueError("Invalid video keyframe index header.")
        if metadata_count > cls.MAX_METADATA_RANGE_COUNT:
            raise ValueError(
                "Video keyframe index exceeds the %s-metadata-range limit."
                % cls.MAX_METADATA_RANGE_COUNT
            )
        if keyframe_count > cls.MAX_KEYFRAME_COUNT:
            raise ValueError(
                "Video keyframe index exceeds the %s-entry limit."
                % cls.MAX_KEYFRAME_COUNT
            )
        entries_offset = (
            cls.HEADER.size + metadata_count * cls.METADATA_RANGE.size)
        if entries_offset >= len(data):
            raise ValueError("Invalid video keyframe index metadata ranges.")
        return metadata_count, keyframe_count, entries_offset

    @classmethod
    def _iter_metadata_ranges(cls, data, count, payload_length):
        previous_end = 0
        offset = cls.HEADER.size
        for _ in range(count):
            start, length = cls.METADATA_RANGE.unpack_from(data, offset)
            end = start + length
            if start < previous_end or length <= 0:
                raise ValueError(
                    "Invalid video keyframe index metadata ranges."
                )
            if payload_length is not None and end > payload_length:
                raise ValueError(
                    "Video metadata range is outside the video payload."
                )
            previous_end = end
            offset += cls.METADATA_RANGE.size
            yield start, length

    @classmethod
    def _iter_keyframes(cls, data, input_offset, count, payload_length):
        try:
            decoder = zlib.decompressobj()
            pending = b''
            remainder = b''
            decoded_count = 0
            previous_ordinal, previous_pts, previous_position = -1, None, -1
            while not decoder.eof:
                if not pending and input_offset < len(data):
                    end = min(input_offset + cls._CHUNK_SIZE, len(data))
                    pending = data[input_offset:end]
                    input_offset = end
                output = decoder.decompress(pending, cls._CHUNK_SIZE)
                pending = decoder.unconsumed_tail
                entries = remainder + output
                complete = len(entries) - len(entries) % cls.ENTRY.size
                remainder = entries[complete:]
                for ordinal, pts, position in cls.ENTRY.iter_unpack(
                        entries[:complete]):
                    if decoded_count >= count:
                        raise ValueError(
                            "Invalid video keyframe index entries."
                        )
                    if decoded_count == 0 and ordinal != 0:
                        raise ValueError(
                            "Video keyframe index requires an initial "
                            "keyframe."
                        )
                    if ordinal <= previous_ordinal:
                        raise ValueError(
                            "Keyframe ordinals must be strictly increasing."
                        )
                    if previous_pts is not None and pts <= previous_pts:
                        raise ValueError(
                            "Keyframe timestamps must be strictly increasing."
                        )
                    if (position <= previous_position
                            or (payload_length is not None
                                and position >= payload_length)):
                        raise ValueError(
                            "Keyframe packet positions must be within the "
                            "video payload and strictly increasing."
                        )
                    decoded_count += 1
                    previous_ordinal, previous_pts, previous_position = (
                        ordinal, pts, position)
                    yield ordinal, pts, position
                if not output and not pending and input_offset == len(data):
                    break
            if (not decoder.eof or decoder.unused_data
                    or pending or input_offset != len(data)
                    or remainder or decoded_count != count):
                raise ValueError("Invalid video keyframe index entries.")
        except (struct.error, zlib.error) as error:
            raise ValueError(
                "Invalid video keyframe index payload."
            ) from error
