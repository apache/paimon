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

"""Paged seek metadata for encoded video payloads."""

import io
import operator
import sqlite3
import struct
import tempfile
import threading
import zlib
from collections import OrderedDict
from collections.abc import Sequence
from contextlib import closing


class _PagedKeyframes(Sequence):
    ENTRY = struct.Struct('<qqq')
    PAGE_ENTRIES = 4096
    PAGE_BYTES = PAGE_ENTRIES * ENTRY.size
    CACHE_BYTES = 1024 * 1024

    def __init__(self, read_at, count, payload_length=None, owner=None):
        self.read_at, self.count = read_at, count
        self.payload_length, self.owner = payload_length, owner
        self.cache = OrderedDict()
        self.lock = threading.RLock()

    def __len__(self):
        return self.count

    def _page(self, page):
        with self.lock:
            data = self.cache.pop(page, None)
            if data is None:
                count = min(self.PAGE_ENTRIES, self.count - page * self.PAGE_ENTRIES)
                size = count * self.ENTRY.size
                block = self.read_at(page * (self.PAGE_BYTES + 4), size + 4)
                if len(block) != size + 4 or zlib.crc32(block[:size]) != struct.unpack('<I', block[size:])[0]:
                    raise ValueError("Invalid video keyframe index page checksum or length.")
                data = block[:size]
                _validate_entries(data, self.payload_length, initial=page == 0)
                previous = self.cache.get(page - 1)
                following = self.cache.get(page + 1)
                if previous is not None:
                    _validate_boundary(self.ENTRY.unpack_from(previous, len(previous) - 24),
                                       self.ENTRY.unpack_from(data))
                if following is not None:
                    _validate_boundary(self.ENTRY.unpack_from(data, len(data) - 24),
                                       self.ENTRY.unpack_from(following))
            self.cache[page] = data
            while len(self.cache) > max(1, self.CACHE_BYTES // self.PAGE_BYTES):
                self.cache.popitem(last=False)
            return data

    def __getitem__(self, index):
        if isinstance(index, slice):
            return tuple(self[i] for i in range(*index.indices(len(self))))
        index = operator.index(index)
        if index < 0:
            index += len(self)
        if not 0 <= index < len(self):
            raise IndexError("Keyframe index out of range.")
        page, offset = divmod(index, self.PAGE_ENTRIES)
        return self.ENTRY.unpack_from(self._page(page), offset * self.ENTRY.size)

    def __iter__(self):
        for page in range((self.count + self.PAGE_ENTRIES - 1) // self.PAGE_ENTRIES):
            yield from self.ENTRY.iter_unpack(self._page(page))

    def __eq__(self, other):
        if not isinstance(other, Sequence):
            return NotImplemented
        return len(self) == len(other) and all(a == b for a, b in zip(self, other))

    def close(self):
        if self.owner is not None:
            self.owner.close()
        self.cache.clear()


class _Ordinals(Sequence):
    def __init__(self, keyframes):
        self.keyframes = keyframes

    def __len__(self):
        return len(self.keyframes)

    def __getitem__(self, index):
        return self.keyframes[index][0]


def _validate_boundary(previous, current):
    if any(a >= b for a, b in zip(previous, current)):
        raise ValueError("Keyframe ordinals, timestamps and packet positions must be strictly increasing.")


def _validate_entries(data, payload_length, initial=False):
    import numpy as np

    entries = np.frombuffer(data, dtype='<i8').reshape(-1, 3)
    if (not len(entries) or entries[0, 0] < 0 or (initial and entries[0, 0] != 0)
            or np.any(entries[1:] <= entries[:-1])):
        raise ValueError("Keyframe ordinals, timestamps and packet positions must be strictly increasing from zero.")
    if entries[0, 2] < 0 or (payload_length is not None and entries[-1, 2] >= payload_length):
        raise ValueError("Keyframe packet positions must be within the video payload.")


class VideoKeyframeIndex:
    VERSION = 1
    MAGIC = 0x564944454F4B4649
    HEADER = struct.Struct('<BQIQ')
    METADATA_RANGE = struct.Struct('<qq')
    ENTRY = struct.Struct('<qqq')
    MAX_METADATA_RANGE_COUNT = 64 * 1024

    def __init__(self, metadata_ranges, keyframes):
        self.metadata_ranges = self._metadata(metadata_ranges)
        self.keyframes = keyframes

    @classmethod
    def _metadata(cls, ranges, payload_length=None):
        result = []
        previous_end = 0
        for i, (start, length) in enumerate(ranges):
            if i >= cls.MAX_METADATA_RANGE_COUNT:
                raise ValueError("Video keyframe index exceeds the metadata-range limit.")
            start, length = operator.index(start), operator.index(length)
            if start < previous_end or length <= 0:
                raise ValueError("Invalid video keyframe index metadata ranges.")
            if payload_length is not None and start + length > payload_length:
                raise ValueError("Video metadata range is outside the video payload.")
            cls.METADATA_RANGE.pack(start, length)
            if result and start == previous_end:
                result[-1] = (result[-1][0], result[-1][1] + length)
            else:
                result.append((start, length))
            previous_end = start + length
        return tuple(result)

    @property
    def keyframes(self):
        return self._keyframes

    @keyframes.setter
    def keyframes(self, entries):
        storage = tempfile.TemporaryFile()
        count, previous = 0, None
        page = bytearray()
        try:
            for entry in entries:
                entry = tuple(operator.index(v) for v in entry)
                packed = self.ENTRY.pack(*entry)
                if previous is None:
                    if entry[0] != 0 or entry[2] < 0:
                        raise ValueError("Video keyframe index requires an initial keyframe.")
                else:
                    _validate_boundary(previous, entry)
                page.extend(packed)
                count += 1
                previous = entry
                if len(page) == _PagedKeyframes.PAGE_BYTES:
                    storage.write(page)
                    storage.write(struct.pack('<I', zlib.crc32(page)))
                    page.clear()
            if not count:
                raise ValueError("Video keyframe index requires an initial keyframe.")
            if page:
                storage.write(page)
                storage.write(struct.pack('<I', zlib.crc32(page)))
            storage.flush()
        except BaseException:
            storage.close()
            raise

        def read_at(offset, size):
            storage.seek(offset)
            return storage.read(size)

        old = getattr(self, '_keyframes', None)
        self._keyframes = _PagedKeyframes(read_at, count, owner=storage)
        if old is not None:
            old.close()

    @property
    def ordinals(self):
        return _Ordinals(self.keyframes)

    def close(self):
        self.keyframes.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __reduce__(self):
        return (type(self).deserialize, (self.serialize(),))

    def write_to(self, output):
        output.write(self.HEADER.pack(
            self.VERSION, self.MAGIC, len(self.metadata_ranges), len(self.keyframes)))
        for value in self.metadata_ranges:
            output.write(self.METADATA_RANGE.pack(*value))
        for page in range((len(self.keyframes) + _PagedKeyframes.PAGE_ENTRIES - 1) // _PagedKeyframes.PAGE_ENTRIES):
            data = self.keyframes._page(page)
            output.write(data)
            output.write(struct.pack('<I', zlib.crc32(data)))

    def serialize(self):
        output = io.BytesIO()
        self.write_to(output)
        return output.getvalue()

    @classmethod
    def open(cls, read_at, length, payload_length=None):
        if length < cls.HEADER.size:
            raise ValueError("Invalid video keyframe index length.")
        prefix = read_at(0, min(length, cls.HEADER.size + _PagedKeyframes.PAGE_BYTES + 4))
        if len(prefix) != min(length, cls.HEADER.size + _PagedKeyframes.PAGE_BYTES + 4):
            raise ValueError("Invalid video keyframe index: truncated read.")

        def read(offset, size):
            if offset < 0 or size < 0 or offset + size > length:
                raise ValueError("Invalid video keyframe index length.")
            if size == 0:
                return b''
            if offset + size <= len(prefix):
                return prefix[offset:offset + size]
            data = read_at(offset, size)
            if len(data) != size:
                raise ValueError("Invalid video keyframe index: truncated read.")
            return data

        version = read(0, 1)[0]
        if version != cls.VERSION:
            raise ValueError("Invalid video keyframe index version.")
        header = cls.HEADER
        _, magic, metadata_count, count = header.unpack(read(0, header.size))
        if magic != cls.MAGIC or count == 0:
            raise ValueError("Invalid video keyframe index header.")
        if metadata_count > cls.MAX_METADATA_RANGE_COUNT:
            raise ValueError("Video keyframe index exceeds the metadata-range limit.")
        offset = header.size + metadata_count * cls.METADATA_RANGE.size
        ranges = cls._metadata(cls.METADATA_RANGE.iter_unpack(
            read(header.size, metadata_count * 16)), payload_length)
        pages = (count + _PagedKeyframes.PAGE_ENTRIES - 1) // _PagedKeyframes.PAGE_ENTRIES
        if offset + count * 24 + pages * 4 != length:
            raise ValueError("Invalid video keyframe index entries length.")
        result = cls.__new__(cls)
        result.metadata_ranges = ranges
        result._keyframes = _PagedKeyframes(lambda start, size: read(offset + start, size), count, payload_length)
        result.keyframes[0]
        return result

    def _validate_all(self):
        previous = None
        for page in range((len(self.keyframes) + _PagedKeyframes.PAGE_ENTRIES - 1) // _PagedKeyframes.PAGE_ENTRIES):
            data = self.keyframes._page(page)
            if previous is not None:
                _validate_boundary(previous, self.ENTRY.unpack_from(data))
            previous = self.ENTRY.unpack_from(data, len(data) - self.ENTRY.size)

    @classmethod
    def validate_stream(cls, source, length, payload_length=None):
        def read_at(offset, size):
            source.seek(offset)
            return source.read(size)
        with cls.open(read_at, length, payload_length) as index:
            index._validate_all()

    @classmethod
    def validate(cls, data, payload_length=None):
        cls.validate_stream(io.BytesIO(data), len(data), payload_length)

    @classmethod
    def deserialize(cls, data, payload_length=None):
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("VideoKeyframeIndex expects bytes.")
        index = cls.open(lambda start, size: data[start:start + size], len(data), payload_length)
        try:
            index._validate_all()
            return index
        except BaseException:
            index.close()
            raise

    @classmethod
    def inspect(cls, source, payload_length):
        """Build an index using a disk-backed packet lookup."""
        try:
            import av
        except ImportError as error:
            raise ImportError("Video seek-index generation requires PyAV.") from error
        metadata_ranges = cls._iso_bmff_metadata_ranges(source, payload_length)
        with tempfile.TemporaryDirectory(prefix="paimon_keyframes_") as directory:
            with closing(sqlite3.connect(directory + "/packets.db")) as packets:
                packets.execute("PRAGMA cache_size = -2048")
                packets.execute("CREATE TABLE packets (pts INTEGER PRIMARY KEY, position INTEGER)")
                source.seek(0)
                with av.open(source) as container:
                    if not container.streams.video:
                        raise ValueError("Video has no video stream.")
                    stream = container.streams.video[0]
                    for packet in container.demux(stream):
                        if (not packet.is_keyframe or packet.pts is None or packet.pos is None
                                or packet.pos < 0 or getattr(packet, 'is_discard', False)):
                            continue
                        pts = cls._stream_pts(packet.pts, packet.time_base or stream.time_base, stream.time_base)
                        try:
                            packets.execute("INSERT INTO packets VALUES (?, ?)", (pts, packet.pos))
                        except sqlite3.IntegrityError as error:
                            raise ValueError("Video has duplicate keyframe timestamps.") from error
                source.seek(0)
                with av.open(source) as container:
                    stream = container.streams.video[0]

                    def entries():
                        previous_pts = None
                        for ordinal, frame in enumerate(container.decode(stream)):
                            if frame.pts is None:
                                raise ValueError("Video frame has no presentation timestamp.")
                            pts = cls._stream_pts(frame.pts, frame.time_base or stream.time_base, stream.time_base)
                            if previous_pts is not None and pts <= previous_pts:
                                raise ValueError("Video frame timestamps must be strictly increasing.")
                            if frame.key_frame:
                                match = packets.execute("SELECT position FROM packets WHERE pts = ?", (pts,)).fetchone()
                                if match is None:
                                    raise ValueError("Video keyframe has no packet byte position.")
                                yield ordinal, pts, match[0]
                            previous_pts = pts
                    return cls(metadata_ranges, entries())

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
