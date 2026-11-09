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

"""Bounded MP4 sample-table parsing for frame-addressed video reads."""

import struct
from fractions import Fraction

import numpy as np

_MAX_METADATA_BYTES = 16 * 1024 * 1024
_MAX_SAMPLES = 1000000
_MAX_TOP_LEVEL_BOXES = 1024


def _boxes(data):
    data = memoryview(data)
    offset = 0
    box_count = 0
    while offset < len(data):
        box_count += 1
        if box_count > 65536:
            raise ValueError("Too many MP4 boxes.")
        if len(data) - offset < 8:
            raise ValueError("Truncated MP4 box.")
        size, kind = struct.unpack_from(">I4s", data, offset)
        header = 8
        if size == 1:
            if len(data) - offset < 16:
                raise ValueError("Truncated extended MP4 box.")
            size = struct.unpack_from(">Q", data, offset + 8)[0]
            header = 16
        elif size == 0:
            size = len(data) - offset
        if size < header or size > len(data) - offset:
            raise ValueError("Invalid MP4 box size.")
        yield kind, data[offset + header:offset + size]
        offset += size


def _child(data, kind, required=True):
    matches = [body for name, body in _boxes(data) if name == kind]
    if len(matches) > 1 or (required and not matches):
        raise ValueError("Missing or duplicate MP4 box: %r." % kind)
    return matches[0] if matches else None


def _uint(data, offset):
    if offset < 0 or offset + 4 > len(data):
        raise ValueError("Truncated MP4 field.")
    return struct.unpack_from(">I", data, offset)[0]


def _scale(data):
    if not len(data) or data[0] not in (0, 1):
        raise ValueError("Unsupported MP4 timing version.")
    value = _uint(data, 20 if data[0] else 12)
    if value == 0:
        raise ValueError("Invalid MP4 time scale.")
    return value


def _runs(data, composition=False):
    count = _uint(data, 4)
    if len(data) != 8 + count * 8 or count > _MAX_SAMPLES:
        raise ValueError("Invalid MP4 timing run count.")
    if data[0] not in ((0, 1) if composition else (0,)):
        raise ValueError("Unsupported MP4 timing version.")
    entries = np.frombuffer(data, dtype=">u4", offset=8).reshape(count, 2)
    counts = entries[:, 0].astype(np.int64)
    total = int(counts.sum())
    if total == 0 or total > _MAX_SAMPLES or np.any(counts == 0):
        raise ValueError("MP4 sample count exceeds supported bounds.")
    values = entries[:, 1]
    if composition and data[0] == 1:
        values = values.view(">i4")
    return counts, values.astype(np.int64)


def _timeline(source, length, with_index=False):
    hint = getattr(source, "video_moov_range", (0, 0))
    offset, size = hint
    if size:
        try:
            if offset < 0 or size < 8 or offset > length or size > length - offset:
                raise ValueError("Invalid moov hint.")
            source.seek(offset)
            header = source.read(min(16, size))
            actual, kind = struct.unpack_from(">I4s", header)
            if actual == 1:
                actual = struct.unpack_from(">Q", header, 8)[0]
            elif actual == 0:
                actual = length - offset
            if kind != b"moov" or actual != size:
                raise ValueError("Invalid moov hint.")
        except (ValueError, struct.error):
            offset = 0
    source.seek(offset)
    moov = None
    for unused in range(_MAX_TOP_LEVEL_BOXES):
        offset = source.tell()
        if offset >= length:
            break
        header = source.read(8)
        if len(header) != 8:
            raise ValueError("Truncated video header.")
        size, kind = struct.unpack(">I4s", header)
        header_size = 8
        if size == 1:
            extended = source.read(8)
            if len(extended) != 8:
                raise ValueError("Truncated extended video header.")
            size = struct.unpack(">Q", extended)[0]
            header_size = 16
        elif size == 0:
            size = length - offset
        if size < header_size or size > length - offset:
            raise ValueError("Invalid video box extent.")
        if kind == b"moov":
            if size > _MAX_METADATA_BYTES:
                raise ValueError("MP4 metadata exceeds supported bounds.")
            metadata_range = (offset, size)
            moov = source.read(size - header_size)
            if len(moov) != size - header_size:
                raise ValueError("Truncated MP4 metadata.")
            break
        source.seek(offset + size)
    if moov is None or _child(moov, b"mvex", required=False) is not None:
        raise ValueError("A non-fragmented MP4 sample table is required.")
    tracks = [body for kind, body in _boxes(moov) if kind == b"trak"]
    selected = None
    for stream_index, track in enumerate(tracks):
        media = _child(track, b"mdia")
        handler = _child(media, b"hdlr")
        if len(handler) < 12:
            raise ValueError("Truncated MP4 handler.")
        if bytes(handler[8:12]) == b"vide":
            if selected is not None:
                raise ValueError("Multiple video tracks require exact mode.")
            selected = (stream_index, track, media)
    if selected is None:
        raise ValueError("No video track.")
    stream_index, track, media = selected
    scale = _scale(_child(media, b"mdhd"))
    samples = _child(_child(media, b"minf"), b"stbl")
    counts, values = _runs(_child(samples, b"stts"))
    count = int(counts.sum())
    if np.any(values <= 0) or _uint(_child(samples, b"stsz"), 8) != count:
        raise ValueError("Invalid MP4 sample durations or count.")
    durations = np.repeat(values, counts)
    pts = np.cumsum(durations)
    pts -= durations
    composition = _child(samples, b"ctts", required=False)
    if composition is not None:
        counts, values = _runs(composition, composition=True)
        if int(counts.sum()) != count:
            raise ValueError("MP4 composition count mismatch.")
        pts += np.repeat(values, counts)
    sample_pts = pts.copy() if with_index else None
    if np.any(pts[1:] <= pts[:-1]):
        order = np.argsort(pts, kind="stable")
        pts, durations = pts[order], durations[order]
    edits = _child(track, b"edts", required=False)
    if edits is not None:
        edit = _child(edits, b"elst")
        n = _uint(edit, 4)
        if edit[0] not in (0, 1) or n not in (1, 2):
            raise ValueError("Unsupported MP4 edit list.")
        fmt = struct.Struct(">Qqhh" if edit[0] else ">Iihh")
        if len(edit) != 8 + n * fmt.size:
            raise ValueError("Truncated MP4 edit list.")
        entries = [fmt.unpack_from(edit, 8 + i * fmt.size) for i in range(n)]
        if any((rate, fraction) != (1, 0) for _, _, rate, fraction in entries):
            raise ValueError("Unsupported MP4 playback rate.")
        if entries[-1][1] < 0 or (n == 2 and entries[0][1] != -1):
            raise ValueError("Unsupported repeated MP4 media edit.")
        movie_scale = _scale(_child(moov, b"mvhd"))
        empty = entries[0][0] if n == 2 else 0
        shift = Fraction(empty * scale, movie_scale) - entries[-1][1]
        limit = np.iinfo(np.int64)
        if (shift.denominator != 1 or int(pts[0]) + shift < limit.min
                or int(pts[-1]) + shift > limit.max):
            raise ValueError("Unsupported MP4 edit shift.")
        pts += int(shift)
        if sample_pts is not None:
            sample_pts += int(shift)
        start = Fraction(empty * scale, movie_scale)
        end = start + Fraction(entries[-1][0] * scale, movie_scale)
        # Cropping/discarded samples would change presentation ordinals.
        if pts[0] < start or pts[-1] >= end:
            raise ValueError("MP4 edit list removes samples.")
    if pts[0] < 0 or np.any(pts[1:] <= pts[:-1]):
        raise ValueError("Ambiguous MP4 presentation timestamps.")
    source.seek(0)
    if with_index:
        offsets = _sample_offsets(samples, count, length)
        sync = _child(samples, b"stss", required=False)
        if sync is None:
            ids = np.arange(count, dtype=np.int64)
        else:
            n = _uint(sync, 4)
            if len(sync) != 8 + 4 * n or n == 0 or n > count:
                raise ValueError("Invalid MP4 sync samples.")
            ids = np.frombuffer(sync, dtype=">u4", offset=8).astype(np.int64) - 1
            if ids[0] < 0 or ids[-1] >= count or np.any(ids[1:] <= ids[:-1]):
                raise ValueError("Invalid MP4 sync sample order.")
        key_pts = sample_pts[ids]
        ordinals = np.searchsorted(pts, key_pts)
        positions = offsets[ids]
        if (ordinals[0] != 0 or np.any(ordinals[1:] <= ordinals[:-1])
                or np.any(positions[1:] <= positions[:-1])):
            raise ValueError("Unsupported MP4 keyframe order.")
        return pts, durations, scale, stream_index, metadata_range, np.column_stack(
            (ordinals, key_pts, positions))
    return pts, durations, scale, stream_index


def _sample_offsets(samples, count, length):
    sizes_box = _child(samples, b"stsz")
    fixed = _uint(sizes_box, 4)
    if len(sizes_box) != 12 + (0 if fixed else count * 4):
        raise ValueError("Invalid MP4 sample sizes.")
    sizes = np.full(count, fixed, dtype=np.int64) if fixed else np.frombuffer(
        sizes_box, dtype=">u4", offset=12).astype(np.int64)
    if np.any(sizes <= 0):
        raise ValueError("Empty MP4 sample.")
    chunks = _child(samples, b"stco", required=False)
    wide = _child(samples, b"co64", required=False)
    if (chunks is None) == (wide is None):
        raise ValueError("Missing or duplicate MP4 chunk offsets.")
    box, dtype, width = (wide, ">u8", 8) if wide is not None else (chunks, ">u4", 4)
    n = _uint(box, 4)
    if n == 0 or n > count or len(box) != 8 + n * width:
        raise ValueError("Invalid MP4 chunk count.")
    chunk_offsets = np.frombuffer(box, dtype=dtype, offset=8)
    runs = _child(samples, b"stsc")
    nr = _uint(runs, 4)
    if nr == 0 or nr > n or len(runs) != 8 + nr * 12:
        raise ValueError("Invalid MP4 sample-to-chunk table.")
    entries = np.frombuffer(runs, dtype=">u4", offset=8).reshape(nr, 3)
    if (entries[0, 0] != 1 or entries[-1, 0] > n
            or np.any(entries[1:, 0] <= entries[:-1, 0])
            or np.any(entries[:, 1] == 0) or np.any(entries[:, 2] != 1)):
        raise ValueError("Unsupported MP4 sample-to-chunk mapping.")
    run_lengths = np.diff(np.append(entries[:, 0].astype(np.int64), n + 1))
    samples_per_chunk = np.repeat(entries[:, 1].astype(np.int64), run_lengths)
    if int(samples_per_chunk.sum()) != count:
        raise ValueError("MP4 chunks do not cover samples.")
    starts = np.cumsum(samples_per_chunk) - samples_per_chunk
    byte_offsets = np.empty(count + 1, dtype=np.int64)
    byte_offsets[0] = 0
    np.cumsum(sizes, out=byte_offsets[1:])
    chunk_bytes = byte_offsets[starts + samples_per_chunk] - byte_offsets[starts]
    if np.any(chunk_offsets > length):
        raise ValueError("MP4 sample exceeds video payload.")
    chunk_offsets = chunk_offsets.astype(np.int64)
    if np.any(chunk_bytes > length) or np.any(chunk_offsets > length - chunk_bytes):
        raise ValueError("MP4 sample exceeds video payload.")
    return byte_offsets[:-1] + np.repeat(
        chunk_offsets - byte_offsets[starts], samples_per_chunk)


class _TimingValues:
    """Keep arithmetic sequences compact, otherwise retain a read-only array."""

    def __init__(self, values):
        self.count = len(values)
        self.start = int(values[0])
        self.step = int(values[1]) - self.start if self.count > 1 else 0
        self.values = None
        for start in range(1, self.count, 65536):
            end = min(start + 65536, self.count)
            if np.any(values[start:end] - values[start - 1:end - 1] != self.step):
                values.flags.writeable = False
                self.values = values
                break

    def __len__(self):
        return self.count

    def __getitem__(self, positions):
        if self.values is not None:
            return self.values[positions]
        if isinstance(positions, (int, np.integer)):
            if positions < 0:
                positions += self.count
            if not 0 <= positions < self.count:
                raise IndexError("Timing position is out of range.")
            return self.start + int(positions) * self.step
        if isinstance(positions, slice):
            positions = np.arange(*positions.indices(self.count), dtype=np.int64)
        positions = np.asarray(positions, dtype=np.int64)
        return self.start + positions * self.step


class ContainerFrameIndex:
    """In-memory frame locations derived from an ordinary MP4 sample table."""

    def __init__(self, source, length):
        values = _timeline(source, length, with_index=True)
        self.pts, self.durations, self.scale, self.stream_index, region, self.keyframes = values
        self.pts = _TimingValues(self.pts)
        self.durations = _TimingValues(self.durations)
        self.metadata_ranges = (region,)
        self.keyframes.flags.writeable = False

    @property
    def nbytes(self):
        return self.keyframes.nbytes + sum(
            value.values.nbytes for value in (self.pts, self.durations)
            if value.values is not None)

    def preceding(self, ordinal):
        return int(np.searchsorted(self.keyframes[:, 0], ordinal, side="right")) - 1

    def close(self):
        pass
