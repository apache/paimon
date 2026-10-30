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

"""Indexed TorchCodec reads with verified MP4 presentation timestamps."""

import bisect
import operator
import struct
from fractions import Fraction

import numpy as np


_MAX_METADATA_BYTES = 16 * 1024 * 1024
_MAX_SAMPLES = 1000000
_MAX_TOP_LEVEL_BOXES = 1024


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
        positions = np.asarray(positions, dtype=np.int64)
        return self.start + positions * self.step


class _SharedTimeline:
    def __init__(self, source, length):
        pts, durations, self.scale, self.stream_index = _timeline(source, length)
        minimum = float('inf')
        previous = None
        for start in range(0, len(pts), 65536):
            seconds = pts[start:start + 65536] / self.scale
            if previous is not None:
                minimum = min(minimum, float(seconds[0] - previous))
            if len(seconds) > 1:
                minimum = min(minimum, float(np.diff(seconds).min()))
            previous = seconds[-1]
        if minimum <= 0:
            raise ValueError("MP4 timestamps cannot be distinguished as seconds.")
        self.tolerance = min(1e-7, minimum / 4)
        self.pts = _TimingValues(pts)
        self.durations = _TimingValues(durations)


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


def _timeline(source, length):
    source.seek(0)
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
                # The persisted index describes the first video track; keep
                # TorchCodec's existing selection for multi-video containers.
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
        start = Fraction(empty * scale, movie_scale)
        end = start + Fraction(entries[-1][0] * scale, movie_scale)
        # Cropping/discarded samples would change presentation ordinals.
        if pts[0] < start or pts[-1] >= end:
            raise ValueError("MP4 edit list removes samples.")
    if pts[0] < 0 or np.any(pts[1:] <= pts[:-1]):
        raise ValueError("Ambiguous MP4 presentation timestamps.")
    source.seek(0)
    return pts, durations, scale, stream_index


class _IndexedTorchCodecDecoder:
    """Use timestamp seeks only when both the timeline and output are verified."""

    def __init__(self, stream, index, codec_threads=1):
        from pypaimon.multimodal.lerobot.dataset import (
            _RangeBackedVideo, _open_torchcodec_decoder)
        from torchcodec.decoders import VideoDecoder

        self._stream = stream
        self._index = index
        self._range_source = None
        self._decoder = None
        self._indexed = False
        self._exact = _open_torchcodec_decoder
        self._timeline = None
        try:
            self._length = operator.index(stream.video_length)
            self._range_source = _RangeBackedVideo(
                self._length, stream.video_read_ranges)
            source = self._range_source
            first = index.keyframes[0][2]
            source.prefetch([(0, min(65536, self._length)),
                             (first, min(65536, self._length - first))]
                            + list(index.metadata_ranges))
            cache = getattr(stream, "video_metadata_cache", None)
            cache_key = getattr(stream, "video_metadata_key", None)
            timeline = cache.get(cache_key) if cache is not None else None
            if timeline is None:
                timeline = _SharedTimeline(source, self._length)
            self._pts, self._durations, self._scale = timeline.pts, timeline.durations, timeline.scale
            if index.keyframes[-1][0] >= len(self._pts):
                raise ValueError("Keyframe ordinal exceeds the MP4 timeline.")
            # MP4 stream time base is 1 / media timescale.
            if any(self._pts[o] != pts for o, pts, _ in index.keyframes):
                raise ValueError("Keyframe PTS does not match the MP4 timeline.")
            self._timeline = timeline
            self._pts_tolerance = timeline.tolerance
            if cache is not None:
                cache[cache_key] = timeline
            self._decoder = VideoDecoder(
                source, stream_index=timeline.stream_index, device="cpu",
                num_ffmpeg_threads=codec_threads, seek_mode="approximate")
            self._indexed = True
        except (AttributeError, TypeError, ValueError, OSError, RuntimeError, OverflowError):
            self._fallback()

    def _fallback(self):
        self._decoder = None
        if self._range_source is not None:
            self._range_source.close()
            self._range_source = None
        self._stream.seek(0)
        self._decoder = self._exact(self._stream)
        self._indexed = False
        self._pts = self._durations = self._timeline = None

    def get_frames_at(self, *, indices):
        positions = [operator.index(index) for index in indices]
        if not self._indexed:
            return self._decoder.get_frames_at(indices=positions)
        if any(index < 0 or index >= len(self._pts) for index in positions):
            raise IndexError("Video frame index is out of range.")
        if not positions:
            return self._decoder.get_frames_at(indices=positions)
        ranges = []
        for index in sorted(set(positions)):
            anchor = bisect.bisect_right(self._index.keyframes, (index, float("inf"), float("inf"))) - 1
            start = self._index.keyframes[anchor][2]
            end = (self._index.keyframes[anchor + 1][2]
                   if anchor + 1 < len(self._index.keyframes) else self._length)
            ranges.append((start, end - start))
        try:
            self._range_source.prefetch(ranges)
            expected = self._pts[positions] / self._scale
            # Query inside the frame's presentation interval, avoiding floating
            # point boundary rounding. Never return a neighbouring frame.
            seconds = expected + self._durations[positions] / (2 * self._scale)
            result = self._decoder.get_frames_played_at(seconds=seconds.tolist())
            actual = result.pts_seconds.cpu().numpy()
            if actual.shape != expected.shape or not np.allclose(
                    actual, expected, rtol=0, atol=self._pts_tolerance):
                raise ValueError("TorchCodec returned a different frame PTS.")
            return result
        except (TypeError, ValueError, OSError, RuntimeError, AttributeError, IndexError):
            self._fallback()
            return self._decoder.get_frames_at(indices=positions)

    def __getitem__(self, index):
        return self.get_frames_at(indices=[index]).data[0]

    def close(self):
        self._decoder = None
        if self._range_source is not None:
            self._range_source.close()
            self._range_source = None
        self._pts = self._durations = self._timeline = None
