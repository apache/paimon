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

import operator

import numpy as np

from pypaimon.multimodal.mp4 import _timeline, _TimingValues


class _SharedTimeline:
    def __init__(self, source, length, index=None):
        if index is None:
            pts, durations, self.scale, self.stream_index = _timeline(source, length)
        else:
            pts, durations, self.scale, self.stream_index = (
                index.pts, index.durations, index.scale, index.stream_index)
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
        self.pts = pts if isinstance(pts, _TimingValues) else _TimingValues(pts)
        self.durations = durations if isinstance(durations, _TimingValues) else _TimingValues(durations)


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
            self._range_source = getattr(stream, "video_bootstrap", None)
            if self._range_source is None:
                self._range_source = _RangeBackedVideo(
                    self._length, stream.video_read_ranges)
            else:
                stream.video_bootstrap = None
            source = self._range_source
            first = index.keyframes[0][2]
            source.prefetch([(0, min(65536, self._length)),
                             (first, min(65536, self._length - first))]
                            + list(index.metadata_ranges))
            cache = getattr(stream, "video_metadata_cache", None)
            cache_key = ("timeline", getattr(stream, "video_metadata_key", None))
            timeline = cache.get(cache_key) if cache is not None else None
            if timeline is None:
                timeline = _SharedTimeline(source, self._length, index if hasattr(index, "pts") else None)
            self._pts, self._durations, self._scale = timeline.pts, timeline.durations, timeline.scale
            if index.keyframes[-1][0] >= len(self._pts):
                raise ValueError("Keyframe ordinal exceeds the MP4 timeline.")
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
        data = frame = None
        try:
            ranges = []
            for position in sorted(set(positions)):
                anchor = self._index.preceding(position)
                if anchor < 0:
                    raise ValueError("No preceding sync sample.")
                ordinal, pts, start = self._index.keyframes[anchor]
                # Check the anchor timestamp before seeking.
                if self._pts[ordinal] != pts:
                    raise ValueError("Keyframe PTS does not match the MP4 timeline.")
                end = (self._index.keyframes[anchor + 1][2]
                       if anchor + 1 < len(self._index.keyframes) else self._length)
                ranges.append((start, end - start))
            self._range_source.prefetch(ranges)
            expected = self._pts[positions] / self._scale
            # Query inside the frame's presentation interval, avoiding floating
            # point boundary rounding. Never return a neighbouring frame.
            seconds = expected + self._durations[positions] / (2 * self._scale)
            # TorchCodec's approximate batch API may turn timestamps into
            # ordinals using the nominal header frame rate. Remuxed videos can
            # have a different sample-table cadence. The scalar timestamp API
            # seeks by PTS instead; sort/deduplicate requests, then restore order.
            import torch
            from torchcodec import FrameBatch
            destinations = {}
            for output, position in enumerate(positions):
                destinations.setdefault(position, []).append(output)
            pts_seconds = torch.empty(len(positions), dtype=torch.float64, device="cpu")
            duration_seconds = torch.empty_like(pts_seconds)
            for position, second in sorted(set(zip(positions, seconds))):
                frame = self._decoder.get_frame_played_at(float(second))
                if data is None:
                    data = frame.data.new_empty((len(positions),) + tuple(frame.data.shape))
                for output in destinations[position]:
                    data[output].copy_(frame.data)
                    pts_seconds[output] = frame.pts_seconds
                    duration_seconds[output] = frame.duration_seconds
                frame = None
            result = FrameBatch(data=data, pts_seconds=pts_seconds, duration_seconds=duration_seconds)
            actual = result.pts_seconds.cpu().numpy()
            if actual.shape != expected.shape or not np.allclose(
                    actual, expected, rtol=0, atol=self._pts_tolerance):
                raise ValueError("TorchCodec returned a different frame PTS.")
            return result
        except (TypeError, ValueError, OSError, RuntimeError, AttributeError, IndexError):
            data = frame = result = None
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
