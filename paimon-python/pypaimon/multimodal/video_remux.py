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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Copy Episode video packets into bounded MP4 payloads without re-encoding."""

import io
from bisect import bisect_left
from fractions import Fraction
from itertools import chain


class _BoundedBuffer(io.BytesIO):
    def __init__(self, limit):
        super().__init__()
        self.limit = limit

    def write(self, data):
        if self.tell() + len(data) > self.limit:
            raise ValueError("Remuxed video exceeds the memory limit.")
        return super().write(data)


def remux_video_interval(source, start, end, max_bytes=64 * 1024 * 1024):
    """Return MP4 bytes and (source timestamp, output ordinal) pairs.

    The half-open interval is in seconds. Preserve a preceding keyframe and
    one following GOP for decoder dependencies. Copy encoded packets; decode
    only the resulting short clip to verify its presentation timestamps.
    source is a path or seekable binary stream, owned by the caller.
    """
    import av

    start = Fraction(str(start))
    end = Fraction(str(end))
    if start < 0 or end <= start or max_bytes <= 0:
        raise ValueError("Invalid video interval or memory limit.")
    output = _BoundedBuffer(max_bytes)
    selected = []
    seen_pts = set()
    with av.open(source) as container:
        if not container.streams.video:
            raise ValueError("Source has no video stream.")
        stream = container.streams.video[0]
        if stream.time_base is None:
            raise ValueError("Video has no time base.")
        seek_time = start
        backoff = Fraction(1)
        while True:
            container.seek(int(seek_time / stream.time_base), stream=stream,
                           backward=True, any_frame=False)
            packets = container.demux(stream)
            first = next((p for p in packets if p.size), None)
            if first is None or first.pts is None:
                raise ValueError("Cannot find a preceding keyframe.")
            first_time = first.pts * (first.time_base or stream.time_base)
            if first.is_keyframe and first_time <= start:
                break
            # Some demuxers seek by DTS; with B-frames this may land after
            # the requested presentation time. Retry an earlier seek point.
            if seek_time == 0:
                raise ValueError("Cannot find a preceding keyframe.")
            seek_time = max(Fraction(0), start - backoff)
            backoff *= 2
        with av.open(output, mode="w", format="mp4") as muxer:
            target = muxer.add_stream_from_template(stream, opaque=True)
            started = False
            origin = None
            following_gops = 0
            for packet in chain([first], packets):
                if not packet.size:
                    continue
                if packet.pts is None or packet.dts is None or packet.is_discard:
                    raise ValueError("Video packet has ambiguous timestamps.")
                timestamp = packet.pts * (packet.time_base or stream.time_base)
                if not started:
                    if not packet.is_keyframe or timestamp > start:
                        raise ValueError("Cannot find a preceding keyframe.")
                    started = True
                    origin = timestamp
                if packet.is_keyframe and timestamp >= end:
                    following_gops += 1
                    if following_gops == 2:
                        break
                if timestamp in seen_pts:
                    raise ValueError("Video has duplicate presentation timestamps.")
                if len(selected) >= 65536:
                    raise ValueError("Remuxed video exceeds the frame limit.")
                seen_pts.add(timestamp)
                selected.append(timestamp)
                shift = origin / (packet.time_base or stream.time_base)
                if shift.denominator != 1:
                    raise ValueError("Cannot normalize packet time base.")
                packet.pts -= int(shift)
                packet.dts -= int(shift)
                packet.stream = target
                muxer.mux(packet)
    payload = output.getvalue()
    # Leading B-frames of an open GOP may precede its keyframe in
    # presentation order. They are outside the requested interval and the
    # normalized MP4 discards them; do not assign output ordinals to them.
    expected = sorted(timestamp for timestamp in selected if timestamp >= origin)
    actual = []
    with av.open(io.BytesIO(payload)) as container:
        for frame in container.decode(video=0):
            if frame.pts is None:
                raise ValueError("Remuxed frame has no presentation timestamp.")
            actual.append(frame.pts * frame.time_base + origin)
            if len(actual) > len(expected):
                raise ValueError("Remux changed the number of frames.")
    if actual != expected:
        raise ValueError("Remux changed presentation timestamps or frame count.")
    mapping = [(timestamp, ordinal) for ordinal, timestamp in enumerate(actual)
               if start - Fraction(1, 10**7) <= timestamp < end - Fraction(1, 10**7)]
    if not mapping:
        raise ValueError("Requested interval contains no video frames.")
    return payload, mapping


class _EpisodeVideoSources(dict):
    """URI reader adapter retaining only the active Episode payloads."""

    def __init__(self, delegate):
        super().__init__()
        self.delegate = delegate
        self.payloads = {}
        self.clips = {}
        self.sequence = 0

    def create(self, uri):
        return self if uri.startswith("paimon-remux:") else self.delegate.create(uri)

    def new_input_stream(self, uri):
        return io.BytesIO(self.payloads[uri])

    def next_episode(self):
        self.clips.clear()
        self.payloads.clear()

    def descriptors(self, uri, start, end, timestamps):
        from pypaimon.table.row.blob import VideoFrameDescriptor
        from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex

        key = (uri, start, end)
        clip = self.clips.get(key)
        if clip is None:
            with self.delegate.create(uri).new_input_stream(uri) as source:
                payload, mapping = remux_video_interval(source, start, end)
            index = VideoKeyframeIndex.inspect(io.BytesIO(payload), len(payload)).serialize()
            name = "paimon-remux:episode-%d" % self.sequence
            self.sequence += 1
            self.payloads[name] = payload + index
            clip = (name, len(payload), len(index),
                    [float(t) for t, unused in mapping],
                    [ordinal for unused, ordinal in mapping])
            self.clips[key] = clip
        name, length, index_length, times, ordinals = clip
        result = []
        for timestamp in timestamps:
            # Source timestamps have already been validated by the importer.
            position = bisect_left(times, timestamp)
            candidates = [i for i in (position - 1, position)
                          if 0 <= i < len(times) and abs(times[i] - timestamp) < 1e-7]
            if len(candidates) != 1:
                raise ValueError("Remux cannot map source timestamp %r in [%r, %r); clip range %r."
                                 % (timestamp, start, end, (times[:1], times[-1:])))
            result.append(VideoFrameDescriptor(
                name, 0, length, ordinals[candidates[0]], length, index_length).serialize())
        return result
