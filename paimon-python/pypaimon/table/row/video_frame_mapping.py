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

"""Persisted exact video-frame mappings shared by decoder instances."""

import json
import struct
import zlib


class VideoFrameMapping:

    VERSION = 1
    MAGIC = 0x564944454F494458  # "VIDEOIDX"
    HEADER_SIZE = 1 + 8 + 4

    def __init__(self, stream_index, frames):
        self.stream_index = int(stream_index)
        self.frames = [
            {
                'pts': int(frame['pts']),
                'duration': int(frame['duration']),
                'key_frame': int(frame['key_frame']),
            }
            for frame in frames
        ]
        if self.stream_index < 0:
            raise ValueError("Video stream index must be non-negative.")
        if not self.frames:
            raise ValueError("Video frame mapping must not be empty.")
        if any(frame['duration'] <= 0 for frame in self.frames):
            raise ValueError("Video frame durations must be positive.")
        if not any(frame['key_frame'] for frame in self.frames):
            raise ValueError("Video frame mapping requires a key frame.")

    @property
    def pts(self):
        return [frame['pts'] for frame in self.frames]

    @property
    def key_frames(self):
        return [
            index for index, frame in enumerate(self.frames)
            if frame['key_frame']
        ]

    def torchcodec_json(self):
        return json.dumps(
            {'frames': self.frames}, separators=(',', ':')
        ).encode('utf-8')

    def serialize(self):
        compressed = zlib.compress(self.torchcodec_json())
        return (
            struct.pack('<BQi', self.VERSION, self.MAGIC, self.stream_index)
            + compressed
        )

    @classmethod
    def inspect(cls, source):
        import av

        with av.open(source) as container:
            stream = container.streams.video[0]
            packets = [
                packet for packet in container.demux(stream)
                if packet.pts is not None
                and not getattr(packet, 'is_discard', False)
            ]
            if any(packet.duration is None or packet.duration <= 0
                   for packet in packets):
                raise ValueError("Video packets require positive durations.")
            frames = [{
                'pts': packet.pts,
                'duration': packet.duration,
                'key_frame': packet.is_keyframe,
            } for packet in packets]
            frames.sort(key=lambda frame: frame['pts'])
            return cls(stream.index, frames)

    @classmethod
    def deserialize(cls, data):
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("VideoFrameMapping expects bytes.")
        raw = bytes(data)
        if len(raw) <= cls.HEADER_SIZE:
            raise ValueError("Invalid video frame mapping: too short.")
        version, magic, stream_index = struct.unpack(
            '<BQi', raw[:cls.HEADER_SIZE])
        if version != cls.VERSION:
            raise ValueError(
                "Unsupported video frame mapping version: %s." % version)
        if magic != cls.MAGIC:
            raise ValueError("Invalid video frame mapping magic.")
        try:
            payload = json.loads(zlib.decompress(
                raw[cls.HEADER_SIZE:]).decode('utf-8'))
            frames = payload['frames']
        except (KeyError, TypeError, ValueError, zlib.error) as error:
            raise ValueError("Invalid video frame mapping payload.") from error
        return cls(stream_index, frames)
