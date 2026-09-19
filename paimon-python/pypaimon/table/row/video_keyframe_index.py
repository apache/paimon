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

"""Sparse, presentation-order keyframe indexes for encoded video payloads."""

import operator
import struct
import zlib


class VideoKeyframeIndex:
    """Persist only keyframe ordinals and timestamps, not a per-frame map."""

    VERSION = 1
    MAGIC = 0x564944454F4B4649  # "VIDEOKFI"
    HEADER = struct.Struct('<BQI')
    ENTRY = struct.Struct('<qq')

    def __init__(self, keyframes):
        self.keyframes = tuple(
            (operator.index(index), operator.index(pts))
            for index, pts in keyframes
        )
        if not self.keyframes or self.keyframes[0][0] != 0:
            raise ValueError("Video keyframe index requires an initial keyframe.")
        previous_index, previous_pts = -1, None
        for index, pts in self.keyframes:
            if index <= previous_index:
                raise ValueError("Keyframe ordinals must be strictly increasing.")
            if previous_pts is not None and pts <= previous_pts:
                raise ValueError("Keyframe timestamps must be strictly increasing.")
            previous_index, previous_pts = index, pts

    def serialize(self):
        entries = b''.join(self.ENTRY.pack(*entry) for entry in self.keyframes)
        return self.HEADER.pack(
            self.VERSION, self.MAGIC, len(self.keyframes),
        ) + zlib.compress(entries)

    @classmethod
    def deserialize(cls, data):
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("VideoKeyframeIndex expects bytes.")
        if len(data) <= cls.HEADER.size:
            raise ValueError("Invalid video keyframe index: too short.")
        version, magic, count = (
            cls.HEADER.unpack(data[:cls.HEADER.size]))
        if version != cls.VERSION or magic != cls.MAGIC:
            raise ValueError("Invalid video keyframe index version or magic.")
        if count == 0:
            raise ValueError("Invalid video keyframe index header.")
        try:
            decoder = zlib.decompressobj()
            expected = count * cls.ENTRY.size
            entries = decoder.decompress(data[cls.HEADER.size:], expected + 1)
            if (len(entries) != expected or not decoder.eof
                    or decoder.unused_data or decoder.unconsumed_tail):
                raise ValueError("Invalid video keyframe index entries.")
            keyframes = list(cls.ENTRY.iter_unpack(entries))
        except (struct.error, zlib.error) as error:
            raise ValueError("Invalid video keyframe index payload.") from error
        return cls(keyframes)
