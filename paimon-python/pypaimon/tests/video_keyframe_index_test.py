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

import struct
import unittest
import zlib
from unittest import mock

import pypaimon.table.row.video_keyframe_index as keyframe_index_module
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex


class VideoKeyframeIndexTest(unittest.TestCase):

    def test_round_trip_and_validation(self):
        index = VideoKeyframeIndex([(0, 500), (60, 9000)])
        restored = VideoKeyframeIndex.deserialize(index.serialize())
        self.assertEqual(index.keyframes, restored.keyframes)
        self.assertEqual(13, index.HEADER.size)
        self.assertFalse(hasattr(restored, 'time_base'))
        self.assertFalse(hasattr(restored, 'frame_count'))
        self.assertFalse(hasattr(restored, 'stream_index'))
        for anchors in ([], [(1, 0)], [(0, 5), (0, 9)],
                        [(0, 5), (2, 5)], [(0, 5), (2, 9), (1, 10)]):
            with self.subTest(anchors=anchors):
                with self.assertRaises(ValueError):
                    VideoKeyframeIndex(anchors)
        for data in (b'', index.serialize()[:-1], index.serialize() + b'junk'):
            with self.assertRaises(ValueError):
                VideoKeyframeIndex.deserialize(data)
        header = index.serialize()[:index.HEADER.size]
        with self.assertRaises(ValueError):
            VideoKeyframeIndex.deserialize(header + zlib.compress(struct.pack('<qq', 0, 1)))

    def test_valid_large_index_uses_bounded_input_chunks(self):
        count = 100_000
        entries = b''.join(
            VideoKeyframeIndex.ENTRY.pack(value, value)
            for value in range(count)
        )
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, count
        ) + zlib.compress(entries)
        real_decompressobj = zlib.decompressobj
        input_sizes = []

        class TrackingDecompressor:

            def __init__(self):
                self._delegate = real_decompressobj()

            def decompress(self, value, max_length=0):
                input_sizes.append(len(value))
                return self._delegate.decompress(value, max_length)

            def __getattr__(self, name):
                return getattr(self._delegate, name)

        with mock.patch.object(
                keyframe_index_module.zlib,
                'decompressobj',
                side_effect=TrackingDecompressor):
            VideoKeyframeIndex.validate(data)

        self.assertGreater(len(data), VideoKeyframeIndex._CHUNK_SIZE)
        self.assertLessEqual(
            max(input_sizes), VideoKeyframeIndex._CHUNK_SIZE
        )

if __name__ == '__main__':
    unittest.main()
