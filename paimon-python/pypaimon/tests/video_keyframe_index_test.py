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

if __name__ == '__main__':
    unittest.main()
