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

import io
import struct
import unittest
import zlib
from unittest import mock

import pypaimon.table.row.video_keyframe_index as keyframe_index_module
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex


class VideoKeyframeIndexTest(unittest.TestCase):

    def test_iso_bmff_metadata_ranges_exclude_padding_bodies(self):
        def box(box_type, body):
            return struct.pack(">I4s", len(body) + 8, box_type) + body

        boxes = [
            box(b"ftyp", b"isom"),
            box(b"free", b"x" * 1024),
            box(b"skip", b"y" * 512),
            box(b"mdat", b"video"),
            box(b"moov", b"metadata"),
        ]
        payload = b''.join(boxes)
        offsets = []
        offset = 0
        for value in boxes:
            offsets.append(offset)
            offset += len(value)

        self.assertEqual(
            [
                (offsets[0], len(boxes[0])),
                (offsets[1], 8),
                (offsets[2], 8),
                (offsets[3], 8),
                (offsets[4], len(boxes[4])),
            ],
            VideoKeyframeIndex._iso_bmff_metadata_ranges(
                io.BytesIO(payload), len(payload)),
        )

    def test_round_trip_and_validation(self):
        index = VideoKeyframeIndex(
            [(0, 128), (256, 64)],
            [(0, 500, 512), (60, 9000, 4096)],
        )
        restored = VideoKeyframeIndex.deserialize(index.serialize())
        self.assertEqual(index.metadata_ranges, restored.metadata_ranges)
        self.assertEqual(index.keyframes, restored.keyframes)
        self.assertEqual(17, index.HEADER.size)
        self.assertFalse(hasattr(restored, 'time_base'))
        self.assertFalse(hasattr(restored, 'frame_count'))
        self.assertFalse(hasattr(restored, 'stream_index'))
        for ranges in ([(-1, 1)], [(0, 0)], [(4, 2), (3, 1)]):
            with self.subTest(ranges=ranges):
                with self.assertRaises(ValueError):
                    VideoKeyframeIndex(ranges, [(0, 0, 0)])
        for anchors in (
            [], [(1, 0, 0)], [(0, 5, 0), (0, 9, 1)],
            [(0, 5, 0), (2, 5, 1)],
            [(0, 5, 0), (2, 9, 0)],
            [(0, 5, 0), (2, 9, 2), (1, 10, 3)],
        ):
            with self.subTest(anchors=anchors):
                with self.assertRaises(ValueError):
                    VideoKeyframeIndex([], anchors)
        for data in (b'', index.serialize()[:-1], index.serialize() + b'junk'):
            with self.assertRaises(ValueError):
                VideoKeyframeIndex.deserialize(data)
        header = index.serialize()[:index.HEADER.size]
        with self.assertRaises(ValueError):
            VideoKeyframeIndex.deserialize(
                header + zlib.compress(struct.pack('<qqq', 0, 1, 2)))

    def test_validate_rejects_ranges_outside_video_payload(self):
        metadata_outside = VideoKeyframeIndex(
            [(8, 4)], [(0, 0, 1)]).serialize()
        packet_outside = VideoKeyframeIndex(
            [], [(0, 0, 12)]).serialize()

        for data in (metadata_outside, packet_outside):
            with self.subTest(data=data):
                with self.assertRaisesRegex(ValueError, "video payload"):
                    VideoKeyframeIndex.validate(data, payload_length=10)
                with self.assertRaisesRegex(ValueError, "video payload"):
                    VideoKeyframeIndex.deserialize(data, payload_length=10)

    def test_valid_large_index_uses_bounded_input_chunks(self):
        count = 100_000
        entries = b''.join(
            VideoKeyframeIndex.ENTRY.pack(value, value, value)
            for value in range(count)
        )
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, 0, count
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
