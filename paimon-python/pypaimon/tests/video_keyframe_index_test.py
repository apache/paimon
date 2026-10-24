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
from fractions import Fraction
import struct
import sys
from types import SimpleNamespace
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
                (offsets[0], len(boxes[0]) + 8),
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
        self.assertEqual(
            ((0, 2),),
            VideoKeyframeIndex(
                [(0, 1), (1, 1)], [(0, 0, 0)]).metadata_ranges,
        )
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

    def test_deserialize_matches_streaming_validation(self):
        def encode(entries, count=None, compressed=None):
            return VideoKeyframeIndex.HEADER.pack(
                VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC,
                0, len(entries) if count is None else count,
            ) + (zlib.compress(b''.join(
                VideoKeyframeIndex.ENTRY.pack(*entry) for entry in entries
            )) if compressed is None else compressed)

        valid = [(0, -(2 ** 63), 0), (2 ** 63 - 1, 2 ** 63 - 1, 99)]
        for entries in ([(0, -10, 0)], valid):
            data = encode(entries)
            VideoKeyframeIndex.validate(data, 100)
            restored = VideoKeyframeIndex.deserialize(data, 100)
            self.assertEqual(tuple(entries), restored.keyframes)
            self.assertTrue(all(type(value) is int
                                for entry in restored.keyframes
                                for value in entry))

        invalid = [
            encode([(1, 0, 0)]),
            encode([(0, 0, -1)]),
            encode([(0, 0, 0), (0, 1, 1)]),
            encode([(0, 0, 0), (-1, 1, 1)]),
            encode([(0, 1, 0), (1, 1, 1)]),
            encode([(0, 1, 0), (1, -1, 1)]),
            encode([(0, 0, 1), (1, 1, 1)]),
            encode([(0, 0, 1), (1, 1, 0)]),
            encode([(0, 0, 100)]),
            encode(valid, count=1),
            encode(valid, count=3),
            encode(valid)[:-1],
            encode(valid) + b'trailing',
            encode(valid) + zlib.compress(b''),
            encode(valid, compressed=b'not zlib'),
            encode(valid, compressed=zlib.compress(b'partial entry')),
        ]
        for data in invalid:
            with self.subTest(data=data):
                for read in (VideoKeyframeIndex.validate,
                             VideoKeyframeIndex.deserialize):
                    with self.assertRaises(ValueError):
                        read(data, 100)

    def test_deserialize_preserves_metadata_normalization(self):
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, 3, 1
        ) + b''.join(VideoKeyframeIndex.METADATA_RANGE.pack(*entry)
                     for entry in [(0, 2), (2, 3), (8, 2)])
        data += zlib.compress(VideoKeyframeIndex.ENTRY.pack(0, -1, 10))
        restored = VideoKeyframeIndex.deserialize(data, 11)
        self.assertEqual(((0, 5), (8, 2)), restored.metadata_ranges)
        self.assertEqual(((0, -1, 10),), restored.keyframes)

    def test_deserialize_bounds_output_before_materializing_entries(self):
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, 0, 1
        ) + zlib.compress(b'\0' * (4 * 1024 * 1024))
        real_decompressobj = zlib.decompressobj
        output_sizes = []

        class TrackingDecompressor:
            def __init__(self):
                self._delegate = real_decompressobj()

            def decompress(self, value, max_length=0):
                output = self._delegate.decompress(value, max_length)
                output_sizes.append(len(output))
                return output

            def __getattr__(self, name):
                return getattr(self._delegate, name)

        with mock.patch.object(keyframe_index_module.zlib, 'decompressobj',
                               side_effect=TrackingDecompressor):
            with self.assertRaises(ValueError):
                VideoKeyframeIndex.deserialize(data)
        self.assertEqual([VideoKeyframeIndex.ENTRY.size + 1], output_sizes)

    def test_deserialize_at_entry_limit(self):
        entries = tuple((value, value - 100, value * 2)
                        for value in range(VideoKeyframeIndex.MAX_KEYFRAME_COUNT))
        index = VideoKeyframeIndex([], entries)
        restored = VideoKeyframeIndex.deserialize(index.serialize())
        self.assertEqual(entries, restored.keyframes)

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
        count = 60_000
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

    def test_rejects_too_many_keyframes_before_decompression(self):
        count = VideoKeyframeIndex.MAX_KEYFRAME_COUNT + 1
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, 0, count
        ) + zlib.compress(VideoKeyframeIndex.ENTRY.pack(0, 0, 0))

        with mock.patch.object(
                keyframe_index_module.zlib,
                'decompressobj') as decompress:
            with self.assertRaisesRegex(ValueError, "entry limit"):
                VideoKeyframeIndex.deserialize(data)
            decompress.assert_not_called()

        with self.assertRaisesRegex(ValueError, "entry limit"):
            VideoKeyframeIndex(
                [],
                ((value, value, value) for value in range(count)),
            )

    def test_rejects_too_many_metadata_ranges_before_materialization(self):
        count = VideoKeyframeIndex.MAX_METADATA_RANGE_COUNT + 1
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, count, 1
        ) + zlib.compress(VideoKeyframeIndex.ENTRY.pack(0, 0, 0))

        with mock.patch.object(
                VideoKeyframeIndex, '_iter_metadata_ranges') as ranges:
            with self.assertRaisesRegex(ValueError, "metadata-range limit"):
                VideoKeyframeIndex.deserialize(data)
            ranges.assert_not_called()

        with self.assertRaisesRegex(ValueError, "metadata-range limit"):
            VideoKeyframeIndex(
                ((value * 2, 1) for value in range(count)),
                [(0, 0, 0)],
            )

    def test_iso_bmff_metadata_range_count_is_bounded_after_merging(self):
        def box(box_type, body):
            return struct.pack(">I4s", len(body) + 8, box_type) + body

        payload = b''.join([
            box(b"free", b"x"),
            box(b"skip", b"y"),
            box(b"moov", b"metadata"),
        ])
        with mock.patch.object(
                VideoKeyframeIndex, 'MAX_METADATA_RANGE_COUNT', 2):
            with self.assertRaisesRegex(ValueError, "metadata-range limit"):
                VideoKeyframeIndex._iso_bmff_metadata_ranges(
                    io.BytesIO(payload), len(payload))

    def test_inspect_bounds_keyframe_packets(self):
        stream = SimpleNamespace(time_base=Fraction(1, 30))
        packets = [
            SimpleNamespace(
                is_keyframe=True,
                pts=value,
                pos=value,
                time_base=stream.time_base,
                is_discard=False,
            )
            for value in range(2)
        ]
        container = mock.MagicMock()
        container.__enter__.return_value = container
        container.streams.video = [stream]
        container.demux.return_value = packets
        av = SimpleNamespace(open=mock.Mock(return_value=container))

        with mock.patch.dict(sys.modules, {'av': av}), mock.patch.object(
                VideoKeyframeIndex,
                '_iso_bmff_metadata_ranges',
                return_value=[]), mock.patch.object(
                VideoKeyframeIndex,
                'MAX_KEYFRAME_COUNT',
                1):
            with self.assertRaisesRegex(ValueError, "entry limit"):
                VideoKeyframeIndex.inspect(mock.Mock(), 10)
        self.assertEqual(1, av.open.call_count)


if __name__ == '__main__':
    unittest.main()
