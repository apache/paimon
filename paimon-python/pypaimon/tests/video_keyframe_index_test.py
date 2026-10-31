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
import pickle
from fractions import Fraction
import struct
import sys
from types import SimpleNamespace
import unittest
import zlib
from unittest import mock

from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex


class VideoKeyframeIndexTest(unittest.TestCase):

    def test_keyframes_stay_compact_when_read_and_pickled(self):
        values = ((0, -100, 20), (12, 120, 200))
        index = VideoKeyframeIndex([(0, 10)], values)
        data = index.serialize()
        restored = pickle.loads(pickle.dumps(VideoKeyframeIndex.deserialize(data, 1000)))
        self.assertEqual(values, restored.keyframes)
        self.assertEqual(values[-1], restored.keyframes[-1])
        self.assertEqual(values[::-1], restored.keyframes[::-1])
        self.assertEqual(list(values), list(restored.keyframes))
        self.assertLessEqual(sum(map(len, restored.keyframes.cache.values())), 1024 * 1024)
        with self.assertRaises(IndexError):
            _ = restored.keyframes[2]
        with self.assertRaises(IndexError):
            _ = restored.keyframes[-3]
        self.assertEqual(data, restored.serialize())

    def test_real_video_above_old_keyframe_limit(self):
        import tempfile
        from pathlib import Path
        import pytest
        av = pytest.importorskip("av")
        np = pytest.importorskip("numpy")
        pytest.importorskip("torch")
        from pypaimon.multimodal.lerobot.dataset import _PyAVVideoDecoder

        count = 65537
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "all-keyframes.mp4")
            with av.open(path, "w") as output:
                stream = output.add_stream("libx264", rate=30, options={"preset": "ultrafast", "crf": "0"})
                stream.width = stream.height = 16
                stream.pix_fmt = "yuv420p"
                stream.gop_size = 1
                stream.codec_context.max_b_frames = 0
                for ordinal in range(count):
                    frame = av.VideoFrame.from_ndarray(
                        np.full((16, 16, 3), ordinal % 200, dtype=np.uint8), format="rgb24")
                    frame.pts = ordinal
                    for packet in stream.encode(frame):
                        output.mux(packet)
                for packet in stream.encode():
                    output.mux(packet)
            with open(path, "rb") as source:
                index = VideoKeyframeIndex.inspect(source, Path(path).stat().st_size)
            self.assertEqual(count, len(index.keyframes))
            restored = VideoKeyframeIndex.deserialize(index.serialize())
            ordinals = [count - 1, 0, 32768]
            with av.open(path) as container:
                expected = {i: frame.to_ndarray(format="rgb24")
                            for i, frame in enumerate(container.decode(video=0)) if i in ordinals}
            with open(path, "rb") as source:
                decoder = _PyAVVideoDecoder(source, keyframe_index=restored, thread_count=1)
                try:
                    actual = decoder.get_frames_at(indices=ordinals).data.numpy()
                    for ordinal, pixels in zip(ordinals, actual):
                        np.testing.assert_array_equal(expected[ordinal], pixels.transpose(1, 2, 0))
                finally:
                    decoder.close()
            self.assertLessEqual(sum(map(len, restored.keyframes.cache.values())), 1024 * 1024)

    def test_replace_unmaterialized_keyframes(self):
        index = VideoKeyframeIndex([(0, 10)], [(0, 0, 20), (12, 120, 200)])
        restored = VideoKeyframeIndex.deserialize(index.serialize(), 1000)
        restored.keyframes = ((0, 100, 30),)
        self.assertEqual(
            restored.keyframes,
            VideoKeyframeIndex.deserialize(restored.serialize()).keyframes,
        )

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
        self.assertEqual(21, index.HEADER.size)
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
        for data in (b'', index.serialize()[:-1], index.serialize() + b'junk',
                     b'\x02' + index.serialize()[1:]):
            with self.assertRaises(ValueError):
                VideoKeyframeIndex.deserialize(data)
        header = index.serialize()[:index.HEADER.size]
        with self.assertRaises(ValueError):
            VideoKeyframeIndex.deserialize(
                header + zlib.compress(struct.pack('<qqq', 0, 1, 2)))

    def test_deserialize_matches_streaming_validation(self):
        def encode(entries, count=None, page=None):
            if page is None:
                page = b''.join(VideoKeyframeIndex.ENTRY.pack(*entry) for entry in entries)
                page += struct.pack('<I', zlib.crc32(page))
            return VideoKeyframeIndex.HEADER.pack(
                VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC,
                0, len(entries) if count is None else count,
            ) + page

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
            encode(valid, page=b'invalid page'),
            encode(valid, page=b'partial entry'),
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
        page = VideoKeyframeIndex.ENTRY.pack(0, -1, 10)
        data += page + struct.pack('<I', zlib.crc32(page))
        restored = VideoKeyframeIndex.deserialize(data, 11)
        self.assertEqual(((0, 5), (8, 2)), restored.metadata_ranges)
        self.assertEqual(((0, -1, 10),), restored.keyframes)

    def test_large_index_is_read_in_pages(self):
        import bisect
        import tempfile
        count = 1_000_000
        with VideoKeyframeIndex([], ((i, i - 100, i * 2) for i in range(count))) as index:
            with tempfile.TemporaryFile() as stored:
                index.write_to(stored)
                length = stored.tell()
                self.assertGreater(length, 16 * 1024 * 1024)
                reads = []

                def read_at(offset, size):
                    reads.append((offset, size))
                    stored.seek(offset)
                    return stored.read(size)

                with VideoKeyframeIndex.open(read_at, length, count * 2) as restored:
                    self.assertEqual(count, len(restored.keyframes))
                    self.assertLess(sum(size for _, size in reads), 200_000)
                    for ordinal in (count - 1, 32768, 0, 499999):
                        before = len(reads)
                        self.assertEqual((ordinal, ordinal - 100, ordinal * 2), restored.keyframes[ordinal])
                        self.assertLessEqual(len(reads) - before, 1)
                        before = len(reads)
                        self.assertEqual((ordinal, ordinal - 100, ordinal * 2), restored.keyframes[ordinal])
                        self.assertEqual(before, len(reads))
                        self.assertEqual(ordinal, bisect.bisect_left(restored.ordinals, ordinal))
                    self.assertLessEqual(max(size for _, size in reads), 3 * (4096 * 24 + 4))
                    self.assertLessEqual(sum(map(len, restored.keyframes.cache.values())), 1024 * 1024)
                VideoKeyframeIndex.validate_stream(stored, length, count * 2)

    def test_binary_search_coalesces_adjacent_page_reads(self):
        import bisect
        import tempfile
        count = 1_000_000
        with VideoKeyframeIndex([], ((i, i, i) for i in range(count))) as original:
            with tempfile.TemporaryFile() as stored:
                original.write_to(stored)
                length = stored.tell()
                reads = []

                def read_at(offset, size):
                    reads.append((offset, size))
                    stored.seek(offset)
                    return stored.read(size)

                for target in (count - 1, 123456, 500000):
                    with self.subTest(target=target):
                        reads.clear()
                        with VideoKeyframeIndex.open(read_at, length, count) as index:
                            reads.clear()
                            self.assertEqual(target, bisect.bisect_left(index.ordinals, target))
                            self.assertLessEqual(len(reads), 9)
                            self.assertLessEqual(max(size for _, size in reads), 3 * (4096 * 24 + 4))
                            before = len(reads)
                            self.assertEqual((target, target, target), index.keyframes[target])
                            self.assertEqual(before, len(reads))
                            self.assertLessEqual(sum(map(len, index.keyframes.cache.values())), 1024 * 1024)

    def test_paged_corruption_and_boundary_validation(self):
        with VideoKeyframeIndex([], ((i, i, i) for i in range(8193))) as index:
            data = index.serialize()
        second = VideoKeyframeIndex.HEADER.size + 4096 * 24 + 4
        corrupt = bytearray(data)
        corrupt[second] ^= 1
        with self.assertRaisesRegex(ValueError, 'checksum'):
            with VideoKeyframeIndex.open(lambda o, n: corrupt[o:o + n], len(corrupt)) as index:
                _ = index.keyframes[4096]
        # A valid page checksum must not hide a descending page boundary.
        corrupt = bytearray(data)
        page = b''.join(VideoKeyframeIndex.ENTRY.pack(i, i, i) for i in range(4096))
        corrupt[second:second + len(page)] = page
        struct.pack_into('<I', corrupt, second + len(page), zlib.crc32(page))
        for invalid in (corrupt, data[:-1], data + b'x'):
            with self.assertRaises(ValueError):
                VideoKeyframeIndex.validate(invalid)

    def test_lazy_reads_validate_uncached_page_boundaries(self):
        page_entries = 4096
        stride = page_entries * 24 + 4
        with VideoKeyframeIndex([], ((i, i, i) for i in range(16 * page_entries))) as index:
            original = index.serialize()
        for column in range(3):
            corrupt = bytearray(original)
            offset = VideoKeyframeIndex.HEADER.size + 12 * stride
            page = bytearray(corrupt[offset:offset + page_entries * 24])
            for entry in range(page_entries):
                struct.pack_into('<q', page, entry * 24 + column * 8, 11 * page_entries + entry)
            corrupt[offset:offset + len(page)] = page
            struct.pack_into('<I', corrupt, offset + len(page), zlib.crc32(page))
            for target in (11, 12):
                for cache_state in ('cold', 'cached', 'evicted'):
                    with self.subTest(column=column, target=target, cache_state=cache_state):
                        with VideoKeyframeIndex.open(
                                lambda o, n: corrupt[o:o + n], len(corrupt)) as index:
                            if cache_state != 'cold':
                                index.keyframes[(target - 1 if target == 11 else target + 1) * page_entries]
                            if cache_state == 'evicted':
                                for prior in range(10):
                                    index.keyframes[prior * page_entries]
                            with self.assertRaisesRegex(ValueError, 'strictly increasing'):
                                index.keyframes[target * page_entries]

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

    def test_rejects_forged_paged_count_before_reading_entries(self):
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, 0, 2 ** 63)
        read = mock.Mock(side_effect=lambda o, n: data[o:o + n])
        with self.assertRaisesRegex(ValueError, 'entries length'):
            VideoKeyframeIndex.open(read, len(data))
        self.assertLessEqual(max(call.args[1] for call in read.call_args_list), 21)

    def test_rejects_too_many_metadata_ranges_before_materialization(self):
        count = VideoKeyframeIndex.MAX_METADATA_RANGE_COUNT + 1
        data = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, count, 1
        ) + zlib.compress(VideoKeyframeIndex.ENTRY.pack(0, 0, 0))

        with mock.patch.object(
                VideoKeyframeIndex, '_metadata') as ranges:
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

    def test_inspect_uses_packet_positions_in_presentation_order(self):
        stream = SimpleNamespace(time_base=Fraction(1, 30))
        container = mock.MagicMock()
        container.__enter__.return_value = container
        container.streams.video = [stream]
        container.demux.return_value = [
            SimpleNamespace(is_keyframe=True, pts=i, pos=i, time_base=stream.time_base, is_discard=False)
            for i in (0, 2, 1)]
        container.decode.return_value = [
            SimpleNamespace(key_frame=True, pts=i, time_base=stream.time_base) for i in range(3)]
        av = SimpleNamespace(open=mock.Mock(return_value=container))
        with mock.patch.dict(sys.modules, {'av': av}), mock.patch.object(
                VideoKeyframeIndex, '_iso_bmff_metadata_ranges', return_value=[]):
            with VideoKeyframeIndex.inspect(mock.Mock(), 10) as index:
                self.assertEqual(((0, 0, 0), (1, 1, 1), (2, 2, 2)), index.keyframes)
        self.assertEqual(2, av.open.call_count)


if __name__ == '__main__':
    unittest.main()
