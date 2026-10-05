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

import bisect
import io
import struct
import tempfile
import tracemalloc
import unittest
import zlib
from fractions import Fraction
from pathlib import Path
from unittest import mock

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.options import Options
from pypaimon.filesystem.jindo_file_system_handler import JindoInputFile
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.read.reader.video_format_reader import VideoFileMeta
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import (
    Blob,
    BlobData,
    BlobDescriptor,
    VideoFrameDescriptor,
)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex
from pypaimon.write.video_format_writer import VideoFormatWriter

try:
    import av
    import numpy as np
except ImportError:
    av = None
    np = None


class VideoFormatTest(unittest.TestCase):

    REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
    DESCRIPTOR_FIXTURE = (
        REPOSITORY_ROOT
        / "paimon-common/src/test/resources/org/apache/paimon/data/"
        "video-frame-descriptor-v1.hex"
    )
    VIDEO_FIXTURE = (
        REPOSITORY_ROOT
        / "paimon-format/src/test/resources/org/apache/paimon/format/blob/"
        "video-v1.hex"
    )

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.file_io = LocalFileIO(str(self.root), Options({}))
        self.field = DataField(0, "video", AtomicType("BLOB"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_descriptor_round_trip_preserves_payload_and_frame(self):
        descriptor = VideoFrameDescriptor("s3://bucket/a.video", 7, 99, 42, -1, 0)
        serialized = descriptor.serialize()

        self.assertTrue(
            VideoFrameDescriptor.is_video_frame_descriptor(serialized)
        )
        self.assertFalse(BlobDescriptor.is_blob_descriptor(serialized))
        self.assertEqual(descriptor, BlobDescriptor.deserialize(serialized))
        self.assertEqual(descriptor, VideoFrameDescriptor.deserialize(serialized))
        self.assertEqual(
            BlobDescriptor("s3://bucket/a.video", 7, 99),
            descriptor.payload_descriptor,
        )
        restored = Blob.from_bytes(serialized, file_io=self.file_io)
        self.assertIsInstance(restored.to_descriptor(), VideoFrameDescriptor)
        self.assertEqual(descriptor, restored.to_descriptor())
        with self.assertRaisesRegex(ValueError, "trailing bytes"):
            VideoFrameDescriptor.deserialize(serialized + b"x")
        with self.assertRaisesRegex(ValueError, "non-negative"):
            VideoFrameDescriptor("x", 0, 1, -1, -1, 0)

        indexed = VideoFrameDescriptor("s3://bucket/a.video", 7, 99, 42, 106, 8)
        restored = VideoFrameDescriptor.deserialize(indexed.serialize())
        self.assertEqual(indexed, restored)
        self.assertEqual(
            BlobDescriptor("s3://bucket/a.video", 106, 8),
            restored.keyframe_index_descriptor,
        )

    def test_cross_language_descriptor_fixture(self):
        fixture = self._fixture_bytes(self.DESCRIPTOR_FIXTURE)
        expected = VideoFrameDescriptor(
            "s3://bucket/视频.mp4", 7, 99, 42, 106, 8)
        self.assertEqual(fixture, expected.serialize())
        self.assertEqual(expected, BlobDescriptor.deserialize(fixture))

    def test_pack_raw_videos_and_map_frame_runs(self):
        first_bytes = b"first-mp4"
        second_bytes = b"second-mp4"
        first0 = self._source_frame("first.mp4", first_bytes, 0)
        first1 = self._source_frame("first.mp4", first_bytes, 1)
        second7 = self._source_frame("second.mp4", second_bytes, 7)
        first4 = self._source_frame("first.mp4", first_bytes, 4)
        target = (self.root / "data.video").as_uri()

        writer = VideoFormatWriter(
            self.file_io.new_output_stream(target), file_path=target
        )
        for value in (first0, first1, second7, first4, None, Blob.PLACE_HOLDER):
            writer.add_element(
                GenericRow([value], [self.field], RowKind.INSERT)
            )
        self.assertEqual(2, writer.physical_video_count)
        self.assertEqual(5, writer.run_count)
        writer.close()

        stored = (self.root / "data.video").read_bytes()
        self.assertTrue(stored.startswith(first_bytes + second_bytes))
        with self.file_io.new_input_stream(target) as stream:
            meta = VideoFileMeta(stream, len(stored))
        self.assertEqual(6, meta.record_count)
        self.assertEqual((0, len(first_bytes), 0, -1, 0), meta.frame(0))
        self.assertEqual((0, len(first_bytes), 1, -1, 0), meta.frame(1))
        self.assertEqual(
            (len(first_bytes), len(second_bytes), 7, -1, 0), meta.frame(2)
        )
        self.assertEqual((0, len(first_bytes), 4, -1, 0), meta.frame(3))
        self.assertIsNone(meta.frame(4))
        self.assertIs(Blob.PLACE_HOLDER, meta.frame(5))

        values = self._read(target, row_indices=range(5))
        frames = [VideoFrameDescriptor.deserialize(value) for value in values[:4]]
        self.assertEqual([0, 1, 7, 4], [frame.frame_index for frame in frames])
        self.assertEqual(frames[0].payload_descriptor, frames[1].payload_descriptor)
        self.assertEqual(frames[0].payload_descriptor, frames[3].payload_descriptor)
        self.assertNotEqual(frames[0].payload_descriptor, frames[2].payload_descriptor)
        self.assertIsNone(values[4])

    def test_cross_language_video_v1_fixture(self):
        fixture = self._fixture_bytes(self.VIDEO_FIXTURE)
        encoded = VideoKeyframeIndex(
            [(0, 1)], [(0, 0, 0), (12, 36000, 2)]
        ).serialize()
        video = b"abc"
        source_path = self.root / "indexed.mp4"
        source_path.write_bytes(video + encoded)

        def indexed_frame(frame_index):
            descriptor = VideoFrameDescriptor(
                source_path.as_uri(), 0, len(video), frame_index,
                len(video), len(encoded)
            )
            return Blob.from_descriptor(
                self.file_io.uri_reader_factory.create(descriptor.uri),
                descriptor,
            )
        target = (self.root / "indexed.video").as_uri()

        writer = VideoFormatWriter(self.file_io.new_output_stream(target))
        values = (
            indexed_frame(2),
            indexed_frame(3),
            None,
            Blob.PLACE_HOLDER,
            self._source_frame("b.mp4", b"WXYZ", 7),
            self._source_frame("b.mp4", b"WXYZ", 8),
            indexed_frame(10),
        )
        for value in values:
            writer.add_element(GenericRow([value], [self.field], RowKind.INSERT))
        writer.close()

        stored = (self.root / "indexed.video").read_bytes()
        self.assertEqual(fixture, stored)
        with self.file_io.new_input_stream(target) as stream:
            meta = VideoFileMeta(stream, len(stored))
        self.assertEqual(
            (0, len(video), 2, 7, len(encoded)), meta.frame(0)
        )
        self.assertEqual((3, 4, 7, -1, 0), meta.frame(4))
        serialized = self._read(target, row_indices=[0])[0]
        value = VideoFrameDescriptor.deserialize(serialized)
        self.assertEqual(2, value.frame_index)
        mapping_descriptor = value.keyframe_index_descriptor
        self.assertEqual(
            encoded,
            Blob.from_file(
                self.file_io,
                mapping_descriptor.uri,
                mapping_descriptor.offset,
                mapping_descriptor.length,
            ).to_data(),
        )

        rewritten = (self.root / "rewritten.video").as_uri()
        writer = VideoFormatWriter(self.file_io.new_output_stream(rewritten))
        writer.add_element(GenericRow([
            Blob.from_bytes(serialized, file_io=self.file_io)
        ], [self.field], RowKind.INSERT))
        writer.close()
        rewritten_value = VideoFrameDescriptor.deserialize(
            self._read(rewritten)[0]
        )
        rewritten_index = rewritten_value.keyframe_index_descriptor
        self.assertEqual(
            encoded,
            Blob.from_file(
                self.file_io,
                rewritten_index.uri,
                rewritten_index.offset,
                rewritten_index.length,
            ).to_data(),
        )

    @unittest.skipUnless(
        av is not None and np is not None,
        "PyAV and NumPy are required for sparse video seek validation",
    )
    def test_real_mp4_index_supports_sparse_random_reads(self):
        payload = self._real_mp4()
        metadata_ranges, keyframes, has_b_frames, is_vfr = (
            self._mp4_seek_index(payload))
        index = VideoKeyframeIndex(metadata_ranges, keyframes)
        source = self.root / "real-indexed.mp4"
        source.write_bytes(payload)
        target_frames = [44, 1, 30, 14, 59, 15]
        target = (self.root / "real-indexed.video").as_uri()

        writer = VideoFormatWriter(self.file_io.new_output_stream(target))
        for frame_index in target_frames:
            descriptor = VideoFrameDescriptor(
                source.as_uri(), 0, len(payload), frame_index,
                -1, 0)
            value = Blob.from_descriptor(
                self.file_io.uri_reader_factory.create(descriptor.uri),
                descriptor,
            )
            writer.add_element(
                GenericRow([value], [self.field], RowKind.INSERT))
        writer.close()

        descriptors = [
            VideoFrameDescriptor.deserialize(value)
            for value in self._read(target)
        ]
        payload_descriptor = descriptors[0].payload_descriptor
        index_descriptor = descriptors[0].keyframe_index_descriptor
        stored_payload = Blob.from_file(
            self.file_io,
            payload_descriptor.uri,
            payload_descriptor.offset,
            payload_descriptor.length,
        ).to_data()
        stored_index = VideoKeyframeIndex.deserialize(
            Blob.from_file(
                self.file_io,
                index_descriptor.uri,
                index_descriptor.offset,
                index_descriptor.length,
            ).to_data())

        self.assertEqual(target_frames, [value.frame_index for value in descriptors])
        self.assertEqual(index.metadata_ranges, stored_index.metadata_ranges)
        self.assertEqual(index.keyframes, stored_index.keyframes)
        self.assertTrue(has_b_frames)
        self.assertTrue(is_vfr)

        with av.open(io.BytesIO(stored_payload)) as container:
            expected = [
                frame.to_ndarray(format="rgb24")
                for frame in container.decode(video=0)
            ]
        ordinals = [entry[0] for entry in stored_index.keyframes]
        sparse_read_sizes = []
        for target_frame in target_frames:
            keyframe = bisect.bisect_right(ordinals, target_frame) - 1
            range_end = (
                stored_index.keyframes[keyframe + 2][2]
                if keyframe + 2 < len(stored_index.keyframes)
                else len(stored_payload)
            )
            for retry in range(3):
                anchor = max(0, keyframe - retry)
                anchor_ordinal, anchor_pts, unused_position = (
                    stored_index.keyframes[anchor])
                range_start = stored_index.keyframes[
                    max(0, anchor - 1)][2]
                sparse, available_bytes = self._sparse_video(
                    stored_payload,
                    list(stored_index.metadata_ranges)
                    + [(range_start, range_end - range_start)],
                )
                try:
                    actual = self._decode_sparse_frame(
                        sparse, anchor_ordinal, anchor_pts, target_frame)
                    break
                except (av.error.FFmpegError, ValueError):
                    continue
            else:
                self.fail("Sparse video retries did not reach the target frame.")
            np.testing.assert_array_equal(expected[target_frame], actual)
            sparse_read_sizes.append(available_bytes)
        self.assertLess(min(sparse_read_sizes), len(stored_payload))

    @unittest.skipUnless(
        av is not None and np is not None,
        "PyAV and NumPy are required for video index generation",
    )
    def test_unknown_length_jindo_video_generates_index(self):
        payload = self._real_mp4()

        class JindoStream:

            def __init__(self):
                self._stream = io.BytesIO(payload)

            @property
            def closed(self):
                return self._stream.closed

            def read(self, size=-1):
                return self._stream.read(size)

            def seek(self, offset, whence=io.SEEK_SET):
                self._stream.seek(offset, whence)

            def tell(self):
                return self._stream.tell()

            def close(self):
                self._stream.close()

        reader = mock.Mock()
        reader.new_input_stream.side_effect = lambda unused: JindoInputFile(
            JindoStream())
        descriptor = VideoFrameDescriptor(
            "oss://bucket/video.mp4", 0, -1, 0, -1, 0)
        value = Blob.from_descriptor(reader, descriptor)
        target = (self.root / "jindo.video").as_uri()

        writer = VideoFormatWriter(self.file_io.new_output_stream(target))
        writer.add_element(GenericRow([value], [self.field], RowKind.INSERT))
        writer.close()

        stored = VideoFrameDescriptor.deserialize(self._read(target)[0])
        self.assertEqual(len(payload), stored.length)
        self.assertGreater(stored.keyframe_index_descriptor.length, 0)

    def test_target_size_counts_buffered_keyframe_index(self):
        mapping = VideoKeyframeIndex([], [(0, 0, 0)]).serialize()
        video = b"video"
        source = self.root / "target-size.mp4"
        source.write_bytes(video + mapping)
        descriptor = VideoFrameDescriptor(
            source.as_uri(), 0, len(video), 0, len(video), len(mapping)
        )
        blob = Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )
        writer = VideoFormatWriter(io.BytesIO())

        writer.add_element(GenericRow([blob], [self.field], RowKind.INSERT))

        self.assertFalse(writer.reach_target_size(len(video) + len(mapping) + 1))
        self.assertTrue(writer.reach_target_size(len(video) + len(mapping)))

    def test_rejects_invalid_keyframe_index(self):
        video = b"video"
        mapping = b"mapping"
        source = self.root / "invalid-index.mp4"
        source.write_bytes(video + mapping)
        descriptor = VideoFrameDescriptor(
            source.as_uri(), 0, len(video), 0, len(video), len(mapping)
        )
        blob = Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )
        writer = VideoFormatWriter(io.BytesIO())

        with self.assertRaisesRegex(ValueError, "Invalid video keyframe index"):
            writer.add_element(
                GenericRow([blob], [self.field], RowKind.INSERT)
            )

    def test_rejects_oversized_keyframe_indexes_before_fetch(self):
        descriptor = VideoFrameDescriptor(
            (self.root / "missing.mp4").as_uri(),
            0,
            1,
            0,
            1,
            VideoFormatWriter.MAX_KEYFRAME_INDEX_BYTES + 1,
        )
        blob = Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )

        with self.assertRaisesRegex(ValueError, "keyframe index length.*limit"):
            VideoFormatWriter(io.BytesIO()).add_element(
                GenericRow([blob], [self.field], RowKind.INSERT)
            )

    def test_rejects_cumulative_keyframe_indexes_before_fetch(self):
        mapping = VideoKeyframeIndex([], [(0, 0, 0)]).serialize()
        first = self._indexed_frame("first-indexed.mp4", b"a", mapping)
        second = self._indexed_frame("second-indexed.mp4", b"b", mapping)
        writer = VideoFormatWriter(io.BytesIO())

        with mock.patch.object(
                VideoFormatWriter,
                'MAX_TOTAL_KEYFRAME_INDEX_BYTES',
                len(mapping)):
            writer.add_element(
                GenericRow([first], [self.field], RowKind.INSERT)
            )
            with self.assertRaisesRegex(
                    ValueError, "Buffered video keyframe indexes.*limit"):
                writer.add_element(
                    GenericRow([second], [self.field], RowKind.INSERT)
                )

    def test_rejects_seek_offsets_outside_video_payload(self):
        video = b"video"
        mapping = VideoKeyframeIndex(
            [(0, len(video) + 1)], [(0, 0, 0)]
        ).serialize()
        source = self.root / "out-of-range-index.mp4"
        source.write_bytes(video + mapping)
        descriptor = VideoFrameDescriptor(
            source.as_uri(), 0, len(video), 0, len(video), len(mapping)
        )
        blob = Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )

        with self.assertRaisesRegex(ValueError, "video payload"):
            VideoFormatWriter(io.BytesIO()).add_element(
                GenericRow([blob], [self.field], RowKind.INSERT)
            )

    def test_rejects_compressed_invalid_index_without_expanding_it(self):
        count = 500_000
        entry = VideoKeyframeIndex.ENTRY.pack(0, 0, 0)
        mapping = VideoKeyframeIndex.HEADER.pack(
            VideoKeyframeIndex.VERSION, VideoKeyframeIndex.MAGIC, 0, count
        ) + zlib.compress(entry * count)
        video = b"video"
        source = self.root / "compressed-invalid-index.mp4"
        source.write_bytes(video + mapping)
        descriptor = VideoFrameDescriptor(
            source.as_uri(), 0, len(video), 0, len(video), len(mapping)
        )
        blob = Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )
        writer = VideoFormatWriter(io.BytesIO())

        self.assertLess(len(mapping), 20_000)
        tracemalloc.start()
        try:
            with self.assertRaisesRegex(
                    ValueError,
                    "entry limit"):
                writer.add_element(
                    GenericRow([blob], [self.field], RowKind.INSERT)
                )
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 1_000_000)

    def test_rejects_inconsistent_keyframe_indexes_for_same_payload(self):
        video = b"video"
        first_index = VideoKeyframeIndex(
            [(0, 1)], [(0, 0, 0), (12, 36000, 4)]
        ).serialize()
        second_index = VideoKeyframeIndex(
            [], [(0, 0, 0), (6, 18000, 3)]
        ).serialize()
        source = self.root / "inconsistent-index.mp4"
        source.write_bytes(video + first_index + second_index)

        def frame(frame_index, index_offset=-1, index_length=0):
            descriptor = VideoFrameDescriptor(
                source.as_uri(), 0, len(video), frame_index,
                index_offset, index_length
            )
            return Blob.from_descriptor(
                self.file_io.uri_reader_factory.create(descriptor.uri),
                descriptor,
            )

        unindexed = frame(0)
        first = frame(1, len(video), len(first_index))
        second = frame(
            2, len(video) + len(first_index), len(second_index)
        )
        for values in (
            (unindexed, first),
            (first, unindexed),
            (first, second),
        ):
            with self.subTest(values=values):
                writer = VideoFormatWriter(io.BytesIO())
                writer.add_element(
                    GenericRow([values[0]], [self.field], RowKind.INSERT)
                )
                with self.assertRaisesRegex(ValueError, "same payload"):
                    writer.add_element(
                        GenericRow(
                            [values[1]], [self.field], RowKind.INSERT
                        )
                    )

    def test_selection_keeps_logical_frame_positions(self):
        target = (self.root / "selection.video").as_uri()
        writer = VideoFormatWriter(self.file_io.new_output_stream(target))
        source = [
            self._source_frame("selection.mp4", b"video", frame)
            for frame in range(4)
        ]
        for value in source:
            writer.add_element(GenericRow([value], [self.field], RowKind.INSERT))
        writer.close()

        values = self._read(target, row_indices=[1, 3])
        frames = [VideoFrameDescriptor.deserialize(value) for value in values]
        self.assertEqual([1, 3], [frame.frame_index for frame in frames])

    def test_reader_exposes_record_count_without_blob_indexes(self):
        target = (self.root / "record-count.video").as_uri()
        writer = VideoFormatWriter(self.file_io.new_output_stream(target))
        for frame_index in range(4):
            frame = self._source_frame(
                "record-count.mp4", b"video", frame_index
            )
            writer.add_element(
                GenericRow([frame], [self.field], RowKind.INSERT)
            )
        writer.close()

        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=target,
            read_fields=["video"],
            full_fields=[self.field],
            push_down_predicate=None,
            blob_as_descriptor=True,
            row_indices=[1, 3],
        )
        try:
            self.assertEqual(2, reader.record_count)
            self.assertEqual([], reader.blob_lengths)
            self.assertEqual([], reader.blob_offsets)
            values = reader.read_values_at([0, 1])
            self.assertEqual(
                [1, 3],
                [value.to_descriptor().frame_index for value in values],
            )
        finally:
            reader.close()

    def test_rejects_non_video_frame_input(self):
        target = (self.root / "reject.video").as_uri()
        writer = VideoFormatWriter(self.file_io.new_output_stream(target))
        for value in (
            BlobData(b"inline"),
            self._source_blob("ordinary.mp4", b"ordinary"),
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "VideoFrameDescriptor"):
                    writer.add_element(
                        GenericRow([value], [self.field], RowKind.INSERT)
                    )
        writer.close()

    def test_rejects_out_of_range_run_reference(self):
        target_path = self.root / "corrupt.video"
        indexes = [
            DeltaVarintCompressor.compress([]),
            DeltaVarintCompressor.compress([]),
            DeltaVarintCompressor.compress([1]),
            DeltaVarintCompressor.compress([0]),
            DeltaVarintCompressor.compress([0]),
        ]
        target_path.write_bytes(
            b"".join(indexes)
            + struct.pack(
                '<IIIIIIB',
                *(len(index) for index in indexes),
                VideoFormatWriter.FOOTER_MAGIC_NUMBER,
                VideoFormatWriter.VERSION,
            )
        )

        with self.assertRaisesRegex(IOError, "physical video count is 0"):
            FormatBlobReader(
                file_io=self.file_io,
                file_path=target_path.as_uri(),
                read_fields=["video"],
                full_fields=[self.field],
                push_down_predicate=None,
                blob_as_descriptor=True,
            )

    def _read(self, target, row_indices=None):
        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=target,
            read_fields=["video"],
            full_fields=[self.field],
            push_down_predicate=None,
            blob_as_descriptor=False,
            row_indices=row_indices,
        )
        try:
            return reader.read_arrow_batch().column(0).to_pylist()
        finally:
            reader.close()

    def _source_blob(self, name, data):
        source = self.root / name
        if not source.exists():
            source.write_bytes(data)
        return Blob.from_file(self.file_io, source.as_uri(), 0, len(data))

    def _source_frame(self, name, data, frame_index):
        source = self._source_blob(name, data)
        payload = source.to_descriptor()
        descriptor = VideoFrameDescriptor(
            payload.uri, payload.offset, payload.length, frame_index, -1, 0
        )
        return Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )

    def _indexed_frame(self, name, video, keyframe_index):
        source = self.root / name
        source.write_bytes(video + keyframe_index)
        descriptor = VideoFrameDescriptor(
            source.as_uri(),
            0,
            len(video),
            0,
            len(video),
            len(keyframe_index),
        )
        return Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )

    def _real_mp4(self):
        output = io.BytesIO()
        with av.open(output, mode="w", format="mp4") as container:
            streams = [
                container.add_stream("libx264", rate=30) for _ in range(2)
            ]
            for stream in streams:
                stream.width = stream.height = 32
                stream.pix_fmt = "yuv420p"
                stream.time_base = Fraction(1, 90_000)
                stream.codec_context.time_base = Fraction(1, 90_000)
                stream.options = {"g": "15", "bf": "3", "sc_threshold": "0"}
            pts = 450_000
            for frame_index in range(60):
                for stream_index, stream in enumerate(streams):
                    pixels = np.empty((32, 32, 3), dtype=np.uint8)
                    pixels[:, :, 0] = (frame_index + stream_index * 100) % 256
                    pixels[:, :, 1] = np.arange(32, dtype=np.uint8)[:, None]
                    pixels[:, :, 2] = np.arange(32, dtype=np.uint8)
                    frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
                    frame.pts = pts
                    frame.time_base = Fraction(1, 90_000)
                    for packet in stream.encode(frame):
                        container.mux(packet)
                pts += (3_000, 6_000, 1_500)[frame_index % 3]
            for stream in streams:
                for packet in stream.encode():
                    container.mux(packet)
        return output.getvalue()

    def _mp4_seek_index(self, payload):
        metadata_ranges = []
        box_types = []
        offset = 0
        while offset < len(payload):
            size, box_type = struct.unpack_from(">I4s", payload, offset)
            header_size = 8
            if size == 1:
                size = struct.unpack_from(">Q", payload, offset + 8)[0]
                header_size = 16
            elif size == 0:
                size = len(payload) - offset
            self.assertGreaterEqual(size, header_size)
            self.assertLessEqual(offset + size, len(payload))
            metadata_ranges.append(
                (offset, header_size if box_type == b"mdat" else size))
            box_types.append(box_type)
            offset += size
        self.assertEqual(b"moov", box_types[-1])

        with av.open(io.BytesIO(payload)) as container:
            self.assertEqual(2, len(container.streams.video))
            stream = container.streams.video[0]
            packet_positions = {}
            has_b_frames = False
            for packet in container.demux(stream):
                if packet.pts is not None and packet.dts is not None:
                    has_b_frames |= packet.pts != packet.dts
                if (packet.is_keyframe and packet.pts is not None
                        and packet.pos is not None and packet.pos >= 0):
                    packet_positions[int(packet.pts)] = int(packet.pos)

        with av.open(io.BytesIO(payload)) as container:
            stream = container.streams.video[0]
            keyframes = []
            frame_pts = []
            for ordinal, frame in enumerate(container.decode(stream)):
                frame_pts.append(int(frame.pts))
                if frame.key_frame:
                    keyframes.append((
                        ordinal,
                        int(frame.pts),
                        packet_positions[int(frame.pts)],
                    ))
        self.assertEqual(60, len(frame_pts))
        is_vfr = len({
            right - left for left, right in zip(frame_pts, frame_pts[1:])
        }) > 1
        return metadata_ranges, keyframes, has_b_frames, is_vfr

    def _decode_sparse_frame(
            self, source, anchor_ordinal, anchor_pts, target_ordinal):
        with av.open(source) as container:
            stream = container.streams.video[0]
            container.seek(
                anchor_pts, backward=True, any_frame=False, stream=stream)
            ordinal = anchor_ordinal
            found_anchor = False
            for frame in container.decode(stream):
                if not found_anchor:
                    if frame.pts != anchor_pts or not frame.key_frame:
                        continue
                    found_anchor = True
                if ordinal == target_ordinal:
                    return frame.to_ndarray(format="rgb24")
                ordinal += 1
        raise ValueError("Sparse video read did not reach the target frame.")

    @staticmethod
    def _sparse_video(payload, ranges):
        sparse = bytearray(len(payload))
        loaded = bytearray(len(payload))
        for offset, length in ranges:
            sparse[offset:offset + length] = payload[offset:offset + length]
            loaded[offset:offset + length] = b'\1' * length
        return io.BytesIO(sparse), sum(loaded)

    @staticmethod
    def _fixture_bytes(path):
        hex_value = "".join(
            line for line in path.read_text().splitlines()
            if not line.startswith("#")
        )
        return bytes.fromhex(hex_value)


if __name__ == '__main__':
    unittest.main()
