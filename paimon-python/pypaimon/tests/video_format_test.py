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
import tempfile
import unittest
from pathlib import Path

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.options import Options
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


class VideoFormatTest(unittest.TestCase):

    REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
    DESCRIPTOR_V1_FIXTURE = (
        REPOSITORY_ROOT
        / "paimon-common/src/test/resources/org/apache/paimon/data/"
        "video-frame-descriptor-v1.hex"
    )
    DESCRIPTOR_V2_FIXTURE = (
        REPOSITORY_ROOT
        / "paimon-common/src/test/resources/org/apache/paimon/data/"
        "video-frame-descriptor-v2.hex"
    )
    VIDEO_FIXTURE = (
        REPOSITORY_ROOT
        / "paimon-format/src/test/resources/org/apache/paimon/format/blob/"
        "video-v1.hex"
    )
    VIDEO_V2_FIXTURE = (
        REPOSITORY_ROOT
        / "paimon-format/src/test/resources/org/apache/paimon/format/blob/"
        "video-v2.hex"
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

    def test_cross_language_descriptor_fixtures(self):
        v1 = self._fixture_bytes(self.DESCRIPTOR_V1_FIXTURE)
        unindexed = VideoFrameDescriptor(
            "s3://bucket/视频.mp4", 7, 99, 42, -1, 0)
        restored = BlobDescriptor.deserialize(v1)
        self.assertEqual(unindexed, restored)
        self.assertEqual(v1, restored.serialize())

        v2 = self._fixture_bytes(self.DESCRIPTOR_V2_FIXTURE)
        indexed = VideoFrameDescriptor(
            "s3://bucket/视频.mp4", 7, 99, 42, 106, 8)
        self.assertEqual(v2, indexed.serialize())
        self.assertEqual(indexed, BlobDescriptor.deserialize(v2))

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
        fixture_path = self.root / "fixture.video"
        fixture_path.write_bytes(fixture)
        target = fixture_path.as_uri()

        with self.file_io.new_input_stream(target) as stream:
            meta = VideoFileMeta(stream, len(fixture))
        self.assertEqual(7, meta.record_count)
        self.assertEqual((0, 3, 2, -1, 0), meta.frame(0))
        self.assertEqual((0, 3, 3, -1, 0), meta.frame(1))
        self.assertIsNone(meta.frame(2))
        self.assertIs(Blob.PLACE_HOLDER, meta.frame(3))
        self.assertEqual((3, 4, 7, -1, 0), meta.frame(4))
        self.assertEqual((3, 4, 8, -1, 0), meta.frame(5))
        self.assertEqual((0, 3, 10, -1, 0), meta.frame(6))

        written_target = (self.root / "written.video").as_uri()
        writer = VideoFormatWriter(
            self.file_io.new_output_stream(written_target),
            file_path=written_target,
        )
        values = (
            self._source_frame("a.mp4", b"abc", 2),
            self._source_frame("a.mp4", b"abc", 3),
            None,
            Blob.PLACE_HOLDER,
            self._source_frame("b.mp4", b"WXYZ", 7),
            self._source_frame("b.mp4", b"WXYZ", 8),
            self._source_frame("a.mp4", b"abc", 10),
        )
        for value in values:
            writer.add_element(GenericRow([value], [self.field], RowKind.INSERT))
        writer.close()
        self.assertEqual(fixture, (self.root / "written.video").read_bytes())

    def test_cross_language_video_v2_fixture(self):
        fixture = self._fixture_bytes(self.VIDEO_V2_FIXTURE)
        encoded = VideoKeyframeIndex([(0, 0), (12, 36000)]).serialize()
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

    def test_target_size_counts_buffered_keyframe_index(self):
        mapping = VideoKeyframeIndex([(0, 0)]).serialize()
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

    def test_rejects_inconsistent_keyframe_indexes_for_same_payload(self):
        video = b"video"
        first_index = VideoKeyframeIndex([
            (0, 0), (12, 36000)
        ]).serialize()
        second_index = VideoKeyframeIndex([
            (0, 0), (6, 18000)
        ]).serialize()
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
            DeltaVarintCompressor.compress([1]),
            DeltaVarintCompressor.compress([0]),
            DeltaVarintCompressor.compress([0]),
        ]
        target_path.write_bytes(
            b"".join(indexes)
            + struct.pack(
                '<IIIIIB',
                *(len(index) for index in indexes),
                VideoFormatWriter.FOOTER_MAGIC_NUMBER,
                VideoFormatWriter.V1_VERSION,
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

    @staticmethod
    def _fixture_bytes(path):
        hex_value = "".join(
            line for line in path.read_text().splitlines()
            if not line.startswith("#")
        )
        return bytes.fromhex(hex_value)


if __name__ == '__main__':
    unittest.main()
