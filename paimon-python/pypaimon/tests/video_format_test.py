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
import tempfile
import unittest
from pathlib import Path

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_blob_reader import FormatBlobReader, VideoFileMeta
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import (
    Blob,
    BlobData,
    BlobDescriptor,
    VideoFrameDescriptor,
)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.video_format_writer import VideoFormatWriter


class VideoFormatTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.file_io = LocalFileIO(str(self.root), Options({}))
        self.field = DataField(0, "video", AtomicType("BLOB"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_descriptor_round_trip_preserves_payload_and_frame(self):
        descriptor = VideoFrameDescriptor("s3://bucket/a.video", 7, 99, 42)
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
            VideoFrameDescriptor("x", 0, 1, -1)

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
        self.assertEqual((0, len(first_bytes), 0), meta.frame(0))
        self.assertEqual((0, len(first_bytes), 1), meta.frame(1))
        self.assertEqual(
            (len(first_bytes), len(second_bytes), 7), meta.frame(2)
        )
        self.assertEqual((0, len(first_bytes), 4), meta.frame(3))
        self.assertIsNone(meta.frame(4))
        self.assertIs(Blob.PLACE_HOLDER, meta.frame(5))

        values = self._read(target, row_indices=range(5))
        frames = [VideoFrameDescriptor.deserialize(value) for value in values[:4]]
        self.assertEqual([0, 1, 7, 4], [frame.frame_index for frame in frames])
        self.assertEqual(frames[0].payload_descriptor, frames[1].payload_descriptor)
        self.assertEqual(frames[0].payload_descriptor, frames[3].payload_descriptor)
        self.assertNotEqual(frames[0].payload_descriptor, frames[2].payload_descriptor)
        self.assertIsNone(values[4])

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
            payload.uri, payload.offset, payload.length, frame_index
        )
        return Blob.from_descriptor(
            self.file_io.uri_reader_factory.create(descriptor.uri), descriptor
        )


if __name__ == '__main__':
    unittest.main()
