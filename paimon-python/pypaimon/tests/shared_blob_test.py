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
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import Blob, BlobData, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.blob_format_writer import BlobFormatWriter
from pypaimon.write.shared_blob_format_writer import SharedBlobFormatWriter


class SharedBlobFormatTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.file_io = LocalFileIO(str(self.root), Options({}))
        self.field = DataField(0, "video", AtomicType("BLOB"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_duplicate_descriptors_share_physical_payload(self):
        first = self._source_blob("first.mp4", b"first-video")
        second = self._source_blob("second.mp4", b"second-video")
        target = (self.root / "data.shared-blob").as_uri()

        writer = SharedBlobFormatWriter(
            self.file_io.new_output_stream(target), file_path=target
        )
        for value in (first, first, second, first, None):
            writer.add_element(GenericRow([value], [self.field], RowKind.INSERT))
        self.assertEqual(2, writer.physical_blob_count)
        writer.close()
        with self.file_io.new_input_stream(target) as stream:
            self.assertEqual(
                BlobFormatWriter.MAGIC_NUMBER,
                struct.unpack('<I', stream.read(4))[0],
            )

        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=target,
            read_fields=["video"],
            full_fields=[self.field],
            push_down_predicate=None,
            blob_as_descriptor=True,
        )
        try:
            batch = reader.read_arrow_batch()
            values = batch.column(0).to_pylist()
        finally:
            reader.close()

        self.assertEqual(5, len(values))
        first_descriptor = BlobDescriptor.deserialize(values[0])
        self.assertEqual(first_descriptor, BlobDescriptor.deserialize(values[1]))
        self.assertEqual(first_descriptor, BlobDescriptor.deserialize(values[3]))
        self.assertNotEqual(first_descriptor, BlobDescriptor.deserialize(values[2]))
        self.assertIsNone(values[4])

        inline_reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=target,
            read_fields=["video"],
            full_fields=[self.field],
            push_down_predicate=None,
            blob_as_descriptor=False,
        )
        try:
            inline_values = inline_reader.read_arrow_batch().column(0).to_pylist()
        finally:
            inline_reader.close()
        self.assertEqual(
            [b"first-video", b"first-video", b"second-video", b"first-video", None],
            inline_values,
        )

    def test_rejects_inline_blob_data(self):
        target = (self.root / "reject.shared-blob").as_uri()
        writer = SharedBlobFormatWriter(self.file_io.new_output_stream(target))
        with self.assertRaisesRegex(ValueError, "exact BlobRef"):
            writer.add_element(
                GenericRow([BlobData(b"inline")], [self.field], RowKind.INSERT)
            )
        writer.close()

    def test_rejects_out_of_range_row_reference(self):
        target_path = self.root / "corrupt.shared-blob"
        physical_index = DeltaVarintCompressor.compress([])
        row_index = DeltaVarintCompressor.compress([0])
        target_path.write_bytes(
            physical_index
            + row_index
            + struct.pack(
                '<IIIB',
                len(physical_index),
                len(row_index),
                SharedBlobFormatWriter.FOOTER_MAGIC_NUMBER,
                SharedBlobFormatWriter.VERSION,
            )
        )

        with self.assertRaisesRegex(IOError, "physical blob count is 0"):
            FormatBlobReader(
                file_io=self.file_io,
                file_path=target_path.as_uri(),
                read_fields=["video"],
                full_fields=[self.field],
                push_down_predicate=None,
                blob_as_descriptor=True,
            )

    def _source_blob(self, name, data):
        source = self.root / name
        source.write_bytes(data)
        return Blob.from_file(self.file_io, source.as_uri(), 0, len(data))


if __name__ == '__main__':
    unittest.main()
