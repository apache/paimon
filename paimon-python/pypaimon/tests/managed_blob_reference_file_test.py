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

from pypaimon.blob.managed_blob_reference_file import (
    ManagedBlobReferenceFile,
    Reference,
    _read_modified_utf,
    _write_modified_utf,
)
from pypaimon.blob.managed_blob_reference_collector import (
    ManagedBlobReferenceCollector,
)
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.options import Options


class ManagedBlobReferenceFileTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, Options({}))

    def test_round_trip_and_deduplicate_references(self):
        path = f"{self.temp_dir}/data.avro.blobref"
        first = Reference(f"{self.temp_dir}/bucket-0", "data-a.managed.blob")
        second = Reference(f"{self.temp_dir}/bucket-0", "data-b.managed.blob")

        ManagedBlobReferenceFile.write(self.file_io, path, [second, first, second])
        self.assertEqual(
            ManagedBlobReferenceFile.read(self.file_io, path),
            [first, second],
        )

        empty_path = f"{self.temp_dir}/empty.avro.blobref"
        ManagedBlobReferenceFile.write(self.file_io, empty_path, [])
        self.assertEqual(ManagedBlobReferenceFile.read(self.file_io, empty_path), [])

    def test_classify_managed_blob_path(self):
        managed = f"{self.temp_dir}/bucket-0/data-b.managed.blob"
        ordinary = f"{self.temp_dir}/bucket-0/data-d.blob"
        reference = ManagedBlobReferenceFile.from_descriptor_uri(managed)
        self.assertEqual(
            reference,
            Reference(f"{self.temp_dir}/bucket-0", "data-b.managed.blob"),
        )
        self.assertIsNone(ManagedBlobReferenceFile.from_descriptor_uri(ordinary))
        self.assertEqual(
            ManagedBlobReferenceFile.sidecar_name("data-a.avro"),
            "data-a.avro.blobref",
        )

    def test_reference_collector_close_is_idempotent(self):
        collector = ManagedBlobReferenceCollector(
            self.file_io,
            f"{self.temp_dir}/data.avro",
            [],
            set(),
        )

        first = collector.close()
        second = collector.close()

        self.assertEqual(first, "data.avro.blobref")
        self.assertEqual(second, first)

    def test_modified_utf8_round_trip(self):
        payload = io.BytesIO()
        value = "storage/root/测试"
        _write_modified_utf(payload, value)
        decoded, offset = _read_modified_utf(payload.getvalue(), 0)
        self.assertEqual(decoded, value)
        self.assertEqual(offset, len(payload.getvalue()))

    def test_reference_file_matches_fixed_binary_fixture(self):
        # Fixed bytes guard the on-disk layout independently of this module's
        # writer/reader round trip. The fixture covers big-endian framing,
        # modified UTF-8, and the CRC payload boundary.
        fixture = bytes.fromhex(
            "50424c520100000001001866696c653a2f2f2f77617265686f7573652f"
            "eda0bdedb8800013646174612d612e6d616e616765642e626c6f62f2cb93c4"
        )
        expected = [Reference("file:///warehouse/😀", "data-a.managed.blob")]
        fixture_path = f"{self.temp_dir}/fixture.blobref"
        with self.file_io.new_output_stream(fixture_path) as out:
            out.write(fixture)

        self.assertEqual(ManagedBlobReferenceFile.read(
            self.file_io, fixture_path), expected)

        written_path = f"{self.temp_dir}/written.blobref"
        ManagedBlobReferenceFile.write(self.file_io, written_path, expected)
        with self.file_io.new_input_stream(written_path) as stream:
            self.assertEqual(stream.read(), fixture)

    def test_reject_unsupported_version(self):
        path = f"{self.temp_dir}/unsupported.avro.blobref"
        with self.file_io.new_output_stream(path) as out:
            out.write(struct.pack(">i", ManagedBlobReferenceFile.MAGIC))
            out.write(struct.pack(">B", 99))
            out.write(struct.pack(">i", 0))
            out.write(struct.pack(">i", 0))
        with self.assertRaisesRegex(IOError, "Unsupported managed BLOB reference file version"):
            ManagedBlobReferenceFile.read(self.file_io, path)

    def test_reject_corrupt_checksum(self):
        path = f"{self.temp_dir}/corrupt.avro.blobref"
        payload = io.BytesIO()
        payload.write(struct.pack(">B", ManagedBlobReferenceFile.VERSION))
        payload.write(struct.pack(">i", 0))
        payload_bytes = payload.getvalue()
        with self.file_io.new_output_stream(path) as out:
            out.write(struct.pack(">i", ManagedBlobReferenceFile.MAGIC))
            out.write(payload_bytes)
            out.write(struct.pack(">i", 12345))
        with self.assertRaisesRegex(IOError, "Invalid managed BLOB reference file checksum"):
            ManagedBlobReferenceFile.read(self.file_io, path)


if __name__ == "__main__":
    unittest.main()
