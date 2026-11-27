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
import zlib

import pyarrow as pa

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
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.table.row.row_kind import RowKind


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

    def test_reference_is_hashable(self):
        reference = Reference(f"{self.temp_dir}/bucket-0", "data-a.managed.blob")
        self.assertIn(reference, {reference})

    def test_close_after_abort_raises(self):
        collector = ManagedBlobReferenceCollector(
            self.file_io,
            f"{self.temp_dir}/data.avro",
            [],
            set(),
        )
        collector.abort()
        with self.assertRaisesRegex(RuntimeError, "aborted"):
            collector.close()

    def test_reject_truncated_reference_file(self):
        path = f"{self.temp_dir}/short.avro.blobref"
        with self.file_io.new_output_stream(path) as out:
            out.write(struct.pack(">i", ManagedBlobReferenceFile.MAGIC))
            out.write(struct.pack(">i", 0))
        with self.assertRaisesRegex(IOError, "too short"):
            ManagedBlobReferenceFile.read(self.file_io, path)

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

    def test_reject_crc_valid_malformed_modified_utf8(self):
        # Java DataInputStream.readUTF rejects b"\xc0A": the second byte is not
        # a 10xxxxxx continuation. The CRC covers that payload, so a loose
        # decoder would accept it as U+0001 instead of failing the checksum.
        root = b"file:///warehouse"
        payload = bytearray()
        payload.append(ManagedBlobReferenceFile.VERSION)
        payload.extend(struct.pack(">i", 1))
        payload.extend(struct.pack(">H", len(root)))
        payload.extend(root)
        payload.extend(struct.pack(">H", 2))
        payload.extend(b"\xc0A")
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        if checksum >= 0x80000000:
            checksum -= 0x100000000
        path = f"{self.temp_dir}/malformed.avro.blobref"
        with self.file_io.new_output_stream(path) as out:
            out.write(struct.pack(">i", ManagedBlobReferenceFile.MAGIC))
            out.write(payload)
            out.write(struct.pack(">i", checksum))
        with self.assertRaisesRegex(IOError, "Invalid modified UTF-8"):
            ManagedBlobReferenceFile.read(self.file_io, path)

    def test_collect_table_keeps_add_rows_and_dedupes_packs(self):
        blob = AtomicType("BLOB")
        fields = [
            DataField(0, "scalar_blob", blob),
            DataField(1, "array_blob", ArrayType(True, blob)),
            DataField(2, "map_blob", MapType(True, AtomicType("STRING"), blob)),
        ]
        data_path = f"{self.temp_dir}/data.avro"
        collector = ManagedBlobReferenceCollector(
            self.file_io, data_path, fields,
            {"scalar_blob", "array_blob", "map_blob"},
        )

        def descriptor(name):
            return BlobDescriptor(
                "%s/%s.managed.blob" % (self.temp_dir, name), 0, 4).serialize()

        ordinary = BlobDescriptor(
            "%s/plain.blob" % self.temp_dir, 0, 4).serialize()
        table = pa.table({
            "scalar_blob": [
                descriptor("a"),
                descriptor("a"),
                descriptor("retract-scalar"),
                descriptor("b"),
                descriptor("deleted"),
                ordinary,
            ],
            "array_blob": [
                [descriptor("arr"), None],
                [descriptor("arr")],
                [descriptor("retract-arr")],
                [descriptor("arr2")],
                [descriptor("deleted-arr")],
                None,
            ],
            "map_blob": [
                {"k": descriptor("map")},
                {"k": descriptor("map"), "again": descriptor("a")},
                {"k": descriptor("retract-map")},
                {"k2": descriptor("map2"), "empty": None},
                {"k": descriptor("deleted-map")},
                {"k": ordinary},
            ],
            "_VALUE_KIND": [
                RowKind.INSERT.value,
                RowKind.UPDATE_AFTER.value,
                RowKind.UPDATE_BEFORE.value,
                RowKind.INSERT.value,
                RowKind.DELETE.value,
                RowKind.INSERT.value,
            ],
        })

        collector.collect_table(table)
        self.assertEqual(collector.close(), "data.avro.blobref")
        self.assertEqual(
            ManagedBlobReferenceFile.read(
                self.file_io, data_path + ".blobref"),
            [
                Reference(self.temp_dir, "a.managed.blob"),
                Reference(self.temp_dir, "arr.managed.blob"),
                Reference(self.temp_dir, "arr2.managed.blob"),
                Reference(self.temp_dir, "b.managed.blob"),
                Reference(self.temp_dir, "map.managed.blob"),
                Reference(self.temp_dir, "map2.managed.blob"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
