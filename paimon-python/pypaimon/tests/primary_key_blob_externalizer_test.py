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

import shutil
import struct
import tempfile
import unittest

import pyarrow as pa

from pypaimon.blob.primary_key_blob_externalizer import PrimaryKeyBlobExternalizer
from pypaimon.common.file_io import FileIO
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType
from pypaimon.table.row.blob import Blob, BlobDescriptor


class PrimaryKeyBlobExternalizerTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir, True)
        self.file_io = FileIO.get(self.temp_dir, Options({}))

    def _externalizer(self, field, pack_name, descriptor_reader_factory=None):
        return PrimaryKeyBlobExternalizer(
            self.file_io,
            [field],
            {field.name},
            lambda: "%s/%s.managed.blob" % (self.temp_dir, pack_name),
            1024 * 1024,
            4096,
            "data-",
            descriptor_reader_factory=descriptor_reader_factory,
        )

    def test_map_blob_preserves_integer_key_type_and_value_field(self):
        value_field = pa.field(
            "value",
            pa.large_binary(),
            nullable=False,
        )
        map_type = pa.map_(
            pa.field("key", pa.int32(), nullable=False),
            value_field,
        )
        batch_field = pa.field(
            "payload",
            map_type,
            metadata={b"field-meta": b"preserved"},
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array([[(7, b"payload")]], type=map_type)],
            schema=pa.schema([batch_field]),
        )
        externalizer = self._externalizer(
            DataField(
                0,
                "payload",
                MapType(
                    True,
                    AtomicType("INT", False),
                    AtomicType("BLOB", False),
                ),
            ),
            "map",
        )

        try:
            result = externalizer.externalize_record_batch(batch)
            result_field = result.schema.field("payload")
            self.assertEqual(result_field.type.key_type, pa.int32())
            self.assertFalse(result_field.type.item_field.nullable)
            self.assertEqual(result_field.metadata, batch_field.metadata)
            self.assertEqual(result.column("payload").to_pylist()[0][0][0], 7)
        finally:
            externalizer.abort()

    def test_array_blob_empty_then_nonempty_batches_keep_schema(self):
        item_field = pa.field(
            "item",
            pa.large_binary(),
            nullable=False,
            metadata={b"item-meta": b"preserved"},
        )
        array_type = pa.list_(item_field)
        batch_field = pa.field(
            "payload",
            array_type,
            metadata={b"field-meta": b"preserved"},
        )
        schema = pa.schema([batch_field])
        empty_batch = pa.RecordBatch.from_arrays(
            [pa.array([[]], type=array_type)],
            schema=schema,
        )
        nonempty_batch = pa.RecordBatch.from_arrays(
            [pa.array([[b"payload"]], type=array_type)],
            schema=schema,
        )
        externalizer = self._externalizer(
            DataField(
                0,
                "payload",
                ArrayType(True, AtomicType("BLOB", False)),
            ),
            "array",
        )

        try:
            empty_result = externalizer.externalize_record_batch(empty_batch)
            nonempty_result = externalizer.externalize_record_batch(nonempty_batch)
            combined = pa.concat_tables([
                pa.Table.from_batches([empty_result]),
                pa.Table.from_batches([nonempty_result]),
            ])
            result_field = nonempty_result.schema.field("payload")
            self.assertEqual(result_field.type, array_type)
            self.assertFalse(result_field.type.value_field.nullable)
            self.assertEqual(
                result_field.type.value_field.metadata,
                item_field.metadata,
            )
            self.assertEqual(result_field.metadata, batch_field.metadata)
            self.assertEqual(combined.num_rows, 2)
        finally:
            externalizer.abort()

    def test_v1_descriptor_bytes_with_force_descriptor_contract(self):
        payload = b"legacy-v1-payload"
        pack_bytes = b"prefix" + payload + b"suffix"
        source_path = "%s/source-v1.managed.blob" % self.temp_dir
        with self.file_io.new_output_stream(source_path) as out:
            out.write(pack_bytes)
        uri_bytes = source_path.encode("utf-8")
        v1_descriptor = (
            b"\x01"
            + struct.pack("<I", len(uri_bytes))
            + uri_bytes
            + struct.pack("<q", len(b"prefix"))
            + struct.pack("<q", len(payload))
        )
        descriptor_factory = self.file_io.uri_reader_factory
        descriptor_factory.force_descriptor_bytes = True
        batch = pa.RecordBatch.from_arrays(
            [pa.array([v1_descriptor], type=pa.large_binary())],
            names=["payload"],
        )
        externalizer = self._externalizer(
            DataField(0, "payload", AtomicType("BLOB")),
            "v1-force-descriptor",
            descriptor_reader_factory=descriptor_factory,
        )
        try:
            result = externalizer.externalize_record_batch(batch)
            externalizer.prepare_commit()
            copied = Blob.from_descriptor_bytes(
                result.column("payload")[0].as_py(), self.file_io).to_data()
            self.assertEqual(payload, copied)
        finally:
            externalizer.abort()

    def test_inline_payload_bytes_are_not_treated_as_v1_descriptor(self):
        from pypaimon.blob.primary_key_blob_externalizer import _to_blob_value
        from pypaimon.table.row.blob import BlobData

        # Exact-length v1-shaped collision: heuristic may accept it, but the
        # default write path must still treat it as inline payload.
        collision_payload = (
            b"\x01"
            + struct.pack("<I", 0)
            + struct.pack("<q", 0)
            + struct.pack("<q", 0)
        )
        self.assertEqual(len(collision_payload), 21)
        self.assertIsNotNone(BlobDescriptor.parse_if_serialized(collision_payload))
        blob_value = _to_blob_value(collision_payload, self.file_io)
        self.assertIsInstance(blob_value, BlobData)
        self.assertEqual(blob_value.to_data(), collision_payload)

        # Near-miss v1 shape: enters heuristic checks but fails exact length.
        uri_body = b"inline-payload-body"
        inline_payload = (
            b"\x01"
            + struct.pack("<I", len(uri_body))
            + uri_body
            + struct.pack("<q", 0)
            + struct.pack("<q", len(uri_body))
            + b"\xff"
        )
        self.assertGreaterEqual(len(inline_payload), 21)
        self.assertIsNone(BlobDescriptor.parse_if_serialized(inline_payload))
        blob_value = _to_blob_value(inline_payload, self.file_io)
        self.assertIsInstance(blob_value, BlobData)
        self.assertEqual(blob_value.to_data(), inline_payload)

        batch = pa.RecordBatch.from_arrays(
            [pa.array([collision_payload], type=pa.large_binary())],
            names=["payload"],
        )
        externalizer = self._externalizer(
            DataField(0, "payload", AtomicType("BLOB")),
            "inline",
        )
        try:
            result = externalizer.externalize_record_batch(batch)
            externalizer.prepare_commit()
            copied = Blob.from_descriptor_bytes(
                result.column("payload")[0].as_py(), self.file_io).to_data()
            self.assertEqual(collision_payload, copied)
        finally:
            externalizer.abort()


if __name__ == "__main__":
    unittest.main()
