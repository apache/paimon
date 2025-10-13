"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import tempfile
import unittest

from pypaimon.table.row.blob import Blob, BlobData, BlobRef
from pypaimon.table.row.generic_row import GenericRowDeserializer, GenericRowSerializer, GenericRow
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.row_kind import RowKind


class BlobTest(unittest.TestCase):
    """Tests for Blob interface following org.apache.paimon.data.BlobTest."""

    def setUp(self):
        """Set up test environment with temporary file."""
        # Create a temporary directory and file
        self.temp_dir = tempfile.mkdtemp()
        self.file = os.path.join(self.temp_dir, "test.txt")
        
        # Write test data to the file
        with open(self.file, 'wb') as f:
            f.write(b"test data")

    def tearDown(self):
        """Clean up temporary files."""
        try:
            if os.path.exists(self.file):
                os.remove(self.file)
            os.rmdir(self.temp_dir)
        except OSError:
            pass  # Ignore cleanup errors

    def test_from_data(self):
        """Test Blob.from_data() method."""
        test_data = b"test data"
        blob = Blob.from_data(test_data)
        
        # Verify it returns a BlobData instance
        self.assertIsInstance(blob, BlobData)
        
        # Verify the data matches
        self.assertEqual(blob.to_data(), test_data)

    def test_from_local(self):
        """Test Blob.from_local() method."""
        blob = Blob.from_local(self.file)
        
        # Verify it returns a BlobRef instance
        self.assertIsInstance(blob, BlobRef)
        
        # Verify the data matches
        self.assertEqual(blob.to_data(), b"test data")

    def test_from_file_with_offset_and_length(self):
        """Test Blob.from_file() method with offset and length."""
        blob = Blob.from_file(self.file, 0, 4)
        
        # Verify it returns a BlobRef instance
        self.assertIsInstance(blob, BlobRef)
        
        # Verify the data matches (first 4 bytes: "test")
        self.assertEqual(blob.to_data(), b"test")

    def test_from_file_full(self):
        """Test Blob.from_file() method without offset and length."""
        blob = Blob.from_file(self.file)
        
        # Verify it returns a BlobRef instance
        self.assertIsInstance(blob, BlobRef)
        
        # Verify the data matches
        self.assertEqual(blob.to_data(), b"test data")

    def test_from_http(self):
        """Test Blob.from_http() method."""
        uri = "http://example.com/file.txt"
        blob = Blob.from_http(uri)
        
        # Verify it returns a BlobRef instance
        self.assertIsInstance(blob, BlobRef)
        
        # Verify the descriptor has the correct URI
        descriptor = blob.to_descriptor()
        self.assertEqual(descriptor.uri, uri)
        self.assertEqual(descriptor.offset, 0)
        self.assertEqual(descriptor.length, -1)

    def test_blob_data_interface_compliance(self):
        """Test that BlobData properly implements Blob interface."""
        test_data = b"interface test data"
        blob_data = BlobData(test_data)
        
        # Test that it's a Blob
        self.assertIsInstance(blob_data, Blob)
        
        # Test interface methods
        self.assertEqual(blob_data.to_data(), test_data)
        
        # Test to_descriptor raises RuntimeError
        with self.assertRaises(RuntimeError) as context:
            blob_data.to_descriptor()
        self.assertIn("Blob data can not convert to descriptor", str(context.exception))
        
        # Test new_input_stream
        stream = blob_data.new_input_stream()
        self.assertEqual(stream.read(), test_data)
        stream.close()

    def test_blob_ref_interface_compliance(self):
        """Test that BlobRef properly implements Blob interface."""
        blob_ref = BlobRef.from_local_file(self.file)
        
        # Test that it's a Blob
        self.assertIsInstance(blob_ref, Blob)
        
        # Test interface methods
        self.assertEqual(blob_ref.to_data(), b"test data")
        
        # Test to_descriptor returns valid descriptor
        descriptor = blob_ref.to_descriptor()
        self.assertEqual(descriptor.uri, self.file)
        self.assertEqual(descriptor.offset, 0)
        self.assertEqual(descriptor.length, -1)
        
        # Test new_input_stream
        stream = blob_ref.new_input_stream()
        self.assertEqual(stream.read(), b"test data")
        stream.close()

    def test_blob_equality_and_hashing(self):
        """Test blob equality and hashing behavior."""
        # Test BlobData equality
        data1 = BlobData(b"same data")
        data2 = BlobData(b"same data")
        data3 = BlobData(b"different data")
        
        self.assertEqual(data1, data2)
        self.assertNotEqual(data1, data3)
        self.assertEqual(hash(data1), hash(data2))
        
        # Test BlobRef equality
        ref1 = BlobRef.from_local_file(self.file)
        ref2 = BlobRef.from_local_file(self.file)
        
        self.assertEqual(ref1, ref2)
        self.assertEqual(hash(ref1), hash(ref2))

    def test_blob_factory_methods_return_correct_types(self):
        """Test that all factory methods return the expected types."""
        # from_data should return BlobData
        blob_data = Blob.from_data(b"test")
        self.assertIsInstance(blob_data, BlobData)
        self.assertIsInstance(blob_data, Blob)
        
        # from_local should return BlobRef
        blob_ref = Blob.from_local(self.file)
        self.assertIsInstance(blob_ref, BlobRef)
        self.assertIsInstance(blob_ref, Blob)
        
        # from_file should return BlobRef
        blob_file = Blob.from_file(self.file)
        self.assertIsInstance(blob_file, BlobRef)
        self.assertIsInstance(blob_file, Blob)
        
        # from_http should return BlobRef
        blob_http = Blob.from_http("http://example.com/test.bin")
        self.assertIsInstance(blob_http, BlobRef)
        self.assertIsInstance(blob_http, Blob)

    def test_blob_data_convenience_methods(self):
        """Test BlobData convenience methods."""
        blob = BlobData(b"convenience test")
        
        # Test from_bytes class method
        blob2 = BlobData.from_bytes(b"from bytes")
        self.assertEqual(blob2.to_data(), b"from bytes")

    def test_generic_row_deserializer_parse_blob(self):
        """Test GenericRowDeserializer._parse_blob method."""
        # Create test data with BLOB field
        test_blob_data = b"Test BLOB data for parsing"
        blob_data = BlobData(test_blob_data)
        
        # Create fields with BLOB type
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "blob_field", AtomicType("BLOB")),
        ]
        
        # Create and serialize a row with blob data
        original_row = GenericRow([42, blob_data], fields, RowKind.INSERT)
        serialized_bytes = GenericRowSerializer.to_bytes(original_row)
        
        # Test the full deserialization process (which uses _parse_blob internally)
        deserialized_row = GenericRowDeserializer.from_bytes(serialized_bytes, fields)
        
        # Verify the deserialized blob
        deserialized_blob = deserialized_row.values[1]
        self.assertIsInstance(deserialized_blob, BlobData)
        self.assertEqual(deserialized_blob.to_data(), test_blob_data)
        
        # Test with empty blob data
        empty_blob = BlobData(b"")
        empty_row = GenericRow([1, empty_blob], fields, RowKind.INSERT)
        empty_serialized = GenericRowSerializer.to_bytes(empty_row)
        empty_deserialized = GenericRowDeserializer.from_bytes(empty_serialized, fields)
        
        empty_deserialized_blob = empty_deserialized.values[1]
        self.assertIsInstance(empty_deserialized_blob, BlobData)
        self.assertEqual(empty_deserialized_blob.to_data(), b"")
        
        # Test with binary data containing null bytes
        binary_blob_data = b"\x00\x01\x02\x03\xff\xfe\xfd"
        binary_blob = BlobData(binary_blob_data)
        binary_row = GenericRow([99, binary_blob], fields, RowKind.INSERT)
        binary_serialized = GenericRowSerializer.to_bytes(binary_row)
        binary_deserialized = GenericRowDeserializer.from_bytes(binary_serialized, fields)
        
        binary_deserialized_blob = binary_deserialized.values[1]
        self.assertIsInstance(binary_deserialized_blob, BlobData)
        self.assertEqual(binary_deserialized_blob.to_data(), binary_blob_data)

    def test_generic_row_deserializer_parse_blob_with_multiple_fields(self):
        """Test _parse_blob with multiple BLOB fields in a row."""
        # Create test data with multiple BLOB fields
        blob1_data = b"First BLOB data"
        blob2_data = b"Second BLOB with different content"
        blob3_data = b""  # Empty blob
        
        blob1 = BlobData(blob1_data)
        blob2 = BlobData(blob2_data)
        blob3 = BlobData(blob3_data)
        
        # Create fields with multiple BLOB types
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "blob1", AtomicType("BLOB")),
            DataField(3, "blob2", AtomicType("BLOB")),
            DataField(4, "blob3", AtomicType("BLOB")),
        ]
        
        # Create and serialize a row with multiple blobs
        original_row = GenericRow([123, "test_row", blob1, blob2, blob3], fields, RowKind.INSERT)
        serialized_bytes = GenericRowSerializer.to_bytes(original_row)
        
        # Deserialize and verify all blobs
        deserialized_row = GenericRowDeserializer.from_bytes(serialized_bytes, fields)
        
        # Verify each blob field
        self.assertEqual(deserialized_row.values[0], 123)
        self.assertEqual(deserialized_row.values[1], "test_row")
        
        deserialized_blob1 = deserialized_row.values[2]
        self.assertIsInstance(deserialized_blob1, BlobData)
        self.assertEqual(deserialized_blob1.to_data(), blob1_data)
        
        deserialized_blob2 = deserialized_row.values[3]
        self.assertIsInstance(deserialized_blob2, BlobData)
        self.assertEqual(deserialized_blob2.to_data(), blob2_data)
        
        deserialized_blob3 = deserialized_row.values[4]
        self.assertIsInstance(deserialized_blob3, BlobData)
        self.assertEqual(deserialized_blob3.to_data(), blob3_data)

    def test_generic_row_deserializer_parse_blob_with_null_values(self):
        """Test _parse_blob with null BLOB values."""
        # Create fields with BLOB type
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "blob_field", AtomicType("BLOB")),
            DataField(2, "name", AtomicType("STRING")),
        ]
        
        # Create row with null blob (None value)
        original_row = GenericRow([456, None, "test_with_null"], fields, RowKind.INSERT)
        serialized_bytes = GenericRowSerializer.to_bytes(original_row)
        
        # Deserialize and verify null blob is handled correctly
        deserialized_row = GenericRowDeserializer.from_bytes(serialized_bytes, fields)
        
        self.assertEqual(deserialized_row.values[0], 456)
        self.assertIsNone(deserialized_row.values[1])  # Null blob should remain None
        self.assertEqual(deserialized_row.values[2], "test_with_null")

    def test_generic_row_deserializer_parse_blob_large_data(self):
        """Test _parse_blob with large BLOB data."""
        # Create large blob data (1MB)
        large_blob_data = b"X" * (1024 * 1024)  # 1MB of 'X' characters
        large_blob = BlobData(large_blob_data)
        
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "large_blob", AtomicType("BLOB")),
        ]
        
        # Create and serialize row with large blob
        original_row = GenericRow([789, large_blob], fields, RowKind.INSERT)
        serialized_bytes = GenericRowSerializer.to_bytes(original_row)
        
        # Deserialize and verify large blob
        deserialized_row = GenericRowDeserializer.from_bytes(serialized_bytes, fields)
        
        deserialized_large_blob = deserialized_row.values[1]
        self.assertIsInstance(deserialized_large_blob, BlobData)
        self.assertEqual(len(deserialized_large_blob.to_data()), 1024 * 1024)
        self.assertEqual(deserialized_large_blob.to_data(), large_blob_data)


if __name__ == '__main__':
    unittest.main()
