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
import shutil
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa

from pypaimon import CatalogFactory
from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.schema.data_types import (
    AtomicType, DataField, DataTypeParser, PyarrowFieldParser, Keyword
)
from pypaimon.table.row.blob import Blob, BlobData, BlobRef, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRowDeserializer, GenericRowSerializer, GenericRow
from pypaimon.table.row.row_kind import RowKind


class MockFileIO:
    """Mock FileIO for testing."""

    def __init__(self, file_io: FileIO):
        self._file_io = file_io

    def get_file_size(self, path: str) -> int:
        """Get file size."""
        return self._file_io.get_file_size(Path(path))


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
        blob_ref = Blob.from_local(self.file)

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
        ref1 = Blob.from_local(self.file)
        ref2 = Blob.from_local(self.file)

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

    def test_blob_descriptor_creation(self):
        """Test BlobDescriptor creation and properties."""
        # Test basic creation
        descriptor = BlobDescriptor("test://example.uri", 100, 200)

        self.assertEqual(descriptor.uri, "test://example.uri")
        self.assertEqual(descriptor.offset, 100)
        self.assertEqual(descriptor.length, 200)
        self.assertEqual(descriptor.version, BlobDescriptor.CURRENT_VERSION)

    def test_blob_descriptor_creation_with_version(self):
        """Test BlobDescriptor creation with explicit version."""
        descriptor = BlobDescriptor("test://example.uri", 50, 150, version=2)

        self.assertEqual(descriptor.uri, "test://example.uri")
        self.assertEqual(descriptor.offset, 50)
        self.assertEqual(descriptor.length, 150)
        self.assertEqual(descriptor.version, 2)

    def test_blob_descriptor_serialization_deserialization(self):
        """Test BlobDescriptor serialization and deserialization."""
        # Test with various URIs and parameters
        test_cases = [
            ("file:///path/to/file.bin", 0, -1),
            ("https://example.com/data.blob", 1024, 2048),
            ("s3://bucket/key", 0, 1000000),
            ("test://simple", 42, 84),
        ]

        for uri, offset, length in test_cases:
            with self.subTest(uri=uri, offset=offset, length=length):
                # Create original descriptor
                original = BlobDescriptor(uri, offset, length)

                # Serialize
                serialized = original.serialize()
                self.assertIsInstance(serialized, bytes)
                self.assertGreater(len(serialized), 0)

                # Deserialize
                deserialized = BlobDescriptor.deserialize(serialized)

                # Verify equality
                self.assertEqual(deserialized, original)
                self.assertEqual(deserialized.uri, uri)
                self.assertEqual(deserialized.offset, offset)
                self.assertEqual(deserialized.length, length)
                self.assertEqual(deserialized.version, BlobDescriptor.CURRENT_VERSION)

    def test_blob_descriptor_serialization_with_unicode(self):
        """Test BlobDescriptor serialization with Unicode characters."""
        # Test with Unicode characters in URI
        unicode_uri = "file:///测试/文件.bin"
        descriptor = BlobDescriptor(unicode_uri, 0, 100)

        # Serialize and deserialize
        serialized = descriptor.serialize()
        deserialized = BlobDescriptor.deserialize(serialized)

        # Verify Unicode is preserved
        self.assertEqual(deserialized.uri, unicode_uri)
        self.assertEqual(deserialized, descriptor)

    def test_blob_descriptor_deserialization_invalid_data(self):
        """Test BlobDescriptor deserialization with invalid data."""
        # Test with too short data
        with self.assertRaises(ValueError) as context:
            BlobDescriptor.deserialize(b"sho")  # Only 3 bytes, need at least 5
        self.assertIn("too short", str(context.exception))

        # Test with invalid version (version 0)
        # Create valid data but with wrong version
        valid_descriptor = BlobDescriptor("test://uri", 0, 100)
        valid_data = bytearray(valid_descriptor.serialize())
        valid_data[0] = 0  # Set invalid version (0)

        with self.assertRaises(ValueError) as context:
            BlobDescriptor.deserialize(bytes(valid_data))
        self.assertIn("Unsupported BlobDescriptor version", str(context.exception))

        # Test with incomplete data (missing URI bytes)
        incomplete_data = b'\x01\x00\x00\x00\x10'  # Version 1, URI length 16, but no URI bytes
        with self.assertRaises(ValueError) as context:
            BlobDescriptor.deserialize(incomplete_data)
        self.assertIn("URI length exceeds data size", str(context.exception))

    def test_blob_descriptor_equality_and_hashing(self):
        """Test BlobDescriptor equality and hashing."""
        # Create identical descriptors
        desc1 = BlobDescriptor("test://uri", 100, 200)
        desc2 = BlobDescriptor("test://uri", 100, 200)
        desc3 = BlobDescriptor("test://uri", 100, 201)  # Different length
        desc4 = BlobDescriptor("test://other", 100, 200)  # Different URI

        # Test equality
        self.assertEqual(desc1, desc2)
        self.assertNotEqual(desc1, desc3)
        self.assertNotEqual(desc1, desc4)
        self.assertNotEqual(desc1, None)
        self.assertNotEqual(desc1, "not a descriptor")

        # Test hashing
        self.assertEqual(hash(desc1), hash(desc2))
        # Hash should be different for different descriptors (though not guaranteed)
        self.assertNotEqual(hash(desc1), hash(desc3))
        self.assertNotEqual(hash(desc1), hash(desc4))

    def test_blob_descriptor_string_representation(self):
        """Test BlobDescriptor string representation."""
        descriptor = BlobDescriptor("test://example.uri", 42, 84)

        str_repr = str(descriptor)
        self.assertIn("test://example.uri", str_repr)
        self.assertIn("42", str_repr)
        self.assertIn("84", str_repr)
        self.assertIn("BlobDescriptor", str_repr)

        # __repr__ should be the same as __str__
        self.assertEqual(str_repr, repr(descriptor))

    def test_blob_descriptor_version_handling(self):
        """Test BlobDescriptor version handling."""
        # Test current version
        descriptor = BlobDescriptor("test://uri", 0, 100)
        self.assertEqual(descriptor.version, BlobDescriptor.CURRENT_VERSION)

        # Test explicit version
        descriptor_v2 = BlobDescriptor("test://uri", 0, 100, version=2)
        self.assertEqual(descriptor_v2.version, 2)

        # Serialize and deserialize should preserve version
        serialized = descriptor_v2.serialize()
        deserialized = BlobDescriptor.deserialize(serialized)
        self.assertEqual(deserialized.version, 2)

    def test_blob_descriptor_edge_cases(self):
        """Test BlobDescriptor with edge cases."""
        # Test with empty URI
        empty_uri_desc = BlobDescriptor("", 0, 0)
        serialized = empty_uri_desc.serialize()
        deserialized = BlobDescriptor.deserialize(serialized)
        self.assertEqual(deserialized.uri, "")

        # Test with very long URI
        long_uri = "file://" + "a" * 1000 + "/file.bin"
        long_uri_desc = BlobDescriptor(long_uri, 0, 1000000)
        serialized = long_uri_desc.serialize()
        deserialized = BlobDescriptor.deserialize(serialized)
        self.assertEqual(deserialized.uri, long_uri)

        # Test with negative values
        negative_desc = BlobDescriptor("test://uri", -1, -1)
        serialized = negative_desc.serialize()
        deserialized = BlobDescriptor.deserialize(serialized)
        self.assertEqual(deserialized.offset, -1)
        self.assertEqual(deserialized.length, -1)

    def test_blob_descriptor_with_blob_ref(self):
        """Test BlobDescriptor integration with BlobRef."""
        # Create a descriptor
        descriptor = BlobDescriptor(self.file, 0, -1)

        # Create BlobRef from descriptor
        blob_ref = BlobRef(descriptor)

        # Verify descriptor is preserved
        returned_descriptor = blob_ref.to_descriptor()
        self.assertEqual(returned_descriptor, descriptor)

        # Verify data can be read through BlobRef
        data = blob_ref.to_data()
        self.assertEqual(data, b"test data")

    def test_blob_descriptor_serialization_format(self):
        """Test BlobDescriptor serialization format details."""
        descriptor = BlobDescriptor("test", 12345, 67890)
        serialized = descriptor.serialize()

        # Check that serialized data starts with version byte
        self.assertEqual(serialized[0], BlobDescriptor.CURRENT_VERSION)

        # Check minimum length (version + uri_length + uri + offset + length)
        # 1 + 4 + len("test") + 8 + 8 = 25 bytes
        self.assertEqual(len(serialized), 25)

        # Verify round-trip consistency
        deserialized = BlobDescriptor.deserialize(serialized)
        re_serialized = deserialized.serialize()
        self.assertEqual(serialized, re_serialized)


class DataTypesTest(unittest.TestCase):
    """Tests for BLOB data type support."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_blob_keyword_enum(self):
        """Test that BLOB is properly defined in the Keyword enum"""
        self.assertEqual(Keyword.BLOB.value, "BLOB")
        self.assertIn(Keyword.BLOB, list(Keyword))

    def test_blob_atomic_type_creation(self):
        """Test creating BLOB AtomicType"""
        blob_type = AtomicType("BLOB", nullable=True)
        self.assertEqual(blob_type.type, "BLOB")
        self.assertTrue(blob_type.nullable)
        self.assertEqual(str(blob_type), "BLOB")

        blob_type_not_null = AtomicType("BLOB", nullable=False)
        self.assertEqual(str(blob_type_not_null), "BLOB NOT NULL")

    def test_blob_data_type_parser(self):
        """Test parsing BLOB type from string"""
        blob_type = DataTypeParser.parse_atomic_type_sql_string("BLOB")
        self.assertIsInstance(blob_type, AtomicType)
        self.assertEqual(blob_type.type, "BLOB")
        self.assertTrue(blob_type.nullable)

        blob_type_not_null = DataTypeParser.parse_atomic_type_sql_string("BLOB NOT NULL")
        self.assertIsInstance(blob_type_not_null, AtomicType)
        self.assertEqual(blob_type_not_null.type, "BLOB")
        self.assertFalse(blob_type_not_null.nullable)

    def test_blob_to_pyarrow_conversion(self):
        """Test converting BLOB type to PyArrow large_binary"""
        blob_type = AtomicType("BLOB")
        pa_type = PyarrowFieldParser.from_paimon_type(blob_type)
        self.assertTrue(pa.types.is_large_binary(pa_type))

    def test_pyarrow_large_binary_to_blob_conversion(self):
        """Test converting PyArrow large_binary to BLOB type"""
        pa_type = pa.large_binary()
        paimon_type = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
        self.assertIsInstance(paimon_type, AtomicType)
        self.assertEqual(paimon_type.type, "BLOB")
        self.assertTrue(paimon_type.nullable)

    def test_blob_field_conversion(self):
        """Test converting BLOB DataField to PyArrow field and back"""
        # Create a BLOB DataField
        blob_field = DataField(
            id=1,
            name="binary_data",
            type=AtomicType("BLOB"),
            description="Binary large object field"
        )

        # Convert to PyArrow field
        pa_field = PyarrowFieldParser.from_paimon_field(blob_field)
        self.assertEqual(pa_field.name, "binary_data")
        self.assertTrue(pa.types.is_large_binary(pa_field.type))
        self.assertTrue(pa_field.nullable)
        self.assertEqual(
            pa_field.metadata[b'description'].decode('utf-8'),
            "Binary large object field"
        )

        # Convert back to Paimon field
        paimon_field = PyarrowFieldParser.to_paimon_field(1, pa_field)
        self.assertEqual(paimon_field.id, 1)
        self.assertEqual(paimon_field.name, "binary_data")
        self.assertIsInstance(paimon_field.type, AtomicType)
        self.assertEqual(paimon_field.type.type, "BLOB")
        self.assertTrue(paimon_field.type.nullable)
        self.assertEqual(paimon_field.description, "Binary large object field")

    def test_blob_schema_conversion(self):
        """Test schema conversion with BLOB fields"""
        # Create PyArrow schema with large_binary
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary()),
            ('regular_binary', pa.binary())
        ])

        # Convert to Paimon schema
        paimon_fields = PyarrowFieldParser.to_paimon_schema(pa_schema)
        self.assertEqual(len(paimon_fields), 4)

        # Check BLOB field
        blob_field = paimon_fields[2]
        self.assertEqual(blob_field.name, "blob_data")
        self.assertIsInstance(blob_field.type, AtomicType)
        self.assertEqual(blob_field.type.type, "BLOB")

        # Check regular binary field
        binary_field = paimon_fields[3]
        self.assertEqual(binary_field.name, "regular_binary")
        self.assertIsInstance(binary_field.type, AtomicType)
        self.assertEqual(binary_field.type.type, "BYTES")

        # Convert back to PyArrow schema
        pa_schema_converted = PyarrowFieldParser.from_paimon_schema(paimon_fields)
        self.assertEqual(len(pa_schema_converted), 4)
        self.assertTrue(pa.types.is_large_binary(pa_schema_converted.field('blob_data').type))
        self.assertTrue(pa.types.is_binary(pa_schema_converted.field('regular_binary').type))

    def test_blob_avro_conversion(self):
        """Test BLOB type conversion to Avro"""
        pa_type = pa.large_binary()
        avro_type = PyarrowFieldParser.to_avro_type(pa_type, "blob_field")
        self.assertEqual(avro_type, "bytes")

    def test_blob_json_serialization(self):
        """Test BLOB type JSON serialization and deserialization"""
        blob_field = DataField(
            id=1,
            name="blob_field",
            type=AtomicType("BLOB", nullable=True)
        )

        # Serialize to dict
        field_dict = blob_field.to_dict()
        self.assertEqual(field_dict['name'], 'blob_field')
        self.assertEqual(field_dict['type'], 'BLOB')

        # Deserialize from dict
        restored_field = DataField.from_dict(field_dict)
        self.assertEqual(restored_field.name, 'blob_field')
        self.assertIsInstance(restored_field.type, AtomicType)
        self.assertEqual(restored_field.type.type, 'BLOB')
        self.assertTrue(restored_field.type.nullable)


class FileIOWriteBlobTest(unittest.TestCase):
    """Tests for FileIO.write_blob functionality."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO(self.temp_dir, {})

    def tearDown(self):
        """Clean up test environment."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def test_write_blob_basic(self):
        """Test basic write_blob functionality."""
        # Create test data
        blob_data = [
            b"First blob content",
            b"Second blob with more data",
            b"Third blob: binary \x00\x01\x02\x03",
        ]

        # Create PyArrow table with blob data
        schema = pa.schema([
            pa.field("blob_field", pa.large_binary())
        ])
        table = pa.table([blob_data], schema=schema)

        # Write blob file
        blob_file = Path(self.temp_dir) / "test.blob"
        self.file_io.write_blob(blob_file, table)

        # Verify file was created
        self.assertTrue(self.file_io.exists(blob_file))
        self.assertGreater(self.file_io.get_file_size(blob_file), 0)

        # Verify we can read the data back
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=str(blob_file),
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, len(blob_data))

        # Verify data integrity
        for i, expected_blob in enumerate(blob_data):
            actual_blob = batch.column(0)[i].as_py()
            self.assertEqual(actual_blob, expected_blob)

        reader.close()

    def test_write_blob_empty_data(self):
        """Test write_blob with empty data."""
        # Create empty PyArrow table
        schema = pa.schema([
            pa.field("blob_field", pa.large_binary())
        ])
        table = pa.table([pa.array([], type=pa.large_binary())], schema=schema)

        # Write blob file
        blob_file = Path(self.temp_dir) / "empty.blob"
        self.file_io.write_blob(blob_file, table)

        # Verify file was created
        self.assertTrue(self.file_io.exists(blob_file))

        # Verify we can read the empty data back
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=str(blob_file),
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        batch = reader.read_arrow_batch()
        # Empty file should return None
        self.assertIsNone(batch)

        reader.close()

    def test_write_blob_with_nulls(self):
        """Test write_blob with null values should raise error."""
        # Create test data with nulls
        blob_data = [
            b"First blob content",
            None,  # Null value
            b"Third blob after null",
        ]

        # Create PyArrow table with blob data including nulls
        schema = pa.schema([
            pa.field("blob_field", pa.large_binary())
        ])
        table = pa.table([blob_data], schema=schema)

        # Write blob file should fail
        blob_file = Path(self.temp_dir) / "with_nulls.blob"

        with self.assertRaises(RuntimeError) as context:
            self.file_io.write_blob(blob_file, table)

        # Verify the error message mentions null values
        self.assertIn("null values", str(context.exception))

    def test_write_blob_large_data(self):
        """Test write_blob with large blob data."""
        # Create large blob data
        large_blob = b"X" * (1024 * 1024)  # 1MB
        small_blob = b"small"

        blob_data = [large_blob, small_blob]

        # Create PyArrow table
        schema = pa.schema([
            pa.field("blob_field", pa.large_binary())
        ])
        table = pa.table([blob_data], schema=schema)

        # Write blob file
        blob_file = Path(self.temp_dir) / "large.blob"
        self.file_io.write_blob(blob_file, table)

        # Verify file was created
        self.assertTrue(self.file_io.exists(blob_file))
        self.assertGreater(self.file_io.get_file_size(blob_file), 1024 * 1024)  # Should be > 1MB

        # Verify we can read the data back
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=str(blob_file),
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, len(blob_data))

        # Verify large data integrity
        actual_large = batch.column(0)[0].as_py()
        actual_small = batch.column(0)[1].as_py()

        self.assertEqual(len(actual_large), len(large_blob))
        self.assertEqual(actual_large, large_blob)
        self.assertEqual(actual_small, small_blob)

        reader.close()

    def test_write_blob_binary_patterns(self):
        """Test write_blob with various binary data patterns."""
        # Test with different binary patterns
        blob_data = [
            b"\x00\x01\x02\x03\xff\xfe\xfd",  # Binary with null bytes
            b"\x89PNG\r\n\x1a\n",  # PNG header
            b"%PDF-1.4",  # PDF header
            b"",  # Empty blob
            b"Regular text content",  # Text content
        ]

        # Create PyArrow table
        schema = pa.schema([
            pa.field("blob_field", pa.large_binary())
        ])
        table = pa.table([blob_data], schema=schema)

        # Write blob file
        blob_file = Path(self.temp_dir) / "binary_patterns.blob"
        self.file_io.write_blob(blob_file, table)

        # Verify file was created
        self.assertTrue(self.file_io.exists(blob_file))

        # Verify we can read the data back
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=str(blob_file),
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, len(blob_data))

        # Verify each blob's data integrity
        for i, expected_blob in enumerate(blob_data):
            actual_blob = batch.column(0)[i].as_py()
            self.assertEqual(actual_blob, expected_blob)

        reader.close()

    def test_write_blob_multiple_columns(self):
        """Test write_blob with multiple blob columns should raise error."""
        # Create test data with multiple blob columns
        blob_data1 = [b"First column blob 1", b"First column blob 2"]
        blob_data2 = [b"Second column blob 1", b"Second column blob 2"]

        # Create PyArrow table with multiple blob columns
        schema = pa.schema([
            pa.field("blob_field1", pa.large_binary()),
            pa.field("blob_field2", pa.large_binary())
        ])
        table = pa.table([blob_data1, blob_data2], schema=schema)

        # Write blob file should fail
        blob_file = Path(self.temp_dir) / "multi_column.blob"

        with self.assertRaises(RuntimeError) as context:
            self.file_io.write_blob(blob_file, table)

        # Verify the error message mentions single column constraint
        self.assertIn("single column", str(context.exception))

    def test_write_blob_error_handling(self):
        """Test write_blob error handling."""
        # Test with invalid path (should create parent directories)
        invalid_parent_dir = Path(self.temp_dir) / "nonexistent" / "nested"
        blob_file = invalid_parent_dir / "test.blob"

        # Create test data
        blob_data = [b"test content"]
        schema = pa.schema([pa.field("blob_field", pa.large_binary())])
        table = pa.table([blob_data], schema=schema)

        # Should succeed (FileIO creates parent directories)
        self.file_io.write_blob(blob_file, table)
        self.assertTrue(self.file_io.exists(blob_file))

    def test_write_blob_overwrite(self):
        """Test write_blob overwrites existing files."""
        blob_file = Path(self.temp_dir) / "overwrite.blob"

        # Write first blob file
        blob_data1 = [b"First content"]
        schema = pa.schema([pa.field("blob_field", pa.large_binary())])
        table1 = pa.table([blob_data1], schema=schema)

        self.file_io.write_blob(blob_file, table1)
        first_size = self.file_io.get_file_size(blob_file)

        # Write second blob file (should overwrite)
        blob_data2 = [b"Second content that is much longer"]
        table2 = pa.table([blob_data2], schema=schema)

        self.file_io.write_blob(blob_file, table2)
        second_size = self.file_io.get_file_size(blob_file)

        # File size should be different
        self.assertNotEqual(first_size, second_size)

        # Verify the content is from the second write
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        reader = FormatBlobReader(
            file_io=self.file_io,
            file_path=str(blob_file),
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        actual_blob = batch.column(0)[0].as_py()
        self.assertEqual(actual_blob, blob_data2[0])

        reader.close()


class FormatBlobReaderTest(unittest.TestCase):
    """Tests for FormatBlobReader."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO(self.temp_dir, {})
        self.mock_file_io = MockFileIO(self.file_io)

    def tearDown(self):
        """Clean up test environment."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def _create_test_blob_file(self, blob_data_list, field_name="blob_field"):
        """Helper method to create a test blob file."""
        # Create PyArrow table with blob data
        schema = pa.schema([pa.field(field_name, pa.large_binary())])
        table = pa.table([blob_data_list], schema=schema)

        # Write blob file
        blob_file = Path(self.temp_dir) / "test.blob"
        self.file_io.write_blob(blob_file, table)

        return str(blob_file)

    def test_format_blob_reader_basic(self):
        """Test basic FormatBlobReader functionality."""
        # Create test data
        test_blobs = [
            b"First blob content",
            b"Second blob with more data",
            b"Third blob: binary \x00\x01\x02\x03",
        ]

        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Create FormatBlobReader
        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        # Read data
        total_records = 0
        batches_read = 0

        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break

            batches_read += 1
            total_records += batch.num_rows

            # Verify batch structure
            self.assertEqual(batch.num_columns, 1)
            self.assertEqual(batch.schema.names, ["blob_field"])

        reader.close()

        # Verify results
        self.assertEqual(total_records, len(test_blobs))
        self.assertGreater(batches_read, 0)

    def test_format_blob_reader_content_verification(self):
        """Test FormatBlobReader with content verification."""
        # Create test data
        test_blobs = [
            b"Test blob 1",
            b"Test blob 2 with special chars \xff\xfe",
            b"Test blob 3"
        ]

        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Create FormatBlobReader
        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        # Read and verify data
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, len(test_blobs))

        # Verify each blob's content
        for i, expected_blob in enumerate(test_blobs):
            actual_blob = batch.column(0)[i].as_py()
            self.assertEqual(actual_blob, expected_blob)

        reader.close()

    def test_format_blob_reader_empty_file(self):
        """Test FormatBlobReader with empty blob file."""
        # Create empty blob file
        blob_file = self._create_test_blob_file([])
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Create FormatBlobReader
        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        # Read data - should return None immediately
        batch = reader.read_arrow_batch()
        self.assertIsNone(batch)

        reader.close()

    def test_format_blob_reader_large_data(self):
        """Test FormatBlobReader with large blob data."""
        # Create large blob data
        large_blob = b"X" * (1024 * 1024)  # 1MB
        small_blob = b"small"

        test_blobs = [large_blob, small_blob]
        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Create FormatBlobReader
        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None,
            batch_size=1  # Batch size parameter is now ignored
        )

        # Read data
        total_records = 0
        batches_read = 0

        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break

            batches_read += 1
            total_records += batch.num_rows

            # Verify data integrity for first record
            if batches_read == 1 and batch.num_rows > 0:
                blob_data = batch.column(0)[0].as_py()
                self.assertEqual(len(blob_data), len(large_blob))

        reader.close()

        # Verify results
        self.assertEqual(total_records, len(test_blobs))

    def test_format_blob_reader_binary_data(self):
        """Test FormatBlobReader with various binary data patterns."""
        # Test with different binary patterns
        test_blobs = [
            b"\x00\x01\x02\x03\xff\xfe\xfd",  # Binary with null bytes
            b"\x89PNG\r\n\x1a\n",  # PNG header
            b"%PDF-1.4",  # PDF header
            b"",  # Empty blob
            b"Regular text content",  # Text content
        ]

        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Create FormatBlobReader
        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        # Read and verify data
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, len(test_blobs))

        # Verify each blob's data integrity
        for i, expected_blob in enumerate(test_blobs):
            actual_blob = batch.column(0)[i].as_py()
            self.assertEqual(actual_blob, expected_blob)

        reader.close()

    def test_format_blob_reader_batch_size(self):
        """Test FormatBlobReader reads all records in single batch."""
        # Create test data
        test_blobs = [f"Blob {i}".encode() for i in range(10)]
        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Test with batch size parameter (now ignored)
        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None,
            batch_size=3
        )

        # Read data and count batches
        total_records = 0
        batches_read = 0

        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break

            batches_read += 1
            batch_size = batch.num_rows
            total_records += batch_size

        reader.close()

        # Verify all records were read
        self.assertEqual(total_records, len(test_blobs))
        # Should read all records in a single batch since batch_size is no longer used
        self.assertEqual(batches_read, 1)
        self.assertEqual(total_records, 10)

    def test_format_blob_reader_close(self):
        """Test FormatBlobReader close functionality."""
        test_blobs = [b"test data"]
        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        # Read some data
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)

        # Close reader
        reader.close()

        # Verify reader is properly closed
        self.assertIsNone(reader._blob_iterator)

    def test_format_blob_reader_different_field_names(self):
        """Test FormatBlobReader with different field names."""
        test_blobs = [b"test content"]

        # Test with different field names
        for field_name in ["blob_data", "content", "binary_field", "data"]:
            with self.subTest(field_name=field_name):
                blob_file = self._create_test_blob_file(test_blobs, field_name)
                fields = [DataField(0, field_name, AtomicType("BLOB"))]

                reader = FormatBlobReader(
                    file_io=self.mock_file_io,
                    file_path=blob_file,
                    read_fields=[field_name],
                    full_fields=fields,
                    push_down_predicate=None
                )

                # Should handle the field name appropriately
                batch = reader.read_arrow_batch()
                self.assertIsNotNone(batch)
                self.assertEqual(batch.num_columns, 1)
                self.assertEqual(batch.schema.names[0], field_name)

                # Verify content
                actual_blob = batch.column(0)[0].as_py()
                self.assertEqual(actual_blob, test_blobs[0])

                reader.close()

    def test_format_blob_reader_multiple_reads(self):
        """Test FormatBlobReader reads all data in single operation."""
        test_blobs = [f"Blob content {i}".encode() for i in range(5)]
        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None,
            batch_size=2  # Batch size parameter is now ignored
        )

        all_blobs = []
        read_count = 0

        # Read all data (now in single batch)
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break

            read_count += 1
            for i in range(batch.num_rows):
                blob_data = batch.column(0)[i].as_py()
                all_blobs.append(blob_data)

        reader.close()

        # Verify all data was read correctly
        self.assertEqual(len(all_blobs), len(test_blobs))
        self.assertEqual(read_count, 1)  # Should have single read since batch_size is ignored

        # Verify content matches
        for i, expected_blob in enumerate(test_blobs):
            self.assertEqual(all_blobs[i], expected_blob)

    def test_format_blob_reader_schema_validation(self):
        """Test FormatBlobReader schema validation."""
        test_blobs = [b"schema test"]
        blob_file = self._create_test_blob_file(test_blobs)
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        reader = FormatBlobReader(
            file_io=self.mock_file_io,
            file_path=blob_file,
            read_fields=["blob_field"],
            full_fields=fields,
            push_down_predicate=None
        )

        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)

        # Verify schema properties
        schema = batch.schema
        self.assertEqual(len(schema), 1)
        self.assertEqual(schema.names[0], "blob_field")
        self.assertEqual(schema.field(0).type, pa.large_binary())

        reader.close()

    def test_format_blob_reader_error_handling(self):
        """Test FormatBlobReader error handling."""
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Test with non-existent file
        with self.assertRaises(Exception):
            reader = FormatBlobReader(
                file_io=self.mock_file_io,
                file_path="/non/existent/file.blob",
                read_fields=["blob_field"],
                full_fields=fields,
                push_down_predicate=None
            )
            reader.read_arrow_batch()


if __name__ == '__main__':
    unittest.main()
