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

    def new_input_stream(self, path: Path):
        """Create new input stream for reading."""
        return self._file_io.new_input_stream(path)


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
            push_down_predicate=None
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
            push_down_predicate=None
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


class BlobEndToEndTest(unittest.TestCase):
    """End-to-end tests for blob functionality with schema definition, file writing, and reading."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.temp_dir, 'warehouse')
        # Create catalog for table operations
        from pypaimon import CatalogFactory
        self.catalog = CatalogFactory.create({
            'warehouse': self.warehouse
        })
        self.catalog.create_database('test_db', False)

    def tearDown(self):
        """Clean up test environment."""
        try:
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def test_blob_end_to_end(self):
        """Test comprehensive end-to-end blob functionality with AtomicType BLOB: check type, write data, read data."""
        from pypaimon.schema.data_types import DataField, AtomicType
        from pypaimon.table.row.blob import BlobData
        from pypaimon.common.file_io import FileIO
        from pypaimon.read.reader.format_blob_reader import FormatBlobReader
        from pathlib import Path
        import pyarrow as pa

        # Set up file I/O
        file_io = FileIO(self.temp_dir, {})

        # Define schema with multiple AtomicType("BLOB") fields
        comprehensive_fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "simple_blob", AtomicType("BLOB")),
            DataField(2, "document_blob", AtomicType("BLOB")),
            DataField(3, "image_blob", AtomicType("BLOB")),
            DataField(4, "metadata_blob", AtomicType("BLOB")),
            DataField(5, "description", AtomicType("STRING")),
        ]

        # ========== Step 1: Check Type Validation ==========
        # Validate all BLOB fields are AtomicType("BLOB")
        blob_fields = [
            comprehensive_fields[1], comprehensive_fields[2],
            comprehensive_fields[3], comprehensive_fields[4]
        ]
        for blob_field in blob_fields:
            self.assertIsInstance(blob_field.type, AtomicType)
            self.assertEqual(blob_field.type.type, "BLOB")

        # ========== Step 2: Write Data ==========
        # Prepare test data with multiple BLOB fields
        test_data = {
            'id': 1,
            'simple_blob': BlobData(b'End-to-end test: PDF header %PDF-1.4\n...'),
            'document_blob': BlobData(b'Document content: Lorem ipsum dolor sit amet...'),
            'image_blob': BlobData(b'\x89PNG\r\n\x1a\n...complete_image_data...'),
            'metadata_blob': BlobData(b'{"user_id": 12345, "created": "2024-01-01", "type": "profile"}'),
            'description': "Comprehensive end-to-end BLOB test with atomic types only"
        }

        # Write each BLOB field to separate files (blob format supports single column only)
        blob_files = {}

        # Write simple_blob
        simple_blob_data = [test_data['simple_blob'].to_data()]
        simple_schema = pa.schema([pa.field("simple_blob", pa.large_binary())])
        simple_table = pa.table([simple_blob_data], schema=simple_schema)
        blob_files['simple_blob'] = Path(self.temp_dir) / "simple_blob.blob"
        file_io.write_blob(blob_files['simple_blob'], simple_table)
        self.assertTrue(file_io.exists(blob_files['simple_blob']))

        # Write document_blob
        document_blob_data = [test_data['document_blob'].to_data()]
        document_schema = pa.schema([pa.field("document_blob", pa.large_binary())])
        document_table = pa.table([document_blob_data], schema=document_schema)
        blob_files['document_blob'] = Path(self.temp_dir) / "document_blob.blob"
        file_io.write_blob(blob_files['document_blob'], document_table)
        self.assertTrue(file_io.exists(blob_files['document_blob']))

        # Write image_blob
        image_blob_data = [test_data['image_blob'].to_data()]
        image_schema = pa.schema([pa.field("image_blob", pa.large_binary())])
        image_table = pa.table([image_blob_data], schema=image_schema)
        blob_files['image_blob'] = Path(self.temp_dir) / "image_blob.blob"
        file_io.write_blob(blob_files['image_blob'], image_table)
        self.assertTrue(file_io.exists(blob_files['image_blob']))

        # Write metadata_blob
        metadata_blob_data = [test_data['metadata_blob'].to_data()]
        metadata_schema = pa.schema([pa.field("metadata_blob", pa.large_binary())])
        metadata_table = pa.table([metadata_blob_data], schema=metadata_schema)
        blob_files['metadata_blob'] = Path(self.temp_dir) / "metadata_blob.blob"
        file_io.write_blob(blob_files['metadata_blob'], metadata_table)
        self.assertTrue(file_io.exists(blob_files['metadata_blob']))

        # Verify all files were created with correct sizes
        for field_name, file_path in blob_files.items():
            file_size = file_io.get_file_size(file_path)
            self.assertGreater(file_size, 0, f"{field_name} file should have content")

        # ========== Step 3: Read Data and Check Data ==========
        # Read and verify each BLOB field
        for field_name, file_path in blob_files.items():
            # Create field definition for reading
            read_fields = [DataField(0, field_name, AtomicType("BLOB"))]

            # Create reader
            reader = FormatBlobReader(
                file_io=file_io,
                file_path=str(file_path),
                read_fields=[field_name],
                full_fields=read_fields,
                push_down_predicate=None
            )

            # Read data
            batch = reader.read_arrow_batch()
            self.assertIsNotNone(batch, f"{field_name} batch should not be None")
            self.assertEqual(batch.num_rows, 1, f"{field_name} should have 1 row")

            # Verify data integrity
            read_blob_data = batch.column(0)[0].as_py()
            expected_blob_data = test_data[field_name].to_data()
            self.assertEqual(read_blob_data, expected_blob_data, f"{field_name} data should match")

            reader.close()

        # Test multiple BLOB records in a single file
        multiple_blob_data = [
            test_data['simple_blob'].to_data(),
            test_data['document_blob'].to_data(),
            test_data['image_blob'].to_data(),
            test_data['metadata_blob'].to_data()
        ]
        multiple_schema = pa.schema([pa.field("multiple_blobs", pa.large_binary())])
        multiple_table = pa.table([multiple_blob_data], schema=multiple_schema)

        multiple_blob_file = Path(self.temp_dir) / "multiple_blobs.blob"
        file_io.write_blob(multiple_blob_file, multiple_table)
        self.assertTrue(file_io.exists(multiple_blob_file))

        # Read multiple BLOB records
        multiple_fields = [DataField(0, "multiple_blobs", AtomicType("BLOB"))]
        multiple_reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(multiple_blob_file),
            read_fields=["multiple_blobs"],
            full_fields=multiple_fields,
            push_down_predicate=None
        )

        multiple_batch = multiple_reader.read_arrow_batch()
        self.assertIsNotNone(multiple_batch)
        self.assertEqual(multiple_batch.num_rows, 4)

        # Verify each record in the multiple BLOB file
        for i in range(multiple_batch.num_rows):
            read_blob = multiple_batch.column(0)[i].as_py()
            expected_blob = multiple_blob_data[i]
            self.assertEqual(read_blob, expected_blob, f"Multiple blob record {i} should match")

        multiple_reader.close()

        # Final verification summary
        total_files = len(blob_files) + 1  # Individual files + multiple blob file
        total_records = len(blob_files) + len(multiple_blob_data)  # Individual records + multiple records
        total_size = (
            sum(file_io.get_file_size(path) for path in blob_files.values()) +
            file_io.get_file_size(multiple_blob_file)
        )

        # Verify all operations completed successfully
        self.assertEqual(total_files, 5, "Should have created 5 blob files")
        self.assertEqual(total_records, 8, "Should have processed 8 blob records total")
        self.assertGreater(total_size, 0, "Total file size should be greater than 0")

    def test_blob_complex_types_throw_exception(self):
        """Test that complex types containing BLOB elements throw exceptions during read/write operations."""
        from pypaimon.schema.data_types import DataField, AtomicType, ArrayType, MultisetType, MapType
        from pypaimon.table.row.blob import BlobData
        from pypaimon.common.file_io import FileIO
        from pypaimon.read.reader.format_blob_reader import FormatBlobReader
        from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
        from pypaimon.table.row.row_kind import RowKind
        from pathlib import Path
        import pyarrow as pa

        # Set up file I/O
        file_io = FileIO(self.temp_dir, {})

        # ========== Test ArrayType(nullable=True, element_type=AtomicType("BLOB")) ==========
        array_fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "blob_array", ArrayType(nullable=True, element_type=AtomicType("BLOB"))),
        ]

        # Test serialization throws exception for ArrayType<BLOB>
        array_blob_data = [
            BlobData(b"Array blob 1"),
            BlobData(b"Array blob 2"),
            BlobData(b"Array blob 3")
        ]

        array_row = GenericRow([1, array_blob_data], array_fields, RowKind.INSERT)

        # GenericRowSerializer should throw exception for complex types
        with self.assertRaises(ValueError) as context:
            GenericRowSerializer.to_bytes(array_row)
        self.assertIn("AtomicType", str(context.exception))

        # Note: FileIO.write_blob validation for complex types is tested separately below

        # ========== Test MultisetType(nullable=True, element_type=AtomicType("BLOB")) ==========
        multiset_fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "blob_multiset", MultisetType(nullable=True, element_type=AtomicType("BLOB"))),
        ]

        # Test serialization throws exception for MultisetType<BLOB>
        multiset_blob_data = [
            BlobData(b"Multiset blob 1"),
            BlobData(b"Multiset blob 2"),
            BlobData(b"Multiset blob 1"),  # Duplicate allowed in multiset
        ]

        multiset_row = GenericRow([2, multiset_blob_data], multiset_fields, RowKind.INSERT)

        # GenericRowSerializer should throw exception for complex types
        with self.assertRaises(ValueError) as context:
            GenericRowSerializer.to_bytes(multiset_row)
        self.assertIn("AtomicType", str(context.exception))
        map_fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "blob_map", MapType(
                nullable=True, key_type=AtomicType("STRING"), value_type=AtomicType("BLOB")
            )),
        ]

        # Test serialization throws exception for MapType<STRING, BLOB>
        map_blob_data = {
            "document": BlobData(b"Document content"),
            "image": BlobData(b"Image data"),
            "metadata": BlobData(b"Metadata content")
        }

        map_row = GenericRow([3, map_blob_data], map_fields, RowKind.INSERT)

        # GenericRowSerializer should throw exception for complex types
        with self.assertRaises(ValueError) as context:
            GenericRowSerializer.to_bytes(map_row)
        self.assertIn("AtomicType", str(context.exception))

        # ========== Test FileIO.write_blob validation for complex types ==========
        # Test that FileIO.write_blob properly validates and rejects complex types

        # Create a table with multiple columns (should fail - blob format requires single column)
        multi_column_schema = pa.schema([
            pa.field("blob1", pa.large_binary()),
            pa.field("blob2", pa.large_binary())
        ])
        multi_column_table = pa.table([
            [b"blob1_data"],
            [b"blob2_data"]
        ], schema=multi_column_schema)

        multi_column_file = Path(self.temp_dir) / "multi_column.blob"

        # Should throw RuntimeError for multiple columns
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(multi_column_file, multi_column_table)
        self.assertIn("single column", str(context.exception))

        # Test that FileIO.write_blob rejects null values
        null_schema = pa.schema([pa.field("blob_with_nulls", pa.large_binary())])
        null_table = pa.table([[b"data", None]], schema=null_schema)

        null_file = Path(self.temp_dir) / "null_data.blob"

        # Should throw RuntimeError for null values
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(null_file, null_table)
        self.assertIn("null values", str(context.exception))

        # ========== Test FormatBlobReader with complex type schema ==========
        # Create a valid blob file first
        valid_blob_data = [b"Valid blob content"]
        valid_schema = pa.schema([pa.field("valid_blob", pa.large_binary())])
        valid_table = pa.table([valid_blob_data], schema=valid_schema)

        valid_blob_file = Path(self.temp_dir) / "valid_blob.blob"
        file_io.write_blob(valid_blob_file, valid_table)

        # Try to read with complex type field definition - this should fail
        # because FormatBlobReader tries to create PyArrow schema with complex types
        complex_read_fields = [
            DataField(0, "valid_blob", ArrayType(nullable=True, element_type=AtomicType("BLOB")))
        ]

        # FormatBlobReader creation should work, but reading should fail due to schema mismatch
        reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(valid_blob_file),
            read_fields=["valid_blob"],
            full_fields=complex_read_fields,
            push_down_predicate=None
        )

        # Reading should fail because the schema expects complex type but data is atomic
        with self.assertRaises(Exception) as context:
            reader.read_arrow_batch()
        # The error could be ArrowTypeError or other PyArrow-related errors
        self.assertTrue(
            "ArrowTypeError" in str(type(context.exception)) or
            "TypeError" in str(type(context.exception)) or
            "ValueError" in str(type(context.exception))
        )

        reader.close()

    def test_blob_advanced_scenarios(self):
        """Test advanced blob scenarios: corruption, truncation, zero-length, large blobs, compression, cross-format."""
        from pypaimon.schema.data_types import DataField, AtomicType
        from pypaimon.common.file_io import FileIO
        from pypaimon.read.reader.format_blob_reader import FormatBlobReader
        from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
        from pathlib import Path
        import pyarrow as pa

        # Set up file I/O
        file_io = FileIO(self.temp_dir, {})

        # ========== Test 1: Corrupted file header test ==========

        # Create a valid blob file first
        valid_blob_data = [b"Test blob content for corruption test"]
        valid_schema = pa.schema([pa.field("test_blob", pa.large_binary())])
        valid_table = pa.table([valid_blob_data], schema=valid_schema)

        header_test_file = Path(self.temp_dir) / "header_test.blob"
        file_io.write_blob(header_test_file, valid_table)

        # Read the file and corrupt the header (last 5 bytes: index_length + version)
        with open(header_test_file, 'rb') as f:
            original_data = f.read()

        # Corrupt the version byte (last byte)
        corrupted_data = bytearray(original_data)
        corrupted_data[-1] = 99  # Invalid version (should be 1)

        corrupted_header_file = Path(self.temp_dir) / "corrupted_header.blob"
        with open(corrupted_header_file, 'wb') as f:
            f.write(corrupted_data)

        # Try to read corrupted file - should detect invalid version
        fields = [DataField(0, "test_blob", AtomicType("BLOB"))]

        # Reading should fail due to invalid version
        with self.assertRaises(IOError) as context:
            FormatBlobReader(
                file_io=file_io,
                file_path=str(corrupted_header_file),
                read_fields=["test_blob"],
                full_fields=fields,
                push_down_predicate=None
            )
        self.assertIn("Unsupported blob file version", str(context.exception))

        # ========== Test 2: Truncated blob file (mid-blob) read ==========

        # Create a blob file with substantial content
        large_content = b"Large blob content: " + b"X" * 1000 + b" End of content"
        large_blob_data = [large_content]
        large_schema = pa.schema([pa.field("large_blob", pa.large_binary())])
        large_table = pa.table([large_blob_data], schema=large_schema)

        full_blob_file = Path(self.temp_dir) / "full_blob.blob"
        file_io.write_blob(full_blob_file, large_table)

        # Read the full file and truncate it in the middle
        with open(full_blob_file, 'rb') as f:
            full_data = f.read()

        # Truncate to about 50% of original size (mid-blob)
        truncated_size = len(full_data) // 2
        truncated_data = full_data[:truncated_size]

        truncated_file = Path(self.temp_dir) / "truncated.blob"
        with open(truncated_file, 'wb') as f:
            f.write(truncated_data)

        # Try to read truncated file - should fail gracefully
        with self.assertRaises((IOError, OSError)) as context:
            FormatBlobReader(
                file_io=file_io,
                file_path=str(truncated_file),
                read_fields=["large_blob"],
                full_fields=fields,
                push_down_predicate=None
            )
        # Should detect truncation/incomplete data (either invalid header or invalid version)
        self.assertTrue(
            "cannot read header" in str(context.exception) or
            "Unsupported blob file version" in str(context.exception)
        )

        # ========== Test 3: Zero-length blob handling ==========

        # Create blob with zero-length content
        zero_blob_data = [b""]  # Empty blob
        zero_schema = pa.schema([pa.field("zero_blob", pa.large_binary())])
        zero_table = pa.table([zero_blob_data], schema=zero_schema)

        zero_blob_file = Path(self.temp_dir) / "zero_length.blob"
        file_io.write_blob(zero_blob_file, zero_table)

        # Verify file was created
        self.assertTrue(file_io.exists(zero_blob_file))
        file_size = file_io.get_file_size(zero_blob_file)
        self.assertGreater(file_size, 0)  # File should have headers even with empty blob

        # Read zero-length blob
        zero_fields = [DataField(0, "zero_blob", AtomicType("BLOB"))]
        zero_reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(zero_blob_file),
            read_fields=["zero_blob"],
            full_fields=zero_fields,
            push_down_predicate=None
        )

        zero_batch = zero_reader.read_arrow_batch()
        self.assertIsNotNone(zero_batch)
        self.assertEqual(zero_batch.num_rows, 1)

        # Verify empty blob content
        read_zero_blob = zero_batch.column(0)[0].as_py()
        self.assertEqual(read_zero_blob, b"")
        self.assertEqual(len(read_zero_blob), 0)
        zero_reader.close()

        # ========== Test 4: Large blob (multi-GB range) simulation ==========
        # Simulate large blob without actually creating multi-GB data
        # Test chunked writing and memory-safe reading patterns

        # Create moderately large blob (10MB) to test chunking behavior
        chunk_size = 1024 * 1024  # 1MB chunks
        large_blob_content = b"LARGE_BLOB_CHUNK:" + b"L" * (chunk_size - 17)  # Fill to 1MB

        # Simulate multiple chunks
        simulated_large_data = [large_blob_content * 10]  # 10MB total
        large_sim_schema = pa.schema([pa.field("large_sim_blob", pa.large_binary())])
        large_sim_table = pa.table([simulated_large_data], schema=large_sim_schema)

        large_sim_file = Path(self.temp_dir) / "large_simulation.blob"
        file_io.write_blob(large_sim_file, large_sim_table)

        # Verify large file was written
        large_sim_size = file_io.get_file_size(large_sim_file)
        self.assertGreater(large_sim_size, 10 * 1024 * 1024)  # Should be > 10MB

        # Read large blob in memory-safe manner
        large_sim_fields = [DataField(0, "large_sim_blob", AtomicType("BLOB"))]
        large_sim_reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(large_sim_file),
            read_fields=["large_sim_blob"],
            full_fields=large_sim_fields,
            push_down_predicate=None
        )

        large_sim_batch = large_sim_reader.read_arrow_batch()
        self.assertIsNotNone(large_sim_batch)
        self.assertEqual(large_sim_batch.num_rows, 1)

        # Verify large blob content (check prefix to avoid loading all into memory for comparison)
        read_large_blob = large_sim_batch.column(0)[0].as_py()
        self.assertTrue(read_large_blob.startswith(b"LARGE_BLOB_CHUNK:"))
        self.assertEqual(len(read_large_blob), len(large_blob_content) * 10)
        large_sim_reader.close()

        # ========== Test 5: Index compression/decompression validation ==========
        # Test DeltaVarintCompressor roundtrip
        test_indices = [0, 100, 250, 1000, 5000, 10000, 50000]

        # Compress indices
        compressed_indices = DeltaVarintCompressor.compress(test_indices)
        self.assertIsInstance(compressed_indices, bytes)
        self.assertGreater(len(compressed_indices), 0)

        # Decompress indices
        decompressed_indices = DeltaVarintCompressor.decompress(compressed_indices)
        self.assertEqual(decompressed_indices, test_indices)

        # Test corruption detection in compressed indices
        if len(compressed_indices) > 1:
            # Corrupt the compressed data
            corrupted_indices = bytearray(compressed_indices)
            corrupted_indices[-1] = (corrupted_indices[-1] + 1) % 256  # Flip last byte

            # Decompression should fail or produce different results
            try:
                corrupted_result = DeltaVarintCompressor.decompress(bytes(corrupted_indices))
                # If decompression succeeds, result should be different
                self.assertNotEqual(corrupted_result, test_indices)
            except Exception:
                pass

        # ========== Test 6: Cross-format guard (multi-field tables) ==========
        # Test that blob format rejects multi-field tables
        multi_field_schema = pa.schema([
            pa.field("blob_field", pa.large_binary()),
            pa.field("string_field", pa.string()),
            pa.field("int_field", pa.int64())
        ])

        multi_field_table = pa.table([
            [b"blob_data_1", b"blob_data_2"],
            ["string_1", "string_2"],
            [100, 200]
        ], schema=multi_field_schema)

        multi_field_file = Path(self.temp_dir) / "multi_field.blob"

        # Should reject multi-field table
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(multi_field_file, multi_field_table)
        self.assertIn("single column", str(context.exception))

        # Test that blob format rejects non-binary field types
        non_binary_schema = pa.schema([pa.field("string_field", pa.string())])
        non_binary_table = pa.table([["not_binary_data"]], schema=non_binary_schema)

        non_binary_file = Path(self.temp_dir) / "non_binary.blob"

        # Should reject non-binary field
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(non_binary_file, non_binary_table)
        # Should fail due to type conversion issues (non-binary field can't be converted to BLOB)
        self.assertTrue(
            "large_binary" in str(context.exception) or
            "to_paimon_type" in str(context.exception) or
            "missing" in str(context.exception) or
            "Field must be Blob/BlobData instance" in str(context.exception)
        )

        # Test that blob format rejects tables with null values
        null_schema = pa.schema([pa.field("blob_with_null", pa.large_binary())])
        null_table = pa.table([[b"data", None, b"more_data"]], schema=null_schema)

        null_file = Path(self.temp_dir) / "with_nulls.blob"

        # Should reject null values
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(null_file, null_table)
        self.assertIn("null values", str(context.exception))


if __name__ == '__main__':
    unittest.main()
