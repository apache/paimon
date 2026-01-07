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
from pypaimon.common.options import Options
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import Blob, BlobData, BlobRef, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRowDeserializer, GenericRowSerializer, GenericRow
from pypaimon.table.row.row_kind import RowKind


class MockFileIO:
    """Mock FileIO for testing."""

    def __init__(self, file_io: FileIO):
        self._file_io = file_io

    def get_file_size(self, path: str) -> int:
        """Get file size."""
        return self._file_io.get_file_size(path)

    def new_input_stream(self, path):
        """Create new input stream for reading."""
        if not isinstance(path, (str, type(None))):
            path = str(path)
        return self._file_io.new_input_stream(path)


def _to_url(path):
    """Convert Path to file:// URI string."""
    if isinstance(path, Path):
        path_str = str(path)
        is_absolute = os.path.isabs(path_str) or (len(path_str) >= 2 and path_str[1] == ':')
        if is_absolute:
            if path_str.startswith('/'):
                return f"file://{path_str}"
            else:
                return f"file:///{path_str}"
        else:
            return f"file://{path_str}"
    return str(path) if path else path


class BlobTest(unittest.TestCase):
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
        file_io = FileIO(self.file if self.file.startswith('file://') else f"file://{self.file}", Options({}))
        blob = Blob.from_file(file_io, self.file, 0, 4)

        # Verify it returns a BlobRef instance
        self.assertIsInstance(blob, BlobRef)

        # Verify the data matches (first 4 bytes: "test")
        self.assertEqual(blob.to_data(), b"test")

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
        file_io = FileIO(self.file if self.file.startswith('file://') else f"file://{self.file}", Options({}))
        blob_file = Blob.from_file(file_io, self.file, 0, os.path.getsize(self.file))
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
        blob_ref = Blob.from_local(self.file)

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


class BlobEndToEndTest(unittest.TestCase):
    """End-to-end tests for blob functionality with schema definition, file writing, and reading."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.temp_dir, 'warehouse')
        # Create catalog for table operations
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
        # Set up file I/O
        file_io = FileIO(self.temp_dir, Options({}))

        blob_field_name = "blob_field"
        # ========== Step 1: Check Type Validation ==========
        blob_fields = [DataField(0, blob_field_name, AtomicType("BLOB"))]
        for blob_field in blob_fields:
            self.assertIsInstance(blob_field.type, AtomicType)
            self.assertEqual(blob_field.type.type, "BLOB")

        # ========== Step 2: Write Data ==========
        test_data = {blob_field_name: BlobData(b'End-to-end test: PDF header %PDF-1.4\n...')}
        blob_files = {}
        blob_data = [test_data[blob_field_name].to_data()]
        schema = pa.schema([pa.field(blob_field_name, pa.large_binary())])
        table = pa.table([blob_data], schema=schema)
        blob_file_path = Path(self.temp_dir) / (blob_field_name + ".blob")
        blob_file_url = _to_url(blob_file_path)
        blob_files[blob_field_name] = blob_file_url
        file_io.write_blob(blob_file_url, table, False)
        self.assertTrue(file_io.exists(blob_file_url))

        # ========== Step 3: Read Data and Check Data ==========
        for field_name, file_path in blob_files.items():
            read_fields = blob_fields
            reader = FormatBlobReader(
                file_io=file_io,
                file_path=str(file_path),
                read_fields=[field_name],
                full_fields=read_fields,
                push_down_predicate=None,
                blob_as_descriptor=False
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

    def test_blob_complex_types_throw_exception(self):
        """Test that complex types containing BLOB elements throw exceptions during read/write operations."""
        from pypaimon.schema.data_types import DataField, AtomicType, ArrayType, MultisetType, MapType
        from pypaimon.table.row.blob import BlobData
        from pypaimon.common.file_io import FileIO
        from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
        from pypaimon.table.row.row_kind import RowKind

        # Set up file I/O
        file_io = FileIO(self.temp_dir, Options({}))

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
        multi_column_url = _to_url(multi_column_file)

        # Should throw RuntimeError for multiple columns
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(multi_column_url, multi_column_table, False)
        self.assertIn("single column", str(context.exception))

        # Test that FileIO.write_blob rejects null values
        null_schema = pa.schema([pa.field("blob_with_nulls", pa.large_binary())])
        null_table = pa.table([[b"data", None]], schema=null_schema)

        null_file = Path(self.temp_dir) / "null_data.blob"
        null_file_url = _to_url(null_file)

        # Should throw RuntimeError for null values
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(null_file_url, null_table, False)
        self.assertIn("null values", str(context.exception))

        # ========== Test FormatBlobReader with complex type schema ==========
        # Create a valid blob file first
        valid_blob_data = [b"Valid blob content"]
        valid_schema = pa.schema([pa.field("valid_blob", pa.large_binary())])
        valid_table = pa.table([valid_blob_data], schema=valid_schema)

        valid_blob_file = Path(self.temp_dir) / "valid_blob.blob"
        valid_blob_url = _to_url(valid_blob_file)
        file_io.write_blob(valid_blob_url, valid_table, False)

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
            push_down_predicate=None,
            blob_as_descriptor=False
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
        from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor

        # Set up file I/O
        file_io = FileIO(self.temp_dir, Options({}))

        # ========== Test 1: Corrupted file header test ==========

        # Create a valid blob file first
        valid_blob_data = [b"Test blob content for corruption test"]
        valid_schema = pa.schema([pa.field("test_blob", pa.large_binary())])
        valid_table = pa.table([valid_blob_data], schema=valid_schema)

        header_test_file = Path(self.temp_dir) / "header_test.blob"
        header_test_url = _to_url(header_test_file)
        file_io.write_blob(header_test_url, valid_table, False)

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
                push_down_predicate=None,
                blob_as_descriptor=False
            )
        self.assertIn("Unsupported blob file version", str(context.exception))

        # ========== Test 2: Truncated blob file (mid-blob) read ==========

        # Create a blob file with substantial content
        large_content = b"Large blob content: " + b"X" * 1000 + b" End of content"
        large_blob_data = [large_content]
        large_schema = pa.schema([pa.field("large_blob", pa.large_binary())])
        large_table = pa.table([large_blob_data], schema=large_schema)

        full_blob_file = Path(self.temp_dir) / "full_blob.blob"
        full_blob_url = _to_url(full_blob_file)
        file_io.write_blob(full_blob_url, large_table, False)

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
                push_down_predicate=None,
                blob_as_descriptor=False
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
        zero_blob_url = _to_url(zero_blob_file)
        file_io.write_blob(zero_blob_url, zero_table, False)

        # Verify file was created
        self.assertTrue(file_io.exists(zero_blob_url))
        file_size = file_io.get_file_size(zero_blob_url)
        self.assertGreater(file_size, 0)  # File should have headers even with empty blob

        # Read zero-length blob
        zero_fields = [DataField(0, "zero_blob", AtomicType("BLOB"))]
        zero_reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(zero_blob_file),
            read_fields=["zero_blob"],
            full_fields=zero_fields,
            push_down_predicate=None,
            blob_as_descriptor=False
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
        large_sim_url = _to_url(large_sim_file)
        file_io.write_blob(large_sim_url, large_sim_table, False)

        # Verify large file was written
        large_sim_size = file_io.get_file_size(large_sim_url)
        self.assertGreater(large_sim_size, 10 * 1024 * 1024)  # Should be > 10MB

        # Read large blob in memory-safe manner
        large_sim_fields = [DataField(0, "large_sim_blob", AtomicType("BLOB"))]
        large_sim_reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(large_sim_file),
            read_fields=["large_sim_blob"],
            full_fields=large_sim_fields,
            push_down_predicate=None,
            blob_as_descriptor=False
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
        multi_field_url = _to_url(multi_field_file)

        # Should reject multi-field table
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(multi_field_url, multi_field_table, False)
        self.assertIn("single column", str(context.exception))

        # Test that blob format rejects non-binary field types
        non_binary_schema = pa.schema([pa.field("string_field", pa.string())])
        non_binary_table = pa.table([["not_binary_data"]], schema=non_binary_schema)

        non_binary_file = Path(self.temp_dir) / "non_binary.blob"
        non_binary_url = _to_url(non_binary_file)

        # Should reject non-binary field
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(non_binary_url, non_binary_table, False)
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
        null_file_url = _to_url(null_file)

        # Should reject null values
        with self.assertRaises(RuntimeError) as context:
            file_io.write_blob(null_file_url, null_table, False)
        self.assertIn("null values", str(context.exception))

    def test_blob_end_to_end_with_descriptor(self):
        # Set up file I/O
        file_io = FileIO(self.temp_dir, Options({}))

        # ========== Step 1: Write data to local file ==========
        # Create test data and write it to a local file
        test_content = b'This is test blob content stored in an external file for descriptor testing.'
        # Write the test content to a local file
        local_data_file = Path(self.temp_dir) / "external_blob"
        with open(local_data_file, 'wb') as f:
            f.write(test_content)
        # Verify the file was created and has the correct content
        self.assertTrue(local_data_file.exists())
        with open(local_data_file, 'rb') as f:
            written_content = f.read()
        self.assertEqual(written_content, test_content)

        # ========== Step 2: Use this file as blob descriptor ==========
        # Create a BlobDescriptor pointing to the local file
        blob_descriptor = BlobDescriptor(
            uri=str(local_data_file),
            offset=0,
            length=len(test_content)
        )
        # Serialize the descriptor to bytes (this is what would be stored in the blob column)
        descriptor_bytes = blob_descriptor.serialize()
        self.assertIsInstance(descriptor_bytes, bytes)
        self.assertGreater(len(descriptor_bytes), 0)

        # Create PyArrow table with the serialized descriptor
        blob_field_name = "blob_descriptor_field"
        schema = pa.schema([pa.field(blob_field_name, pa.large_binary())])
        table = pa.table([[descriptor_bytes]], schema=schema)

        # Write the blob file with blob_as_descriptor=True
        blob_file_path = Path(self.temp_dir) / "descriptor_blob.blob"
        blob_file_url = _to_url(blob_file_path)
        file_io.write_blob(blob_file_url, table, blob_as_descriptor=True)
        # Verify the blob file was created
        self.assertTrue(file_io.exists(blob_file_url))
        file_size = file_io.get_file_size(blob_file_url)
        self.assertGreater(file_size, 0)

        # ========== Step 3: Read data and check ==========
        # Define schema for reading
        read_fields = [DataField(0, blob_field_name, AtomicType("BLOB"))]
        reader = FormatBlobReader(
            file_io=file_io,
            file_path=str(blob_file_path),
            read_fields=[blob_field_name],
            full_fields=read_fields,
            push_down_predicate=None,
            blob_as_descriptor=True
        )

        # Read the data with blob_as_descriptor=True (should return a descriptor)
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, 1)
        self.assertEqual(batch.num_columns, 1)

        read_blob_bytes = batch.column(0)[0].as_py()
        self.assertIsInstance(read_blob_bytes, bytes)

        # Deserialize the returned descriptor
        returned_descriptor = BlobDescriptor.deserialize(read_blob_bytes)

        # The returned descriptor should point to the blob file (simplified implementation)
        # because the current implementation creates a descriptor pointing to the blob file location
        self.assertEqual(returned_descriptor.uri, str(blob_file_path))
        self.assertGreater(returned_descriptor.offset, 0)  # Should have some offset in the blob file

        reader.close()

        reader_content = FormatBlobReader(
            file_io=file_io,
            file_path=str(blob_file_path),
            read_fields=[blob_field_name],
            full_fields=read_fields,
            push_down_predicate=None,
            blob_as_descriptor=False
        )
        batch_content = reader_content.read_arrow_batch()
        self.assertIsNotNone(batch_content)
        self.assertEqual(batch_content.num_rows, 1)
        read_content_bytes = batch_content.column(0)[0].as_py()
        self.assertIsInstance(read_content_bytes, bytes)
        # When blob_as_descriptor=False, we should get the actual file content
        self.assertEqual(read_content_bytes, test_content)
        reader_content.close()


if __name__ == '__main__':
    unittest.main()
