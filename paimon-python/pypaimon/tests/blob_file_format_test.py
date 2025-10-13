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

from pypaimon.format.blob import BlobFileFormat, BlobFileFormatFactory, FormatReaderContext
from pypaimon.table.row.blob import BlobData
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.schema.data_types import DataField, AtomicType
from pypaimon.table.row.row_kind import RowKind


class BlobFileFormatTest(unittest.TestCase):
    """Tests for BlobFileFormat following org.apache.paimon.format.blob.BlobFileFormatTest."""

    def setUp(self):
        """Set up test environment with temporary file."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.temp_dir, "test.blob")

    def tearDown(self):
        """Clean up temporary files."""
        try:
            if os.path.exists(self.test_file):
                os.remove(self.test_file)
            os.rmdir(self.temp_dir)
        except OSError:
            pass  # Ignore cleanup errors

    def test_blob_file_format_basic(self):
        """Test basic blob file format write and read functionality."""
        format_instance = BlobFileFormat()

        # Create row type with single blob field
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Test data
        blob_data_list = [b"hello", b"world", b"blob", b"format", b"test"]

        # Write blob file
        writer_factory = format_instance.create_writer_factory(fields)

        with open(self.test_file, 'wb') as f:
            writer = writer_factory.create(f)

            for blob_bytes in blob_data_list:
                blob_data = BlobData(blob_bytes)
                row = GenericRow([blob_data], fields, RowKind.INSERT)
                writer.add_element(row)

            writer.close()

        # Verify file was created
        self.assertTrue(os.path.exists(self.test_file))
        file_size = os.path.getsize(self.test_file)
        self.assertGreater(file_size, 0)

        # Read blob file
        reader_factory = format_instance.create_reader_factory()
        context = FormatReaderContext(self.test_file, file_size)
        reader = reader_factory.create_reader(context)

        # Read all records
        result_data = []
        batch_iterator = reader.read_batch()
        if batch_iterator:
            for row in batch_iterator:
                blob = row.values[0]
                result_data.append(blob.to_data())

        reader.close()

        # Verify results
        self.assertEqual(len(result_data), len(blob_data_list))
        self.assertEqual(result_data, blob_data_list)

    def test_blob_file_format_with_selection(self):
        """Test blob file format with record selection."""
        format_instance = BlobFileFormat()
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Test data
        blob_data_list = [b"first", b"second", b"third", b"fourth"]

        # Write blob file
        writer_factory = format_instance.create_writer_factory(fields)

        with open(self.test_file, 'wb') as f:
            writer = writer_factory.create(f)

            for blob_bytes in blob_data_list:
                blob_data = BlobData(blob_bytes)
                row = GenericRow([blob_data], fields, RowKind.INSERT)
                writer.add_element(row)

            writer.close()

        file_size = os.path.getsize(self.test_file)

        # Read with selection (indices 1 and 3)
        reader_factory = format_instance.create_reader_factory()
        context = FormatReaderContext(self.test_file, file_size, selection=[1, 3])
        reader = reader_factory.create_reader(context)

        # Read selected records
        result_data = []
        batch_iterator = reader.read_batch()
        if batch_iterator:
            for row in batch_iterator:
                blob = row.values[0]
                result_data.append(blob.to_data())

        reader.close()

        # Verify only selected records were read
        expected_data = [blob_data_list[1], blob_data_list[3]]  # "second", "fourth"
        self.assertEqual(result_data, expected_data)

    def test_blob_file_format_empty_file(self):
        """Test blob file format with empty file."""
        format_instance = BlobFileFormat()
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Write empty blob file
        writer_factory = format_instance.create_writer_factory(fields)

        with open(self.test_file, 'wb') as f:
            writer = writer_factory.create(f)
            writer.close()  # Close without writing any records

        file_size = os.path.getsize(self.test_file)

        # Read empty file
        reader_factory = format_instance.create_reader_factory()
        context = FormatReaderContext(self.test_file, file_size)
        reader = reader_factory.create_reader(context)

        # Should return empty results
        result_data = []
        batch_iterator = reader.read_batch()
        if batch_iterator:
            for row in batch_iterator:
                blob = row.values[0]
                result_data.append(blob.to_data())

        reader.close()

        # Verify no records were read
        self.assertEqual(len(result_data), 0)

    def test_blob_file_format_large_data(self):
        """Test blob file format with large blob data."""
        format_instance = BlobFileFormat()
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Create large blob data (1MB)
        large_blob_data = b"X" * (1024 * 1024)
        small_blob_data = b"small"

        blob_data_list = [large_blob_data, small_blob_data]

        # Write blob file
        writer_factory = format_instance.create_writer_factory(fields)

        with open(self.test_file, 'wb') as f:
            writer = writer_factory.create(f)

            for blob_bytes in blob_data_list:
                blob_data = BlobData(blob_bytes)
                row = GenericRow([blob_data], fields, RowKind.INSERT)
                writer.add_element(row)

            writer.close()

        file_size = os.path.getsize(self.test_file)

        # Read blob file
        reader_factory = format_instance.create_reader_factory()
        context = FormatReaderContext(self.test_file, file_size)
        reader = reader_factory.create_reader(context)

        # Read all records
        result_data = []
        batch_iterator = reader.read_batch()
        if batch_iterator:
            for row in batch_iterator:
                blob = row.values[0]
                result_data.append(blob.to_data())

        reader.close()

        # Verify results
        self.assertEqual(len(result_data), 2)
        self.assertEqual(result_data[0], large_blob_data)
        self.assertEqual(result_data[1], small_blob_data)

    def test_blob_file_format_validation(self):
        """Test blob file format field validation."""
        format_instance = BlobFileFormat()

        # Test valid single blob field
        valid_fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        format_instance.validate_data_fields(valid_fields)  # Should not raise

        # Test invalid: multiple fields
        with self.assertRaises(ValueError) as context:
            invalid_fields = [
                DataField(0, "blob_field", AtomicType("BLOB")),
                DataField(1, "other_field", AtomicType("STRING"))
            ]
            format_instance.validate_data_fields(invalid_fields)
        self.assertIn("only supports one field", str(context.exception))

        # Test invalid: non-blob field
        with self.assertRaises(ValueError) as context:
            invalid_fields = [DataField(0, "string_field", AtomicType("STRING"))]
            format_instance.validate_data_fields(invalid_fields)
        self.assertIn("only supports blob type", str(context.exception))

    def test_blob_file_format_factory(self):
        """Test BlobFileFormatFactory functionality."""
        factory = BlobFileFormatFactory()

        # Test identifier
        self.assertEqual(factory.identifier(), "blob")

        # Test format creation
        format_instance = factory.create()
        self.assertIsInstance(format_instance, BlobFileFormat)

        # Test is_blob_file method
        self.assertTrue(BlobFileFormat.is_blob_file("test.blob"))
        self.assertFalse(BlobFileFormat.is_blob_file("test.parquet"))
        self.assertFalse(BlobFileFormat.is_blob_file("test.orc"))

    def test_blob_format_writer_validation(self):
        """Test BlobFormatWriter validation."""
        format_instance = BlobFileFormat()
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        writer_factory = format_instance.create_writer_factory(fields)

        with open(self.test_file, 'wb') as f:
            writer = writer_factory.create(f)

            # Test valid blob data
            blob_data = BlobData(b"valid data")
            row = GenericRow([blob_data], fields, RowKind.INSERT)
            writer.add_element(row)  # Should not raise

            # Test invalid: null blob
            with self.assertRaises(ValueError) as context:
                null_row = GenericRow([None], fields, RowKind.INSERT)
                writer.add_element(null_row)
            self.assertIn("non-null blob", str(context.exception))

            # Test invalid: wrong number of fields
            with self.assertRaises(ValueError) as context:
                extra_fields = fields + [DataField(1, "extra", AtomicType("STRING"))]
                multi_field_row = GenericRow([blob_data, "extra"], extra_fields, RowKind.INSERT)
                writer.add_element(multi_field_row)
            self.assertIn("only supports one field", str(context.exception))

            writer.close()

    def test_blob_format_binary_data(self):
        """Test blob file format with binary data containing null bytes."""
        format_instance = BlobFileFormat()
        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]

        # Test data with null bytes and various binary patterns
        binary_data_list = [
            b"\x00\x01\x02\x03\xff\xfe\xfd",
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR",
            b"",  # Empty blob
            b"\x00" * 100,  # All null bytes
        ]

        # Write blob file
        writer_factory = format_instance.create_writer_factory(fields)

        with open(self.test_file, 'wb') as f:
            writer = writer_factory.create(f)

            for blob_bytes in binary_data_list:
                blob_data = BlobData(blob_bytes)
                row = GenericRow([blob_data], fields, RowKind.INSERT)
                writer.add_element(row)

            writer.close()

        file_size = os.path.getsize(self.test_file)

        # Read blob file
        reader_factory = format_instance.create_reader_factory()
        context = FormatReaderContext(self.test_file, file_size)
        reader = reader_factory.create_reader(context)

        # Read all records
        result_data = []
        batch_iterator = reader.read_batch()
        if batch_iterator:
            for row in batch_iterator:
                blob = row.values[0]
                result_data.append(blob.to_data())

        reader.close()

        # Verify binary data integrity
        self.assertEqual(len(result_data), len(binary_data_list))
        for i, expected in enumerate(binary_data_list):
            self.assertEqual(result_data[i], expected, f"Binary data mismatch at index {i}")


if __name__ == '__main__':
    unittest.main()
