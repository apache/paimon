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
import struct
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.write.commit_message import CommitMessage


class DataBlobWriterTest(unittest.TestCase):
    """Tests for DataBlobWriter functionality with paimon table operations."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, 'warehouse')
        # Create catalog for table operations
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('test_db', False)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        try:
            shutil.rmtree(cls.temp_dir)
        except OSError:
            pass

    def test_data_blob_writer_basic_functionality(self):
        """Test basic DataBlobWriter functionality with paimon table."""
        from pypaimon import Schema

        # Create schema with normal and blob columns
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary()),  # This will be detected as blob
        ])

        # Create Paimon schema
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )

        # Create table
        self.catalog.create_table('test_db.blob_writer_test', schema, False)
        table = self.catalog.get_table('test_db.blob_writer_test')

        # Test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'blob_data': [b'blob_data_1', b'blob_data_2', b'blob_data_3']
        }, schema=pa_schema)

        # Test DataBlobWriter initialization using proper table API
        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Write test data using BatchTableWrite API
        blob_writer.write_arrow(test_data)

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)
        self.assertGreater(len(commit_messages), 0)

        # Verify commit message structure
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
            self.assertGreater(len(commit_msg.new_files), 0)

            # Verify file metadata structure
            for file_meta in commit_msg.new_files:
                self.assertIsNotNone(file_meta.file_name)
                self.assertGreater(file_meta.file_size, 0)
                self.assertGreater(file_meta.row_count, 0)

        blob_writer.close()

    def test_data_blob_writer_schema_detection(self):
        """Test that DataBlobWriter correctly detects blob columns from schema."""
        from pypaimon import Schema

        # Test schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('blob_field', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_detection_test', schema, False)
        table = self.catalog.get_table('test_db.blob_detection_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test that DataBlobWriter was created internally
        # We can verify this by checking the internal data writers
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'blob_field': [b'blob1', b'blob2', b'blob3']
        }, schema=pa_schema)

        # Write data to trigger writer creation
        blob_writer.write_arrow(test_data)

        # Verify that a DataBlobWriter was created internally
        data_writers = blob_writer.file_store_write.data_writers
        self.assertGreater(len(data_writers), 0)

        # Check that the writer is a DataBlobWriter
        for writer in data_writers.values():
            from pypaimon.write.writer.data_blob_writer import DataBlobWriter
            self.assertIsInstance(writer, DataBlobWriter)

        blob_writer.close()

    def test_data_blob_writer_no_blob_column(self):
        """Test that DataBlobWriter raises error when no blob column is found."""
        from pypaimon import Schema

        # Test schema without blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.no_blob_test', schema, False)
        table = self.catalog.get_table('test_db.no_blob_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()

        # Test that a regular writer (not DataBlobWriter) was created
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        }, schema=pa_schema)

        # Write data to trigger writer creation
        writer.write_arrow(test_data)

        # Verify that a regular writer was created (not DataBlobWriter)
        data_writers = writer.file_store_write.data_writers
        self.assertGreater(len(data_writers), 0)

        # Check that the writer is NOT a DataBlobWriter
        for writer_instance in data_writers.values():
            from pypaimon.write.writer.data_blob_writer import DataBlobWriter
            self.assertNotIsInstance(writer_instance, DataBlobWriter)

        writer.close()

    def test_data_blob_writer_multiple_blob_columns(self):
        """Test that DataBlobWriter raises error when multiple blob columns are found."""
        from pypaimon import Schema

        # Test schema with multiple blob columns
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('blob1', pa.large_binary()),
            ('blob2', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.multiple_blob_test', schema, False)
        table = self.catalog.get_table('test_db.multiple_blob_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()

        # Test data with multiple blob columns
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'blob1': [b'blob1_1', b'blob1_2', b'blob1_3'],
            'blob2': [b'blob2_1', b'blob2_2', b'blob2_3']
        }, schema=pa_schema)

        # This should raise an error when DataBlobWriter is created internally
        with self.assertRaises(ValueError) as context:
            writer.write_arrow(test_data)
        self.assertIn("Limit exactly one blob field in one paimon table yet", str(context.exception))

    def test_data_blob_writer_write_operations(self):
        """Test DataBlobWriter write operations with real data."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('document', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.write_test', schema, False)
        table = self.catalog.get_table('test_db.write_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'document': [b'document_content_1', b'document_content_2']
        }, schema=pa_schema)

        # Test writing data
        for batch in test_data.to_batches():
            blob_writer.write_arrow_batch(batch)

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)

        blob_writer.close()

    def test_data_blob_writer_write_large_blob(self):
        """Test DataBlobWriter with very large blob data (50MB per item) in 10 batches."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('description', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.large_blob_test', schema, False)
        table = self.catalog.get_table('test_db.large_blob_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Create 50MB blob data per item
        # Using a pattern to make the data more realistic and compressible
        target_size = 50 * 1024 * 1024  # 50MB in bytes
        blob_pattern = b'LARGE_BLOB_DATA_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = target_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        # Verify the blob size is approximately 50MB
        blob_size_mb = len(large_blob_data) / (1024 * 1024)
        self.assertGreater(blob_size_mb, 49)  # Should be at least 49MB
        self.assertLess(blob_size_mb, 51)  # Should be less than 51MB

        total_rows = 0

        # Write 10 batches, each with 5 rows (50 rows total)
        # Total data volume: 50 rows * 50MB = 2.5GB of blob data
        for batch_num in range(10):
            batch_data = pa.Table.from_pydict({
                'id': [batch_num * 5 + i for i in range(5)],
                'description': [f'Large blob batch {batch_num}, row {i}' for i in range(5)],
                'large_blob': [large_blob_data] * 5  # 5 rows per batch, each with 50MB blob
            }, schema=pa_schema)

            # Write each batch
            for batch in batch_data.to_batches():
                blob_writer.write_arrow_batch(batch)
                total_rows += batch.num_rows

            # Log progress for large data processing
            print(f"Completed batch {batch_num + 1}/10 with {batch.num_rows} rows")

        # Record count is tracked internally by DataBlobWriter

        # Test prepare commit
        commit_messages: CommitMessage = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)
        # Verify we have commit messages
        self.assertEqual(len(commit_messages), 1)
        commit_message = commit_messages[0]
        normal_file_meta = commit_message.new_files[0]
        blob_file_metas = commit_message.new_files[1:]
        # Validate row count consistency
        parquet_row_count = normal_file_meta.row_count
        blob_row_count_sum = sum(meta.row_count for meta in blob_file_metas)
        self.assertEqual(parquet_row_count, blob_row_count_sum,
                         f"Parquet row count ({parquet_row_count}) should equal "
                         f"sum of blob row counts ({blob_row_count_sum})")

        # Verify commit message structure and file metadata
        total_file_size = 0
        total_row_count = parquet_row_count
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
            self.assertGreater(len(commit_msg.new_files), 0)

            # Verify file metadata structure
            for file_meta in commit_msg.new_files:
                self.assertIsNotNone(file_meta.file_name)
                self.assertGreater(file_meta.file_size, 0)
                self.assertGreater(file_meta.row_count, 0)
                total_file_size += file_meta.file_size

        # Verify total data written (50 rows of normal data + 50 rows of blob data = 100 total)
        self.assertEqual(total_row_count, 50)

        # Verify total file size is substantial (should be much larger than 2.5GB due to overhead)
        total_size_mb = total_file_size / (1024 * 1024)
        self.assertGreater(total_size_mb, 2000)  # Should be at least 2GB due to overhead

        total_files = sum(len(commit_msg.new_files) for commit_msg in commit_messages)
        print(f"Total data written: {total_size_mb:.2f}MB across {total_files} files")
        print(f"Total rows processed: {total_row_count}")

        blob_writer.close()

    def test_data_blob_writer_abort_functionality(self):
        """Test DataBlobWriter abort functionality."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('blob_data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.abort_test', schema, False)
        table = self.catalog.get_table('test_db.abort_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test data
        test_data = pa.Table.from_pydict({
            'id': [1, 2],
            'blob_data': [b'blob_1', b'blob_2']
        }, schema=pa_schema)

        # Write some data
        for batch in test_data.to_batches():
            blob_writer.write_arrow_batch(batch)

        # Test abort - BatchTableWrite doesn't have abort method
        # The abort functionality is handled internally by DataBlobWriter

        blob_writer.close()

    def test_data_blob_writer_multiple_batches(self):
        """Test DataBlobWriter with multiple batches and verify results."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('document', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.multiple_batches_test', schema, False)
        table = self.catalog.get_table('test_db.multiple_batches_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test data - multiple batches
        batch1_data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'document': [b'document_1_content', b'document_2_content']
        }, schema=pa_schema)

        batch2_data = pa.Table.from_pydict({
            'id': [3, 4, 5],
            'name': ['Charlie', 'David', 'Eve'],
            'document': [b'document_3_content', b'document_4_content', b'document_5_content']
        }, schema=pa_schema)

        batch3_data = pa.Table.from_pydict({
            'id': [6],
            'name': ['Frank'],
            'document': [b'document_6_content']
        }, schema=pa_schema)

        # Write multiple batches
        total_rows = 0
        for batch in batch1_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        for batch in batch2_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        for batch in batch3_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        # Record count is tracked internally by DataBlobWriter

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)

        # Verify we have committed files
        self.assertGreater(len(commit_messages), 0)

        blob_writer.close()

    def test_data_blob_writer_large_batches(self):
        """Test DataBlobWriter with large batches to test rolling behavior."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('description', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.large_batches_test', schema, False)
        table = self.catalog.get_table('test_db.large_batches_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Create large batches with substantial blob data
        large_blob_data = b'L' * 10000  # 10KB blob data

        # Batch 1: 100 rows
        batch1_data = pa.Table.from_pydict({
            'id': list(range(1, 101)),
            'description': [f'Description for row {i}' for i in range(1, 101)],
            'large_blob': [large_blob_data] * 100
        }, schema=pa_schema)

        # Batch 2: 50 rows
        batch2_data = pa.Table.from_pydict({
            'id': list(range(101, 151)),
            'description': [f'Description for row {i}' for i in range(101, 151)],
            'large_blob': [large_blob_data] * 50
        }, schema=pa_schema)

        # Write large batches
        total_rows = 0
        for batch in batch1_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        for batch in batch2_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        # Record count is tracked internally by DataBlobWriter

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)

        # Verify we have committed files
        self.assertGreater(len(commit_messages), 0)

        blob_writer.close()

    def test_data_blob_writer_mixed_data_types(self):
        """Test DataBlobWriter with mixed data types in blob column."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('type', pa.string()),
            ('data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.mixed_data_test', schema, False)
        table = self.catalog.get_table('test_db.mixed_data_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test data with different types of blob content
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'type': ['text', 'json', 'binary', 'image', 'pdf'],
            'data': [
                b'This is text content',
                b'{"key": "value", "number": 42}',
                b'\x00\x01\x02\x03\xff\xfe\xfd',
                b'PNG_IMAGE_DATA_PLACEHOLDER',
                b'%PDF-1.4\nPDF_CONTENT_PLACEHOLDER'
            ]
        }, schema=pa_schema)

        # Write mixed data
        total_rows = 0
        for batch in test_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        # Record count is tracked internally by DataBlobWriter

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)

        # Verify we have committed files
        self.assertGreater(len(commit_messages), 0)

        # Verify commit message structure
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
            self.assertGreater(len(commit_msg.new_files), 0)

            # Verify file metadata structure
            for file_meta in commit_msg.new_files:
                self.assertIsNotNone(file_meta.file_name)
                self.assertGreater(file_meta.file_size, 0)
                self.assertGreater(file_meta.row_count, 0)

        # Should have both normal and blob files
        file_names = [f.file_name for f in commit_msg.new_files]
        parquet_files = [f for f in file_names if f.endswith('.parquet')]
        blob_files = [f for f in file_names if f.endswith('.blob')]

        self.assertGreater(len(parquet_files), 0, "Should have at least one parquet file")
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        # Create commit and commit the data
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        blob_writer.close()

        # Read data back using table API
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data was read back correctly
        self.assertEqual(result.num_rows, 5, "Should have 5 rows")
        self.assertEqual(result.num_columns, 3, "Should have 3 columns")

        # Convert result to pandas for easier comparison
        result_df = result.to_pandas()

        # Verify each row matches the original data
        for i in range(5):
            original_id = test_data.column('id')[i].as_py()
            original_type = test_data.column('type')[i].as_py()
            original_data = test_data.column('data')[i].as_py()

            result_id = result_df.iloc[i]['id']
            result_type = result_df.iloc[i]['type']
            result_data = result_df.iloc[i]['data']

            self.assertEqual(result_id, original_id, f"Row {i + 1}: ID should match")
            self.assertEqual(result_type, original_type, f"Row {i + 1}: Type should match")
            self.assertEqual(result_data, original_data, f"Row {i + 1}: Blob data should match")

    def test_data_blob_writer_empty_batches(self):
        """Test DataBlobWriter with empty batches."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.empty_batches_test', schema, False)
        table = self.catalog.get_table('test_db.empty_batches_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test data with some empty batches
        batch1_data = pa.Table.from_pydict({
            'id': [1, 2],
            'data': [b'data1', b'data2']
        }, schema=pa_schema)

        # Empty batch
        empty_batch = pa.Table.from_pydict({
            'id': [],
            'data': []
        }, schema=pa_schema)

        batch2_data = pa.Table.from_pydict({
            'id': [3],
            'data': [b'data3']
        }, schema=pa_schema)

        # Write batches including empty ones
        total_rows = 0
        for batch in batch1_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        for batch in empty_batch.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        for batch in batch2_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        # Verify record count (empty batch should not affect count)
        # Record count is tracked internally by DataBlobWriter
        # Record count is tracked internally by DataBlobWriter

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)

        blob_writer.close()

    def test_data_blob_writer_rolling_behavior(self):
        """Test DataBlobWriter rolling behavior with multiple commits."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('content', pa.string()),
            ('blob_data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.rolling_test', schema, False)
        table = self.catalog.get_table('test_db.rolling_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Create data that should trigger rolling
        large_content = 'X' * 1000  # Large string content
        large_blob = b'B' * 5000  # Large blob data

        # Write multiple batches to test rolling
        for i in range(10):  # 10 batches
            batch_data = pa.Table.from_pydict({
                'id': [i * 10 + j for j in range(10)],
                'content': [f'{large_content}_{i}_{j}' for j in range(10)],
                'blob_data': [large_blob] * 10
            }, schema=pa_schema)

            for batch in batch_data.to_batches():
                blob_writer.write_arrow_batch(batch)

        # Verify total record count
        # Record count is tracked internally by DataBlobWriter

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        self.assertIsInstance(commit_messages, list)

        # Verify we have committed files
        self.assertGreater(len(commit_messages), 0)

        # Verify file metadata structure
        for commit_msg in commit_messages:
            for file_meta in commit_msg.new_files:
                self.assertIsNotNone(file_meta.file_name)
                self.assertGreater(file_meta.file_size, 0)
                self.assertGreater(file_meta.row_count, 0)

        blob_writer.close()

    def test_blob_write_read_end_to_end(self):
        """Test complete end-to-end blob functionality: write blob data and read it back to verify correctness."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('description', pa.string()),
            ('blob_data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_write_read_e2e', schema, False)
        table = self.catalog.get_table('test_db.blob_write_read_e2e')

        # Test data with various blob sizes and types
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'description': ['User 1', 'User 2', 'User 3', 'User 4', 'User 5'],
            'blob_data': [
                b'small_blob_1',
                b'medium_blob_data_2_with_more_content',
                b'large_blob_data_3_with_even_more_content_and_details',
                b'very_large_blob_data_4_with_extensive_content_and_multiple_details_here',  # noqa: E501
                b'extremely_large_blob_data_5_with_comprehensive_content_and_'
                b'extensive_details_covering_multiple_aspects'  # noqa: E501
            ]
        }, schema=pa_schema)

        # Write data using table API
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        # Commit the data
        commit_messages = writer.prepare_commit()
        self.assertGreater(len(commit_messages), 0)

        # Verify commit message structure
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
        self.assertGreater(len(commit_msg.new_files), 0)

        # Should have both normal and blob files
        file_names = [f.file_name for f in commit_msg.new_files]
        parquet_files = [f for f in file_names if f.endswith('.parquet')]
        blob_files = [f for f in file_names if f.endswith('.blob')]

        self.assertGreater(len(parquet_files), 0, "Should have at least one parquet file")
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        # Create commit and commit the data
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back using table API
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data was read back correctly
        self.assertEqual(result.num_rows, 5, "Should have 5 rows")
        self.assertEqual(result.num_columns, 4, "Should have 4 columns")

        # Verify normal columns
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3, 4, 5], "ID column should match")
        self.assertEqual(result.column('name').to_pylist(), ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
                         "Name column should match")  # noqa: E501
        self.assertEqual(result.column('description').to_pylist(), ['User 1', 'User 2', 'User 3', 'User 4', 'User 5'],
                         "Description column should match")  # noqa: E501

        # Verify blob data correctness
        blob_data = result.column('blob_data').to_pylist()
        expected_blobs = [
            b'small_blob_1',
            b'medium_blob_data_2_with_more_content',
            b'large_blob_data_3_with_even_more_content_and_details',
            b'very_large_blob_data_4_with_extensive_content_and_multiple_details_here',  # noqa: E501
            b'extremely_large_blob_data_5_with_comprehensive_content_and_extensive_details_covering_multiple_aspects'
            # noqa: E501
        ]

        self.assertEqual(len(blob_data), 5, "Should have 5 blob records")
        self.assertEqual(blob_data, expected_blobs, "Blob data should match exactly")

        # Verify individual blob sizes
        for i, (actual_blob, expected_blob) in enumerate(zip(blob_data, expected_blobs)):
            self.assertEqual(len(actual_blob), len(expected_blob), f"Blob {i + 1} size should match")
            self.assertEqual(actual_blob, expected_blob, f"Blob {i + 1} content should match exactly")

        print(
            f"✅ End-to-end blob write/read test passed: wrote and read back {len(blob_data)} blob records correctly")  # noqa: E501

    def test_blob_write_read_partition(self):
        """Test complete end-to-end blob functionality: write blob data and read it back to verify correctness."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('description', pa.string()),
            ('blob_data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=['name'],
            # pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_write_read_partition', schema, False)
        table = self.catalog.get_table('test_db.blob_write_read_partition')

        # Test data with various blob sizes and types
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Alice', 'David', 'David', 'David'],
            'description': ['User 1', 'User 2', 'User 3', 'User 4', 'User 5'],
            'blob_data': [
                b'small_blob_1',
                b'medium_blob_data_2_with_more_content',
                b'large_blob_data_3_with_even_more_content_and_details',
                b'very_large_blob_data_4_with_extensive_content_and_multiple_details_here',  # noqa: E501
                b'extremely_large_blob_data_5_with_comprehensive_content_and_'
                b'extensive_details_covering_multiple_aspects'
                # noqa: E501
            ]
        }, schema=pa_schema)

        # Write data using table API
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        # Commit the data
        commit_messages = writer.prepare_commit()

        # Create commit and commit the data
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back using table API
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        result = table_read.to_arrow(splits)

        # Verify the data was read back correctly
        self.assertEqual(result.num_rows, 5, "Should have 5 rows")
        self.assertEqual(result.num_columns, 4, "Should have 4 columns")

        # Verify normal columns
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3, 4, 5], "ID column should match")
        self.assertEqual(result.column('name').to_pylist(), ['Alice', 'Alice', 'David', 'David', 'David'],
                         "Name column should match")  # noqa: E501
        self.assertEqual(result.column('description').to_pylist(), ['User 1', 'User 2', 'User 3', 'User 4', 'User 5'],
                         "Description column should match")  # noqa: E501

        # Verify blob data correctness
        blob_data = result.column('blob_data').to_pylist()
        expected_blobs = [
            b'small_blob_1',
            b'medium_blob_data_2_with_more_content',
            b'large_blob_data_3_with_even_more_content_and_details',
            b'very_large_blob_data_4_with_extensive_content_and_multiple_details_here',  # noqa: E501
            b'extremely_large_blob_data_5_with_comprehensive_content_and_extensive_details_covering_multiple_aspects'
            # noqa: E501
        ]

        self.assertEqual(len(blob_data), 5, "Should have 5 blob records")
        self.assertEqual(blob_data, expected_blobs, "Blob data should match exactly")

        # Verify individual blob sizes
        for i, (actual_blob, expected_blob) in enumerate(zip(blob_data, expected_blobs)):
            self.assertEqual(len(actual_blob), len(expected_blob), f"Blob {i + 1} size should match")
            self.assertEqual(actual_blob, expected_blob, f"Blob {i + 1} content should match exactly")

        print(
            f"✅ End-to-end blob write/read test passed: wrote and read back {len(blob_data)} blob records correctly")  # noqa: E501

    def test_blob_write_read_end_to_end_with_descriptor(self):
        """Test end-to-end blob functionality using blob descriptors."""
        import random
        from pypaimon import Schema
        from pypaimon.table.row.blob import BlobDescriptor, Blob
        from pypaimon.common.uri_reader import UriReaderFactory
        from pypaimon.common.options.config import CatalogOptions

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('picture', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob-as-descriptor': 'true'
            }
        )

        # Create table
        self.catalog.create_table('test_db.blob_descriptor_test', schema, False)
        table: FileStoreTable = self.catalog.get_table('test_db.blob_descriptor_test')

        # Create test blob data (1MB)
        blob_data = bytearray(1024 * 1024)
        random.seed(42)  # For reproducible tests
        for i in range(len(blob_data)):
            blob_data[i] = random.randint(0, 255)
        blob_data = bytes(blob_data)

        # Create external blob file
        external_blob_path = os.path.join(self.temp_dir, 'external_blob')
        with open(external_blob_path, 'wb') as f:
            f.write(blob_data)

        # Create blob descriptor pointing to external file
        blob_descriptor = BlobDescriptor(external_blob_path, 0, len(blob_data))

        # Create test data with blob descriptor
        test_data = pa.Table.from_pydict({
            'id': [1],
            'name': ['paimon'],
            'picture': [blob_descriptor.serialize()]
        }, schema=pa_schema)

        # Write data using table API
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        # Commit the data
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data was written and read correctly
        self.assertEqual(result.num_rows, 1, "Should have 1 row")
        self.assertEqual(result.column('id').to_pylist(), [1], "ID should match")
        self.assertEqual(result.column('name').to_pylist(), ['paimon'], "Name should match")

        # Get the blob descriptor bytes from the result
        picture_bytes = result.column('picture').to_pylist()[0]
        self.assertIsInstance(picture_bytes, bytes, "Picture should be bytes")

        # Deserialize the blob descriptor
        new_blob_descriptor = BlobDescriptor.deserialize(picture_bytes)

        # The URI might be different if the blob was stored in the table's data directory
        # Let's verify the descriptor properties and try to read the data
        # Note: offset might be non-zero due to blob file format overhead
        self.assertGreaterEqual(new_blob_descriptor.offset, 0, "Offset should be non-negative")
        self.assertEqual(new_blob_descriptor.length, len(blob_data), "Length should match")

        # Create URI reader factory and read the blob data
        catalog_options = {CatalogOptions.WAREHOUSE.key(): self.warehouse}
        uri_reader_factory = UriReaderFactory(catalog_options)
        uri_reader = uri_reader_factory.create(new_blob_descriptor.uri)
        blob = Blob.from_descriptor(uri_reader, new_blob_descriptor)

        blob_descriptor_from_blob = blob.to_descriptor()
        self.assertEqual(
            blob_descriptor_from_blob.uri,
            new_blob_descriptor.uri,
            f"URI should be preserved. Expected: {new_blob_descriptor.uri}, Got: {blob_descriptor_from_blob.uri}"
        )

        # Verify the blob data matches the original
        self.assertEqual(blob.to_data(), blob_data, "Blob data should match original")

        print("✅ Blob descriptor end-to-end test passed:")
        print("   - Created external blob file and descriptor")
        print("   - Wrote and read blob descriptor successfully")
        print("   - Verified blob data can be read from descriptor")
        print("   - Tested blob-as-descriptor=true mode")

    def test_blob_write_read_large_data_end_to_end(self):
        """Test end-to-end blob functionality with large blob data (1MB per blob)."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_large_write_read_e2e', schema, False)
        table = self.catalog.get_table('test_db.blob_large_write_read_e2e')

        # Create large blob data (1MB per blob)
        large_blob_size = 1024 * 1024  # 1MB
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        # Test data with large blobs
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'metadata': ['Large blob 1', 'Large blob 2', 'Large blob 3'],
            'large_blob': [large_blob_data, large_blob_data, large_blob_data]
        }, schema=pa_schema)

        # Write data
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        # Commit the data
        commit_messages = writer.prepare_commit()
        self.assertGreater(len(commit_messages), 0)

        # Verify commit message structure
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
        self.assertGreater(len(commit_msg.new_files), 0)

        # Should have both normal and blob files
        file_names = [f.file_name for f in commit_msg.new_files]
        parquet_files = [f for f in file_names if f.endswith('.parquet')]
        blob_files = [f for f in file_names if f.endswith('.blob')]

        self.assertGreater(len(parquet_files), 0, "Should have at least one parquet file")
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data
        self.assertEqual(result.num_rows, 3, "Should have 3 rows")
        self.assertEqual(result.num_columns, 3, "Should have 3 columns")

        # Verify normal columns
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3], "ID column should match")
        self.assertEqual(result.column('metadata').to_pylist(), ['Large blob 1', 'Large blob 2', 'Large blob 3'],
                         "Metadata column should match")  # noqa: E501

        # Verify blob data integrity
        blob_data = result.column('large_blob').to_pylist()
        self.assertEqual(len(blob_data), 3, "Should have 3 blob records")

        for i, blob in enumerate(blob_data):
            self.assertEqual(len(blob), len(large_blob_data), f"Blob {i + 1} should be {large_blob_size} bytes")
            self.assertEqual(blob, large_blob_data, f"Blob {i + 1} content should match exactly")
            print(f"✅ Verified large blob {i + 1}: {len(blob)} bytes")

        print(
            f"✅ Large blob end-to-end test passed: wrote and read back {len(blob_data)} large blob records correctly")  # noqa: E501

    def test_blob_write_read_large_data_volume(self):
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('batch_id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_large_data_volume', schema, False)
        table = self.catalog.get_table('test_db.blob_large_data_volume')

        large_blob_size = 5 * 1024
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        num_row = 20000
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        expected = pa.Table.from_pydict({
            'id': [1] * num_row,
            'batch_id': [11] * num_row,
            'metadata': [f'Large blob batch {11}'] * num_row,
            'large_blob': [i.to_bytes(2, byteorder='little') + large_blob_data for i in range(num_row)]
        }, schema=pa_schema)
        writer.write_arrow(expected)

        # Commit all data at once
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data
        self.assertEqual(result.num_rows, num_row)
        self.assertEqual(result.num_columns, 4)

        self.assertEqual(expected, result)

    def test_blob_write_read_large_data_volume_rolling(self):
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('batch_id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob.target-file-size': '21MB'
            }
        )
        self.catalog.create_table('test_db.large_data_volume_rolling', schema, False)
        table = self.catalog.get_table('test_db.large_data_volume_rolling')

        # Create large blob data
        large_blob_size = 5 * 1024  #
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        actual_size = len(large_blob_data)
        print(f"Created blob data: {actual_size:,} bytes ({actual_size / (1024 * 1024):.2f} MB)")
        num_row = 20000
        expected = pa.Table.from_pydict({
            'id': [1] * num_row,
            'batch_id': [11] * num_row,
            'metadata': [f'Large blob batch {11}'] * num_row,
            'large_blob': [i.to_bytes(2, byteorder='little') + large_blob_data for i in range(num_row)]
        }, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(expected)

        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())
        self.assertEqual(expected, result)

    def test_blob_write_read_mixed_sizes_end_to_end(self):
        """Test end-to-end blob functionality with mixed blob sizes."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('size_category', pa.string()),
            ('blob_data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_mixed_sizes_write_read_e2e', schema, False)
        table = self.catalog.get_table('test_db.blob_mixed_sizes_write_read_e2e')

        # Create blobs of different sizes
        tiny_blob = b'tiny'
        small_blob = b'small_blob_data' * 10  # ~140 bytes
        medium_blob = b'medium_blob_data' * 100  # ~1.4KB
        large_blob = b'large_blob_data' * 1000  # ~14KB
        huge_blob = b'huge_blob_data' * 10000  # ~140KB

        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'size_category': ['tiny', 'small', 'medium', 'large', 'huge'],
            'blob_data': [tiny_blob, small_blob, medium_blob, large_blob, huge_blob]
        }, schema=pa_schema)

        # Write data
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        # Commit
        commit_messages = writer.prepare_commit()
        self.assertGreater(len(commit_messages), 0)

        # Verify commit message structure
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
        self.assertGreater(len(commit_msg.new_files), 0)

        # Should have both normal and blob files
        file_names = [f.file_name for f in commit_msg.new_files]
        parquet_files = [f for f in file_names if f.endswith('.parquet')]
        blob_files = [f for f in file_names if f.endswith('.blob')]

        self.assertGreater(len(parquet_files), 0, "Should have at least one parquet file")
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify
        self.assertEqual(result.num_rows, 5, "Should have 5 rows")
        self.assertEqual(result.num_columns, 3, "Should have 3 columns")

        # Verify normal columns
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3, 4, 5], "ID column should match")
        self.assertEqual(result.column('size_category').to_pylist(), ['tiny', 'small', 'medium', 'large', 'huge'],
                         "Size category column should match")  # noqa: E501

        # Verify blob data
        blob_data = result.column('blob_data').to_pylist()
        expected_blobs = [tiny_blob, small_blob, medium_blob, large_blob, huge_blob]

        self.assertEqual(len(blob_data), 5, "Should have 5 blob records")
        self.assertEqual(blob_data, expected_blobs, "Blob data should match exactly")

        # Verify sizes
        sizes = [len(blob) for blob in blob_data]
        expected_sizes = [len(blob) for blob in expected_blobs]
        self.assertEqual(sizes, expected_sizes, "Blob sizes should match")

        # Verify individual blob content
        for i, (actual_blob, expected_blob) in enumerate(zip(blob_data, expected_blobs)):
            self.assertEqual(actual_blob, expected_blob, f"Blob {i + 1} content should match exactly")

        print(
            f"✅ Mixed sizes end-to-end test passed: wrote and read back blobs ranging from {min(sizes)} to {max(sizes)} bytes")  # noqa: E501

    def test_blob_write_read_large_data_end_to_end_with_rolling(self):
        """Test end-to-end blob functionality with large blob data (50MB per blob) and rolling behavior (40 blobs)."""
        from pypaimon import Schema

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('batch_id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_large_rolling_e2e', schema, False)
        table = self.catalog.get_table('test_db.blob_large_rolling_e2e')

        # Create large blob data (50MB per blob)
        large_blob_size = 50 * 1024 * 1024  # 50MB
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        # Verify the blob size is exactly 50MB
        actual_size = len(large_blob_data)
        print(f"Created blob data: {actual_size:,} bytes ({actual_size / (1024 * 1024):.2f} MB)")

        # Write 40 batches of data (each with 1 blob of 50MB)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()

        # Write all 40 batches first
        for batch_id in range(40):
            # Create test data for this batch
            test_data = pa.Table.from_pydict({
                'id': [batch_id + 1],
                'batch_id': [batch_id],
                'metadata': [f'Large blob batch {batch_id + 1}'],
                'large_blob': [large_blob_data]
            }, schema=pa_schema)

            # Write data
            writer.write_arrow(test_data)

            # Print progress every 10 batches
            if (batch_id + 1) % 10 == 0:
                print(f"✅ Written batch {batch_id + 1}/40: {len(large_blob_data):,} bytes")

        print("✅ Successfully wrote all 40 batches of 50MB blobs")

        # Commit all data at once
        commit_messages = writer.prepare_commit()
        self.assertGreater(len(commit_messages), 0)

        # Verify commit message structure
        for commit_msg in commit_messages:
            self.assertIsInstance(commit_msg.new_files, list)
        self.assertGreater(len(commit_msg.new_files), 0)

        # Should have both normal and blob files
        file_names = [f.file_name for f in commit_msg.new_files]
        parquet_files = [f for f in file_names if f.endswith('.parquet')]
        blob_files = [f for f in file_names if f.endswith('.blob')]

        self.assertGreater(len(parquet_files), 0, "Should have at least one parquet file")
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        # Commit the data
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        print(f"✅ Successfully committed {len(commit_messages)} commit messages with 40 batches of 50MB blobs")

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data
        self.assertEqual(result.num_rows, 40, "Should have 40 rows")
        self.assertEqual(result.num_columns, 4, "Should have 4 columns")

        # Verify normal columns
        expected_ids = list(range(1, 41))
        expected_batch_ids = list(range(40))
        expected_metadata = [f'Large blob batch {i}' for i in range(1, 41)]

        self.assertEqual(result.column('id').to_pylist(), expected_ids, "ID column should match")
        self.assertEqual(result.column('batch_id').to_pylist(), expected_batch_ids,
                         "Batch ID column should match")  # noqa: E501
        self.assertEqual(result.column('metadata').to_pylist(), expected_metadata,
                         "Metadata column should match")  # noqa: E501

        # Verify blob data integrity
        blob_data = result.column('large_blob').to_pylist()
        self.assertEqual(len(blob_data), 40, "Should have 40 blob records")

        # Verify each blob
        for i, blob in enumerate(blob_data):
            self.assertEqual(len(blob), len(large_blob_data), f"Blob {i + 1} should be {large_blob_size:,} bytes")
            self.assertEqual(blob, large_blob_data, f"Blob {i + 1} content should match exactly")

            # Print progress every 10 blobs
            if (i + 1) % 10 == 0:
                print(f"✅ Verified blob {i + 1}/40: {len(blob):,} bytes")

        # Verify total data size
        total_blob_size = sum(len(blob) for blob in blob_data)
        expected_total_size = 40 * len(large_blob_data)
        self.assertEqual(total_blob_size, expected_total_size,
                         f"Total blob size should be {expected_total_size:,} bytes")

        print("✅ Large blob rolling end-to-end test passed:")
        print("   - Wrote and read back 40 blobs of 50MB each")
        print(
            f"   - Total data size: {total_blob_size:,} bytes ({total_blob_size / (1024 * 1024 * 1024):.2f} GB)")  # noqa: E501
        print("   - All blob content verified as correct")

    def test_blob_read_row_by_row_iterator(self):
        """Test reading blob data row by row using to_iterator()."""
        from pypaimon import Schema
        from pypaimon.table.row.blob import Blob
        from pypaimon.table.row.internal_row import RowKind

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_iterator_test', schema, False)
        table = self.catalog.get_table('test_db.blob_iterator_test')

        expected_data = {
            1: {'name': 'Alice', 'blob': b'blob_1'},
            2: {'name': 'Bob', 'blob': b'blob_2_data'},
            3: {'name': 'Charlie', 'blob': b'blob_3_content'},
            4: {'name': 'David', 'blob': b'blob_4_large_content'},
            5: {'name': 'Eve', 'blob': b'blob_5_very_large_content_data'}
        }

        test_data = pa.Table.from_pydict({
            'id': list(expected_data.keys()),
            'name': [expected_data[i]['name'] for i in expected_data.keys()],
            'blob_data': [expected_data[i]['blob'] for i in expected_data.keys()]
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Verify blob files were created
        file_names = [f.file_name for f in commit_messages[0].new_files]
        self.assertGreater(
            len([f for f in file_names if f.endswith('.blob')]), 0,
            "Should have at least one blob file")

        # Read using to_iterator
        iterator = table.new_read_builder().new_read().to_iterator(
            table.new_read_builder().new_scan().plan().splits())

        rows = []
        value = next(iterator, None)
        while value is not None:
            rows.append(value)
            value = next(iterator, None)

        self.assertEqual(len(rows), 5, "Should have 5 rows")

        for row in rows:
            row_id = row.get_field(0)
            self.assertIn(row_id, expected_data, f"ID {row_id} should be in expected data")

            expected = expected_data[row_id]
            self.assertEqual(row.get_field(1), expected['name'], f"Row {row_id}: name should match")

            row_blob = row.get_field(2)
            blob_bytes = row_blob.to_data() if isinstance(row_blob, Blob) else row_blob
            self.assertIsInstance(blob_bytes, bytes, f"Row {row_id}: blob should be bytes")
            self.assertEqual(blob_bytes, expected['blob'], f"Row {row_id}: blob data should match")
            self.assertEqual(len(blob_bytes), len(expected['blob']), f"Row {row_id}: blob size should match")

            self.assertIn(
                row.get_row_kind(),
                [RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE],
                f"Row {row_id}: RowKind should be valid")

    def test_blob_as_descriptor_target_file_size_rolling(self):
        import random
        import os
        from pypaimon import Schema
        from pypaimon.table.row.blob import BlobDescriptor

        pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string()), ('blob_data', pa.large_binary())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'blob-as-descriptor': 'true', 'target-file-size': '1MB'
        })
        self.catalog.create_table('test_db.blob_target_size_test', schema, False)
        table = self.catalog.get_table('test_db.blob_target_size_test')

        # Create 5 external blob files (2MB each, > target-file-size)
        num_blobs, blob_size = 5, 2 * 1024 * 1024
        random.seed(42)
        descriptors = []
        for i in range(num_blobs):
            path = os.path.join(self.temp_dir, f'external_blob_{i}')
            data = bytes(bytearray([random.randint(0, 255) for _ in range(blob_size)]))
            with open(path, 'wb') as f:
                f.write(data)
            descriptors.append(BlobDescriptor(path, 0, len(data)))

        # Write data
        test_data = pa.Table.from_pydict({
            'id': list(range(1, num_blobs + 1)),
            'name': [f'item_{i}' for i in range(1, num_blobs + 1)],
            'blob_data': [d.serialize() for d in descriptors]
        }, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Check blob files
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]
        total_size = sum(f.file_size for f in blob_files)

        # Verify that rolling works correctly: should have multiple files when total size exceeds target
        # Each blob is 2MB, target-file-size is 1MB, so should have multiple files
        self.assertGreater(
            len(blob_files), 1,
            f"Should have multiple blob files when total size ({total_size / 1024 / 1024:.2f}MB) exceeds target (1MB), "
            f"but got {len(blob_files)} file(s)"
        )

        # Verify data integrity
        result = table.new_read_builder().new_read().to_arrow(table.new_read_builder().new_scan().plan().splits())
        self.assertEqual(result.num_rows, num_blobs)
        self.assertEqual(result.column('id').to_pylist(), list(range(1, num_blobs + 1)))

    def test_blob_file_name_format_with_shared_uuid(self):
        import random
        import re
        from pypaimon import Schema
        from pypaimon.table.row.blob import BlobDescriptor

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-as-descriptor': 'true',
            'blob.target-file-size': '1MB'  # Small target size to trigger multiple rollings
        })

        self.catalog.create_table('test_db.blob_file_name_test', schema, False)
        table = self.catalog.get_table('test_db.blob_file_name_test')

        # Create multiple external blob files (2MB each, > target-file-size)
        # This will trigger multiple blob file rollings
        num_blobs, blob_size = 5, 2 * 1024 * 1024
        random.seed(789)
        descriptors = []
        for i in range(num_blobs):
            path = os.path.join(self.temp_dir, f'external_blob_{i}')
            data = bytes(bytearray([random.randint(0, 255) for _ in range(blob_size)]))
            with open(path, 'wb') as f:
                f.write(data)
            descriptors.append(BlobDescriptor(path, 0, len(data)))

        # Write data
        test_data = pa.Table.from_pydict({
            'id': list(range(1, num_blobs + 1)),
            'name': [f'item_{i}' for i in range(1, num_blobs + 1)],
            'blob_data': [d.serialize() for d in descriptors]
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Extract blob files from commit messages
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]

        # Should have multiple blob files due to rolling
        self.assertGreater(len(blob_files), 1, "Should have multiple blob files due to rolling")

        # Verify file name format: data-{uuid}-{count}.blob
        file_name_pattern = re.compile(
            r'^data-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-'
            r'[a-f0-9]{12}-(\d+)\.blob$'
        )

        first_file_name = blob_files[0].file_name
        self.assertTrue(
            file_name_pattern.match(first_file_name),
            f"File name should match expected format: data-{{uuid}}-{{count}}.blob, got: {first_file_name}"
        )

        first_match = file_name_pattern.match(first_file_name)
        first_counter = int(first_match.group(1))

        # Extract UUID (everything between "data-" and last "-")
        uuid_start = len("data-")
        uuid_end = first_file_name.rfind('-', uuid_start)
        shared_uuid = first_file_name[uuid_start:uuid_end]

        # Verify all blob files use the same UUID and have sequential counters
        for i, blob_file in enumerate(blob_files):
            file_name = blob_file.file_name
            match = file_name_pattern.match(file_name)

            self.assertIsNotNone(
                match,
                f"File name should match expected format: data-{{uuid}}-{{count}}.blob, got: {file_name}"
            )

            counter = int(match.group(1))

            # Extract UUID from this file
            file_uuid = file_name[uuid_start:file_name.rfind('-', uuid_start)]

            self.assertEqual(
                file_uuid,
                shared_uuid,
                f"All blob files should use the same UUID. Expected: {shared_uuid}, got: {file_uuid} in {file_name}"
            )

            self.assertEqual(
                counter,
                first_counter + i,
                f"File counter should be sequential. Expected: {first_counter + i}, got: {counter} in {file_name}"
            )

        # Verify data integrity
        result = table.new_read_builder().new_read().to_arrow(table.new_read_builder().new_scan().plan().splits())
        self.assertEqual(result.num_rows, num_blobs)
        self.assertEqual(result.column('id').to_pylist(), list(range(1, num_blobs + 1)))

    def test_blob_as_descriptor_sequence_number_increment(self):
        import os
        from pypaimon import Schema
        from pypaimon.table.row.blob import BlobDescriptor

        # Create schema with blob column (blob-as-descriptor=true)
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-as-descriptor': 'true'
        })

        self.catalog.create_table('test_db.blob_sequence_test', schema, False)
        table = self.catalog.get_table('test_db.blob_sequence_test')

        # Create multiple external blob files
        num_blobs = 10
        descriptors = []
        for i in range(num_blobs):
            path = os.path.join(self.temp_dir, f'external_blob_seq_{i}')
            data = f"blob data {i}".encode('utf-8')
            with open(path, 'wb') as f:
                f.write(data)
            descriptors.append(BlobDescriptor(path, 0, len(data)))

        # Write data row by row (this triggers the one-by-one writing in blob-as-descriptor mode)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()

        # Write each row separately to ensure one-by-one writing
        for i in range(num_blobs):
            test_data = pa.Table.from_pydict({
                'id': [i + 1],
                'name': [f'item_{i}'],
                'blob_data': [descriptors[i].serialize()]
            }, schema=pa_schema)
            writer.write_arrow(test_data)

        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Extract blob files from commit messages
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]

        # Verify that we have at least one blob file
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        # Verify sequence numbers for each blob file
        for blob_file in blob_files:
            min_seq = blob_file.min_sequence_number
            max_seq = blob_file.max_sequence_number
            row_count = blob_file.row_count

            # Critical assertion: min_seq should NOT equal max_seq when there are multiple rows
            if row_count > 1:
                self.assertNotEqual(
                    min_seq, max_seq,
                    f"Sequence numbers should be different for files with multiple rows. "
                    f"File: {blob_file.file_name}, row_count: {row_count}, "
                    f"min_seq: {min_seq}, max_seq: {max_seq}. "
                    f"This indicates sequence generator was not incremented for each row."
                )
                self.assertEqual(
                    max_seq - min_seq + 1, row_count,
                    f"Sequence number range should match row count. "
                    f"File: {blob_file.file_name}, row_count: {row_count}, "
                    f"min_seq: {min_seq}, max_seq: {max_seq}, "
                    f"expected range: {row_count}, actual range: {max_seq - min_seq + 1}"
                )
            else:
                # For single row files, min_seq == max_seq is acceptable
                self.assertEqual(
                    min_seq, max_seq,
                    f"Single row file should have min_seq == max_seq. "
                    f"File: {blob_file.file_name}, min_seq: {min_seq}, max_seq: {max_seq}"
                )

        # Verify data integrity
        result = table.new_read_builder().new_read().to_arrow(table.new_read_builder().new_scan().plan().splits())
        self.assertEqual(result.num_rows, num_blobs)
        self.assertEqual(result.column('id').to_pylist(), list(range(1, num_blobs + 1)))

    def test_blob_non_descriptor_sequence_number_increment(self):
        from pypaimon import Schema

        # Create schema with blob column (blob-as-descriptor=false, normal mode)
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-as-descriptor': 'false'  # Normal mode, not descriptor mode
        })

        self.catalog.create_table('test_db.blob_sequence_non_desc_test', schema, False)
        table = self.catalog.get_table('test_db.blob_sequence_non_desc_test')

        # Create test data with multiple rows in a batch
        num_rows = 10
        test_data = pa.Table.from_pydict({
            'id': list(range(1, num_rows + 1)),
            'name': [f'item_{i}' for i in range(num_rows)],
            'blob_data': [f'blob data {i}'.encode('utf-8') for i in range(num_rows)]
        }, schema=pa_schema)

        # Write data as a batch (this triggers batch writing in non-descriptor mode)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Extract blob files from commit messages
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]

        # Verify that we have at least one blob file
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        # Verify sequence numbers for each blob file
        for blob_file in blob_files:
            min_seq = blob_file.min_sequence_number
            max_seq = blob_file.max_sequence_number
            row_count = blob_file.row_count

            # Critical assertion: min_seq should NOT equal max_seq when there are multiple rows
            if row_count > 1:
                self.assertNotEqual(
                    min_seq, max_seq,
                    f"Sequence numbers should be different for files with multiple rows. "
                    f"File: {blob_file.file_name}, row_count: {row_count}, "
                    f"min_seq: {min_seq}, max_seq: {max_seq}. "
                    f"This indicates sequence generator was not incremented for each row in batch."
                )
                self.assertEqual(
                    max_seq - min_seq + 1, row_count,
                    f"Sequence number range should match row count. "
                    f"File: {blob_file.file_name}, row_count: {row_count}, "
                    f"min_seq: {min_seq}, max_seq: {max_seq}, "
                    f"expected range: {row_count}, actual range: {max_seq - min_seq + 1}"
                )
            else:
                # For single row files, min_seq == max_seq is acceptable
                self.assertEqual(
                    min_seq, max_seq,
                    f"Single row file should have min_seq == max_seq. "
                    f"File: {blob_file.file_name}, min_seq: {min_seq}, max_seq: {max_seq}"
                )

        print("✅ Non-descriptor mode sequence number increment test passed")

    def test_blob_stats_schema_with_custom_column_name(self):
        from pypaimon import Schema

        # Create schema with blob column using a custom name (not 'blob_data')
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('my_custom_blob', pa.large_binary())  # Custom blob column name
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true'
        })

        self.catalog.create_table('test_db.blob_custom_name_test', schema, False)
        table = self.catalog.get_table('test_db.blob_custom_name_test')

        # Write data
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'my_custom_blob': [b'blob1', b'blob2', b'blob3']
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Extract blob files from commit messages
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]

        # Verify we have at least one blob file
        self.assertGreater(len(blob_files), 0, "Should have at least one blob file")

        # Verify that the stats schema uses the actual blob column name, not hardcoded 'blob_data'
        for blob_file in blob_files:
            value_stats = blob_file.value_stats
            if value_stats is not None and value_stats.min_values is not None:
                # Get the field names from the stats
                # The stats should use 'my_custom_blob', not 'blob_data'
                min_values = value_stats.min_values
                if hasattr(min_values, 'fields') and len(min_values.fields) > 0:
                    # Check if the field name matches the actual blob column name
                    field_name = min_values.fields[0].name
                    self.assertEqual(
                        field_name, 'my_custom_blob',
                        f"Blob stats field name should be 'my_custom_blob' (actual column name), "
                        f"but got '{field_name}'. This indicates the field name was hardcoded "
                        f"instead of using the blob_column parameter."
                    )

        # Verify data integrity
        result = table.new_read_builder().new_read().to_arrow(table.new_read_builder().new_scan().plan().splits())
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3])
        self.assertEqual(result.column('name').to_pylist(), ['Alice', 'Bob', 'Charlie'])

    def test_blob_file_name_format_with_shared_uuid_non_descriptor_mode(self):
        import random
        import re
        from pypaimon import Schema

        # Create schema with blob column (blob-as-descriptor=false)
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-as-descriptor': 'false',  # Non-descriptor mode
            'target-file-size': '1MB'  # Small target size to trigger multiple rollings
        })

        self.catalog.create_table('test_db.blob_file_name_test_non_desc', schema, False)
        table = self.catalog.get_table('test_db.blob_file_name_test_non_desc')

        num_blobs, blob_size = 5, 2 * 1024 * 1024
        random.seed(123)
        blob_data_list = []
        for i in range(num_blobs):
            blob_data = bytes(bytearray([random.randint(0, 255) for _ in range(blob_size)]))
            blob_data_list.append(blob_data)

        # Write data in batches to trigger multiple file rollings
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()

        # Write data that will trigger rolling
        test_data = pa.Table.from_pydict({
            'id': list(range(1, num_blobs + 1)),
            'name': [f'item_{i}' for i in range(1, num_blobs + 1)],
            'blob_data': blob_data_list
        }, schema=pa_schema)

        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Extract blob files from commit messages
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]

        # Should have at least one blob file (may have multiple if rolling occurred)
        self.assertGreaterEqual(len(blob_files), 1, "Should have at least one blob file")

        # Verify file name format: data-{uuid}-{count}.blob
        file_name_pattern = re.compile(
            r'^data-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-'
            r'[a-f0-9]{12}-(\d+)\.blob$'
        )

        first_file_name = blob_files[0].file_name
        self.assertTrue(
            file_name_pattern.match(first_file_name),
            f"File name should match expected format: data-{{uuid}}-{{count}}.blob, got: {first_file_name}"
        )

        # Extract UUID and counter from first file name
        first_match = file_name_pattern.match(first_file_name)
        first_counter = int(first_match.group(1))

        # Extract UUID (everything between "data-" and last "-")
        uuid_start = len("data-")
        uuid_end = first_file_name.rfind('-', uuid_start)
        shared_uuid = first_file_name[uuid_start:uuid_end]

        # Verify all blob files use the same UUID and have sequential counters
        for i, blob_file in enumerate(blob_files):
            file_name = blob_file.file_name
            match = file_name_pattern.match(file_name)

            self.assertIsNotNone(
                match,
                f"File name should match expected format: data-{{uuid}}-{{count}}.blob, got: {file_name}"
            )

            counter = int(match.group(1))

            # Extract UUID from this file
            file_uuid = file_name[uuid_start:file_name.rfind('-', uuid_start)]

            self.assertEqual(
                file_uuid,
                shared_uuid,
                f"All blob files should use the same UUID. Expected: {shared_uuid}, got: {file_uuid} in {file_name}"
            )

            self.assertEqual(
                counter,
                first_counter + i,
                f"File counter should be sequential. Expected: {first_counter + i}, got: {counter} in {file_name}"
            )

        # Verify data integrity
        result = table.new_read_builder().new_read().to_arrow(table.new_read_builder().new_scan().plan().splits())
        self.assertEqual(result.num_rows, num_blobs)
        self.assertEqual(result.column('id').to_pylist(), list(range(1, num_blobs + 1)))

    def test_blob_non_descriptor_target_file_size_rolling(self):
        """Test that blob.target-file-size is respected in non-descriptor mode."""
        from pypaimon import Schema

        # Create schema with blob column (non-descriptor mode)
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('blob_data', pa.large_binary()),
        ])

        # Test with blob.target-file-size set to a small value
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob.target-file-size': '1MB'  # Set blob-specific target file size
            }
        )

        self.catalog.create_table('test_db.blob_non_descriptor_rolling', schema, False)
        table = self.catalog.get_table('test_db.blob_non_descriptor_rolling')

        # Write multiple blobs that together exceed the target size
        # Each blob is 0.6MB, so 3 blobs = 1.8MB > 1MB target
        num_blobs = 3
        blob_size = int(0.6 * 1024 * 1024)  # 0.6MB per blob

        test_data = pa.Table.from_pydict({
            'id': list(range(1, num_blobs + 1)),
            'blob_data': [b'x' * blob_size for _ in range(num_blobs)]
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Extract blob files from commit messages
        all_files = [f for msg in commit_messages for f in msg.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]

        # The key test: verify that blob.target-file-size is used instead of target-file-size
        # If target-file-size (default 256MB for append-only) was used, we'd have 1 file
        # If blob.target-file-size (1MB) is used, we should have multiple files
        total_data_size = num_blobs * blob_size

        # Verify that the rolling logic used blob_target_file_size (1MB) not target_file_size (256MB)
        # If target_file_size was used, all data would fit in one file
        # If blob_target_file_size was used, data should be split
        if total_data_size > 1024 * 1024:  # Total > 1MB
            self.assertGreater(
                len(blob_files), 1,
                f"Should have multiple blob files when using blob.target-file-size (1MB). "
                f"Total data size: {total_data_size / 1024 / 1024:.2f}MB, "
                f"got {len(blob_files)} file(s). "
                f"This indicates blob.target-file-size was ignored and target-file-size was used instead."
            )

        # Verify data integrity
        result = table.new_read_builder().new_read().to_arrow(
            table.new_read_builder().new_scan().plan().splits()
        )
        self.assertEqual(result.num_rows, num_blobs)
        self.assertEqual(result.column('id').to_pylist(), list(range(1, num_blobs + 1)))

    def test_blob_write_read_large_data_with_rolling_with_shard(self):
        """
        Test writing and reading large blob data with file rolling and sharding.

        Test workflow:
        - Creates a table with blob column and 10MB target file size
        - Writes 4 batches of 40 records each (160 total records)
        - Each record contains a 3MB blob
        - Reads data using 3-way sharding (shard 0 - 3)
        - Verifies blob data integrity and size
        - Compares concatenated sharded results with full table scan
        """

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('record_id_of_batch', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob.target-file-size': '10MB'
            }
        )
        self.catalog.create_table('test_db.blob_rolling_with_shard', schema, False)
        table = self.catalog.get_table('test_db.blob_rolling_with_shard')

        # Create large blob data
        large_blob_size = 3 * 1024 * 1024
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        actual_size = len(large_blob_data)
        print(f"Created blob data: {actual_size:,} bytes ({actual_size / (1024 * 1024):.2f} MB)")
        # Write 4 batches of 40 records
        for i in range(4):
            write_builder = table.new_batch_write_builder()
            writer = write_builder.new_write()
            # Write 40 records
            for record_id in range(40):
                test_data = pa.Table.from_pydict({
                    'id': [i * 40 + record_id + 1],  # Unique ID for each row
                    'record_id_of_batch': [record_id],
                    'metadata': [f'Large blob batch {record_id + 1}'],
                    'large_blob': [large_blob_data]
                }, schema=pa_schema)
                writer.write_arrow(test_data)

            commit_messages = writer.prepare_commit()
            commit = write_builder.new_commit()
            commit.commit(commit_messages)
            writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan().with_shard(0, 3)
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data
        self.assertEqual(result.num_rows, 54, "Should have 54 rows")
        self.assertEqual(result.num_columns, 4, "Should have 4 columns")

        # Verify blob data integrity
        blob_data = result.column('large_blob').to_pylist()
        self.assertEqual(len(blob_data), 54, "Should have 54 blob records")
        # Verify each blob
        for i, blob in enumerate(blob_data):
            self.assertEqual(len(blob), len(large_blob_data), f"Blob {i + 1} should be {large_blob_size:,} bytes")
            self.assertEqual(blob, large_blob_data, f"Blob {i + 1} content should match exactly")
        splits = read_builder.new_scan().plan().splits()
        expected = table_read.to_arrow(splits)
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1)
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2)
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3)
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('id')

        # Verify the data
        self.assertEqual(actual.num_rows, 160, "Should have 160 rows")
        self.assertEqual(actual.num_columns, 4, "Should have 4 columns")
        self.assertEqual(actual.column('id').to_pylist(), list(range(1, 161)), "ID column should match")
        self.assertEqual(actual, expected)

    def test_blob_rolling_with_shard(self):
        """
        - Writes 30 records
        - Each record contains a 3MB blob
        """

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob.target-file-size': '10MB'
            }
        )
        self.catalog.create_table('test_db.blob_rolling_with_shard1', schema, False)
        table = self.catalog.get_table('test_db.blob_rolling_with_shard1')

        # Create large blob data
        large_blob_size = 3 * 1024 * 1024  #
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        actual_size = len(large_blob_data)
        print(f"Created blob data: {actual_size:,} bytes ({actual_size / (1024 * 1024):.2f} MB)")

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        # Write 30 records
        for record_id in range(30):
            test_data = pa.Table.from_pydict({
                'id': [record_id],  # Unique ID for each row
                'metadata': [f'Large blob batch {record_id + 1}'],
                'large_blob': [struct.pack('<I', record_id) + large_blob_data]
            }, schema=pa_schema)
            writer.write_arrow(test_data)

        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan().with_shard(1, 3)
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data
        self.assertEqual(result.num_rows, 10, "Should have 10 rows")
        self.assertEqual(result.num_columns, 3, "Should have 3 columns")

        # Verify blob data integrity
        blob_data = result.column('large_blob').to_pylist()
        self.assertEqual(len(blob_data), 10, "Should have 94 blob records")
        # Verify each blob
        for i, blob in enumerate(blob_data):
            self.assertEqual(len(blob), len(large_blob_data) + 4, f"Blob {i + 1} should be {large_blob_size:,} bytes")
        splits = read_builder.new_scan().plan().splits()
        expected = table_read.to_arrow(splits)
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1)
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2)
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3)
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('id')

        # Verify the data
        self.assertEqual(actual.num_rows, 30, "Should have 30 rows")
        self.assertEqual(actual.num_columns, 3, "Should have 3 columns")
        self.assertEqual(actual, expected)

    def test_blob_large_data_volume_with_shard(self):

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('batch_id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.blob_large_data_volume_with_shard', schema, False)
        table = self.catalog.get_table('test_db.blob_large_data_volume_with_shard')

        large_blob_size = 5 * 1024
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        num_row = 20000
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        expected = pa.Table.from_pydict({
            'id': [1] * num_row,
            'batch_id': [11] * num_row,
            'metadata': [f'Large blob batch {11}'] * num_row,
            'large_blob': [i.to_bytes(2, byteorder='little') + large_blob_data for i in range(num_row)]
        }, schema=pa_schema)
        writer.write_arrow(expected)

        # Commit all data at once
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan().with_shard(2, 3)
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify the data
        self.assertEqual(6666, result.num_rows)
        self.assertEqual(4, result.num_columns)

        self.assertEqual(expected.slice(13334, 6666), result)
        splits = read_builder.new_scan().plan().splits()
        expected = table_read.to_arrow(splits)
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1)
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2)
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3)
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('id')
        self.assertEqual(actual, expected)

    def test_data_blob_writer_with_shard(self):
        """Test DataBlobWriter with mixed data types in blob column."""

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('type', pa.string()),
            ('data', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true'
            }
        )
        self.catalog.create_table('test_db.with_shard_test', schema, False)
        table = self.catalog.get_table('test_db.with_shard_test')

        # Use proper table API to create writer
        write_builder = table.new_batch_write_builder()
        blob_writer = write_builder.new_write()

        # Test data with different types of blob content
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'type': ['text', 'json', 'binary', 'image', 'pdf'],
            'data': [
                b'This is text content',
                b'{"key": "value", "number": 42}',
                b'\x00\x01\x02\x03\xff\xfe\xfd',
                b'PNG_IMAGE_DATA_PLACEHOLDER',
                b'%PDF-1.4\nPDF_CONTENT_PLACEHOLDER'
            ]
        }, schema=pa_schema)

        # Write mixed data
        total_rows = 0
        for batch in test_data.to_batches():
            blob_writer.write_arrow_batch(batch)
            total_rows += batch.num_rows

        # Test prepare commit
        commit_messages = blob_writer.prepare_commit()
        # Create commit and commit the data
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        blob_writer.close()

        # Read data back using table API
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan().with_shard(1, 2)
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        result = table_read.to_arrow(splits)

        # Verify the data was read back correctly
        self.assertEqual(result.num_rows, 2, "Should have 2 rows")
        self.assertEqual(result.num_columns, 3, "Should have 3 columns")

    def test_blob_write_read_large_data_volume_rolling_with_shard(self):
        """
        - Writes 10000 records
        - Each record contains a 5KB blob
        - 'blob.target-file-size': '10MB'
        - each blob file contains 2000 records
        """
        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('batch_id', pa.int32()),
            ('metadata', pa.string()),
            ('large_blob', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob.target-file-size': '10MB'
            }
        )
        self.catalog.create_table('test_db.test_blob_write_read_large_data_volume_rolling_with_shard', schema, False)
        table = self.catalog.get_table('test_db.test_blob_write_read_large_data_volume_rolling_with_shard')

        # Create large blob data
        large_blob_size = 5 * 1024  #
        blob_pattern = b'LARGE_BLOB_PATTERN_' + b'X' * 1024  # ~1KB pattern
        pattern_size = len(blob_pattern)
        repetitions = large_blob_size // pattern_size
        large_blob_data = blob_pattern * repetitions

        actual_size = len(large_blob_data)
        print(f"Created blob data: {actual_size:,} bytes ({actual_size / (1024 * 1024):.2f} MB)")
        # Write 10000 records of data
        num_row = 10000
        expected = pa.Table.from_pydict({
            'id': [1] * num_row,
            'batch_id': [11] * num_row,
            'metadata': [f'Large blob batch {11}'] * num_row,
            'large_blob': [i.to_bytes(2, byteorder='little') + large_blob_data for i in range(num_row)]
        }, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(expected)

        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())
        self.assertEqual(expected, result)

        splits = read_builder.new_scan().plan().splits()
        expected = table_read.to_arrow(splits)
        splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        actual1 = table_read.to_arrow(splits1)
        splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
        actual2 = table_read.to_arrow(splits2)
        splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()
        actual3 = table_read.to_arrow(splits3)
        actual = pa.concat_tables([actual1, actual2, actual3]).sort_by('id')

        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
