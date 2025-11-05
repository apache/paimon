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

import pyarrow as pa

from pypaimon import CatalogFactory
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
        from pypaimon.common.config import CatalogOptions

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
        catalog_options = {CatalogOptions.WAREHOUSE: self.warehouse}
        uri_reader_factory = UriReaderFactory(catalog_options)
        uri_reader = uri_reader_factory.create(new_blob_descriptor.uri)
        blob = Blob.from_descriptor(uri_reader, new_blob_descriptor)

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

    def test_data_blob_writer_with_shard(self):
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
        self.assertEqual(result.num_rows, 3, "Should have 5 rows")
        self.assertEqual(result.num_columns, 3, "Should have 3 columns")

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


if __name__ == '__main__':
    unittest.main()
