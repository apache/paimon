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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class ExternalStorageBlobValidationTest(unittest.TestCase):
    """Tests for blob-external-storage-field schema validation."""

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('test_db', False)

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree(cls.temp_dir)
        except OSError:
            pass

    def test_validation_missing_path(self):
        """blob-external-storage-field configured without path should raise error."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('video', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-descriptor-field': 'video',
            'blob-external-storage-field': 'video',
            # Missing blob-external-storage-path
        })
        with self.assertRaises(ValueError) as ctx:
            self.catalog.create_table('test_db.missing_path_test', schema, False)
        self.assertIn('blob-external-storage-path', str(ctx.exception))

    def test_validation_field_not_in_descriptor_field(self):
        """blob-external-storage-field must be a subset of blob-descriptor-field."""
        external_path = os.path.join(self.temp_dir, 'external_storage')
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('video', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            # NOT setting blob-descriptor-field
            'blob-external-storage-field': 'video',
            'blob-external-storage-path': external_path,
        })
        with self.assertRaises(ValueError) as ctx:
            self.catalog.create_table('test_db.not_in_descriptor_test', schema, False)
        self.assertIn('blob-descriptor-field', str(ctx.exception))

    def test_validation_field_not_blob_type(self):
        """blob-external-storage-field must reference BLOB type fields."""
        external_path = os.path.join(self.temp_dir, 'external_storage')
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('video', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-descriptor-field': 'name,video',
            'blob-external-storage-field': 'name',
            'blob-external-storage-path': external_path,
        })
        with self.assertRaises(ValueError) as ctx:
            self.catalog.create_table('test_db.not_blob_type_test', schema, False)
        self.assertIn('must be blob fields', str(ctx.exception))

    def test_validation_blob_not_null_field_passes(self):
        """BLOB NOT NULL fields should pass validation (not be rejected by str comparison)."""
        from pypaimon.schema.data_types import AtomicType, DataField

        external_path = os.path.join(self.temp_dir, 'external_storage')
        schema = Schema(
            fields=[
                DataField(0, 'id', AtomicType('INT', nullable=False)),
                DataField(1, 'video', AtomicType('BLOB', nullable=False)),
            ],
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob-descriptor-field': 'video',
                'blob-external-storage-field': 'video',
                'blob-external-storage-path': external_path,
            },
        )
        # Should NOT raise - BLOB NOT NULL is still a BLOB type
        self.catalog.create_table('test_db.blob_not_null_test', schema, False)
        table = self.catalog.get_table('test_db.blob_not_null_test')
        self.assertIsNotNone(table)


class ExternalStorageBlobWriteTest(unittest.TestCase):
    """Tests for blob external storage write functionality."""

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, 'warehouse')
        cls.external_path = os.path.join(cls.temp_dir, 'external_storage')
        os.makedirs(cls.external_path, exist_ok=True)
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('test_db', False)

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree(cls.temp_dir)
        except OSError:
            pass

    def _create_external_storage_table(self, table_name, extra_options=None):
        """Helper to create a table with external storage configured."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('video', pa.large_binary()),
        ])
        options = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-descriptor-field': 'video',
            'blob-external-storage-field': 'video',
            'blob-external-storage-path': self.external_path,
        }
        if extra_options:
            options.update(extra_options)
        schema = Schema.from_pyarrow_schema(pa_schema, options=options)
        self.catalog.create_table(f'test_db.{table_name}', schema, False)
        return self.catalog.get_table(f'test_db.{table_name}')

    def test_external_storage_basic_write(self):
        """Basic write: raw blob data should be written to external storage as .blob files."""
        table = self._create_external_storage_table('basic_write_test')

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('video', pa.large_binary()),
        ])
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'video': [b'video_data_1', b'video_data_2', b'video_data_3'],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        writer.close()

        # Commit should succeed
        self.assertGreater(len(commit_messages), 0)
        write_builder.new_commit().commit(commit_messages)

        # Verify external storage files were created
        external_files = []
        for root, dirs, files in os.walk(self.external_path):
            for f in files:
                if f.endswith('.blob'):
                    external_files.append(os.path.join(root, f))
        self.assertGreater(len(external_files), 0, "External blob files should be created")

    def test_external_storage_roundtrip(self):
        """Write raw blob data via external storage, read back should return original data."""
        table = self._create_external_storage_table('roundtrip_test')

        video_bytes = b'hello_world_video_content'
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('video', pa.large_binary()),
        ])
        test_data = pa.Table.from_pydict({
            'id': [1],
            'name': ['test'],
            'video': [video_bytes],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        writer.close()
        write_builder.new_commit().commit(commit_messages)

        # Read back - reader resolves BlobDescriptor and returns original data
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 1)
        read_back = result.column('video')[0].as_py()
        self.assertEqual(read_back, video_bytes)

    def test_external_storage_multiple_fields(self):
        """Multiple external storage fields should each write to separate blob files."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('video', pa.large_binary()),
            ('audio', pa.large_binary()),
        ])
        options = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-descriptor-field': 'video,audio',
            'blob-external-storage-field': 'video,audio',
            'blob-external-storage-path': self.external_path,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, options=options)
        self.catalog.create_table('test_db.multi_field_test', schema, False)
        table = self.catalog.get_table('test_db.multi_field_test')

        test_data = pa.Table.from_pydict({
            'id': [1, 2],
            'video': [b'video1', b'video2'],
            'audio': [b'audio1', b'audio2'],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        writer.close()
        write_builder.new_commit().commit(commit_messages)

        # Read back and verify data round-trips correctly
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 2)
        videos = result.column('video').to_pylist()
        audios = result.column('audio').to_pylist()
        self.assertEqual(set(videos), {b'video1', b'video2'})
        self.assertEqual(set(audios), {b'audio1', b'audio2'})

    def test_external_storage_mixed_with_normal_blob(self):
        """External storage field + normal blob field should coexist."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('video', pa.large_binary()),      # external storage
            ('thumbnail', pa.large_binary()),   # normal blob (written to .blob files)
        ])
        options = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-descriptor-field': 'video',
            'blob-external-storage-field': 'video',
            'blob-external-storage-path': self.external_path,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, options=options)
        self.catalog.create_table('test_db.mixed_blob_test', schema, False)
        table = self.catalog.get_table('test_db.mixed_blob_test')

        test_data = pa.Table.from_pydict({
            'id': [1, 2],
            'video': [b'big_video_data', b'another_video'],
            'thumbnail': [b'thumb1', b'thumb2'],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        writer.close()
        write_builder.new_commit().commit(commit_messages)

        # Read back and verify both external storage and normal blob data
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 2)
        videos = set(result.column('video').to_pylist())
        thumbnails = set(result.column('thumbnail').to_pylist())
        self.assertEqual(videos, {b'big_video_data', b'another_video'})
        self.assertEqual(thumbnails, {b'thumb1', b'thumb2'})

    def test_external_storage_null_values(self):
        """Null blob values should remain null (not written to external storage)."""
        table = self._create_external_storage_table('null_test')

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('video', pa.large_binary()),
        ])
        test_data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'video': [b'data', None, b'more_data'],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        writer.close()
        write_builder.new_commit().commit(commit_messages)

        # Read back and verify nulls are preserved
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 3)
        # Build id → video mapping to avoid relying on row order
        id_to_video = {
            result.column('id')[i].as_py(): result.column('video')[i].as_py()
            for i in range(result.num_rows)
        }
        self.assertEqual(id_to_video[1], b'data')
        self.assertIsNone(id_to_video[2])
        self.assertEqual(id_to_video[3], b'more_data')

    def test_external_storage_with_descriptor_input(self):
        """When input is serialized BlobDescriptor bytes, the writer should read
        the source data via BlobRef and re-write it to external storage."""
        from pypaimon.table.row.blob import BlobDescriptor

        table = self._create_external_storage_table('descriptor_input_test')

        # Create a source file with known raw content
        source_data = b'original_video_from_descriptor'
        source_file = os.path.join(self.external_path, 'source.bin')
        with open(source_file, 'wb') as f:
            f.write(source_data)

        # Construct a BlobDescriptor pointing to the source file
        descriptor = BlobDescriptor(source_file, 0, len(source_data))

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('video', pa.large_binary()),
        ])
        test_data = pa.Table.from_pydict({
            'id': [1],
            'name': ['desc_test'],
            'video': [descriptor.serialize()],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        writer.close()
        write_builder.new_commit().commit(commit_messages)

        # Read back and verify the original data round-trips correctly
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 1)
        self.assertEqual(result.column('video')[0].as_py(), source_data)


if __name__ == '__main__':
    unittest.main()
