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

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog import Identifier
from pypaimon.common.options.core_options import CoreOptions, ExternalPathStrategy
from pypaimon.common.external_path_provider import ExternalPathProvider


class ExternalPathProviderTest(unittest.TestCase):
    """Test ExternalPathProvider functionality."""

    def test_path_selection_and_structure(self):
        """Test path selection (round-robin) and path structure with various scenarios."""
        # Test multiple paths with round-robin
        external_paths = [
            "oss://bucket1/external",
            "oss://bucket2/external",
            "oss://bucket3/external",
        ]
        relative_path = "partition=value/bucket-0"
        provider = ExternalPathProvider(external_paths, relative_path)

        paths = [provider.get_next_external_data_path("file.parquet") for _ in range(6)]

        # Verify all buckets are used (2 cycles = 2 times each)
        bucket_counts = {f"bucket{i}": sum(1 for p in paths if f"bucket{i}" in p) for i in [1, 2, 3]}
        self.assertEqual(bucket_counts["bucket1"], 2)
        self.assertEqual(bucket_counts["bucket2"], 2)
        self.assertEqual(bucket_counts["bucket3"], 2)

        # Verify path structure
        self.assertIn("partition=value", paths[0])
        self.assertIn("bucket-0", paths[0])
        self.assertIn("file.parquet", paths[0])

        # Test single path
        single_provider = ExternalPathProvider(["oss://bucket/external"], "bucket-0")
        single_path = single_provider.get_next_external_data_path("data.parquet")
        self.assertIn("bucket/external", single_path)
        self.assertIn("bucket-0", single_path)
        self.assertIn("data.parquet", single_path)

        # Test empty relative path
        empty_provider = ExternalPathProvider(["oss://bucket/external"], "")
        empty_path = empty_provider.get_next_external_data_path("file.parquet")
        self.assertIn("bucket/external", empty_path)
        self.assertIn("file.parquet", empty_path)


class ExternalPathsConfigTest(unittest.TestCase):
    """Test external paths configuration parsing through FileStoreTable._create_external_paths()."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("test_db", False)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _create_table_with_options(self, options: dict) -> 'FileStoreTable':
        """Helper method to create a table with specific options."""
        table_name = "test_db.config_test"
        # Manually delete table directory if it exists to ensure clean test environment
        # FileSystemCatalog doesn't have drop_table method, so we need to delete manually
        try:
            table_path = self.catalog.get_table_path(Identifier.from_string(table_name))
            # file_io.exists and delete accept Union[Path, URL, str]
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass  # Table may not exist, ignore
        pa_schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options=options)
        self.catalog.create_table(table_name, schema, True)
        return self.catalog.get_table(table_name)

    def test_external_paths_strategies(self):
        """Test different external path strategies (round-robin, specific-fs, none)."""
        # Test round-robin strategy
        options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "oss://bucket1/path1,oss://bucket2/path2,oss://bucket3/path3",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        table = self._create_table_with_options(options)
        paths = table._create_external_paths()
        self.assertEqual(len(paths), 3)
        self.assertEqual(str(paths[0]), "oss://bucket1/path1")
        self.assertEqual(str(paths[1]), "oss://bucket2/path2")
        self.assertEqual(str(paths[2]), "oss://bucket3/path3")

        # Test specific-fs strategy
        options2 = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "oss://bucket1/path1,s3://bucket2/path2,oss://bucket3/path3",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.SPECIFIC_FS,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(): "oss",
        }
        table2 = self._create_table_with_options(options2)
        paths2 = table2._create_external_paths()
        self.assertEqual(len(paths2), 2)
        self.assertIn("oss://bucket1/path1", [str(p) for p in paths2])
        self.assertIn("oss://bucket3/path3", [str(p) for p in paths2])
        self.assertNotIn("s3://bucket2/path2", [str(p) for p in paths2])

        # Test none strategy
        options3 = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "oss://bucket1/path1",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.NONE,
        }
        table3 = self._create_table_with_options(options3)
        paths3 = table3._create_external_paths()
        self.assertEqual(len(paths3), 0)

    def test_external_paths_edge_cases(self):
        """Test edge cases: empty string, no config, invalid scheme."""
        # Test empty string
        options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        table = self._create_table_with_options(options)
        self.assertEqual(len(table._create_external_paths()), 0)

        # Test no external paths option
        options2 = {CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN}
        table2 = self._create_table_with_options(options2)
        self.assertEqual(len(table2._create_external_paths()), 0)

        # Test invalid scheme (no scheme)
        options3 = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "/invalid/path",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        table3 = self._create_table_with_options(options3)
        with self.assertRaises(ValueError) as context:
            table3._create_external_paths()
        self.assertIn("scheme", str(context.exception))

    def test_create_external_path_provider(self):
        """Test creating ExternalPathProvider from path factory."""
        table_name = "test_db.config_test"
        # Drop table if exists to ensure clean test environment
        try:
            self.catalog.drop_table(table_name, True)
        except Exception:
            pass  # Table may not exist, ignore
        options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "oss://bucket1/path1,oss://bucket2/path2",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("dt", pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"], options=options)
        self.catalog.create_table(table_name, schema, True)
        table = self.catalog.get_table(table_name)
        path_factory = table.path_factory()

        # Test with external paths configured
        provider = path_factory.create_external_path_provider(("value1",), 0)
        self.assertIsNotNone(provider)
        path = provider.get_next_external_data_path("file.parquet")
        self.assertTrue("bucket1" in str(path) or "bucket2" in str(path))
        self.assertIn("dt=value1", str(path))
        self.assertIn("bucket-0", str(path))

        # Test with none strategy (should return None)
        # Use a different table name to avoid conflicts
        options2 = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): "oss://bucket1/path1",
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.NONE,
        }
        pa_schema2 = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema2 = Schema.from_pyarrow_schema(pa_schema2, options=options2)
        table_name2 = "test_db.config_test_none"
        try:
            # Manually delete table directory if it exists
            table_path2 = self.catalog.get_table_path(Identifier.from_string(table_name2))
            # file_io.exists and delete accept Union[Path, URL, str]
            if self.catalog.file_io.exists(table_path2):
                self.catalog.file_io.delete(table_path2, recursive=True)
        except Exception:
            pass  # Table may not exist, ignore
        self.catalog.create_table(table_name2, schema2, True)
        table2 = self.catalog.get_table(table_name2)
        provider2 = table2.path_factory().create_external_path_provider((), 0)
        self.assertIsNone(provider2)

        # Test without external paths config (should return None)
        # Use a different table name to avoid conflicts
        options3 = {}
        pa_schema3 = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema3 = Schema.from_pyarrow_schema(pa_schema3, options=options3)
        table_name3 = "test_db.config_test_empty"
        try:
            # Manually delete table directory if it exists
            table_path3 = self.catalog.get_table_path(Identifier.from_string(table_name3))
            # file_io.exists and delete accept Union[Path, URL, str]
            if self.catalog.file_io.exists(table_path3):
                self.catalog.file_io.delete(table_path3, recursive=True)
        except Exception:
            pass  # Table may not exist, ignore
        self.catalog.create_table(table_name3, schema3, True)
        table3 = self.catalog.get_table(table_name3)
        provider3 = table3.path_factory().create_external_path_provider((), 0)
        self.assertIsNone(provider3)


class ExternalPathsIntegrationTest(unittest.TestCase):
    """Integration tests for external paths feature."""

    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.external_dir = os.path.join(cls.temp_dir, "external_data")

        # Create external directory
        os.makedirs(cls.external_dir, exist_ok=True)

        cls.catalog = CatalogFactory.create({
            "warehouse": cls.warehouse
        })
        cls.catalog.create_database("test_db", False)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def test_write_with_external_paths(self):
        """Test writing data with external paths configured."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])

        # Create table with external paths
        external_path = f"file://{self.external_dir}"
        table_options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): external_path,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, options=table_options)

        self.catalog.create_table("test_db.external_test", schema, False)
        table = self.catalog.get_table("test_db.external_test")

        # Write data (use explicit schema to match table schema)
        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.7],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()

        # Verify external_path is set in file metadata
        self.assertGreater(len(commit_messages), 0)
        for commit_msg in commit_messages:
            self.assertGreater(len(commit_msg.new_files), 0)
            for file_meta in commit_msg.new_files:
                # External path should be set
                self.assertIsNotNone(file_meta.external_path)
                self.assertTrue(file_meta.external_path.startswith("file://"))
                self.assertIn(self.external_dir, file_meta.external_path)

        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

    def test_read_with_external_paths(self):
        """Test reading data with external paths."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
        ])

        # Create table with external paths
        external_path = f"file://{self.external_dir}"
        table_options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): external_path,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, options=table_options)

        self.catalog.create_table("test_db.external_read_test", schema, False)
        table = self.catalog.get_table("test_db.external_read_test")

        # Write data (use explicit schema to match table schema)
        write_data = pa.Table.from_pydict({
            "id": [1, 2, 3, 4, 5],
            "name": ["A", "B", "C", "D", "E"],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(write_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify data
        self.assertEqual(result.num_rows, 5)
        self.assertEqual(result.num_columns, 2)
        self.assertListEqual(result.column("id").to_pylist(), [1, 2, 3, 4, 5])
        self.assertListEqual(result.column("name").to_pylist(), ["A", "B", "C", "D", "E"])

    def test_write_without_external_paths(self):
        """Test that writing without external paths still works."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table("test_db.normal_test", schema, False)
        table = self.catalog.get_table("test_db.normal_test")

        # Write data (use explicit schema to match table schema)
        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["X", "Y", "Z"],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()

        # Verify external_path is None (not configured)
        for commit_msg in commit_messages:
            for file_meta in commit_msg.new_files:
                self.assertIsNone(file_meta.external_path)

        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

    def test_external_paths_with_partition(self):
        """Test external paths with partitioned table."""
        pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("dt", pa.string()),
        ])

        external_path = f"file://{self.external_dir}"
        table_options = {
            CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(): external_path,
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(): ExternalPathStrategy.ROUND_ROBIN,
        }
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"], options=table_options)

        self.catalog.create_table("test_db.partitioned_external", schema, False)
        table = self.catalog.get_table("test_db.partitioned_external")

        # Write data with partition (use explicit schema to match table schema)
        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-02"],
        }, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()

        # Verify external paths include partition info
        for commit_msg in commit_messages:
            for file_meta in commit_msg.new_files:
                self.assertIsNotNone(file_meta.external_path)
                # Should contain partition path
                self.assertIn("dt=", file_meta.external_path)

        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()


if __name__ == "__main__":
    unittest.main()
