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

import pyarrow as pa

from pypaimon import Schema
from pypaimon.table.object import ObjectTable
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTObjectTableTest(RESTBaseTest):

    def _create_object_table(self, table_name, extra_options=None):
        """Helper to create an object table with the given name."""
        options = {"type": "object-table"}
        if extra_options:
            options.update(extra_options)
        schema = Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int32())]),
            options=options,
        )
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        return self.rest_catalog.get_table(table_name)

    def _get_table_location_dir(self, table_name):
        """Get the filesystem directory for a table's location."""
        parts = table_name.split(".")
        database_name = parts[0]
        simple_table_name = parts[1]
        return os.path.join(self.warehouse, database_name, simple_table_name)

    def test_get_object_table(self):
        table_name = "default.object_table_basic"
        table = self._create_object_table(table_name)

        self.assertIsInstance(table, ObjectTable)
        self.assertEqual(table.name(), "object_table_basic")
        self.assertEqual(table.full_name(), table_name)
        self.assertEqual(table.partition_keys, [])
        self.assertEqual(table.primary_keys, [])
        self.assertEqual(table.options().get("type"), "object-table")

    def test_object_table_read_files(self):
        table_name = "default.object_table_read"
        table = self._create_object_table(table_name)

        # Write some test files into the table's location directory
        location_dir = self._get_table_location_dir(table_name)
        os.makedirs(location_dir, exist_ok=True)

        test_files = {
            "file_a.txt": b"hello world",
            "file_b.dat": b"some binary data here",
        }
        for filename, content in test_files.items():
            filepath = os.path.join(location_dir, filename)
            with open(filepath, "wb") as f:
                f.write(content)

        # Read the object table
        read_builder = table.new_read_builder()
        scan = read_builder.new_scan()
        plan = scan.plan()
        splits = plan.splits()

        self.assertEqual(len(splits), 1)

        table_read = read_builder.new_read()
        result = table_read.to_arrow(splits)

        self.assertIsInstance(result, pa.Table)
        # Should contain the schema, snapshot and manifest files plus our test files
        # Filter to only our test files
        result_names = result.column("name").to_pylist()
        for filename in test_files:
            self.assertIn(filename, result_names)

        # Verify file sizes for our test files
        result_dict = {}
        for i in range(result.num_rows):
            name = result.column("name")[i].as_py()
            length = result.column("length")[i].as_py()
            result_dict[name] = length

        for filename, content in test_files.items():
            self.assertEqual(result_dict[filename], len(content))

        # Verify schema columns
        self.assertIn("path", result.column_names)
        self.assertIn("name", result.column_names)
        self.assertIn("length", result.column_names)
        self.assertIn("mtime", result.column_names)
        self.assertIn("atime", result.column_names)
        self.assertIn("owner", result.column_names)

    def test_object_table_read_with_subdirectories(self):
        table_name = "default.object_table_subdir"
        table = self._create_object_table(table_name)

        location_dir = self._get_table_location_dir(table_name)
        sub_dir = os.path.join(location_dir, "subdir")
        os.makedirs(sub_dir, exist_ok=True)

        with open(os.path.join(location_dir, "root_file.txt"), "wb") as f:
            f.write(b"root content")
        with open(os.path.join(sub_dir, "nested_file.txt"), "wb") as f:
            f.write(b"nested content")

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        result_names = result.column("name").to_pylist()
        self.assertIn("root_file.txt", result_names)
        self.assertIn("nested_file.txt", result_names)

        # Verify relative paths contain subdirectory
        result_paths = result.column("path").to_pylist()
        nested_paths = [p for p in result_paths if "nested_file.txt" in p]
        self.assertTrue(len(nested_paths) > 0)
        self.assertIn("subdir/", nested_paths[0])

    def test_object_table_with_projection(self):
        table_name = "default.object_table_projection"
        table = self._create_object_table(table_name)

        location_dir = self._get_table_location_dir(table_name)
        os.makedirs(location_dir, exist_ok=True)
        with open(os.path.join(location_dir, "proj_test.txt"), "wb") as f:
            f.write(b"test data")

        # Test projection with two columns
        read_builder = table.new_read_builder()
        read_builder.with_projection(["name", "length"])
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.column_names, ["name", "length"])
        self.assertNotIn("path", result.column_names)
        self.assertNotIn("mtime", result.column_names)
        self.assertNotIn("atime", result.column_names)
        self.assertNotIn("owner", result.column_names)

        # Verify data content for our test file
        result_names = result.column("name").to_pylist()
        self.assertIn("proj_test.txt", result_names)
        idx = result_names.index("proj_test.txt")
        self.assertEqual(result.column("length")[idx].as_py(), len(b"test data"))

    def test_object_table_with_limit(self):
        table_name = "default.object_table_limit"
        table = self._create_object_table(table_name)

        location_dir = self._get_table_location_dir(table_name)
        os.makedirs(location_dir, exist_ok=True)
        for i in range(5):
            with open(os.path.join(location_dir, f"file_{i}.txt"), "wb") as f:
                f.write(f"content {i}".encode())

        read_builder = table.new_read_builder()
        read_builder.with_limit(2)
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 2)

    def test_object_table_options_and_copy(self):
        table_name = "default.object_table_options"
        table = self._create_object_table(
            table_name, extra_options={"custom.key": "custom_value"}
        )

        self.assertEqual(table.options().get("custom.key"), "custom_value")
        self.assertEqual(table.options().get("type"), "object-table")

        # Test copy with dynamic options
        copied = table.copy({"new.key": "new_value"})
        self.assertIsInstance(copied, ObjectTable)
        self.assertEqual(copied.options().get("custom.key"), "custom_value")
        self.assertEqual(copied.options().get("new.key"), "new_value")

        # Original should not be modified
        self.assertIsNone(table.options().get("new.key"))

        # Test copy with override
        overridden = table.copy({"custom.key": "overridden"})
        self.assertEqual(overridden.options().get("custom.key"), "overridden")
        self.assertEqual(table.options().get("custom.key"), "custom_value")

    def test_object_table_unsupported_write(self):
        table_name = "default.object_table_no_write"
        table = self._create_object_table(table_name)

        with self.assertRaises(NotImplementedError):
            table.new_batch_write_builder()
        with self.assertRaises(NotImplementedError):
            table.new_stream_write_builder()

    def test_object_table_unsupported_drop_partitions(self):
        table_name = "default.object_table_no_drop_partitions"
        self._create_object_table(table_name)

        with self.assertRaisesRegex(
            ValueError,
            "drop_partitions is not supported for table type 'ObjectTable'",
        ):
            self.rest_catalog.drop_partitions(
                table_name,
                [{"dt": "20250101"}],
            )

    def test_object_table_to_pandas(self):
        table_name = "default.object_table_pandas"
        table = self._create_object_table(table_name)

        location_dir = self._get_table_location_dir(table_name)
        os.makedirs(location_dir, exist_ok=True)
        with open(os.path.join(location_dir, "pandas_test.txt"), "wb") as f:
            f.write(b"pandas data")

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result_df = read_builder.new_read().to_pandas(splits)

        self.assertIn("name", result_df.columns)
        self.assertIn("pandas_test.txt", result_df["name"].values)
