#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions


class SimpleTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('pt', pa.int32()),
            ('k', pa.int32()),
            ('v', pa.int64())
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_tag_scan(self):
        """
        Test reading from a specific tag.

        1. Write data in 3 commits
        2. Create a tag at snapshot 2
        3. Read from the tag and verify only data from snapshots 1 and 2 is returned
        """
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={'bucket': '3'}
        )
        self.catalog.create_table('default.test_tag_scan', schema, False)
        table = self.catalog.get_table('default.test_tag_scan')

        write_builder = table.new_batch_write_builder()

        # First commit: pt=1, k=10, v=100 and pt=1, k=20, v=200
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = pa.Table.from_pydict({
            'pt': [1, 1],
            'k': [10, 20],
            'v': [100, 200]
        }, schema=self.pa_schema)
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Second commit: pt=2, k=30, v=101 and pt=2, k=40, v=201
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'pt': [2, 2],
            'k': [30, 40],
            'v': [101, 201]
        }, schema=self.pa_schema)
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Third commit: pt=3, k=50, v=500 and pt=3, k=60, v=600
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data3 = pa.Table.from_pydict({
            'pt': [3, 3],
            'k': [50, 60],
            'v': [500, 600]
        }, schema=self.pa_schema)
        table_write.write_arrow(data3)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Create tag at snapshot 2
        table.create_tag("tag2", snapshot_id=2)

        # Read from tag2 using scan.tag-name option
        table_with_tag = table.copy({CoreOptions.SCAN_TAG_NAME.key(): "tag2"})
        read_builder = table_with_tag.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_arrow(table_scan.plan().splits())

        # Verify: should only contain data from snapshot 1 and 2
        # (pt=1, k=10, v=100), (pt=1, k=20, v=200), (pt=2, k=30, v=101), (pt=2, k=40, v=201)
        result_sorted = result.sort_by([('pt', 'ascending'), ('k', 'ascending')])

        expected = pa.Table.from_pydict({
            'pt': [1, 1, 2, 2],
            'k': [10, 20, 30, 40],
            'v': [100, 200, 101, 201]
        }, schema=self.pa_schema)

        self.assertEqual(result_sorted.num_rows, 4)
        self.assertEqual(result_sorted.column('pt').to_pylist(), expected.column('pt').to_pylist())
        self.assertEqual(result_sorted.column('k').to_pylist(), expected.column('k').to_pylist())
        self.assertEqual(result_sorted.column('v').to_pylist(), expected.column('v').to_pylist())

    def test_non_existing_tag(self):
        """
        Test that reading from a non-existing tag raises an error.
        """
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={'bucket': '3'}
        )
        self.catalog.create_table('default.test_non_existing_tag', schema, False)
        table = self.catalog.get_table('default.test_non_existing_tag')

        # Try to read from a non-existing tag
        table_with_tag = table.copy({CoreOptions.SCAN_TAG_NAME.key(): "non-existing"})
        read_builder = table_with_tag.new_read_builder()
        table_scan = read_builder.new_scan()

        with self.assertRaises(ValueError) as context:
            table_scan.plan()

        self.assertIn("non-existing", str(context.exception))
        self.assertIn("doesn't exist", str(context.exception))

    def test_tag_create_and_delete(self):
        """Test creating and deleting tags."""
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={'bucket': '3'}
        )
        self.catalog.create_table('default.test_tag_create_delete', schema, False)
        table = self.catalog.get_table('default.test_tag_create_delete')

        write_builder = table.new_batch_write_builder()

        # Write some data
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'pt': [1, 1],
            'k': [10, 20],
            'v': [100, 200]
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Create a tag
        table.create_tag("test_tag")

        # Verify tag exists
        tag_manager = table.tag_manager()
        self.assertTrue(tag_manager.tag_exists("test_tag"))

        # Get the tag
        tag = tag_manager.get("test_tag")
        self.assertIsNotNone(tag)
        self.assertEqual(tag.id, 1)

        # Delete the tag
        result = table.delete_tag("test_tag")
        self.assertTrue(result)

        # Verify tag no longer exists
        self.assertFalse(tag_manager.tag_exists("test_tag"))

    def test_tag_ignore_if_exists(self):
        """Test creating a tag with ignore_if_exists=True."""
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={'bucket': '3'}
        )
        self.catalog.create_table('default.test_tag_ignore_exists', schema, False)
        table = self.catalog.get_table('default.test_tag_ignore_exists')

        write_builder = table.new_batch_write_builder()

        # Write some data
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'pt': [1],
            'k': [10],
            'v': [100]
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Create a tag
        table.create_tag("duplicate_tag")

        # Try to create the same tag again without ignore_if_exists - should raise error
        with self.assertRaises(ValueError) as context:
            table.create_tag("duplicate_tag")
        self.assertIn("already exists", str(context.exception))

        # Create the same tag with ignore_if_exists=True - should not raise error
        table.create_tag("duplicate_tag", ignore_if_exists=True)
