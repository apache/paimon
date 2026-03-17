# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import (DatabaseAlreadyExistException,
                                                DatabaseNotExistException,
                                                TableAlreadyExistException,
                                                TableNotExistException)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.file_store_table import FileStoreTable


class FileSystemCatalogTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.warehouse = os.path.join(self.temp_dir, 'test_dir')

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_database(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db"))

        with self.assertRaises(DatabaseAlreadyExistException):
            catalog.create_database("test_db", False)

        catalog.create_database("test_db", True)

        with self.assertRaises(DatabaseNotExistException):
            catalog.get_database("test_db_x")

        database = catalog.get_database("test_db")
        self.assertEqual(database.name, "test_db")

    def test_table(self):
        fields = [
            DataField.from_dict({"id": 1, "name": "f0", "type": "INT"}),
            DataField.from_dict({"id": 2, "name": "f1", "type": "INT"}),
            DataField.from_dict({"id": 3, "name": "f2", "type": "STRING"}),
        ]
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)
        catalog.create_table("test_db.test_table", Schema(fields=fields), False)
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db/test_table/schema/schema-0"))

        with self.assertRaises(TableAlreadyExistException):
            catalog.create_table("test_db.test_table", Schema(fields=fields), False)

        catalog.create_table("test_db.test_table", Schema(fields=fields), True)

        database = catalog.get_database("test_db")
        self.assertEqual(database.name, "test_db")

        with self.assertRaises(TableNotExistException):
            catalog.get_table("test_db.test_table_x")

        table = catalog.get_table("test_db.test_table")
        self.assertTrue(table is not None)
        self.assertTrue(isinstance(table, FileStoreTable))
        self.assertEqual(table.fields[2].name, "f2")
        self.assertTrue(isinstance(table.fields[2].type, AtomicType))
        self.assertEqual(table.fields[2].type.type, "STRING")

    def test_alter_table(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)

        identifier = "test_db.test_table"
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "col1", "type": "STRING", "description": "field1"}),
                DataField.from_dict({"id": 1, "name": "col2", "type": "STRING", "description": "field2"})
            ],
            partition_keys=[],
            primary_keys=[],
            options={},
            comment="comment"
        )
        catalog.create_table(identifier, schema, False)

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("col3", AtomicType("DATE"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 3)
        self.assertEqual(table.fields[2].name, "col3")
        self.assertEqual(table.fields[2].type.type, "DATE")

        catalog.alter_table(
            identifier,
            [SchemaChange.update_comment("new comment")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.table_schema.comment, "new comment")

        catalog.alter_table(
            identifier,
            [SchemaChange.rename_column("col1", "new_col1")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[0].name, "new_col1")

        catalog.alter_table(
            identifier,
            [SchemaChange.update_column_type("col2", AtomicType("BIGINT"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[1].type.type, "BIGINT")

        catalog.alter_table(
            identifier,
            [SchemaChange.update_column_comment("col2", "col2 field")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.fields[1].description, "col2 field")

        catalog.alter_table(
            identifier,
            [SchemaChange.set_option("write-buffer-size", "256 MB")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(table.table_schema.options.get("write-buffer-size"), "256 MB")

        catalog.alter_table(
            identifier,
            [SchemaChange.remove_option("write-buffer-size")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertNotIn("write-buffer-size", table.table_schema.options)

        catalog.alter_table(
            identifier,
            [SchemaChange.drop_column("col3")],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 2)

    def test_add_column_before_partition(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)

        identifier = "test_db.test_table"
        schema = Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "col1", "type": "STRING", "description": "field1"}),
                DataField.from_dict(
                    {"id": 1, "name": "partition_col", "type": "STRING", "description": "partition field"})
            ],
            partition_keys=["partition_col"],
            primary_keys=[],
            options={},
            comment="comment"
        )
        catalog.create_table(identifier, schema, False)

        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 2)
        self.assertEqual(table.fields[1].name, "partition_col")

        catalog.alter_table(
            identifier,
            [SchemaChange.set_option("add-column-before-partition", "true")],
            False
        )

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("new_col", AtomicType("INT"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 3)
        self.assertEqual(table.fields[0].name, "col1")
        self.assertEqual(table.fields[1].name, "new_col")
        self.assertEqual(table.fields[2].name, "partition_col")

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("col_multi1", AtomicType("INT")),
             SchemaChange.add_column("col_multi2", AtomicType("STRING")),
             SchemaChange.add_column("col_multi3", AtomicType("DOUBLE"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 6)
        self.assertEqual(table.fields[0].name, "col1")
        self.assertEqual(table.fields[1].name, "new_col")
        self.assertEqual(table.fields[2].name, "col_multi1")
        self.assertEqual(table.fields[3].name, "col_multi2")
        self.assertEqual(table.fields[4].name, "col_multi3")
        self.assertEqual(table.fields[5].name, "partition_col")

        catalog.alter_table(
            identifier,
            [SchemaChange.set_option("add-column-before-partition", "false")],
            False
        )

        catalog.alter_table(
            identifier,
            [SchemaChange.add_column("another_col", AtomicType("BIGINT"))],
            False
        )
        table = catalog.get_table(identifier)
        self.assertEqual(len(table.fields), 7)
        self.assertEqual(table.fields[0].name, "col1")
        self.assertEqual(table.fields[1].name, "new_col")
        self.assertEqual(table.fields[2].name, "col_multi1")
        self.assertEqual(table.fields[3].name, "col_multi2")
        self.assertEqual(table.fields[4].name, "col_multi3")
        self.assertEqual(table.fields[5].name, "partition_col")
        self.assertEqual(table.fields[6].name, "another_col")

    def test_get_database_propagates_exists_error(self):
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })

        with self.assertRaises(DatabaseNotExistException):
            catalog.get_database("nonexistent_db")

        catalog.create_database("test_db", False)

        # FileSystemCatalog has file_io attribute
        from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
        self.assertIsInstance(catalog, FileSystemCatalog)
        filesystem_catalog = catalog  # type: FileSystemCatalog

        original_exists = filesystem_catalog.file_io.exists
        filesystem_catalog.file_io.exists = MagicMock(side_effect=OSError("Permission denied"))

        # Now get_database should propagate OSError, not DatabaseNotExistException
        with self.assertRaises(OSError) as context:
            catalog.get_database("test_db")
        self.assertIn("Permission denied", str(context.exception))
        self.assertNotIsInstance(context.exception, DatabaseNotExistException)

        # Restore original method
        filesystem_catalog.file_io.exists = original_exists

    def _create_partitioned_table_with_data(self, catalog, identifier, partitions_data):
        """Helper to create a partitioned table and write data for each partition.

        Args:
            catalog: The catalog instance.
            identifier: Table identifier string (e.g. 'test_db.tbl').
            partitions_data: List of dicts, each with 'dt' and rows count.
                e.g. [{'dt': '2024-01-01', 'rows': 2}, {'dt': '2024-01-02', 'rows': 3}]
        """
        pa_schema = pa.schema([
            ('dt', pa.string()),
            ('col1', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        catalog.create_table(identifier, schema, True)
        table = catalog.get_table(identifier)

        for part in partitions_data:
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'dt': [part['dt']] * part['rows'],
                'col1': list(range(part['rows'])),
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

    def test_list_partitions_paged(self):
        """Test list_partitions_paged with real data from manifest files."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.part_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-03', 'rows': 3},
            {'dt': '2024-01-01', 'rows': 2},
            {'dt': '2024-01-02', 'rows': 5},
        ])

        # List all partitions
        result = catalog.list_partitions_paged(identifier)
        self.assertEqual(len(result.elements), 3)
        self.assertIsNone(result.next_page_token)

        # Verify partitions are sorted by spec
        specs = [p.spec['dt'] for p in result.elements]
        self.assertEqual(specs, sorted(specs))

        # Verify aggregated statistics
        part_map = {p.spec['dt']: p for p in result.elements}
        self.assertEqual(part_map['2024-01-01'].record_count, 2)
        self.assertEqual(part_map['2024-01-02'].record_count, 5)
        self.assertEqual(part_map['2024-01-03'].record_count, 3)
        for p in result.elements:
            self.assertGreater(p.file_size_in_bytes, 0)
            self.assertGreater(p.file_count, 0)
            self.assertGreater(p.last_file_creation_time, 0)

    def test_list_partitions_paged_pagination(self):
        """Test list_partitions_paged pagination with max_results and page_token."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.paged_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
            {'dt': '2024-01-02', 'rows': 1},
            {'dt': '2024-01-03', 'rows': 1},
        ])

        # First page: max_results=2
        page1 = catalog.list_partitions_paged(identifier, max_results=2)
        self.assertEqual(len(page1.elements), 2)
        self.assertIsNotNone(page1.next_page_token)

        # Second page: use next_page_token
        page2 = catalog.list_partitions_paged(
            identifier, max_results=2, page_token=page1.next_page_token
        )
        self.assertEqual(len(page2.elements), 1)
        self.assertIsNone(page2.next_page_token)

        # All specs across pages should cover all 3 partitions
        all_specs = [p.spec['dt'] for p in page1.elements + page2.elements]
        self.assertEqual(sorted(all_specs), ['2024-01-01', '2024-01-02', '2024-01-03'])

        # max_results larger than total returns all
        result = catalog.list_partitions_paged(identifier, max_results=100)
        self.assertEqual(len(result.elements), 3)
        self.assertIsNone(result.next_page_token)

    def test_list_partitions_paged_pattern(self):
        """Test list_partitions_paged with partition_name_pattern filter."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.pattern_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
            {'dt': '2024-02-01', 'rows': 1},
            {'dt': '2024-02-15', 'rows': 1},
        ])

        # Exact match
        result = catalog.list_partitions_paged(
            identifier, partition_name_pattern='dt=2024-01-01'
        )
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].spec['dt'], '2024-01-01')

        # Wildcard match
        result = catalog.list_partitions_paged(
            identifier, partition_name_pattern='dt=2024-02*'
        )
        self.assertEqual(len(result.elements), 2)
        specs = sorted(p.spec['dt'] for p in result.elements)
        self.assertEqual(specs, ['2024-02-01', '2024-02-15'])

        # No match
        result = catalog.list_partitions_paged(
            identifier, partition_name_pattern='dt=2025*'
        )
        self.assertEqual(len(result.elements), 0)

    def test_list_partitions_paged_empty(self):
        """Test list_partitions_paged on a table with no data."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        pa_schema = pa.schema([('dt', pa.string()), ('val', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        catalog.create_table('test_db.empty_tbl', schema, False)

        result = catalog.list_partitions_paged('test_db.empty_tbl')
        self.assertEqual(len(result.elements), 0)
        self.assertIsNone(result.next_page_token)

    def test_list_partitions_paged_invalid_token(self):
        """Test list_partitions_paged with invalid page_token falls back to start."""
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("test_db", False)

        identifier = "test_db.token_tbl"
        self._create_partitioned_table_with_data(catalog, identifier, [
            {'dt': '2024-01-01', 'rows': 1},
            {'dt': '2024-01-02', 'rows': 1},
        ])

        # Invalid page_token should fall back to start
        result = catalog.list_partitions_paged(
            identifier, max_results=1, page_token='invalid'
        )
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].spec['dt'], '2024-01-01')
        self.assertIsNotNone(result.next_page_token)
