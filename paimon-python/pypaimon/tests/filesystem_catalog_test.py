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
