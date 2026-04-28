################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

import os
import shutil
import sqlite3
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import (
    DatabaseAlreadyExistException,
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException
)
from pypaimon.catalog.jdbc_catalog import JdbcCatalog
from pypaimon.catalog.rest.property_change import PropertyChange
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange


class JdbcCatalogTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.warehouse = os.path.join(self.temp_dir, "warehouse")
        self.jdbc_path = os.path.join(self.temp_dir, "catalog.db")
        self.options = {
            "metastore": "jdbc",
            "warehouse": self.warehouse,
            "uri": "jdbc:sqlite:" + self.jdbc_path,
            "catalog-key": "test-jdbc-catalog",
        }

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_jdbc_catalog(self):
        catalog = CatalogFactory.create(self.options)
        self.assertTrue(isinstance(catalog, JdbcCatalog))

        with sqlite3.connect(self.jdbc_path) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        self.assertIn("paimon_tables", tables)
        self.assertIn("paimon_database_properties", tables)
        self.assertIn("paimon_table_properties", tables)

    def test_database(self):
        catalog = CatalogFactory.create(self.options)
        catalog.create_database("test_db", False, {"owner": "owner1"})

        with self.assertRaises(DatabaseAlreadyExistException):
            catalog.create_database("test_db", False)

        self.assertEqual(catalog.list_databases(), ["test_db"])
        database = catalog.get_database("test_db")
        self.assertEqual(database.name, "test_db")
        self.assertEqual(database.options["owner"], "owner1")
        self.assertEqual(
            database.options["location"],
            os.path.join(self.warehouse, "test_db.db")
        )

        reloaded = CatalogFactory.create(self.options)
        self.assertEqual(reloaded.list_databases(), ["test_db"])
        reloaded.alter_database(
            "test_db",
            [
                PropertyChange.set_property("comment", "new comment"),
                PropertyChange.remove_property("owner"),
            ]
        )
        updated = reloaded.get_database("test_db")
        self.assertEqual(updated.options["comment"], "new comment")
        self.assertNotIn("owner", updated.options)

        reloaded.drop_database("test_db")
        with self.assertRaises(DatabaseNotExistException):
            reloaded.get_database("test_db")

    def test_table(self):
        fields = [
            DataField.from_dict({"id": 1, "name": "f0", "type": "INT"}),
            DataField.from_dict({"id": 2, "name": "f1", "type": "STRING"}),
        ]
        catalog = CatalogFactory.create(self.options)
        catalog.create_database("test_db", False)
        catalog.create_table(
            "test_db.test_table",
            Schema(fields=fields, partition_keys=["f1"], options={"bucket": "1"}),
            False
        )

        with self.assertRaises(TableAlreadyExistException):
            catalog.create_table("test_db.test_table", Schema(fields=fields), False)

        self.assertEqual(catalog.list_tables("test_db"), ["test_table"])
        self.assertTrue(
            os.path.exists(
                os.path.join(self.warehouse, "test_db.db", "test_table", "schema", "schema-0")
            )
        )

        reloaded = CatalogFactory.create(self.options)
        table = reloaded.get_table("test_db.test_table")
        self.assertEqual(table.fields[0].name, "f0")
        self.assertTrue(isinstance(table.fields[0].type, AtomicType))
        self.assertEqual(table.fields[0].type.type, "INT")

        with sqlite3.connect(self.jdbc_path) as conn:
            properties = dict(
                conn.execute(
                    "SELECT property_key, property_value FROM paimon_table_properties "
                    "WHERE catalog_key = ? AND database_name = ? AND table_name = ?",
                    ("test-jdbc-catalog", "test_db", "test_table")
                ).fetchall()
            )
        self.assertEqual(properties["bucket"], "1")
        self.assertEqual(properties["partition"], "f1")

        reloaded.alter_table(
            "test_db.test_table",
            [SchemaChange.add_column("f2", AtomicType("BIGINT"))]
        )
        self.assertEqual(len(reloaded.get_table("test_db.test_table").fields), 3)

        reloaded.rename_table("test_db.test_table", "test_db.renamed_table")
        self.assertEqual(reloaded.list_tables("test_db"), ["renamed_table"])
        with self.assertRaises(TableNotExistException):
            reloaded.get_table("test_db.test_table")

        reloaded.drop_table("test_db.renamed_table")
        self.assertEqual(reloaded.list_tables("test_db"), [])
        with self.assertRaises(TableNotExistException):
            reloaded.get_table("test_db.renamed_table")

    def test_drop_database_requires_cascade_for_non_empty_database(self):
        fields = [DataField.from_dict({"id": 1, "name": "f0", "type": "INT"})]
        catalog = CatalogFactory.create(self.options)
        catalog.create_database("test_db", False)
        catalog.create_table("test_db.test_table", Schema(fields=fields), False)

        with self.assertRaises(ValueError):
            catalog.drop_database("test_db")

        catalog.drop_database("test_db", cascade=True)
        self.assertEqual(catalog.list_databases(), [])


if __name__ == '__main__':
    unittest.main()
