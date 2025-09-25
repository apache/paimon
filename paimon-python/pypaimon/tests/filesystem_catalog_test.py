# Licensed to the Apache Software Foundation (ASF) under one
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

from pypaimon.catalog.catalog_exception import (DatabaseAlreadyExistException,
                                                DatabaseNotExistException,
                                                TableAlreadyExistException,
                                                TableNotExistException)
from pypaimon import CatalogFactory
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon import Schema
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

    def test_catalog_environment(self):
        fields = [
            DataField.from_dict({"id": 1, "name": "f0", "type": "INT"}),
            DataField.from_dict({"id": 2, "name": "f1", "type": "INT"}),
            DataField.from_dict({"id": 3, "name": "f2", "type": "STRING"}),
        ]
        option_dict = {
            "warehouse": self.warehouse
        }
        catalog = CatalogFactory.create(option_dict)
        catalog.create_database("test_db", False)
        catalog.create_table("test_db.test_table", Schema(fields=fields), False)
        table = catalog.get_table("test_db.test_table")
        self.assertEqual(option_dict, table.get_catalog_options().to_map())
