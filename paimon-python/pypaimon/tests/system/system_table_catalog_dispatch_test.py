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

"""End-to-end tests that ``Catalog.get_table`` dispatches ``$xxx`` requests
to the SystemTableLoader and surfaces the right errors when the name is
unknown.

The dispatch must work identically for FileSystemCatalog and RESTCatalog;
RESTCatalog coverage is added alongside the dispatch change for that
implementation.
"""

import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import TableNotExistException
from pypaimon.schema.data_types import DataField
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.system import system_table_loader


class _FakeSystemTable:
    """Stand-in returned by an injected SystemTableLoader factory."""

    def __init__(self, base_table, marker: str):
        self.base_table = base_table
        self.marker = marker


def _install_fake_factory(name: str, marker: str):
    """Register a fake factory under ``name`` for the lifetime of a test."""

    def factory(base):
        return _FakeSystemTable(base, marker)

    previous = system_table_loader.SYSTEM_TABLE_LOADERS.get(name)
    system_table_loader.SYSTEM_TABLE_LOADERS[name] = factory
    return previous


def _restore_factory(name: str, previous):
    if previous is None:
        system_table_loader.SYSTEM_TABLE_LOADERS.pop(name, None)
    else:
        system_table_loader.SYSTEM_TABLE_LOADERS[name] = previous


class FilesystemCatalogSystemTableDispatchTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="sys_dispatch_")
        self.warehouse = os.path.join(self.temp_dir, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("db", False)
        fields = [
            DataField.from_dict({"id": 1, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 2, "name": "name", "type": "STRING"}),
        ]
        self.catalog.create_table("db.t", Schema(fields=fields), False)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_regular_get_table_still_returns_file_store_table(self):
        table = self.catalog.get_table("db.t")
        self.assertIsInstance(table, FileStoreTable)
        self.assertFalse(table.identifier.is_system_table())

    def test_get_system_table_routes_through_loader(self):
        marker = "phase1-dispatch-marker"
        previous = _install_fake_factory("snapshots", marker)
        try:
            sys_table = self.catalog.get_table("db.t$snapshots")
        finally:
            _restore_factory("snapshots", previous)

        self.assertIsInstance(sys_table, _FakeSystemTable)
        self.assertEqual(marker, sys_table.marker)
        # The loader is passed the BASE table, not a system-table identifier.
        self.assertIsInstance(sys_table.base_table, FileStoreTable)
        self.assertFalse(sys_table.base_table.identifier.is_system_table())
        self.assertEqual("t", sys_table.base_table.identifier.get_table_name())

    def test_unknown_system_table_raises_table_not_exist(self):
        # Pick a name that the registry deliberately does NOT carry.
        with self.assertRaises(TableNotExistException):
            self.catalog.get_table("db.t$definitely_not_a_system_table")

    def test_system_table_request_for_missing_base_propagates_table_not_exist(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.get_table("db.does_not_exist$snapshots")


if __name__ == "__main__":
    unittest.main()
