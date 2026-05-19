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
        marker = "fs-dispatch-marker"
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


class RestCatalogSystemTableDispatchTest(unittest.TestCase):
    """Tests that RESTCatalog.get_table dispatches identically to FileSystem.

    Uses a hand-rolled lightweight stand-in instead of a live REST server:
    the dispatch logic is pure Python and the network-bound code path
    (``_load_data_table``) is already covered by RESTCatalog's own tests.
    Stubbing it lets these tests stay fast and focused on the new branch.
    """

    def _build_catalog(self):
        from pypaimon.catalog.rest.rest_catalog import RESTCatalog
        catalog = RESTCatalog.__new__(RESTCatalog)

        loaded_identifiers = []

        def fake_load_data_table(identifier):
            loaded_identifiers.append(identifier)
            # The system-table loader only needs an object that quacks like
            # a base table; the production factories will exercise the real
            # FileStoreTable surface.
            from pypaimon.common.identifier import Identifier as _Ident
            assert isinstance(identifier, _Ident)
            assert not identifier.is_system_table(), (
                "dispatch must strip the $-suffix before reaching "
                "_load_data_table; got: " + identifier.get_object_name())

            class _FakeBaseTable:
                pass

            base = _FakeBaseTable()
            base.identifier = identifier
            return base

        catalog._load_data_table = fake_load_data_table
        return catalog, loaded_identifiers

    def test_regular_get_table_calls_load_data_table_unchanged(self):
        catalog, loaded = self._build_catalog()
        result = catalog.get_table("db.t")
        self.assertIsNotNone(result)
        self.assertEqual(1, len(loaded))
        self.assertFalse(loaded[0].is_system_table())
        self.assertEqual("t", loaded[0].get_table_name())

    def test_get_system_table_routes_through_loader(self):
        catalog, loaded = self._build_catalog()
        marker = "rest-dispatch-marker"
        previous = _install_fake_factory("snapshots", marker)
        try:
            sys_table = catalog.get_table("db.t$snapshots")
        finally:
            _restore_factory("snapshots", previous)

        self.assertIsInstance(sys_table, _FakeSystemTable)
        self.assertEqual(marker, sys_table.marker)
        # _load_data_table received the BASE identifier (no system suffix).
        self.assertEqual(1, len(loaded))
        self.assertFalse(loaded[0].is_system_table())
        self.assertEqual("t", loaded[0].get_table_name())

    def test_get_system_table_preserves_branch_segment(self):
        catalog, loaded = self._build_catalog()
        marker = "rest-dispatch-branched-marker"
        previous = _install_fake_factory("snapshots", marker)
        try:
            sys_table = catalog.get_table("db.t$branch_dev$snapshots")
        finally:
            _restore_factory("snapshots", previous)

        self.assertIsInstance(sys_table, _FakeSystemTable)
        # The base identifier carries the branch but not the system suffix.
        self.assertEqual(1, len(loaded))
        self.assertFalse(loaded[0].is_system_table())
        self.assertEqual("t", loaded[0].get_table_name())
        self.assertEqual("dev", loaded[0].get_branch_name())

    def test_unknown_system_table_raises_table_not_exist(self):
        catalog, _ = self._build_catalog()
        with self.assertRaises(TableNotExistException):
            catalog.get_table("db.t$definitely_not_a_system_table")


if __name__ == "__main__":
    unittest.main()
