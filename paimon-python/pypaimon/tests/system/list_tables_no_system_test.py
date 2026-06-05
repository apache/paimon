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

"""Regression: list_tables must not surface table-level system tables.

System tables are accessible by name via ``get_table('db.t$<name>')``
but they don't belong in directory listings. The test below pins both
halves of that behaviour for FilesystemCatalog.
"""

import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import DataField
from pypaimon.table.system import system_table_loader
from pypaimon.table.system.system_table import SystemTable


class ListTablesNoSystemTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="list_no_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table("db.t", Schema(fields=fields), False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_list_tables_does_not_include_system_table_suffixes(self):
        names = self.catalog.list_tables("db")
        self.assertIn("t", names)
        for name in names:
            self.assertNotIn(
                "$", name,
                "list_tables surfaced a system-table suffix: " + name)

    def test_get_table_still_works_for_every_registered_system_table(self):
        # Each registered system table should be retrievable by full
        # identifier even though list_tables hid them; this proves the
        # contract is consistent end-to-end.
        for name in system_table_loader.SYSTEM_TABLES:
            sys_table = self.catalog.get_table("db.t${}".format(name))
            self.assertIsInstance(
                sys_table, SystemTable,
                "system table {} did not return a SystemTable".format(name))


if __name__ == "__main__":
    unittest.main()
