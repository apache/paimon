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

"""Tests that each table of a REST catalog takes its io-cache view of the token FileIO."""

import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTTableFileIOViewTest(RESTBaseTest):

    def setUp(self):
        super().setUp()
        self.options['data-token.enabled'] = 'true'
        self.rest_catalog = CatalogFactory.create(self.options)

    def _table(self, name, options):
        schema = Schema.from_pyarrow_schema(pa.schema([("id", pa.int32())]), options=options)
        self.rest_catalog.create_table(name, schema, False)
        table = self.rest_catalog.get_table(name)
        self.assertIsInstance(table.file_io, RESTTokenFileIO)
        return table

    def test_tables_take_their_view_of_the_token_file_io(self):
        # Other writers may replace Format Table files in place, and manage Object Table objects.
        self.assertTrue(self._table("default.format_view", {"type": "format-table", "file.format": "csv"})
                        .file_io._origin_only)
        self.rest_catalog.create_table("default.object_view", Schema(options={"type": "object-table"}), False)
        self.assertTrue(self.rest_catalog.get_table("default.object_view").file_io._origin_only)

        table = self._table("default.custom_view", {"data-file.prefix": "custom-", "changelog-file.prefix": "cl-"})
        path = table.table_path
        self.assertEqual((False, (path, ("custom-", "cl-"), ("main",))),
                         (table.file_io._origin_only, table.file_io._table))
        self.assertIs(table.file_io, table.copy({}).file_io)
        self.assertEqual((path, ("custom-", "cl-", "dynamic-"), ("main",)),
                         table.copy({"data-file.prefix": "dynamic-"}).file_io._table)
        # A branch copy also reads the schemas of the branch.
        self.assertEqual((path, ("custom-", "cl-"), ("main", "dev")), table.copy({"branch": "dev"}).file_io._table)

        plain = self._table("default.plain_view", {})
        self.assertEqual((plain.table_path, (), ("main",)), plain.file_io._table)


if __name__ == '__main__':
    unittest.main()
