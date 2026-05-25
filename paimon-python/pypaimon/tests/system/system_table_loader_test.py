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

"""Tests for the SystemTableLoader registry."""

import unittest

from pypaimon.table.system import system_table_loader as loader

_EXPECTED_SYSTEM_TABLES = (
    "snapshots",
    "schemas",
    "options",
    "manifests",
    "files",
    "partitions",
    "tags",
    "branches",
)

# Short names recognised by the Paimon catalog that this loader does
# not register yet. The loader must not silently surface any of these.
_UNREGISTERED_NAMES = {
    "audit_log",
    "binlog",
    "read_optimized",
    "consumers",
    "statistics",
    "aggregation_fields",
    "buckets",
    "file_key_ranges",
    "table_indexes",
    "row_tracking",
    "all_tables",
    "all_partitions",
    "all_table_options",
    "catalog_options",
}


class SystemTableLoaderTest(unittest.TestCase):

    def test_system_tables_tuple_lists_expected_names_in_order(self):
        self.assertEqual(_EXPECTED_SYSTEM_TABLES, loader.SYSTEM_TABLES)

    def test_registry_keys_match_system_tables_tuple(self):
        self.assertEqual(set(_EXPECTED_SYSTEM_TABLES),
                         set(loader.SYSTEM_TABLE_LOADERS.keys()))

    def test_unregistered_names_stay_out_of_registry(self):
        registered = set(loader.SYSTEM_TABLE_LOADERS.keys())
        intersection = registered & _UNREGISTERED_NAMES
        self.assertEqual(set(), intersection,
                         "unregistered names must stay out of the registry: "
                         + str(sorted(intersection)))

    def test_load_returns_none_for_unknown_name(self):
        self.assertIsNone(loader.load("not_a_real_system_table", base_table=None))

    def test_load_dispatches_to_registered_factory(self):
        sentinel = object()
        loader.SYSTEM_TABLE_LOADERS["__test_only__"] = lambda base: sentinel
        try:
            self.assertIs(sentinel, loader.load("__test_only__", base_table=None))
        finally:
            loader.SYSTEM_TABLE_LOADERS.pop("__test_only__", None)


if __name__ == "__main__":
    unittest.main()
