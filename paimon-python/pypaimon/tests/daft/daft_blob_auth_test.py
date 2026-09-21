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
################################################################################

import asyncio
import json
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.daft.daft_compat import has_file_range_reads

pytest = __import__("pytest")
daft = pytest.importorskip("daft")


def _authorize(table, auth_result):
    table.catalog_environment.table_query_auth = (
        lambda options, identifier: (lambda select: auth_result))


async def _tasks(table, columns):
    from daft import context, runners
    from daft.daft import StorageConfig
    from daft.io.pushdowns import Pushdowns

    from pypaimon.daft.daft_datasource import PaimonDataSource

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig(
        runners.get_or_create_runner().name != "ray", io_config)
    source = PaimonDataSource(
        table, storage_config=storage_config, catalog_options={})
    return [t async for t in source.get_tasks(Pushdowns(columns=columns))]


async def _row_count(table, columns):
    total = 0
    for task in await _tasks(table, columns):
        async for batch in task.read():
            total += len(batch)
    return total


async def _batches(table, columns):
    out = []
    for task in await _tasks(table, columns):
        async for batch in task.read():
            out.append(batch.to_pydict())
    return out


@unittest.skipUnless(has_file_range_reads(),
                     "installed Daft lacks File range reads")
class DaftBlobTableQueryAuthTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        catalog_options = {"warehouse": os.path.join(self.tempdir, "wh")}
        self.catalog = CatalogFactory.create(catalog_options)
        self.catalog.create_database("default", True)
        self.pa_schema = pa.schema([
            ("id", pa.int32()), ("name", pa.string()),
            ("content", pa.large_binary())])
        self.catalog.create_table("default.blob_auth", Schema.from_pyarrow_schema(
            self.pa_schema,
            options={"row-tracking.enabled": "true",
                     "data-evolution.enabled": "true"}), False)
        table = self.catalog.get_table("default.blob_auth")
        writer = table.new_batch_write_builder().new_write()
        writer.write_arrow(pa.Table.from_pydict(
            {"id": list(range(6)),
             "name": ["n%d" % i for i in range(6)],
             "content": [os.urandom(64) for _ in range(6)]},
            schema=self.pa_schema))
        table.new_batch_write_builder().new_commit().commit(
            writer.prepare_commit())
        writer.close()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _table(self):
        return self.catalog.get_table("default.blob_auth")

    def test_scalar_projection_is_native_without_auth(self):
        tasks = asyncio.run(_tasks(self._table(), ["id", "name"]))
        self.assertTrue(tasks)
        self.assertFalse(any(type(t).__name__ == "_PaimonPKSplitTask"
                             for t in tasks))

    def test_masked_scalar_projection_falls_back_and_masks(self):
        table = self._table()
        _authorize(table, TableQueryAuthResult(
            filter=None,
            column_masking={"name": json.dumps({"name": "NULL"})}))

        tasks = asyncio.run(_tasks(table, ["id", "name"]))
        self.assertTrue(tasks)
        self.assertTrue(all(type(t).__name__ == "_PaimonPKSplitTask"
                            for t in tasks))

        names = []
        for batch in asyncio.run(_batches(table, ["id", "name"])):
            names.extend(batch["name"])
        self.assertEqual(len(names), 6)
        self.assertTrue(all(n is None for n in names), names)

    def test_row_filter_falls_back_and_filters(self):
        table = self._table()
        _authorize(table, TableQueryAuthResult(
            filter=[json.dumps({
                "kind": "LEAF",
                "transform": {
                    "name": "FIELD_REF",
                    "fieldRef": {"index": 0, "name": "id", "type": "INT"},
                },
                "function": "LESS_THAN",
                "literals": [3],
            })],
            column_masking=None))

        tasks = asyncio.run(_tasks(table, ["id", "name"]))
        self.assertTrue(all(type(t).__name__ == "_PaimonPKSplitTask"
                            for t in tasks))

        ids = []
        for batch in asyncio.run(_batches(table, ["id", "name"])):
            ids.extend(batch["id"])
        self.assertEqual(sorted(ids), [0, 1, 2])

    def test_zero_column_projection_under_an_auth_filter_is_filtered(self):
        table = self._table()
        _authorize(table, TableQueryAuthResult(
            filter=[json.dumps({
                "kind": "LEAF",
                "transform": {
                    "name": "FIELD_REF",
                    "fieldRef": {"index": 0, "name": "id", "type": "INT"},
                },
                "function": "LESS_THAN",
                "literals": [3],
            })],
            column_masking=None))
        self.assertEqual(asyncio.run(_row_count(table, [])), 3)

    def test_zero_column_projection_under_an_auth_mask(self):
        table = self._table()
        _authorize(table, TableQueryAuthResult(
            filter=None,
            column_masking={"name": json.dumps({"name": "NULL"})}))
        self.assertEqual(asyncio.run(_row_count(table, [])), 6)


if __name__ == '__main__':
    unittest.main()
