#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import bucket_join


class RayBucketJoinTest(unittest.TestCase):
    """Bucket-aligned join between two HASH_FIXED tables must equal a global join,
    with each bucket joined only against the same bucket (no cross-bucket shuffle)."""

    NUM_BUCKETS = 8

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {"warehouse": os.path.join(cls.tempdir, "wh")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _bucketed_table(self, name, schema, key, data):
        opts = {"bucket": str(self.NUM_BUCKETS), "bucket-key": key}
        self.catalog.create_table(name, Schema.from_pyarrow_schema(schema, options=opts), False)
        t = self.catalog.get_table(name)
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return name

    def test_bucket_join_matches_global_join(self):
        loc_schema = pa.schema([("url", pa.string()), ("row_id", pa.int64())])
        in_schema = pa.schema([("url", pa.string())])
        self._bucketed_table(
            "default.locator", loc_schema, "url",
            pa.Table.from_pydict({"url": [f"u{i}" for i in range(1000)],
                                  "row_id": list(range(1000))}, schema=loc_schema))
        self._bucketed_table(
            "default.input", in_schema, "url",
            pa.Table.from_pydict({"url": [f"u{i}" for i in range(0, 400)]}, schema=in_schema))

        ds = bucket_join(
            "default.input", "default.locator", self.catalog_options,
            on="url", left_projection=["url"], right_projection=["url", "row_id"])
        got = {r["url"]: r["row_id"] for r in ds.take_all()}

        # every input url (u0..u399) is matched to its locator row_id
        self.assertEqual(set(got), {f"u{i}" for i in range(400)})
        self.assertEqual(got["u0"], 0)
        self.assertEqual(got["u399"], 399)
        self.assertTrue(all(got[f"u{i}"] == i for i in range(400)))

    def test_rejects_incompatible_bucketing(self):
        # different bucket count -> not bucket-aligned -> must reject
        sch = pa.schema([("url", pa.string())])
        self._bucketed_table("default.in_8", sch, "url", pa.Table.from_pydict({"url": ["a"]}))
        self.catalog.create_table(
            "default.loc_16",
            Schema.from_pyarrow_schema(sch, options={"bucket": "16", "bucket-key": "url"}), False)
        with self.assertRaises(ValueError):
            bucket_join("default.in_8", "default.loc_16", self.catalog_options, on="url")


if __name__ == "__main__":
    unittest.main()
