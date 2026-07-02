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

    def _bucketed_table(self, name, schema, key, data, primary_keys=None):
        opts = {"bucket": str(self.NUM_BUCKETS)}
        if primary_keys is None:  # append table: bucket-key must be explicit
            opts["bucket-key"] = key
        # PK table: leave bucket-key unset so it defaults to the primary key.
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(schema, primary_keys=primary_keys, options=opts),
            False)
        t = self.catalog.get_table(name)
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return name

    def _create_bucketed(self, name, schema, key, num_buckets, partition_keys=None):
        opts = {"bucket": str(num_buckets), "bucket-key": key}
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(schema, partition_keys=partition_keys, options=opts),
            False)
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

    def test_fan_out_one_url_many_row_ids(self):
        # A url may map to several locator rows; every match must be emitted.
        loc_schema = pa.schema([("url", pa.string()), ("row_id", pa.int64())])
        in_schema = pa.schema([("url", pa.string())])
        self._bucketed_table(
            "default.loc_fan", loc_schema, "url",
            pa.Table.from_pydict({"url": ["u0", "u0", "u1"], "row_id": [0, 1, 2]},
                                 schema=loc_schema))
        self._bucketed_table(
            "default.in_fan", in_schema, "url",
            pa.Table.from_pydict({"url": ["u0"]}, schema=in_schema))
        ds = bucket_join(
            "default.in_fan", "default.loc_fan", self.catalog_options,
            on="url", left_projection=["url"], right_projection=["url", "row_id"])
        self.assertEqual(sorted(r["row_id"] for r in ds.take_all()), [0, 1])

    def test_empty_result_keeps_schema(self):
        # No shared bucket -> 0 rows, but the join schema must survive.
        loc_schema = pa.schema([("url", pa.string()), ("row_id", pa.int64())])
        in_schema = pa.schema([("url", pa.string())])
        self._bucketed_table(
            "default.loc_empty", loc_schema, "url",
            pa.Table.from_pydict({"url": ["u0", "u1"], "row_id": [0, 1]}, schema=loc_schema))
        self._bucketed_table(
            "default.in_empty", in_schema, "url",
            pa.Table.from_pydict({"url": []}, schema=in_schema))  # no rows -> no buckets
        ds = bucket_join(
            "default.in_empty", "default.loc_empty", self.catalog_options,
            on="url", left_projection=["url"], right_projection=["url", "row_id"])
        self.assertEqual(ds.count(), 0)
        self.assertIn("row_id", ds.schema().names)

    def test_rejects_different_bucket_count(self):
        sch = pa.schema([("url", pa.string())])
        self._create_bucketed("default.cnt_8", sch, "url", 8)
        self._create_bucketed("default.cnt_16", sch, "url", 16)
        with self.assertRaises(ValueError):
            bucket_join("default.cnt_8", "default.cnt_16", self.catalog_options, on="url")

    def test_rejects_different_bucket_key(self):
        sch = pa.schema([("url", pa.string()), ("k", pa.string())])
        self._create_bucketed("default.by_url", sch, "url", 8)
        self._create_bucketed("default.by_k", sch, "k", 8)
        with self.assertRaises(ValueError):
            bucket_join("default.by_url", "default.by_k", self.catalog_options, on="url")

    def test_rejects_join_key_not_bucket_key(self):
        sch = pa.schema([("url", pa.string()), ("k", pa.string())])
        self._create_bucketed("default.k1", sch, "url", 8)
        self._create_bucketed("default.k2", sch, "url", 8)
        with self.assertRaises(ValueError):  # on=k but bucket-key=url
            bucket_join("default.k1", "default.k2", self.catalog_options, on="k")

    def test_primary_key_default_bucket_key(self):
        # PK tables bucket by their primary key without an explicit bucket-key option;
        # bucket_join must resolve that and join on the PK.
        loc_schema = pa.schema([("url", pa.string()), ("row_id", pa.int64())])
        in_schema = pa.schema([("url", pa.string())])
        self._bucketed_table(
            "default.pk_loc", loc_schema, "url",
            pa.Table.from_pydict({"url": [f"u{i}" for i in range(100)],
                                  "row_id": list(range(100))}, schema=loc_schema),
            primary_keys=["url"])
        self._bucketed_table(
            "default.pk_in", in_schema, "url",
            pa.Table.from_pydict({"url": [f"u{i}" for i in range(40)]}, schema=in_schema),
            primary_keys=["url"])
        ds = bucket_join(
            "default.pk_in", "default.pk_loc", self.catalog_options,
            on="url", left_projection=["url"], right_projection=["url", "row_id"])
        got = {r["url"]: r["row_id"] for r in ds.take_all()}
        self.assertEqual(set(got), {f"u{i}" for i in range(40)})
        self.assertTrue(all(got[f"u{i}"] == i for i in range(40)))

    def test_rejects_projection_missing_join_key(self):
        sch = pa.schema([("url", pa.string()), ("v", pa.int64())])
        self._create_bucketed("default.pmj1", sch, "url", 8)
        self._create_bucketed("default.pmj2", sch, "url", 8)
        with self.assertRaises(ValueError):  # left projection drops the join key
            bucket_join("default.pmj1", "default.pmj2", self.catalog_options,
                        on="url", left_projection=["v"], right_projection=["url"])

    def test_rejects_colliding_columns(self):
        # Both sides expose a non-key column "v" -> pyarrow join would collide.
        sch = pa.schema([("url", pa.string()), ("v", pa.int64())])
        self._create_bucketed("default.col1", sch, "url", 8)
        self._create_bucketed("default.col2", sch, "url", 8)
        with self.assertRaises(ValueError):
            bucket_join("default.col1", "default.col2", self.catalog_options, on="url")

    def test_rejects_partitioned_table(self):
        # Bucket ids are per-partition, so bucket-only grouping would join across
        # partitions; partitioned tables are rejected until (partition, bucket) grouping.
        sch = pa.schema([("url", pa.string()), ("dt", pa.string())])
        self._create_bucketed("default.part_p", sch, "url", 8, partition_keys=["dt"])
        self._create_bucketed("default.part_np", sch, "url", 8)
        with self.assertRaises(ValueError):
            bucket_join("default.part_p", "default.part_np", self.catalog_options, on="url")

    def test_rejects_non_inner_join(self):
        sch = pa.schema([("url", pa.string())])
        self._create_bucketed("default.ji1", sch, "url", 8)
        self._create_bucketed("default.ji2", sch, "url", 8)
        with self.assertRaises(ValueError):
            bucket_join("default.ji1", "default.ji2", self.catalog_options,
                        on="url", join_type="left outer")


if __name__ == "__main__":
    unittest.main()
