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

import collections
import datetime
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
ray = pytest.importorskip("ray")

import importlib
from unittest import mock

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import range_join

rjmod = importlib.import_module("pypaimon.ray.range_join")


class RayRangeJoinTest(unittest.TestCase):
    """Range-aligned join must equal a global inner join, cutting the key space from
    per-file min/max stats so each range is read/joined in its own task (no shuffle)."""

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

    def _table(self, name, schema, commits, primary_keys=None, options=None):
        """Create a table and write each arrow table in ``commits`` as its own commit,
        so the manifest holds several data files with distinct key ranges."""
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(schema, primary_keys=primary_keys, options=options),
            False)
        t = self.catalog.get_table(name)
        for data in commits:
            wb = t.new_batch_write_builder()
            w = wb.new_write()
            w.write_arrow(data)
            wb.new_commit().commit(w.prepare_commit())
            w.close()
        return name

    def test_range_join_matches_global_join(self):
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        # locator: k in 0..599 spread across three files with disjoint key ranges.
        self._table("default.rj_loc", loc, [
            pa.Table.from_pydict({"k": list(range(0, 200)),
                                  "row_id": list(range(0, 200))}, schema=loc),
            pa.Table.from_pydict({"k": list(range(200, 400)),
                                  "row_id": list(range(200, 400))}, schema=loc),
            pa.Table.from_pydict({"k": list(range(400, 600)),
                                  "row_id": list(range(400, 600))}, schema=loc),
        ])
        self._table("default.rj_in", ins, [
            pa.Table.from_pydict({"k": list(range(0, 250))}, schema=ins),
        ])
        ds = range_join(
            "default.rj_in", "default.rj_loc", self.catalog_options,
            on="k", left_projection=["k"], right_projection=["k", "row_id"], num_ranges=4)
        got = {r["k"]: r["row_id"] for r in ds.take_all()}
        self.assertEqual(set(got), set(range(250)))
        self.assertTrue(all(got[i] == i for i in range(250)))

    def test_fan_out_one_key_many_rows(self):
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        self._table("default.rj_fan_loc", loc, [
            pa.Table.from_pydict({"k": [5, 5, 7], "row_id": [0, 1, 2]}, schema=loc)])
        self._table("default.rj_fan_in", ins, [
            pa.Table.from_pydict({"k": [5]}, schema=ins)])
        ds = range_join(
            "default.rj_fan_in", "default.rj_fan_loc", self.catalog_options,
            on="k", left_projection=["k"], right_projection=["k", "row_id"])
        self.assertEqual(sorted(r["row_id"] for r in ds.take_all()), [0, 1])

    def test_left_on_right_on_different_names(self):
        right = pa.schema([("rid", pa.int64()), ("val", pa.string())])
        left = pa.schema([("lid", pa.int64())])
        self._table("default.rj_lr_right", right, [
            pa.Table.from_pydict({"rid": list(range(100)),
                                  "val": [f"v{i}" for i in range(100)]}, schema=right)])
        self._table("default.rj_lr_left", left, [
            pa.Table.from_pydict({"lid": list(range(30))}, schema=left)])
        ds = range_join(
            "default.rj_lr_left", "default.rj_lr_right", self.catalog_options,
            left_on="lid", right_on="rid", num_ranges=3)
        # Output keeps the left key name (pyarrow coalesces the right key into it).
        got = {r["lid"]: r["val"] for r in ds.take_all()}
        self.assertEqual(got, {i: f"v{i}" for i in range(30)})

    def test_num_ranges_one_is_correct(self):
        # A single range degenerates to one local join and must still be exact.
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        self._table("default.rj_one_loc", loc, [
            pa.Table.from_pydict({"k": list(range(50)),
                                  "row_id": list(range(50))}, schema=loc)])
        self._table("default.rj_one_in", ins, [
            pa.Table.from_pydict({"k": list(range(20))}, schema=ins)])
        ds = range_join(
            "default.rj_one_in", "default.rj_one_loc", self.catalog_options,
            on="k", left_projection=["k"], right_projection=["k", "row_id"], num_ranges=1)
        got = {r["k"]: r["row_id"] for r in ds.take_all()}
        self.assertEqual(got, {i: i for i in range(20)})

    def test_dispatches_multiple_range_tasks(self):
        # No global shuffle: several disjoint-range files produce more than one task.
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        self._table("default.rj_disp_loc", loc, [
            pa.Table.from_pydict({"k": list(range(0, 300)),
                                  "row_id": list(range(0, 300))}, schema=loc),
            pa.Table.from_pydict({"k": list(range(300, 600)),
                                  "row_id": list(range(300, 600))}, schema=loc),
        ])
        self._table("default.rj_disp_in", ins, [
            pa.Table.from_pydict({"k": list(range(0, 600))}, schema=ins)])

        captured = {}
        real = ray.data.from_arrow_refs

        def spy(refs):
            captured["n"] = len(refs)
            return real(refs)

        with mock.patch.object(ray.data, "from_arrow_refs", spy):
            ds = range_join(
                "default.rj_disp_in", "default.rj_disp_loc", self.catalog_options,
                on="k", left_projection=["k"], right_projection=["k", "row_id"], num_ranges=4)
            ds.take_all()
        self.assertGreater(captured["n"], 1)

    def test_rejects_shared_non_key_column(self):
        loc = pa.schema([("k", pa.int64()), ("v", pa.int64())])
        ins = pa.schema([("k", pa.int64()), ("v", pa.int64())])
        self._table("default.rj_col_loc", loc, [
            pa.Table.from_pydict({"k": [1], "v": [1]}, schema=loc)])
        self._table("default.rj_col_in", ins, [
            pa.Table.from_pydict({"k": [1], "v": [2]}, schema=ins)])
        with self.assertRaisesRegex(ValueError, "collide"):
            range_join("default.rj_col_in", "default.rj_col_loc", self.catalog_options, on="k")

    def test_rejects_key_type_mismatch(self):
        loc = pa.schema([("k", pa.int32()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        self._table("default.rj_ty_loc", loc, [
            pa.Table.from_pydict({"k": pa.array([1], pa.int32()), "row_id": [1]}, schema=loc)])
        self._table("default.rj_ty_in", ins, [
            pa.Table.from_pydict({"k": [1]}, schema=ins)])
        with self.assertRaisesRegex(ValueError, "same type"):
            range_join("default.rj_ty_in", "default.rj_ty_loc", self.catalog_options, on="k")

    def test_rejects_bad_on_spec(self):
        with self.assertRaisesRegex(ValueError, "exactly one of"):
            range_join("a", "b", self.catalog_options)  # neither on nor left_on/right_on

    def test_rejects_float_range_key(self):
        schema = pa.schema([("k", pa.float64()), ("v", pa.int64())])
        self._table("default.rj_float_a", schema, [
            pa.Table.from_pydict({"k": [1.0], "v": [1]}, schema=schema)])
        self._table("default.rj_float_b", schema, [
            pa.Table.from_pydict({"k": [1.0], "v": [2]}, schema=schema)])
        with self.assertRaisesRegex(ValueError, "FLOAT/DOUBLE"):
            range_join("default.rj_float_a", "default.rj_float_b", self.catalog_options,
                       on="k", left_projection=["k"], right_projection=["k"])

    def test_rejects_left_key_vs_right_column_collision(self):
        left = pa.schema([("lid", pa.int64()), ("x", pa.int64())])
        right = pa.schema([("rid", pa.int64()), ("lid", pa.int64())])
        self._table("default.rj_xn_left", left, [
            pa.Table.from_pydict({"lid": [1], "x": [1]}, schema=left)])
        self._table("default.rj_xn_right", right, [
            pa.Table.from_pydict({"rid": [1], "lid": [9]}, schema=right)])
        # Left key 'lid' collides with the right non-key column 'lid' in the output.
        with self.assertRaisesRegex(ValueError, "collide"):
            range_join("default.rj_xn_left", "default.rj_xn_right", self.catalog_options,
                       left_on="lid", right_on="rid")

    def test_date_to_timestamp_schema_evolution(self):
        # A DATE->TIMESTAMP evolved key yields date footers in old files and datetime in
        # new ones; the planner must coerce both to the key type, not compare them raw.
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange

        a_date = pa.schema([("k", pa.date32())])
        self.catalog.create_table(
            "default.rj_ev_a", Schema.from_pyarrow_schema(a_date), False)
        t = self.catalog.get_table("default.rj_ev_a")
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"k": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]}, schema=a_date))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        self.catalog.alter_table(
            "default.rj_ev_a",
            [SchemaChange.update_column_type("k", AtomicType("TIMESTAMP(6)"))], False)
        t = self.catalog.get_table("default.rj_ev_a")
        a_ts = pa.schema([("k", pa.timestamp("us"))])
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"k": [datetime.datetime(2020, 6, 1), datetime.datetime(2020, 6, 2)]}, schema=a_ts))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        b = pa.schema([("bk", pa.timestamp("us")), ("val", pa.string())])
        self.catalog.create_table("default.rj_ev_b", Schema.from_pyarrow_schema(b), False)
        t = self.catalog.get_table("default.rj_ev_b")
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"bk": [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 6, 1)],
             "val": ["jan1", "jun1"]}, schema=b))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        ds = range_join("default.rj_ev_a", "default.rj_ev_b", self.catalog_options,
                        left_on="k", right_on="bk", num_ranges=3)
        got = sorted((str(r["k"]), r["val"]) for r in ds.take_all())
        self.assertEqual(got, [("2020-01-01 00:00:00", "jan1"),
                               ("2020-06-01 00:00:00", "jun1")])

    def test_int_to_string_schema_evolution_no_dropped_rows(self):
        # INT->STRING isn't order-preserving ('10' < '2'), so an old INT file's footer
        # bounds are invalid under the new string order. Such files must be treated as
        # unknown (join every range), not pruned, or rows are silently dropped.
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange

        a_int = pa.schema([("k", pa.int32())])
        self.catalog.create_table(
            "default.rj_is_a", Schema.from_pyarrow_schema(a_int), False)
        t = self.catalog.get_table("default.rj_is_a")
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        # int order 5<42<100, but as strings '100'<'42'<'5'.
        w.write_arrow(pa.Table.from_pydict({"k": [5, 42, 100]}, schema=a_int))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        self.catalog.alter_table(
            "default.rj_is_a",
            [SchemaChange.update_column_type("k", AtomicType("STRING"))], False)

        b = pa.schema([("bk", pa.string()), ("val", pa.string())])
        self.catalog.create_table("default.rj_is_b", Schema.from_pyarrow_schema(b), False)
        t = self.catalog.get_table("default.rj_is_b")
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"bk": ["5", "42", "100"], "val": ["v5", "v42", "v100"]}, schema=b))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

        for num_ranges in (1, 3):
            ds = range_join("default.rj_is_a", "default.rj_is_b", self.catalog_options,
                            left_on="k", right_on="bk", num_ranges=num_ranges)
            got = sorted((r["k"], r["val"]) for r in ds.take_all())
            self.assertEqual(got, [("100", "v100"), ("42", "v42"), ("5", "v5")])

    def test_range_budget_caps_ranges_when_stats_missing(self):
        Split = collections.namedtuple("Split", "files")
        File = collections.namedtuple("File", "row_count")
        known = [(Split([File(100)]), 0, 99)]      # 100 rows with stats
        unknown = [(Split([File(100)]), None, None)]  # 100 rows without stats
        # unknown == total/2 -> budget 2; all-known -> no cap; all-unknown -> 1.
        self.assertEqual(rjmod._range_budget(known, unknown), 2)
        self.assertEqual(rjmod._range_budget(known, known), rjmod._MAX_RANGES)
        self.assertEqual(rjmod._range_budget(unknown, unknown), 1)

    def test_split_key_range_reads_stats(self):
        # The planner reads a file's min/max for the range column from manifest stats.
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        self._table("default.rj_stats", loc, [
            pa.Table.from_pydict({"k": [10, 20, 15], "row_id": [1, 2, 3]}, schema=loc)])
        ranged, _ = rjmod._plan_ranged_splits(
            "default.rj_stats", self.catalog_options, None, "k")
        self.assertTrue(ranged)
        los = [lo for _, lo, _ in ranged if lo is not None]
        his = [hi for _, _, hi in ranged if hi is not None]
        self.assertEqual(min(los), 10)
        self.assertEqual(max(his), 20)

    def test_stats_mode_none_still_correct(self):
        # metadata.stats-mode=none only drops manifest stats; the parquet footer still
        # carries min/max (range_join's actual source), so ranges still work. The
        # unknown-split fallback itself is covered by the planning-logic tests.
        no_stats = {"metadata.stats-mode": "none"}
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        self._table("default.rj_ns_loc", loc, [
            pa.Table.from_pydict({"k": list(range(0, 100)),
                                  "row_id": list(range(0, 100))}, schema=loc),
            pa.Table.from_pydict({"k": list(range(100, 200)),
                                  "row_id": list(range(100, 200))}, schema=loc),
        ], options=no_stats)
        self._table("default.rj_ns_in", ins, [
            pa.Table.from_pydict({"k": list(range(50, 150))}, schema=ins)],
            options=no_stats)
        ds = range_join(
            "default.rj_ns_in", "default.rj_ns_loc", self.catalog_options,
            on="k", left_projection=["k"], right_projection=["k", "row_id"], num_ranges=4)
        got = sorted((r["k"], r["row_id"]) for r in ds.take_all())
        self.assertEqual(got, [(i, i) for i in range(50, 150)])

    def test_null_keys_dropped_independent_of_num_ranges(self):
        loc = pa.schema([("k", pa.int64()), ("row_id", pa.int64())])
        ins = pa.schema([("k", pa.int64())])
        self._table("default.rj_null_loc", loc, [
            pa.Table.from_pydict({"k": [1, 2, None, 3], "row_id": [1, 2, 99, 3]}, schema=loc)])
        self._table("default.rj_null_in", ins, [
            pa.Table.from_pydict({"k": [1, None, 3, None]}, schema=ins)])
        expected = [(1, 1), (3, 3)]  # null never matches; no duplicates
        for num_ranges in (1, 5):
            ds = range_join(
                "default.rj_null_in", "default.rj_null_loc", self.catalog_options,
                on="k", left_projection=["k"], right_projection=["k", "row_id"],
                num_ranges=num_ranges)
            got = sorted((r["k"], r["row_id"]) for r in ds.take_all())
            self.assertEqual(got, expected)


if __name__ == "__main__":
    unittest.main()
