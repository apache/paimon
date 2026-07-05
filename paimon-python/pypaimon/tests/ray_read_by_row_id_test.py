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
import uuid
from unittest import mock

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import read_by_row_id


class RayReadByRowIdTest(unittest.TestCase):
    """Distributed row-id read: read only the files owning the given row ids (and
    only the matched rows), without reading or joining the whole target. The
    read-side mirror of update_by_row_id."""

    pa_schema = pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
        ("age", pa.int32()),
    ])
    de_options = {"row-tracking.enabled": "true", "data-evolution.enabled": "true"}

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {"warehouse": os.path.join(cls.tempdir, "wh")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, options=None, schema=None):
        name = f"default.r_{uuid.uuid4().hex[:8]}"
        opts = self.de_options if options is None else options
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(schema or self.pa_schema, options=opts), False)
        return name

    def _write(self, target, data):
        t = self.catalog.get_table(target)
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()

    def _read(self, target, projection=None):
        t = self.catalog.get_table(target)
        rb = t.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        return rb.new_read().to_arrow(rb.new_scan().plan().splits())

    def _rowid_by_id(self, target):
        tab = self._read(target, ["_ROW_ID", "id"])
        return dict(zip(tab.column("id").to_pylist(), tab.column("_ROW_ID").to_pylist()))

    def _rows_by_id(self, ds):
        return {r["id"]: r for r in ds.take_all()}

    def test_read_by_row_id_basic(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": list(range(1, 7)), "name": [f"n{i}" for i in range(1, 7)],
             "age": [i * 10 for i in range(1, 7)]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)

        want = [2, 5]
        src = pa.table({"_ROW_ID": [rid[i] for i in want]},
                       schema=pa.schema([("_ROW_ID", pa.int64())]))
        ds = read_by_row_id(target, ray.data.from_arrow(src), self.catalog_options,
                            projection=["id", "name", "age"])
        got = self._rows_by_id(ds)
        self.assertEqual(set(got), set(want))                 # only requested rows
        self.assertEqual(got[2]["age"], 20)
        self.assertEqual(got[5]["name"], "n5")
        self.assertEqual(got[2]["_ROW_ID"], rid[2])           # _ROW_ID carried through

    def test_reads_correct_row_across_files(self):
        # A _ROW_ID owned by a middle data file must return only that row.
        target = self._create()
        for chunk in ([10, 11, 12], [20, 21], [30, 31, 32, 33]):
            self._write(target, pa.Table.from_pydict(
                {"id": chunk, "name": ["x"] * len(chunk), "age": [c for c in chunk]},
                schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[21]]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        ds = read_by_row_id(target, ray.data.from_arrow(src), self.catalog_options,
                            projection=["id", "age"])
        rows = ds.take_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], 21)
        self.assertEqual(rows[0]["age"], 21)

    def test_reads_across_evolution_split_files(self):
        # update_by_row_id writes a column delta, splitting a row's range across the
        # original file and the delta. read_by_row_id must merge them: the updated
        # column comes from the delta, the untouched column from the original file.
        from pypaimon.ray import update_by_row_id
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [10, 20, 30]},
            schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        update_by_row_id(
            target,
            pa.table({"_ROW_ID": [rid[2]], "age": [999]},
                     schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())])),
            self.catalog_options, update_cols=["age"])
        ds = read_by_row_id(
            target,
            pa.table({"_ROW_ID": [rid[2]]}, schema=pa.schema([("_ROW_ID", pa.int64())])),
            self.catalog_options, projection=["id", "name", "age"])
        rows = ds.take_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], 2)
        self.assertEqual(rows[0]["name"], "b")     # untouched, from original file
        self.assertEqual(rows[0]["age"], 999)      # updated, from delta file

    def test_reads_blob_column(self):
        blob_schema = pa.schema([("id", pa.int32()), ("payload", pa.large_binary())])
        target = self._create(schema=blob_schema)
        payloads = [bytes([i]) * (i + 1) for i in range(1, 5)]
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2, 3, 4], "payload": pa.array(payloads, pa.large_binary())},
            schema=blob_schema))
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[2], rid[4]]},
                       schema=pa.schema([("_ROW_ID", pa.int64())]))
        ds = read_by_row_id(target, ray.data.from_arrow(src), self.catalog_options,
                            projection=["id", "payload"])
        got = self._rows_by_id(ds)
        self.assertEqual(set(got), {2, 4})
        self.assertEqual(bytes(got[2]["payload"]), payloads[1])   # blob resolved to payload
        self.assertEqual(bytes(got[4]["payload"]), payloads[3])

    def test_pins_base_snapshot(self):
        # The read pins its base snapshot and threads it to distributed_read_by_row_id.
        import importlib
        m = importlib.import_module("pypaimon.ray.read_by_row_id")  # module, not the fn
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        expected_sid = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[1]]}, schema=pa.schema([("_ROW_ID", pa.int64())]))

        captured = {}

        def fake_read(rid_ds, table, projection, *, num_partitions,
                      ray_remote_args=None, base_snapshot_id=None):
            captured["base_snapshot_id"] = base_snapshot_id
            return ray.data.from_arrow(pa.table({"_ROW_ID": pa.array([], pa.int64())}))

        with mock.patch.object(m, "distributed_read_by_row_id", fake_read):
            read_by_row_id(target, src, self.catalog_options, projection=["age"])
        self.assertEqual(captured["base_snapshot_id"], expected_sid)

    def test_accepts_pyarrow_and_pandas_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [1, 2, 3]},
            schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        # pyarrow.Table source
        ds = read_by_row_id(
            target, pa.table({"_ROW_ID": [rid[2]]}, schema=pa.schema([("_ROW_ID", pa.int64())])),
            self.catalog_options, projection=["name"])
        self.assertEqual([r["name"] for r in ds.take_all()], ["b"])
        # pandas.DataFrame source
        import pandas as pd
        ds = read_by_row_id(
            target, pd.DataFrame({"_ROW_ID": pd.array([rid[3]], dtype="int64")}),
            self.catalog_options, projection=["name"])
        self.assertEqual([r["name"] for r in ds.take_all()], ["c"])

    def test_ignores_extra_source_columns_and_dedups(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        # source carries junk columns and a duplicate row id -> junk ignored, row deduped
        src = pa.table({"_ROW_ID": [rid[2], rid[2]], "junk": ["x", "y"]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("junk", pa.string())]))
        ds = read_by_row_id(target, ray.data.from_arrow(src), self.catalog_options,
                            projection=["id", "name"])
        rows = ds.take_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "b")

    def test_rejects_table_name_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        with self.assertRaises(ValueError):
            read_by_row_id(target, "default.some_source", self.catalog_options,
                           projection=["age"])

    def test_rejects_non_data_evolution_table(self):
        target = self._create(options={})  # plain append table
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=["age"])

    def test_rejects_deletion_vectors_table(self):
        opts = dict(self.de_options, **{"deletion-vectors.enabled": "true"})
        target = self._create(options=opts)
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=["age"])

    def test_rejects_missing_row_id_column(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"id": [1]}, schema=pa.schema([("id", pa.int32())]))
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=["age"])

    def test_rejects_unknown_and_empty_projection(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=["nope"])
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=[])

    def test_empty_target(self):
        src = pa.table({"_ROW_ID": [0]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        empty_src = pa.table({"_ROW_ID": pa.array([], pa.int64())})

        # never written -> a foreign row id raises
        target = self._create()
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=["age"])
        # empty source against an empty target -> empty result, not an error
        ds = read_by_row_id(target, empty_src, self.catalog_options, projection=["age"])
        self.assertEqual(ds.count(), 0)

    def test_empty_source_non_empty_target_keeps_schema(self):
        # A groupby over zero rows yields zero groups, so the output must still carry
        # the projected schema (projection + _ROW_ID), not be schema-less.
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        empty_src = pa.table({"_ROW_ID": pa.array([], pa.int64())})
        ds = read_by_row_id(target, empty_src, self.catalog_options, projection=["id", "age"])
        self.assertEqual(ds.count(), 0)
        self.assertIsNotNone(ds.schema())
        self.assertEqual(set(ds.schema().names), {"id", "age", "_ROW_ID"})

    def test_custom_row_id_col(self):
        # bucket_join locators expose "row_id", not the system "_ROW_ID".
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [1, 2, 3]},
            schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        src = pa.table({"row_id": [rid[2]], "url": ["u2"]},
                       schema=pa.schema([("row_id", pa.int64()), ("url", pa.string())]))
        ds = read_by_row_id(target, src, self.catalog_options,
                            projection=["name"], row_id_col="row_id")
        rows = ds.take_all()
        self.assertEqual([r["name"] for r in rows], ["b"])
        self.assertEqual(rows[0]["_ROW_ID"], rid[2])   # output still uses _ROW_ID


if __name__ == "__main__":
    unittest.main()
