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
        self.assertEqual(set(got), set(want))
        self.assertEqual(got[2]["age"], 20)
        self.assertEqual(got[5]["name"], "n5")
        self.assertEqual(got[2]["_ROW_ID"], rid[2])

    def test_reads_correct_row_across_files(self):
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
        # update_by_row_id writes a column delta (splits the row's range); the read must merge original + delta.
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
        self.assertEqual(rows[0]["name"], "b")
        self.assertEqual(rows[0]["age"], 999)

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
        self.assertEqual(bytes(got[2]["payload"]), payloads[1])
        self.assertEqual(bytes(got[4]["payload"]), payloads[3])

    def test_blob_as_descriptor_via_dynamic_options(self):
        from pypaimon.ray import map_with_blobs
        from pypaimon.table.row.blob import BlobDescriptor
        blob_schema = pa.schema([("id", pa.int32()), ("payload", pa.large_binary())])
        target = self._create(schema=blob_schema)
        payloads = [bytes([i]) * (i + 3) for i in range(1, 5)]
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2, 3, 4], "payload": pa.array(payloads, pa.large_binary())},
            schema=blob_schema))
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[2], rid[4]]},
                       schema=pa.schema([("_ROW_ID", pa.int64())]))
        ds = read_by_row_id(target, ray.data.from_arrow(src), self.catalog_options,
                            projection=["payload"],
                            dynamic_options={"blob-as-descriptor": "true"})
        rows = ds.take_all()
        self.assertTrue(all(BlobDescriptor.is_blob_descriptor(bytes(r["payload"])) for r in rows))

        tbl = self.catalog.get_table(target)

        def fn(scalar_batch, blobs):
            return pa.table({"_ROW_ID": scalar_batch.column("_ROW_ID").to_pylist(),
                             "n": [len(b) if b is not None else 0 for b in blobs["payload"]]})

        res = map_with_blobs(ds, ["payload"], fn, file_io=tbl.file_io,
                             all_blob_columns=["payload"], batch_size=1)
        n = {r["_ROW_ID"]: r["n"] for r in res.take_all()}
        self.assertEqual(n[rid[2]], len(payloads[1]))
        self.assertEqual(n[rid[4]], len(payloads[3]))

    def test_rejects_invariant_dynamic_options(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        with self.assertRaisesRegex(ValueError, "invariant"):
            read_by_row_id(target, src, self.catalog_options, projection=["age"],
                           dynamic_options={"deletion-vectors.enabled": "true"})

    def test_time_travel_via_dynamic_options(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        self._write(target, pa.Table.from_pydict(   # snapshot 2 adds ids 3, 4
            {"id": [3, 4], "name": ["c", "d"], "age": [3, 4]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        idcol = pa.schema([("_ROW_ID", pa.int64())])

        ds = read_by_row_id(target, pa.table({"_ROW_ID": [rid[1]]}, schema=idcol),
                            self.catalog_options, projection=["id", "age"],
                            dynamic_options={"scan.snapshot-id": "1"})
        got = self._rows_by_id(ds)
        self.assertEqual(set(got), {1})

        # id=3 exists only in snapshot 2; at snapshot 1 its row id is foreign
        ds2 = read_by_row_id(target, pa.table({"_ROW_ID": [rid[3]]}, schema=idcol),
                             self.catalog_options, projection=["id", "age"],
                             dynamic_options={"scan.snapshot-id": "1"})
        with self.assertRaisesRegex(Exception, "valid range"):
            ds2.take_all()

    def test_time_travel_via_tag_dynamic_options(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        self.catalog.get_table(target).create_tag("v1", 1)
        self._write(target, pa.Table.from_pydict(
            {"id": [3, 4], "name": ["c", "d"], "age": [3, 4]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        idcol = pa.schema([("_ROW_ID", pa.int64())])

        # tag v1 == snapshot 1: id=1 reads, id=3 (snapshot 2 only) is foreign
        ds = read_by_row_id(target, pa.table({"_ROW_ID": [rid[1]]}, schema=idcol),
                            self.catalog_options, projection=["id", "age"],
                            dynamic_options={"scan.tag-name": "v1"})
        self.assertEqual(set(self._rows_by_id(ds)), {1})
        ds2 = read_by_row_id(target, pa.table({"_ROW_ID": [rid[3]]}, schema=idcol),
                             self.catalog_options, projection=["id", "age"],
                             dynamic_options={"scan.tag-name": "v1"})
        with self.assertRaisesRegex(Exception, "valid range"):
            ds2.take_all()

    def test_rejects_multiple_time_travel_keys(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        with self.assertRaisesRegex(ValueError, "Only one"):
            read_by_row_id(target, src, self.catalog_options, projection=["age"],
                           dynamic_options={"scan.snapshot-id": "1", "scan.tag-name": "x"})

    def test_pins_base_snapshot(self):
        import importlib
        m = importlib.import_module("pypaimon.ray.read_by_row_id")
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
        ds = read_by_row_id(
            target, pa.table({"_ROW_ID": [rid[2]]}, schema=pa.schema([("_ROW_ID", pa.int64())])),
            self.catalog_options, projection=["name"])
        self.assertEqual([r["name"] for r in ds.take_all()], ["b"])
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
        target = self._create(options={})
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

        target = self._create()
        with self.assertRaises(ValueError):
            read_by_row_id(target, src, self.catalog_options, projection=["age"])
        ds = read_by_row_id(target, empty_src, self.catalog_options, projection=["age"])
        self.assertEqual(ds.count(), 0)

    def test_foreign_row_id_raises(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [10_000]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        ds = read_by_row_id(target, src, self.catalog_options, projection=["age"])
        with self.assertRaises(Exception):
            ds.take_all()

    def test_returns_lazy_without_executing_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        marker = os.path.join(self.tempdir, f"exec_{uuid.uuid4().hex}")

        def spy(batch):
            open(marker, "a").close()
            return batch

        src = ray.data.from_arrow(
            pa.table({"_ROW_ID": [rid[1]]}, schema=pa.schema([("_ROW_ID", pa.int64())]))
        ).map_batches(spy, batch_format="pyarrow")
        ds = read_by_row_id(target, src, self.catalog_options, projection=["id", "age"])
        self.assertFalse(os.path.exists(marker), "source was executed at call time")
        rows = ds.take_all()
        self.assertTrue(os.path.exists(marker))
        self.assertEqual({r["id"] for r in rows}, {1})

    def test_empty_source_non_empty_target_keeps_schema(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        empty_src = pa.table({"_ROW_ID": pa.array([], pa.int64())})
        ds = read_by_row_id(target, empty_src, self.catalog_options, projection=["id", "age"])
        self.assertEqual(ds.count(), 0)
        self.assertIsNotNone(ds.schema())
        self.assertEqual(set(ds.schema().names), {"id", "age", "_ROW_ID"})

    def test_empty_result_schema_matches_nonempty_read(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        proj = ["id", "age"]
        nonempty = read_by_row_id(
            target, pa.table({"_ROW_ID": [rid[1]]}, schema=pa.schema([("_ROW_ID", pa.int64())])),
            self.catalog_options, projection=proj)
        real_schema = None
        for b in nonempty.iter_batches(batch_format="pyarrow"):
            real_schema = b.schema
            break
        empty = read_by_row_id(
            target, pa.table({"_ROW_ID": pa.array([], pa.int64())}),
            self.catalog_options, projection=proj)
        self.assertTrue(real_schema.equals(empty.schema().base_schema))
        self.assertFalse(empty.schema().base_schema.field("_ROW_ID").nullable)

    def test_custom_row_id_col(self):
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
        self.assertEqual(rows[0]["_ROW_ID"], rid[2])


if __name__ == "__main__":
    unittest.main()
