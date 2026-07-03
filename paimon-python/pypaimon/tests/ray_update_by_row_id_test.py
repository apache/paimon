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
from pypaimon.ray import update_by_row_id


class RayUpdateByRowIdTest(unittest.TestCase):
    """Distributed row-id update: rewrite only the files owning the given row ids,
    without reading or joining the whole target (unlike merge_into(on=_ROW_ID))."""

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

    def _create(self, options=None):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        opts = self.de_options if options is None else options
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(self.pa_schema, options=opts), False)
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

    def test_update_by_row_id_basic(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": list(range(1, 7)), "name": [f"n{i}" for i in range(1, 7)],
             "age": [i * 10 for i in range(1, 7)]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)

        # update age for ids 2 and 5 only, addressed by their _ROW_ID
        src = pa.table({"_ROW_ID": [rid[2], rid[5]], "age": [999, 888]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        # Proof of no full-target read: read_paimon is never called (source is a
        # Dataset, and the update routes by manifest metadata, not a scan).
        import pypaimon.ray.ray_paimon as rp
        with mock.patch.object(rp, "read_paimon",
                               side_effect=AssertionError("target was read!")):
            stats = update_by_row_id(target, ray.data.from_arrow(src),
                                     self.catalog_options, update_cols=["age"])
        self.assertEqual(stats, {"num_updated": 2})

        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(back["age"], [10, 999, 30, 40, 888, 60])
        self.assertEqual(back["name"], [f"n{i}" for i in range(1, 7)])  # untouched

    def test_pins_base_snapshot_for_conflict_detection(self):
        # The update pins its base snapshot and threads it to distributed_update_apply,
        # which uses it for commit-time conflict detection against concurrent writers.
        import importlib
        m = importlib.import_module("pypaimon.ray.update_by_row_id")  # module, not the fn
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        expected_sid = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[1]], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        captured = {}

        def fake_apply(update_ds, table, cols, *, num_partitions,
                       ray_remote_args=None, base_snapshot_id=None):
            captured["base_snapshot_id"] = base_snapshot_id
            return [], 0, []

        with mock.patch.object(m, "distributed_update_apply", fake_apply):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])
        self.assertEqual(captured["base_snapshot_id"], expected_sid)

    def test_accepts_pyarrow_and_pandas_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        # pyarrow.Table source
        update_by_row_id(
            target,
            pa.table({"_ROW_ID": [rid[1]], "age": [77]},
                     schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())])),
            self.catalog_options, update_cols=["age"])
        self.assertEqual(self._read(target).sort_by("id").to_pydict()["age"], [77, 2])

        # pandas.DataFrame source, updating multiple columns at once
        import pandas as pd
        update_by_row_id(
            target,
            pd.DataFrame({"_ROW_ID": pd.array([rid[2]], dtype="int64"),
                          "name": ["z"], "age": pd.array([88], dtype="int32")}),
            self.catalog_options, update_cols=["name", "age"])
        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(back["age"], [77, 88])
        self.assertEqual(back["name"], ["a", "z"])

    def test_accepts_table_name_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        # a fresh source table's _ROW_ID (0, 1) lines up with the target's
        source = self._create()
        self._write(source, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [55, 66]}, schema=self.pa_schema))
        stats = update_by_row_id(target, source, self.catalog_options, update_cols=["age"])
        self.assertEqual(stats, {"num_updated": 2})
        self.assertEqual(self._read(target).sort_by("id").to_pydict()["age"], [55, 66])

    def test_rejects_non_data_evolution_table(self):
        target = self._create(options={})  # plain append table
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_missing_row_id_column(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"age": [9]}, schema=pa.schema([("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_partition_column_update(self):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        s = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=["name"],
                                       options=self.de_options)
        self.catalog.create_table(name, s, False)
        self._write(name, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "name": ["b"]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("name", pa.string())]))
        with self.assertRaises(ValueError):
            update_by_row_id(name, src, self.catalog_options, update_cols=["name"])

    def test_rejects_deletion_vectors_table(self):
        # A DV-deleted row still lives in its file, so update_by_row_id can't tell it is
        # gone without reading the target; DV tables are refused for now.
        opts = dict(self.de_options, **{"deletion-vectors.enabled": "true"})
        target = self._create(options=opts)
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_blob_column_update(self):
        blob_schema = pa.schema([("id", pa.int32()), ("payload", pa.large_binary())])
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(blob_schema, options=self.de_options), False)
        self._write(name, pa.Table.from_pydict(
            {"id": [1], "payload": pa.array([b"x"], pa.large_binary())}, schema=blob_schema))
        src = pa.table({"_ROW_ID": [0], "payload": pa.array([b"y"], pa.large_binary())},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("payload", pa.large_binary())]))
        with self.assertRaises(ValueError):
            update_by_row_id(name, src, self.catalog_options, update_cols=["payload"])

    def test_empty_target_foreign_row_id_raises(self):
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        empty_src = pa.table({"_ROW_ID": pa.array([], pa.int64()),
                              "age": pa.array([], pa.int32())})

        # (a) never written -> no snapshot
        target = self._create()
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])
        # empty source against an empty target is a no-op, not an error
        self.assertEqual(
            update_by_row_id(target, empty_src, self.catalog_options, update_cols=["age"]),
            {"num_updated": 0})

        # (b) written then emptied by overwrite -> snapshot exists but 0 live rows
        target2 = self._create()
        self._write(target2, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        wb = self.catalog.get_table(target2).new_batch_write_builder().overwrite()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": pa.array([], pa.int32()), "name": pa.array([], pa.string()),
             "age": pa.array([], pa.int32())}, schema=self.pa_schema))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        with self.assertRaises(ValueError):
            update_by_row_id(target2, src, self.catalog_options, update_cols=["age"])

    def test_rejects_unknown_and_empty_update_cols(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["nope"])
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=[])


if __name__ == "__main__":
    unittest.main()
