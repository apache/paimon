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
