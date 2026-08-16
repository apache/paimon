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
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col

from pypaimon import CatalogFactory, Schema
from pypaimon.daft import open_blob, read_blob, read_paimon
from pypaimon.daft.daft_compat import has_file_range_reads


@unittest.skipUnless(has_file_range_reads(), "installed Daft lacks File range reads")
class DaftReadBlobTest(unittest.TestCase):
    """read_blob reads a blob File column to bytes via pypaimon's FileIO and must
    return exactly the same bytes as the native per-row File.open() path."""

    N = 40

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.catalog_options = {"warehouse": os.path.join(self.tempdir, "wh")}
        self.catalog = CatalogFactory.create(self.catalog_options)
        self.catalog.create_database("default", True)
        self.table = "default.blob_read_test"
        pa_schema = pa.schema([("id", pa.int32()), ("content", pa.large_binary())])
        self.catalog.create_table(self.table, Schema.from_pyarrow_schema(
            pa_schema, options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"}), False)
        self.payloads = [bytes([i & 0xFF]) + os.urandom(1023) for i in range(self.N)]
        t = self.catalog.get_table(self.table)
        w = t.new_batch_write_builder().new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": list(range(self.N)), "content": self.payloads}, schema=pa_schema))
        t.new_batch_write_builder().new_commit().commit(w.prepare_commit())
        w.close()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_read_blob_matches_file_open(self):
        df = read_paimon(self.table, self.catalog_options)
        # content is exposed as a lazy File (default read path unchanged)
        self.assertEqual(str(df.schema()["content"].dtype), "File[Unknown]")

        # new path: read_blob -> bytes
        out = df.select(col("id"), read_blob(col("content"), self.catalog_options, self.table)
                        .alias("img")).collect().to_pydict()
        got = dict(zip(out["id"], out["img"]))
        for i in range(self.N):
            self.assertEqual(got[i], self.payloads[i])

        # original path still works and yields identical bytes
        for r in read_paimon(self.table, self.catalog_options).select(col("id"), col("content")).to_pylist():
            with r["content"].open() as h:
                self.assertEqual(h.read(), self.payloads[r["id"]])

    def test_open_blob_full_read(self):
        rows = read_paimon(self.table, self.catalog_options).select(
            col("id"), col("content")).to_pylist()
        for r in rows:
            with open_blob(r["content"], self.catalog_options, self.table) as f:
                self.assertEqual(f.read(), self.payloads[r["id"]])

    def test_open_blob_partial_read(self):
        rows = read_paimon(self.table, self.catalog_options).select(
            col("id"), col("content")).to_pylist()
        for r in rows:
            with open_blob(r["content"], self.catalog_options, self.table) as f:
                header = f.read(16)
                self.assertEqual(header, self.payloads[r["id"]][:16])

    def test_open_blob_seek(self):
        r = read_paimon(self.table, self.catalog_options).select(
            col("id"), col("content")).to_pylist()[0]
        with open_blob(r["content"], self.catalog_options, self.table) as f:
            f.seek(100)
            self.assertEqual(f.tell(), 100)
            chunk = f.read(50)
            self.assertEqual(chunk, self.payloads[r["id"]][100:150])

    def test_no_file_size_stat(self):
        """File.size() does a network stat; read_blob and open_blob must resolve
        ranges from embedded metadata (file._inner) and never call it."""
        rows = read_paimon(self.table, self.catalog_options).select(
            col("id"), col("content")).to_pylist()
        file_cls = type(rows[0]["content"])
        calls = {"n": 0}
        orig = file_cls.size

        def boom(self, *a, **k):
            calls["n"] += 1
            raise AssertionError("File.size() should not be called")

        file_cls.size = boom
        try:
            with open_blob(rows[0]["content"], self.catalog_options, self.table) as f:
                self.assertEqual(f.read(), self.payloads[rows[0]["id"]])
            out = read_paimon(self.table, self.catalog_options).select(
                col("id"), read_blob(col("content"), self.catalog_options, self.table).alias("img")
            ).collect().to_pydict()
            got = dict(zip(out["id"], out["img"]))
            for i in range(self.N):
                self.assertEqual(got[i], self.payloads[i])
        finally:
            file_cls.size = orig
        self.assertEqual(calls["n"], 0)


if __name__ == "__main__":
    unittest.main()
