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
"""Tests for G1: projection-aware native parquet reads on blob tables.

When a query on a blob table does not project any BLOB column, the base
(non-blob) parquet columns should be read via Daft's native parquet reader
instead of falling back to the pypaimon Python reader; all cases that touch a
blob column, need cross-file field merge, or have deletion vectors must keep
falling back.
"""
import os
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from typing import List, Optional

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col

from pypaimon import CatalogFactory, Schema
from pypaimon.daft import explain_paimon_scan, read_paimon
from pypaimon.daft.daft_compat import has_file_range_reads
from pypaimon.daft.daft_datasource import (
    READER_MODE_NATIVE_PARQUET,
    _blob_native_covering_files,
)


@dataclass
class _FakeFile:
    file_name: str
    write_cols: Optional[List[str]]
    first_row_id: Optional[int]
    row_count: int


class BlobNativeCoveringFilesTest(unittest.TestCase):
    """Pure-function safety logic; runs on any installed Daft."""

    BLOBS = {"content"}
    PARTS: List[str] = []

    def _files(self, *specs):
        return [_FakeFile(*s) for s in specs]

    def call(self, files, task_columns, blob=None, parts=None):
        return _blob_native_covering_files(
            files, task_columns,
            self.BLOBS if blob is None else blob,
            self.PARTS if parts is None else parts,
        )

    def test_scalar_projection_returns_covering_parquet_files(self):
        files = self._files(
            ("data-a.parquet", ["id", "name"], 0, 4),
            ("data-a.blob", ["content"], 0, 4),
            ("data-b.parquet", ["id", "name"], 4, 4),
            ("data-b.blob", ["content"], 4, 4),
        )
        got = self.call(files, ["id", "name"])
        self.assertIsNotNone(got)
        self.assertEqual([f.file_name for f in got], ["data-a.parquet", "data-b.parquet"])

    def test_blob_column_projected_returns_none(self):
        files = self._files(
            ("data-a.parquet", ["id", "name"], 0, 4),
            ("data-a.blob", ["content"], 0, 4),
        )
        self.assertIsNone(self.call(files, ["id", "content"]))

    def test_projected_column_only_in_vector_file_returns_none(self):
        files = self._files(
            ("data-a.parquet", ["id"], 0, 4),
            ("data-a.vector.foo", ["emb"], 0, 4),
        )
        self.assertIsNone(self.call(files, ["id", "emb"]))

    def test_field_merge_partial_parquet_coverage_returns_none(self):
        # id and name live in separate parquet files for the SAME rows -> merge.
        files = self._files(
            ("data-a.parquet", ["id"], 0, 4),
            ("data-b.parquet", ["name"], 0, 4),
        )
        self.assertIsNone(self.call(files, ["id", "name"]))

    def test_missing_first_row_id_returns_none(self):
        files = self._files(("data-a.parquet", ["id", "name"], None, 4))
        self.assertIsNone(self.call(files, ["id", "name"]))

    def test_uncovered_row_range_returns_none(self):
        # An older row-id range's parquet lacks the projected column; those rows
        # must still be read (as null) via fallback, not dropped.
        files = self._files(
            ("data-a.parquet", ["id"], 0, 4),          # rows 0-3: no "name"
            ("data-a.blob", ["content"], 0, 4),
            ("data-b.parquet", ["id", "name"], 4, 4),  # rows 4-7: has "name"
            ("data-b.blob", ["content"], 4, 4),
        )
        self.assertIsNone(self.call(files, ["name"]))

    def test_overlapping_covering_row_ranges_returns_none(self):
        files = self._files(
            ("data-a.parquet", ["id", "name"], 0, 10),
            ("data-b.parquet", ["id", "name"], 5, 10),
        )
        self.assertIsNone(self.call(files, ["name"]))

    def test_covering_files_span_full_split_are_kept(self):
        # Every row-id range's projected column is in a covering parquet file.
        files = self._files(
            ("data-a.parquet", ["id", "name"], 0, 4),
            ("data-a.blob", ["content"], 0, 4),
            ("data-b.parquet", ["id", "name"], 4, 4),
            ("data-b.blob", ["content"], 4, 4),
        )
        got = self.call(files, ["name"])
        self.assertIsNotNone(got)
        self.assertEqual([f.file_name for f in got], ["data-a.parquet", "data-b.parquet"])

    def test_partition_column_projection_ignored_for_coverage(self):
        files = self._files(
            ("data-a.parquet", ["id", "name"], 0, 4),
            ("data-a.blob", ["content"], 0, 4),
        )
        got = self.call(files, ["dt", "id", "name"], parts=["dt"])
        self.assertIsNotNone(got)
        self.assertEqual([f.file_name for f in got], ["data-a.parquet"])


@unittest.skipUnless(has_file_range_reads(), "installed Daft lacks File range reads")
class DaftBlobNativeReadE2ETest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.catalog_options = {"warehouse": os.path.join(self.tempdir, "wh")}
        self.catalog = CatalogFactory.create(self.catalog_options)
        self.catalog.create_database("default", True)
        self.table = "default.blob_native"
        self.pa_schema = pa.schema([
            ("id", pa.int32()), ("name", pa.string()), ("content", pa.large_binary())])
        self.catalog.create_table(self.table, Schema.from_pyarrow_schema(
            self.pa_schema,
            options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"}),
            False)
        self.payloads = [os.urandom(64) for _ in range(6)]
        t = self.catalog.get_table(self.table)
        w = t.new_batch_write_builder().new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": list(range(6)), "name": [f"n{i}" for i in range(6)],
             "content": self.payloads}, schema=self.pa_schema))
        t.new_batch_write_builder().new_commit().commit(w.prepare_commit())
        w.close()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_scalar_projection_uses_native_reader(self):
        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["id", "name"], verbose=True)
        self.assertGreater(result.native_parquet_split_count, 0)
        self.assertEqual(result.pypaimon_fallback_split_count, 0)
        self.assertTrue(
            all(s.reader_mode == READER_MODE_NATIVE_PARQUET for s in result.splits))

    def test_verbose_split_detail_excludes_blob_files(self):
        # A blob-native split reads only its parquet files: the verbose per-split
        # detail must not count/list the skipped .blob files, and must agree with
        # the native_parquet_file_count aggregate.
        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["id", "name"], verbose=True)
        native = [s for s in result.splits if s.reader_mode == READER_MODE_NATIVE_PARQUET]
        self.assertTrue(native)
        self.assertEqual(
            sum(s.file_count for s in result.splits), result.native_parquet_file_count)
        for s in native:
            self.assertEqual(s.file_count, len(s.file_paths))
            self.assertTrue(all(p.endswith(".parquet") for p in s.file_paths))

    def test_verbose_split_row_count_matches_native_rows(self):
        # The Paimon split row_count double-counts rows across the parquet and
        # .blob bunches; the per-split detail must report the rows the parquet
        # reader actually returns, matching the collected row count.
        actual_rows = len(read_paimon(self.table, self.catalog_options)
                          .select(col("id"), col("name")).to_pylist())
        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["id", "name"], verbose=True)
        native = [s for s in result.splits if s.reader_mode == READER_MODE_NATIVE_PARQUET]
        self.assertEqual(sum(s.row_count for s in native), actual_rows)

    def test_scalar_projection_results_correct(self):
        out = (read_paimon(self.table, self.catalog_options)
               .select(col("id"), col("name")).to_pylist())
        got = {r["id"]: r["name"] for r in out}
        self.assertEqual(got, {i: f"n{i}" for i in range(6)})

    def test_blob_projection_falls_back(self):
        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["id", "content"], verbose=True)
        self.assertEqual(result.native_parquet_split_count, 0)
        self.assertGreater(result.pypaimon_fallback_split_count, 0)
        self.assertIn("blob columns present", result.fallback_reasons)

    def test_no_projection_falls_back(self):
        result = explain_paimon_scan(self.table, self.catalog_options, verbose=True)
        self.assertEqual(result.native_parquet_split_count, 0)
        self.assertGreater(result.pypaimon_fallback_split_count, 0)

    def test_multi_append_concat_native_and_correct(self):
        # A second append packs another scalar parquet + blob into the split
        # (raw_convertible=False, disjoint row-id ranges). Selecting scalars must
        # still go native and concatenate both parquet bunches correctly.
        t = self.catalog.get_table(self.table)
        w = t.new_batch_write_builder().new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": list(range(6, 10)), "name": [f"n{i}" for i in range(6, 10)],
             "content": [os.urandom(64) for _ in range(4)]}, schema=self.pa_schema))
        t.new_batch_write_builder().new_commit().commit(w.prepare_commit())
        w.close()

        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["id", "name"], verbose=True)
        self.assertGreater(result.native_parquet_split_count, 0)
        self.assertEqual(result.pypaimon_fallback_split_count, 0)

        out = (read_paimon(self.table, self.catalog_options)
               .select(col("id"), col("name")).to_pylist())
        got = {r["id"]: r["name"] for r in out}
        self.assertEqual(got, {i: f"n{i}" for i in range(10)})


@unittest.skipUnless(has_file_range_reads(), "installed Daft lacks File range reads")
class DaftBlobSchemaEvolutionReadTest(unittest.TestCase):
    """A blob-table split whose older row-id range was written without the
    projected column must not be read natively (rows would be dropped); the
    fallback returns those rows with the column as null."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.catalog_options = {"warehouse": os.path.join(self.tempdir, "wh")}
        self.catalog = CatalogFactory.create(self.catalog_options)
        self.catalog.create_database("default", True)
        self.table = "default.blob_evo"
        self.pa_schema = pa.schema([
            ("id", pa.int32()), ("name", pa.string()), ("content", pa.large_binary())])
        self.catalog.create_table(self.table, Schema.from_pyarrow_schema(
            self.pa_schema,
            options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"}),
            False)
        t = self.catalog.get_table(self.table)
        wb = t.new_batch_write_builder()

        # rows 0-3: id + content only (no name), first_row_id = 0
        w = wb.new_write().with_write_type(["id", "content"])
        c = wb.new_commit()
        w.write_arrow(pa.Table.from_pydict(
            {"id": list(range(4)), "content": [os.urandom(16) for _ in range(4)]},
            schema=pa.schema([("id", pa.int32()), ("content", pa.large_binary())])))
        cmts = w.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w.close()
        c.close()

        # rows 4-7: id + name + content, first_row_id = 4
        w = wb.new_write().with_write_type(["id", "name", "content"])
        c = wb.new_commit()
        w.write_arrow(pa.Table.from_pydict(
            {"id": list(range(4, 8)), "name": [f"n{i}" for i in range(4, 8)],
             "content": [os.urandom(16) for _ in range(4)]}, schema=self.pa_schema))
        cmts = w.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 4
        c.commit(cmts)
        w.close()
        c.close()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _pypaimon_names(self, projection):
        t = self.catalog.get_table(self.table).copy({"blob-as-descriptor": "true"})
        rb = t.new_read_builder().with_projection(projection)
        rows = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
        return sorted((r["name"] for r in rows), key=lambda v: (v is not None, v))

    def test_projecting_evolved_only_column_falls_back(self):
        # A covering parquet file exists only for the newer row-id range, so the
        # native path cannot span the whole split and must fall back.
        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["name"], verbose=True)
        self.assertEqual(result.native_parquet_split_count, 0)
        self.assertGreater(result.pypaimon_fallback_split_count, 0)

    def test_projecting_evolved_only_column_matches_pypaimon(self):
        # Transparency contract: the Daft result must equal the pypaimon reader's
        # result for the same projection (native must never diverge from fallback).
        daft_names = sorted(
            (r["name"] for r in read_paimon(self.table, self.catalog_options)
             .select(col("name")).to_pylist()),
            key=lambda v: (v is not None, v))
        self.assertEqual(daft_names, self._pypaimon_names(["name"]))

    def test_projecting_stable_and_evolved_columns_matches_pypaimon(self):
        # Projecting id (present in every range) + name: pypaimon spans all rows
        # with name null for the old range; the native path must match it.
        daft_rows = read_paimon(self.table, self.catalog_options).select(
            col("id"), col("name")).to_pylist()
        daft_names = sorted((r["name"] for r in daft_rows), key=lambda v: (v is not None, v))
        self.assertEqual(daft_names, self._pypaimon_names(["id", "name"]))
        self.assertEqual(len(daft_rows), 8)


@unittest.skipUnless(has_file_range_reads(), "installed Daft lacks File range reads")
class DaftPartitionedBlobNativeReadTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.catalog_options = {"warehouse": os.path.join(self.tempdir, "wh")}
        self.catalog = CatalogFactory.create(self.catalog_options)
        self.catalog.create_database("default", True)
        self.table = "default.blob_native_part"
        self.pa_schema = pa.schema([
            ("dt", pa.string()), ("id", pa.int32()), ("content", pa.large_binary())])
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=["dt"],
            options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"})
        self.catalog.create_table(self.table, schema, False)
        t = self.catalog.get_table(self.table)
        w = t.new_batch_write_builder().new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"dt": ["a", "a", "b"], "id": [1, 2, 3],
             "content": [os.urandom(32) for _ in range(3)]}, schema=self.pa_schema))
        t.new_batch_write_builder().new_commit().commit(w.prepare_commit())
        w.close()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_scalar_projection_native_with_partition_values(self):
        result = explain_paimon_scan(
            self.table, self.catalog_options, columns=["dt", "id"], verbose=True)
        self.assertGreater(result.native_parquet_split_count, 0)
        self.assertEqual(result.pypaimon_fallback_split_count, 0)

        out = (read_paimon(self.table, self.catalog_options)
               .select(col("dt"), col("id")).to_pylist())
        got = {r["id"]: r["dt"] for r in out}
        self.assertEqual(got, {1: "a", 2: "a", 3: "b"})


if __name__ == "__main__":
    unittest.main()
