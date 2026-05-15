# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for ``ReadBuilder.explain``: pruning funnel counters,
split-level execution signals, and pretty-print smoke."""

import os
import shutil
import tempfile
import unittest
from typing import Any, Dict, List

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.read.explain import ExplainResult
from pypaimon.read.explain_render import render_predicate


def _write(table, rows: List[Dict], pa_schema: pa.Schema) -> None:
    wb = table.new_batch_write_builder()
    w = wb.new_write()
    c = wb.new_commit()
    try:
        w.write_arrow(pa.Table.from_pylist(rows, schema=pa_schema))
        c.commit(w.prepare_commit())
    finally:
        w.close()
        c.close()


class ReadBuilderExplainTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    # ---- helpers --------------------------------------------------------

    def _append_table(self, name: str) -> Any:
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={'bucket': '-1', 'file.format': 'parquet'},
        )
        full = 'default.{}'.format(name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full), pa_schema

    def _pk_partitioned_bucketed_table(self, name: str, num_buckets: int = 4) -> Any:
        pa_schema = pa.schema([
            pa.field('dt', pa.string(), nullable=False),
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['dt'],
            primary_keys=['dt', 'id'],
            options={
                'bucket': str(num_buckets),
                'bucket-key': 'id',
                'file.format': 'parquet',
            },
        )
        full = 'default.{}'.format(name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full), pa_schema

    def _pk_dv_table(self, name: str, num_buckets: int = 2) -> Any:
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={
                'bucket': str(num_buckets),
                'file.format': 'parquet',
                'deletion-vectors.enabled': 'true',
            },
        )
        full = 'default.{}'.format(name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full), pa_schema

    # ---- 1. append-only baseline ---------------------------------------

    def test_explain_append_only_no_predicate(self):
        table, pa_schema = self._append_table('explain_basic')
        _write(table, [{'id': i, 'val': i * 2} for i in range(20)], pa_schema)
        _write(table, [{'id': i, 'val': i * 3} for i in range(20, 40)], pa_schema)

        rb = table.new_read_builder()
        result = rb.explain()

        plan_splits = rb.new_scan().plan().splits()
        self.assertEqual(result.split_count, len(plan_splits))
        self.assertGreater(result.file_count, 0)
        self.assertGreater(result.total_file_size, 0)
        self.assertIsNone(result.partition_pruning)
        self.assertIsNone(result.bucket_pruning)
        self.assertIsNone(result.file_skipping)
        self.assertIsNone(result.predicate)

    # ---- 2. PK + partition + bucket pruning ----------------------------

    def test_explain_pk_table_with_partition_and_bucket_predicate(self):
        table, pa_schema = self._pk_partitioned_bucketed_table(
            'explain_pk_pruning', num_buckets=4)
        for day in ['2026-05-01', '2026-05-02', '2026-05-03', '2026-05-04']:
            _write(
                table,
                [{'dt': day, 'id': i, 'val': i + 1} for i in range(20)],
                pa_schema,
            )

        rb = table.new_read_builder()
        pb = rb.new_predicate_builder()
        pred = pb.and_predicates([
            pb.equal('dt', '2026-05-01'),
            pb.equal('id', 7),
        ])
        rb = rb.with_filter(pred)
        result = rb.explain()

        self.assertIsNotNone(result.partition_pruning)
        self.assertIsNotNone(result.bucket_pruning)
        self.assertIsNotNone(result.file_skipping)
        self.assertGreater(
            result.partition_pruning.before, result.partition_pruning.after,
            "partition predicate must drop at least one entry")
        self.assertGreater(
            result.bucket_pruning.before, result.bucket_pruning.after,
            "HASH_FIXED bucket pruning must drop at least one entry")

        # Cross-check against the actual scan
        plan_splits = rb.new_scan().plan().splits()
        self.assertEqual(result.split_count, len(plan_splits))

    # ---- 3. predicate rendering ----------------------------------------

    def test_render_predicate_shapes(self):
        eq = Predicate(method='equal', index=0, field='dt', literals=['2026-05-01'])
        self.assertEqual(render_predicate(eq), "dt = '2026-05-01'")

        in_p = Predicate(method='in', index=1, field='id', literals=[1, 2, 3])
        self.assertEqual(render_predicate(in_p), "id IN [1, 2, 3]")

        between = Predicate(method='between', index=1, field='id', literals=[5, 10])
        self.assertEqual(render_predicate(between), "id BETWEEN 5 AND 10")

        is_null = Predicate(method='isNull', index=0, field='val', literals=None)
        self.assertEqual(render_predicate(is_null), "val IS NULL")

        and_p = Predicate(method='and', index=None, field=None, literals=[eq, in_p])
        self.assertEqual(
            render_predicate(and_p),
            "(dt = '2026-05-01') AND (id IN [1, 2, 3])")

        or_p = Predicate(method='or', index=None, field=None, literals=[between, is_null])
        self.assertEqual(
            render_predicate(or_p),
            "(id BETWEEN 5 AND 10) OR (val IS NULL)")

    # ---- 4. verbose split detail ---------------------------------------

    def test_explain_verbose_lists_per_split_detail(self):
        table, pa_schema = self._append_table('explain_verbose')
        _write(table, [{'id': i, 'val': i} for i in range(50)], pa_schema)
        _write(table, [{'id': i, 'val': i} for i in range(50, 100)], pa_schema)

        rb = table.new_read_builder()
        result = rb.explain(verbose=True)
        self.assertIsNotNone(result.splits)
        self.assertEqual(len(result.splits), result.split_count)

        plan_splits = rb.new_scan().plan().splits()
        for explained, actual in zip(result.splits, plan_splits):
            self.assertEqual(explained.bucket, actual.bucket)
            self.assertEqual(explained.file_count, len(actual.files))
            self.assertEqual(set(explained.file_paths), set(actual.file_paths))

    # ---- 5. empty snapshot path ----------------------------------------

    def test_explain_empty_snapshot(self):
        table, _ = self._append_table('explain_empty')

        rb = table.new_read_builder()
        result = rb.explain()
        self.assertIsNone(result.snapshot_id)
        self.assertEqual(result.split_count, 0)
        self.assertEqual(result.file_count, 0)
        self.assertIn("<none>", str(result))

    # ---- 6. split-level metrics: DV vs append-only ---------------------

    def test_explain_split_level_metrics(self):
        # Append-only: every split is raw-convertible, no deletion vectors.
        table, pa_schema = self._append_table('explain_split_signals_ap')
        _write(table, [{'id': i, 'val': i} for i in range(30)], pa_schema)
        _write(table, [{'id': i, 'val': i} for i in range(30, 60)], pa_schema)
        result = table.new_read_builder().explain()
        self.assertGreater(result.split_count, 0)
        self.assertEqual(result.splits_with_deletion_vectors, 0)
        self.assertEqual(result.splits_raw_convertible, result.split_count)
        # All written files are at L0, and append-only doesn't filter them.
        self.assertIn(0, result.level_histogram)
        self.assertEqual(result.splits_all_above_l0, 0)

        # DV-enabled PK table: pypaimon writes alone don't trigger compaction,
        # so every file stays at L0. ``_filter_manifest_entry`` then drops
        # them all, leaving an empty plan. The vacuous "all above L0"
        # invariant (0 / 0) still has to hold.
        dv_table, dv_pa = self._pk_dv_table('explain_split_signals_dv', num_buckets=2)
        _write(dv_table, [{'id': i, 'val': i} for i in range(20)], dv_pa)
        _write(dv_table, [{'id': i, 'val': i * 10} for i in range(10)], dv_pa)
        dv_result = dv_table.new_read_builder().explain()
        self.assertEqual(dv_result.split_count, 0)
        self.assertEqual(dv_result.splits_all_above_l0, 0)

    # ---- 7. pretty-print smoke -----------------------------------------

    def test_pretty_print_smoke(self):
        table, pa_schema = self._append_table('explain_print_smoke')
        _write(table, [{'id': i, 'val': i} for i in range(40)], pa_schema)

        result = table.new_read_builder().explain()
        self.assertIsInstance(result, ExplainResult)
        printed = str(result)
        for anchor in (
            "Snapshot:",
            "Splits:",
            "raw-convertible:",
            "with DV:",
            "size/split:",
            "Files:",
            "Total size:",
        ):
            self.assertIn(anchor, printed, "missing anchor: " + anchor)


if __name__ == '__main__':
    unittest.main()
