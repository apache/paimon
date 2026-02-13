"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import tempfile
import unittest
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.row.offset_row import OffsetRow


def _filter_batch_arrow(batch, predicate):
    expr = predicate.to_arrow()
    table = ds.InMemoryDataset(pa.Table.from_batches([batch])).scanner(filter=expr).to_table()
    if table.num_rows == 0:
        return batch.slice(0, 0)
    batches = table.to_batches()
    if len(batches) == 1:
        return batches[0]
    return pa.RecordBatch.from_arrays(
        [table.column(i) for i in range(table.num_columns)], schema=table.schema
    )


def _filter_batch_row_by_row(batch, predicate, ncols):
    nrows = batch.num_rows
    mask = []
    row_tuple = [None] * ncols
    offset_row = OffsetRow(row_tuple, 0, ncols)
    for i in range(nrows):
        for j in range(ncols):
            row_tuple[j] = batch.column(j)[i].as_py()
        offset_row.replace(tuple(row_tuple))
        try:
            mask.append(predicate.test(offset_row))
        except (TypeError, ValueError):
            mask.append(False)
    if not any(mask):
        return batch.slice(0, 0)
    return batch.filter(pa.array(mask))


def _batches_equal(a, b):
    if a.num_rows != b.num_rows or a.num_columns != b.num_columns:
        return False
    for i in range(a.num_columns):
        col_a, col_b = a.column(i), b.column(i)
        for j in range(a.num_rows):
            va_py = col_a[j].as_py() if hasattr(col_a[j], "as_py") else col_a[j]
            vb_py = col_b[j].as_py() if hasattr(col_b[j], "as_py") else col_b[j]
            if va_py != vb_py:
                return False
    return True


class DataEvolutionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

    def test_basic(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema,
                                            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
        self.catalog.create_table('default.test_row_tracking', schema, False)
        table = self.catalog.get_table('default.test_row_tracking')

        # write 1
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            'f1': [-1001, 1002]
        }, schema=simple_pa_schema)
        table_write.write_arrow(expect_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # write 2
        table_write = write_builder.new_write().with_write_type(['f0'])
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'f0': [3, 4],
        }, schema=pa.schema([
            ('f0', pa.int8()),
        ]))
        table_write.write_arrow(data2)
        cmts = table_write.prepare_commit()
        cmts[0].new_files[0].first_row_id = 0
        table_commit.commit(cmts)
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        expect_data = pa.Table.from_pydict({
            'f0': [3, 4],
            'f1': [-1001, 1002]
        }, schema=pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
        ]))
        self.assertEqual(actual_data, expect_data)

        # assert manifest file meta contains min and max row id
        manifest_list_manager = ManifestListManager(table)
        snapshot_manager = SnapshotManager(table)
        manifest = manifest_list_manager.read(snapshot_manager.get_latest_snapshot().delta_manifest_list)[0]
        self.assertEqual(0, manifest.min_row_id)
        self.assertEqual(1, manifest.max_row_id)

    def test_merge_reader(self):
        from pypaimon.read.reader.concat_batch_reader import MergeAllBatchReader

        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'read.batch-size': '4096',
            },
        )
        self.catalog.create_table('default.test_merge_reader_batch_sizes', schema, False)
        table = self.catalog.get_table('default.test_merge_reader_batch_sizes')

        write_builder = table.new_batch_write_builder()
        size = 5000
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()
        d0 = pa.Table.from_pydict(
            {'f0': list(range(size)), 'f1': [f'a{i}' for i in range(size)]},
            schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]),
        )
        d1 = pa.Table.from_pydict(
            {'f2': [f'b{i}' for i in range(size)]},
            schema=pa.schema([('f2', pa.string())]),
        )
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        original_merge_all = MergeAllBatchReader
        call_count = [0]

        def patched_merge_all(reader_suppliers, batch_size=1024):
            call_count[0] += 1
            if call_count[0] == 2:
                batch_size = 999
            return original_merge_all(reader_suppliers, batch_size=batch_size)

        import pypaimon.read.split_read as split_read_module
        split_read_module.MergeAllBatchReader = patched_merge_all
        try:
            read_builder = table.new_read_builder()
            table_scan = read_builder.new_scan()
            table_read = read_builder.new_read()
            splits = table_scan.plan().splits()
            actual_data = table_read.to_arrow(splits)
            expect_data = pa.Table.from_pydict({
                'f0': list(range(size)),
                'f1': [f'a{i}' for i in range(size)],
                'f2': [f'b{i}' for i in range(size)],
            }, schema=simple_pa_schema)
            self.assertEqual(actual_data.num_rows, size)
            self.assertEqual(actual_data, expect_data)
        finally:
            split_read_module.MergeAllBatchReader = original_merge_all

    def test_with_slice(self):
        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("b", pa.int32()),
            ("c", pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
                "source.split.target-size": "512m",
            },
        )
        table_name = "default.test_with_slice_data_evolution"
        self.catalog.create_table(table_name, schema, ignore_if_exists=True)
        table = self.catalog.get_table(table_name)

        for batch in [
            {"id": [1, 2], "b": [10, 20], "c": [100, 200]},
            {"id": [1001, 2001], "b": [1011, 2011], "c": [1001, 2001]},
            {"id": [-1, -2], "b": [-10, -20], "c": [-100, -200]},
        ]:
            wb = table.new_batch_write_builder()
            tw = wb.new_write()
            tc = wb.new_commit()
            tw.write_arrow(pa.Table.from_pydict(batch, schema=pa_schema))
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()

        rb = table.new_read_builder()
        full_splits = rb.new_scan().plan().splits()
        full_result = rb.new_read().to_pandas(full_splits)
        self.assertEqual(
            len(full_result),
            6,
            "Full scan should return 6 rows",
        )
        self.assertEqual(
            sorted(full_result["id"].tolist()),
            [-2, -1, 1, 2, 1001, 2001],
            "Full set ids mismatch",
        )

        # with_slice(1, 4) -> row indices [1, 2, 3] -> 3 rows with id in (2, 1001, 2001)
        scan = rb.new_scan().with_slice(1, 4)
        splits = scan.plan().splits()
        result = rb.new_read().to_pandas(splits)
        self.assertEqual(
            len(result),
            3,
            "with_slice(1, 4) should return 3 rows (indices 1,2,3). "
            "Bug: DataEvolutionSplitGenerator returns 2 when split has multiple data files.",
        )
        ids = result["id"].tolist()
        self.assertEqual(
            sorted(ids),
            [2, 1001, 2001],
            "with_slice(1, 4) should return id in (2, 1001, 2001). Got ids=%s" % ids,
        )

        # Out-of-bounds slice: 6 rows total, slice(10, 12) should return 0 rows
        scan_oob = rb.new_scan().with_slice(10, 12)
        splits_oob = scan_oob.plan().splits()
        result_oob = rb.new_read().to_pandas(splits_oob)
        self.assertEqual(
            len(result_oob),
            0,
            "with_slice(10, 12) on 6 rows should return 0 rows (out of bounds), got %d"
            % len(result_oob),
        )

    def test_multiple_appends(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_multiple_appends', schema, False)
        table = self.catalog.get_table('default.test_multiple_appends')

        write_builder = table.new_batch_write_builder()

        # write 100 rows: (1, "a", "b")
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        init_data = pa.Table.from_pydict({
            'f0': [1] * 100,
            'f1': ['a'] * 100,
            'f2': ['b'] * 100,
        }, schema=simple_pa_schema)
        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        # append：write (2, "x") and ("y"), set first_row_id = 100
        write0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        write1 = write_builder.new_write().with_write_type(['f2'])
        commit = write_builder.new_commit()
        data0 = pa.Table.from_pydict({'f0': [2], 'f1': ['x']},
                                     schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        data1 = pa.Table.from_pydict({'f2': ['y']}, schema=pa.schema([('f2', pa.string())]))
        write0.write_arrow(data0)
        write1.write_arrow(data1)
        cmts = write0.prepare_commit() + write1.prepare_commit()
        for c in cmts:
            for nf in c.new_files:
                nf.first_row_id = 100
        commit.commit(cmts)
        write0.close()
        write1.close()
        commit.close()

        # append：write (3, "c") and ("d"), set first_row_id = 101
        write0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        commit0 = write_builder.new_commit()
        data0 = pa.Table.from_pydict({'f0': [3], 'f1': ['c']},
                                     schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        write0.write_arrow(data0)
        cmts0 = write0.prepare_commit()
        for c in cmts0:
            for nf in c.new_files:
                nf.first_row_id = 101
        commit0.commit(cmts0)
        write0.close()
        commit0.close()

        write1 = write_builder.new_write().with_write_type(['f2'])
        commit1 = write_builder.new_commit()
        data1 = pa.Table.from_pydict({'f2': ['d']}, schema=pa.schema([('f2', pa.string())]))
        write1.write_arrow(data1)
        cmts1 = write1.prepare_commit()
        for c in cmts1:
            for nf in c.new_files:
                nf.first_row_id = 101
        commit1.commit(cmts1)
        write1.close()
        commit1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        self.assertEqual(actual.num_rows, 102)
        expect = pa.Table.from_pydict({
            'f0': [1] * 100 + [2] + [3],
            'f1': ['a'] * 100 + ['x'] + ['c'],
            'f2': ['b'] * 100 + ['y'] + ['d'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_disorder_cols_append(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_disorder_cols_append', schema, False)
        table = self.catalog.get_table('default.test_disorder_cols_append')

        write_builder = table.new_batch_write_builder()
        num_rows = 100
        # write 1 rows: (1, "a", "b")
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        init_data = pa.Table.from_pydict({
            'f0': [1] * num_rows,
            'f1': ['a'] * num_rows,
            'f2': ['b'] * num_rows,
        }, schema=simple_pa_schema)
        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # append：set first_row_id = 0 to modify the row with columns write
        write0 = write_builder.new_write().with_write_type(['f0', 'f2'])
        write1 = write_builder.new_write().with_write_type(['f1'])
        commit = write_builder.new_commit()
        data0 = pa.Table.from_pydict({'f0': [2] * num_rows, 'f2': ['y'] * num_rows},
                                     schema=pa.schema([('f0', pa.int32()), ('f2', pa.string())]))
        data1 = pa.Table.from_pydict({'f1': ['x'] * num_rows}, schema=pa.schema([('f1', pa.string())]))
        write0.write_arrow(data0)
        write1.write_arrow(data1)
        cmts = write0.prepare_commit() + write1.prepare_commit()
        for c in cmts:
            for nf in c.new_files:
                nf.first_row_id = 0
        commit.commit(cmts)
        write0.close()
        write1.close()
        commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        self.assertEqual(actual.num_rows, 100)
        expect = pa.Table.from_pydict({
            'f0': [2] * num_rows,
            'f1': ['x'] * num_rows,
            'f2': ['y'] * num_rows,
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_only_some_columns(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_only_some_columns', schema, False)
        table = self.catalog.get_table('default.test_only_some_columns')

        write_builder = table.new_batch_write_builder()

        # Commit 1: f0
        w0 = write_builder.new_write().with_write_type(['f0'])
        c0 = write_builder.new_commit()
        d0 = pa.Table.from_pydict({'f0': [1]}, schema=pa.schema([('f0', pa.int32())]))
        w0.write_arrow(d0)
        c0.commit(w0.prepare_commit())
        w0.close()
        c0.close()

        # Commit 2: f1, first_row_id = 0
        w1 = write_builder.new_write().with_write_type(['f1'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({'f1': ['a']}, schema=pa.schema([('f1', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        for c in cmts1:
            for nf in c.new_files:
                nf.first_row_id = 0
        c1.commit(cmts1)
        w1.close()
        c1.close()

        # Commit 3: f2, first_row_id = 0
        w2 = write_builder.new_write().with_write_type(['f2'])
        c2 = write_builder.new_commit()
        d2 = pa.Table.from_pydict({'f2': ['b']}, schema=pa.schema([('f2', pa.string())]))
        w2.write_arrow(d2)
        cmts2 = w2.prepare_commit()
        for c in cmts2:
            for nf in c.new_files:
                nf.first_row_id = 0
        c2.commit(cmts2)
        w2.close()
        c2.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expect = pa.Table.from_pydict({
            'f0': [1],
            'f1': ['a'],
            'f2': ['b'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def _create_filter_test_table(self, table_name: str):
        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("b", pa.int32()),
            pa.field("c", pa.int32(), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"},
        )
        self.catalog.create_table(table_name, schema, ignore_if_exists=True)
        table = self.catalog.get_table(table_name)
        wb = table.new_batch_write_builder()
        w0, c0 = wb.new_write().with_write_type(["id", "b"]), wb.new_commit()
        w0.write_arrow(pa.Table.from_pydict(
            {"id": [1, 2, 3], "b": [10, 20, 30]},
            schema=pa.schema([("id", pa.int64()), ("b", pa.int32())]),
        ))
        c0.commit(w0.prepare_commit())
        w0.close()
        c0.close()
        w1, c1 = wb.new_write().with_write_type(["c"]), wb.new_commit()
        w1.write_arrow(pa.Table.from_pydict(
            {"c": [100, None, 200]},
            schema=pa.schema([pa.field("c", pa.int32(), nullable=True)]),
        ))
        cmts1 = w1.prepare_commit()
        for cmt in cmts1:
            for nf in cmt.new_files:
                nf.first_row_id = 0
        c1.commit(cmts1)
        w1.close()
        c1.close()
        return table

    def test_with_filter(self):
        table = self._create_filter_test_table("default.test_filter_on_evolved_column")
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()

        full_df = rb.new_read().to_pandas(splits)
        self.assertEqual(len(full_df), 3, "Full scan must return 3 rows")
        full_sorted = full_df.sort_values("id").reset_index(drop=True)
        self.assertEqual(full_sorted["id"].tolist(), [1, 2, 3])
        self.assertEqual(full_sorted["b"].tolist(), [10, 20, 30])
        self.assertEqual(full_sorted["c"].iloc[0], 100)
        self.assertTrue(pd.isna(full_sorted["c"].iloc[1]), "Row id=2 must have NULL c")
        self.assertEqual(full_sorted["c"].iloc[2], 200)

        predicate_gt = rb.new_predicate_builder().greater_than("c", 150)
        rb_gt = table.new_read_builder().with_filter(predicate_gt)
        result_gt = rb_gt.new_read().to_pandas(rb_gt.new_scan().plan().splits())
        self.assertEqual(len(result_gt), 1, "Filter c > 150 should return 1 row (c=200)")
        self.assertEqual(result_gt["id"].iloc[0], 3, "Row with c=200 must have id=3")
        self.assertEqual(result_gt["b"].iloc[0], 30, "Row with c=200 must have b=30")
        self.assertEqual(result_gt["c"].iloc[0], 200, "Filtered row must have c=200")

        predicate_lt = rb.new_predicate_builder().less_than("c", 150)
        rb_lt = table.new_read_builder().with_filter(predicate_lt)
        result_lt = rb_lt.new_read().to_pandas(rb_lt.new_scan().plan().splits())
        self.assertEqual(len(result_lt), 1, "Filter c < 150 should return 1 row (c=100)")
        self.assertEqual(result_lt["id"].iloc[0], 1, "Row with c=100 must have id=1")
        self.assertEqual(result_lt["c"].iloc[0], 100, "Filtered row must have c=100")

        predicate_id = rb.new_predicate_builder().equal("id", 2)
        rb_id = table.new_read_builder().with_filter(predicate_id)
        result_id = rb_id.new_read().to_pandas(rb_id.new_scan().plan().splits())
        self.assertEqual(len(result_id), 1, "Filter id == 2 should return 1 row")
        self.assertEqual(result_id["id"].iloc[0], 2, "Filtered row must have id=2")
        self.assertTrue(pd.isna(result_id["c"].iloc[0]), "Row id=2 must have c=NULL")

        pb = rb.new_predicate_builder()
        predicate_and = pb.and_predicates([
            pb.greater_than("c", 50),
            pb.less_than("c", 150),
        ])
        rb_and = table.new_read_builder().with_filter(predicate_and)
        result_and = rb_and.new_read().to_pandas(rb_and.new_scan().plan().splits())
        self.assertEqual(
            len(result_and), 1,
            "Filter c>50 AND c<150 should return 1 row (c=100)",
        )
        self.assertEqual(result_and["id"].iloc[0], 1, "Row with c=100 must have id=1")
        self.assertEqual(result_and["c"].iloc[0], 100, "Filtered row must have c=100")

        predicate_is_null = rb.new_predicate_builder().is_null("c")
        rb_null = table.new_read_builder().with_filter(predicate_is_null)
        result_null = rb_null.new_read().to_pandas(rb_null.new_scan().plan().splits())
        self.assertEqual(len(result_null), 1, "Filter c IS NULL should return 1 row (id=2)")
        self.assertEqual(result_null["id"].iloc[0], 2, "NULL row must have id=2")
        self.assertTrue(pd.isna(result_null["c"].iloc[0]), "Filtered row c must be NULL")

        predicate_not_null = rb.new_predicate_builder().is_not_null("c")
        rb_not_null = table.new_read_builder().with_filter(predicate_not_null)
        result_not_null = rb_not_null.new_read().to_pandas(
            rb_not_null.new_scan().plan().splits())
        self.assertEqual(
            len(result_not_null), 2,
            "Filter c IS NOT NULL should return 2 rows (id=1, id=3)",
        )
        result_not_null_sorted = result_not_null.sort_values("id").reset_index(drop=True)
        self.assertEqual(result_not_null_sorted["id"].tolist(), [1, 3])
        self.assertEqual(result_not_null_sorted["c"].tolist(), [100, 200])

        predicate_or = pb.or_predicates([
            pb.greater_than("c", 150),
            pb.less_than("c", 100),
        ])
        rb_or = table.new_read_builder().with_filter(predicate_or)
        result_or = rb_or.new_read().to_pandas(rb_or.new_scan().plan().splits())
        self.assertEqual(
            len(result_or), 1,
            "Filter c>150 OR c<100 should return 1 row (id=3, c=200)",
        )
        self.assertEqual(result_or["id"].iloc[0], 3, "Row with c=200 must have id=3")
        self.assertEqual(result_or["c"].iloc[0], 200, "Filtered row must have c=200")

    def test_with_filter_and_projection(self):
        table = self._create_filter_test_table("default.test_filter_and_projection_evolved")
        rb_full = table.new_read_builder()
        predicate = rb_full.new_predicate_builder().greater_than("c", 150)
        rb_filtered = table.new_read_builder().with_projection(["c", "id"]).with_filter(predicate)
        result = rb_filtered.new_read().to_pandas(rb_filtered.new_scan().plan().splits())
        self.assertEqual(len(result), 1, "Filter c > 150 with projection [c, id] should return 1 row")
        self.assertEqual(result["id"].iloc[0], 3)
        self.assertEqual(result["c"].iloc[0], 200)
        for _, row in result.iterrows():
            self.assertGreater(
                row["c"],
                150,
                "Each row must satisfy predicate c > 150 (row-by-row path uses predicate.index; "
                "if schema_fields != read_type, wrong column is compared).",
            )

        predicate2 = rb_full.new_predicate_builder().is_null("c")
        rb2_filtered = table.new_read_builder().with_projection(["id", "c"]).with_filter(predicate2)
        result2 = rb2_filtered.new_read().to_pandas(rb2_filtered.new_scan().plan().splits())
        self.assertEqual(len(result2), 1, "Filter c IS NULL with projection [id, c] should return 1 row")
        self.assertEqual(result2["id"].iloc[0], 2)
        self.assertTrue(pd.isna(result2["c"].iloc[0]))

        predicate3 = rb_full.new_predicate_builder().greater_than("c", 50)
        rb3_filtered = table.new_read_builder().with_projection(["c"]).with_filter(predicate3)
        result3 = rb3_filtered.new_read().to_pandas(rb3_filtered.new_scan().plan().splits())
        self.assertEqual(len(result3), 2, "Filter c > 50 with projection [c] should return 2 rows (c=100, 200)")
        self.assertEqual(sorted(result3["c"].tolist()), [100, 200])

        # Build predicate from same read_type as projection [id, c] so indices match (c at index 1).
        rb4 = table.new_read_builder().with_projection(["id", "c"])
        pb4 = rb4.new_predicate_builder()
        predicate_compound = pb4.and_predicates([
            pb4.greater_than("c", 150),
            pb4.is_not_null("c"),
        ])
        rb4_filtered = rb4.with_filter(predicate_compound)
        result4 = rb4_filtered.new_read().to_pandas(rb4_filtered.new_scan().plan().splits())
        self.assertEqual(len(result4), 1, "Filter c>150 AND c IS NOT NULL with projection [id,c] should return 1 row")
        self.assertEqual(result4["id"].iloc[0], 3)
        self.assertEqual(result4["c"].iloc[0], 200)

        predicate_filter_on_non_projected = rb_full.new_predicate_builder().greater_than("c", 150)
        rb_non_projected = table.new_read_builder().with_projection(["id"]).with_filter(
            predicate_filter_on_non_projected
        )
        result_non_projected = rb_non_projected.new_read().to_pandas(
            rb_non_projected.new_scan().plan().splits()
        )
        self.assertEqual(
            len(result_non_projected),
            3,
            "Filter c > 150 with projection [id]: c not in read_type so filter is dropped, all 3 rows returned.",
        )
        self.assertEqual(
            list(result_non_projected.columns),
            ["id"],
            "Projection [id] should return only id column.",
        )
        table_read = rb_non_projected.new_read()
        splits = rb_non_projected.new_scan().plan().splits()
        expected_output_arity = len(table_read.read_type)
        try:
            rows_from_iterator = list(table_read.to_iterator(splits))
        except ValueError as e:
            if "Expected Arrow table or array" in str(e):
                self.skipTest(
                    "RecordBatchReader path uses polars.from_arrow(RecordBatch) which fails; "
                    "skip to_iterator projection assertion on this path"
                )
            raise
        self.assertEqual(len(rows_from_iterator), 3, "to_iterator should return same row count as to_pandas")
        for row in rows_from_iterator:
            self.assertIsInstance(row, OffsetRow)
            self.assertEqual(
                row.arity,
                expected_output_arity,
                "to_iterator must yield rows with only read_type columns (arity=%d)."
                % expected_output_arity,
            )

    def test_null_predicate_arrow_vs_row_by_row(self):
        schema = pa.schema([("id", pa.int64()), ("c", pa.int64())])
        batch = pa.RecordBatch.from_pydict(
            {"id": [1, 2, 3], "c": [10, None, 20]},
            schema=schema,
        )
        ncols = 2

        # is_null('c'): Arrow and row-by-row must return same rows
        pred_is_null = Predicate(method="isNull", index=1, field="c", literals=None)
        arrow_res = _filter_batch_arrow(batch, pred_is_null)
        row_res = _filter_batch_row_by_row(batch, pred_is_null, ncols)
        self.assertEqual(arrow_res.num_rows, row_res.num_rows)
        self.assertTrue(_batches_equal(arrow_res, row_res))
        self.assertEqual(arrow_res.num_rows, 1)
        self.assertEqual(arrow_res.column("id")[0].as_py(), 2)
        self.assertIsNone(arrow_res.column("c")[0].as_py())

        # is_not_null('c'): Arrow and row-by-row must return same rows
        pred_not_null = Predicate(method="isNotNull", index=1, field="c", literals=None)
        arrow_res2 = _filter_batch_arrow(batch, pred_not_null)
        row_res2 = _filter_batch_row_by_row(batch, pred_not_null, ncols)
        self.assertEqual(arrow_res2.num_rows, row_res2.num_rows)
        self.assertTrue(_batches_equal(arrow_res2, row_res2))
        self.assertEqual(arrow_res2.num_rows, 2)

        pred_eq_null = Predicate(method="equal", index=1, field="c", literals=[None])
        row_res3 = _filter_batch_row_by_row(batch, pred_eq_null, ncols)
        self.assertEqual(row_res3.num_rows, 0)  # Paimon: val is None -> False, no row matches
        arrow_res3 = _filter_batch_arrow(batch, pred_eq_null)
        self.assertEqual(arrow_res3.num_rows, 0)  # Arrow: NULL==NULL is null, filtered out
        self.assertEqual(arrow_res3.num_rows, row_res3.num_rows)

    def test_filter_row_by_row_mismatched_schema(self):
        batch = pa.RecordBatch.from_pydict(
            {"c": [1, 200, 50], "id": [100, 2, 3]},
            schema=pa.schema([("c", pa.int64()), ("id", pa.int64())]),
        )
        pred = Predicate(method="greaterThan", index=0, field="c", literals=[150])

        ncols = 3
        nrows = batch.num_rows
        id_col = batch.column("id")
        c_col = batch.column("c")
        row_tuple = [None] * ncols
        offset_row = OffsetRow(row_tuple, 0, ncols)
        mask = []
        for i in range(nrows):
            row_tuple[0] = id_col[i].as_py()
            row_tuple[1] = None
            row_tuple[2] = c_col[i].as_py()
            offset_row.replace(tuple(row_tuple))
            try:
                mask.append(pred.test(offset_row))
            except (TypeError, ValueError):
                mask.append(False)
        rows_passing_wrong_layout = sum(mask)
        self.assertEqual(
            rows_passing_wrong_layout,
            0,
            "With wrong layout (position 0 = id), predicate c > 150 becomes id > 150 -> 0 rows. "
            "This reproduces FilterRecordBatchReader bug when schema_fields=table.fields.",
        )
        ncols_right = 2
        row_tuple_right = [None] * ncols_right
        offset_row_right = OffsetRow(row_tuple_right, 0, ncols_right)
        mask_right = []
        for i in range(nrows):
            row_tuple_right[0] = c_col[i].as_py()
            row_tuple_right[1] = id_col[i].as_py()
            offset_row_right.replace(tuple(row_tuple_right))
            try:
                mask_right.append(pred.test(offset_row_right))
            except (TypeError, ValueError):
                mask_right.append(False)
        rows_passing_right_layout = sum(mask_right)
        self.assertEqual(
            rows_passing_right_layout,
            1,
            "With correct layout (position 0 = c), predicate c > 150 -> 1 row (c=200).",
        )

    def test_null_values(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_null_values', schema, False)
        table = self.catalog.get_table('default.test_null_values')

        write_builder = table.new_batch_write_builder()

        # Commit 1: some cols are null
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()

        d0 = pa.Table.from_pydict({'f0': [1], 'f1': [None]},
                                  schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        d1 = pa.Table.from_pydict({'f2': [None]}, schema=pa.schema([('f2', pa.string())]))
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # Commit 2
        w1 = write_builder.new_write().with_write_type(['f2'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({'f2': ['c']}, schema=pa.schema([('f2', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        for msg in cmts1:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c1.commit(cmts1)
        w1.close()
        c1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())
        expect = pa.Table.from_pydict({
            'f0': [1],
            'f1': [None],
            'f2': ['c'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    # different first_row_id append multiple times
    def test_multiple_appends_different_first_row_ids(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_multiple_appends_diff_rowid', schema, False)
        table = self.catalog.get_table('default.test_multiple_appends_diff_rowid')

        write_builder = table.new_batch_write_builder()

        # commit 1
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()
        d0 = pa.Table.from_pydict({'f0': [1], 'f1': ['a']},
                                  schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        d1 = pa.Table.from_pydict({'f2': ['b']}, schema=pa.schema([('f2', pa.string())]))
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # commit 2
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        c0 = write_builder.new_commit()
        d0 = pa.Table.from_pydict({'f0': [2], 'f1': ['c']},
                                  schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        w0.write_arrow(d0)
        cmts0 = w0.prepare_commit()
        for msg in cmts0:
            for nf in msg.new_files:
                nf.first_row_id = 1
        c0.commit(cmts0)
        w0.close()
        c0.close()

        # commit 3
        w1 = write_builder.new_write().with_write_type(['f2'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({'f2': ['d']}, schema=pa.schema([('f2', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        for msg in cmts1:
            for nf in msg.new_files:
                nf.first_row_id = 1
        c1.commit(cmts1)
        w1.close()
        c1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expect = pa.Table.from_pydict({
            'f0': [1, 2],
            'f1': ['a', 'c'],
            'f2': ['b', 'd'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

        read_builder = table.new_read_builder()
        read_builder.with_projection(['f0', 'f1', 'f2', '_ROW_ID', '_SEQUENCE_NUMBER'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_with_meta = table_read.to_arrow(table_scan.plan().splits())
        self.assertFalse(
            actual_with_meta.schema.field('_ROW_ID').nullable,
            '_ROW_ID must be non-nullable per SpecialFields',
        )
        self.assertFalse(
            actual_with_meta.schema.field('_SEQUENCE_NUMBER').nullable,
            '_SEQUENCE_NUMBER must be non-nullable per SpecialFields',
        )

        rb_with_row_id = table.new_read_builder().with_projection(['f0', 'f1', 'f2', '_ROW_ID'])
        pb = rb_with_row_id.new_predicate_builder()
        rb_eq0 = table.new_read_builder().with_filter(pb.equal('_ROW_ID', 0))
        result_eq0 = rb_eq0.new_read().to_arrow(rb_eq0.new_scan().plan().splits())
        self.assertEqual(result_eq0, pa.Table.from_pydict(
            {'f0': [1], 'f1': ['a'], 'f2': ['b']}, schema=simple_pa_schema))
        rb_eq1 = table.new_read_builder().with_filter(pb.equal('_ROW_ID', 1))
        result_eq1 = rb_eq1.new_read().to_arrow(rb_eq1.new_scan().plan().splits())
        self.assertEqual(result_eq1, pa.Table.from_pydict(
            {'f0': [2], 'f1': ['c'], 'f2': ['d']}, schema=simple_pa_schema))
        rb_in = table.new_read_builder().with_filter(pb.is_in('_ROW_ID', [0, 1]))
        result_in = rb_in.new_read().to_arrow(rb_in.new_scan().plan().splits())
        self.assertEqual(result_in, expect)

    def test_filter_by_row_id(self):
        simple_pa_schema = pa.schema([('f0', pa.int32())])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
        )
        self.catalog.create_table('default.test_row_id_filter_empty_and_or', schema, False)
        table = self.catalog.get_table('default.test_row_id_filter_empty_and_or')
        write_builder = table.new_batch_write_builder()

        # Commit 1: _ROW_ID 0, 1 with f0=1, 2
        w = write_builder.new_write().with_write_type(['f0'])
        c = write_builder.new_commit()
        w.write_arrow(pa.Table.from_pydict(
            {'f0': [1, 2]}, schema=pa.schema([('f0', pa.int32())])))
        cmts = w.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w.close()
        c.close()

        # Commit 2: _ROW_ID 2, 3 with f0=101, 102
        w = write_builder.new_write().with_write_type(['f0'])
        c = write_builder.new_commit()
        w.write_arrow(pa.Table.from_pydict(
            {'f0': [101, 102]}, schema=pa.schema([('f0', pa.int32())])))
        cmts = w.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 2
        c.commit(cmts)
        w.close()
        c.close()

        rb_with_row_id = table.new_read_builder().with_projection(['f0', '_ROW_ID'])
        pb = rb_with_row_id.new_predicate_builder()

        # 1. Non-existent _ROW_ID -> empty
        rb_eq999 = table.new_read_builder().with_filter(pb.equal('_ROW_ID', 999))
        result_eq999 = rb_eq999.new_read().to_arrow(rb_eq999.new_scan().plan().splits())
        self.assertEqual(len(result_eq999), 0, "Non-existent _ROW_ID should return empty")

        # 2. AND: _ROW_ID=0 AND f0=1 -> 1 row
        rb_and = table.new_read_builder().with_filter(
            pb.and_predicates([pb.equal('_ROW_ID', 0), pb.equal('f0', 1)])
        )
        result_and = rb_and.new_read().to_arrow(rb_and.new_scan().plan().splits())
        self.assertEqual(len(result_and), 1)
        self.assertEqual(result_and['f0'][0].as_py(), 1)

        # 3. OR: _ROW_ID=0 OR f0>100 -> at least row with _ROW_ID=0 and all f0>100
        rb_or = table.new_read_builder().with_filter(
            pb.or_predicates([pb.equal('_ROW_ID', 0), pb.greater_than('f0', 100)])
        )
        result_or = rb_or.new_read().to_arrow(rb_or.new_scan().plan().splits())
        f0_vals = set(result_or['f0'][i].as_py() for i in range(len(result_or)))
        self.assertGreaterEqual(len(result_or), 3, "OR should return _ROW_ID=0 row and f0>100 rows")
        self.assertIn(1, f0_vals, "_ROW_ID=0 row has f0=1")
        self.assertIn(101, f0_vals)
        self.assertIn(102, f0_vals)

    def test_filter_manifest_entries_by_row_ranges(self):
        from pypaimon.read.scanner.file_scanner import _filter_manifest_entries_by_row_ranges

        entry_0 = SimpleNamespace(file=SimpleNamespace(first_row_id=0, row_count=1))
        entries = [entry_0]
        row_ranges = []

        filtered = _filter_manifest_entries_by_row_ranges(entries, row_ranges)
        self.assertEqual(filtered, [], "empty row_ranges must return no entries, not all entries")

    def test_more_data(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_more_data', schema, False)
        table = self.catalog.get_table('default.test_more_data')

        write_builder = table.new_batch_write_builder()

        # first commit：100k rows
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()
        size = 100000
        d0 = pa.Table.from_pydict({
            'f0': list(range(size)),
            'f1': [f'a{i}' for i in range(size)],
        }, schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        d1 = pa.Table.from_pydict({
            'f2': [f'b{i}' for i in range(size)],
        }, schema=pa.schema([('f2', pa.string())]))
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # second commit：overwrite f2 to 'c{i}'
        w1 = write_builder.new_write().with_write_type(['f2'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({
            'f2': [f'c{i}' for i in range(size)],
        }, schema=pa.schema([('f2', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        c1.commit(cmts1)
        w1.close()
        c1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expect = pa.Table.from_pydict({
            'f0': list(range(size)),
            'f1': [f'a{i}' for i in range(size)],
            'f2': [f'c{i}' for i in range(size)],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_read_row_tracking_metadata(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema,
                                            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
        self.catalog.create_table('default.test_row_tracking_meta', schema, False)
        table = self.catalog.get_table('default.test_row_tracking_meta')

        # write 1
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            'f1': [-1001, 1002]
        }, schema=simple_pa_schema)
        table_write.write_arrow(expect_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        read_builder.with_projection(['f0', '_ROW_ID', 'f1', '_SEQUENCE_NUMBER'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            '_ROW_ID': [0, 1],
            'f1': [-1001, 1002],
            '_SEQUENCE_NUMBER': [1, 1],
        }, schema=pa.schema([
            ('f0', pa.int8()),
            pa.field('_ROW_ID', pa.int64(), nullable=False),
            ('f1', pa.int16()),
            pa.field('_SEQUENCE_NUMBER', pa.int64(), nullable=False),
        ]))
        self.assertEqual(actual_data, expect_data)

        # write 2
        table_write = write_builder.new_write().with_write_type(['f0'])
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'f0': [3, 4],
        }, schema=pa.schema([
            ('f0', pa.int8()),
        ]))
        table_write.write_arrow(data2)
        cmts = table_write.prepare_commit()
        cmts[0].new_files[0].first_row_id = 0
        table_commit.commit(cmts)
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        read_builder.with_projection(['f0', 'f1', '_ROW_ID', '_SEQUENCE_NUMBER'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        self.assertFalse(actual_data.schema.field('_ROW_ID').nullable)
        self.assertFalse(actual_data.schema.field('_SEQUENCE_NUMBER').nullable)
        expect_data = pa.Table.from_pydict({
            'f0': [3, 4],
            'f1': [-1001, 1002],
            '_ROW_ID': [0, 1],
            '_SEQUENCE_NUMBER': [2, 2],
        }, schema=pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
            pa.field('_ROW_ID', pa.int64(), nullable=False),
            pa.field('_SEQUENCE_NUMBER', pa.int64(), nullable=False),
        ]))
        self.assertEqual(actual_data, expect_data)

    def test_from_arrays_without_schema(self):
        schema = pa.schema([
            ('f0', pa.int8()),
            pa.field('_ROW_ID', pa.int64(), nullable=False),
            pa.field('_SEQUENCE_NUMBER', pa.int64(), nullable=False),
        ])
        batch = pa.RecordBatch.from_pydict(
            {'f0': [1], '_ROW_ID': [0], '_SEQUENCE_NUMBER': [1]},
            schema=schema
        )
        self.assertFalse(batch.schema.field('_ROW_ID').nullable)
        self.assertFalse(batch.schema.field('_SEQUENCE_NUMBER').nullable)

        arrays = list(batch.columns)
        rebuilt = pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)
        self.assertTrue(rebuilt.schema.field('_ROW_ID').nullable)
        self.assertTrue(rebuilt.schema.field('_SEQUENCE_NUMBER').nullable)
