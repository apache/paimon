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

import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.read.native_plan import native_family_search_modes_available
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.utils.range import Range


def _has_native_planner():
    try:
        from pypaimon_rust.datafusion import PaimonCatalog, Split
    except Exception:
        return False
    return hasattr(PaimonCatalog, 'get_table') and hasattr(Split, 'serialize')


def _has_native_row_ranges():
    try:
        from pypaimon_rust.datafusion import ReadBuilder
    except ImportError:
        return False
    return hasattr(ReadBuilder, 'with_row_ranges')


@unittest.skipUnless(_has_native_planner(),
                     "pypaimon_rust with split-planning API not installed")
class NativePlanIntegrationTest(unittest.TestCase):
    """Live round-trip guarding the cross-language SplitSerializer against drift:
    plan via pypaimon_rust, decode, and require the same rows as the normal plan.
    The golden unit tests only prove self-consistency; this proves byte-compat
    with the real producer."""

    def setUp(self):
        self.cat = CatalogFactory.create({'warehouse': tempfile.mkdtemp(prefix='np_it_')})
        self.cat.create_database('default', True)
        self.schema = pa.schema([('k', pa.int64()), ('v', pa.string())])

    def _write(self, name, rows):
        t = self.cat.get_table('default.%s' % name)
        wb = t.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(pa.Table.from_pylist(rows, schema=self.schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

    def _plan_and_read(self, name, native):
        t = self.cat.get_table('default.%s' % name)
        if native:
            t = t.copy({'scan.native-plan.enabled': 'true'})
        rb = t.new_read_builder()
        plan = rb.new_scan().plan()
        rows = rb.new_read().to_arrow(plan.splits()).to_pylist()
        return plan.snapshot_id, sorted(rows, key=lambda r: r['k'])

    def _assert_matches(self, name, expect_native=True):
        sid_n, rows_n = self._plan_and_read(name, native=False)
        sid_r, rows_r = self._plan_and_read(name, native=True)
        self.assertEqual(rows_r, rows_n)
        self.assertEqual(sid_r, sid_n)   # snapshot id preserved through native plan
        self.assertIsNotNone(sid_r)
        # Guard against a false green where native silently fell back to Python: assert the
        # native planner was (or was not) actually used, as expected for this table.
        native_table = self.cat.get_table('default.%s' % name).copy(
            {'scan.native-plan.enabled': 'true'})
        self.assertEqual(
            native_table.new_read_builder().explain().native_planned, expect_native)

    def test_primary_key_matches_normal_plan(self):
        self.cat.create_table('default.pk_t', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'], options={'bucket': '1'}), False)
        self._write('pk_t', [{'k': 1, 'v': 'a1'}, {'k': 2, 'v': 'b1'}])
        self._write('pk_t', [{'k': 2, 'v': 'b2'}, {'k': 3, 'v': 'c1'}])  # k=2 updated
        self._assert_matches('pk_t')

    def test_pk_equal_to_partition_key_falls_back(self):
        # Empty trimmed PK: native would skip merge and return duplicates -> must fall back.
        self.cat.create_table('default.pkpart_t', Schema.from_pyarrow_schema(
            self.schema, partition_keys=['k'], primary_keys=['k'], options={'bucket': '1'}), False)
        self._write('pkpart_t', [{'k': 1, 'v': 'a1'}, {'k': 2, 'v': 'b1'}])
        self._write('pkpart_t', [{'k': 2, 'v': 'b2'}])
        native_table = self.cat.get_table('default.pkpart_t').copy(
            {'scan.native-plan.enabled': 'true'})
        self.assertFalse(native_table.new_read_builder().explain().native_planned)
        with self.assertRaises(ValueError):
            rb = native_table.new_read_builder()
            rb.new_read().to_arrow(rb.new_scan().plan().splits())

    def test_copy_removed_persisted_scan_option_falls_back(self):
        # copy() removes a persisted scan.snapshot-id that Rust would still reload -> fall back.
        self.cat.create_table('default.snapopt_t', Schema.from_pyarrow_schema(
            self.schema, options={'scan.snapshot-id': '1'}), False)
        self._write('snapopt_t', [{'k': 1, 'v': 'a'}])   # snapshot 1
        self._write('snapopt_t', [{'k': 2, 'v': 'b'}])   # snapshot 2
        native = self.cat.get_table('default.snapopt_t').copy(
            {'scan.snapshot-id': None, 'scan.native-plan.enabled': 'true'})
        self.assertFalse(native.new_read_builder().explain().native_planned)

    def test_first_row_merge_engine_falls_back(self):
        self.cat.create_table('default.fr_t', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'],
            options={'bucket': '1', 'merge-engine': 'first-row'}), False)
        self._write('fr_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        self._write('fr_t', [{'k': 1, 'v': 'X'}, {'k': 3, 'v': 'c'}])  # k=1 stays 'a'
        self._assert_matches('fr_t', expect_native=False)

    def test_append_matches_normal_plan(self):
        self.cat.create_table(
            'default.ap_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('ap_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        self._write('ap_t', [{'k': 3, 'v': 'c'}])
        self._assert_matches('ap_t')

    @unittest.skipUnless(native_family_search_modes_available(),
                         "pypaimon-rust 0.4+ required")
    def test_dynamic_family_search_mode_uses_native_plan(self):
        self.cat.create_table(
            'default.search_mode_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('search_mode_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])

        table = self.cat.get_table('default.search_mode_t').copy({
            'scan.native-plan.enabled': 'true',
            'scalar-index.search-mode': 'full',
        })
        builder = table.new_read_builder()
        plan = builder.new_scan().plan()

        self.assertEqual(
            sorted(builder.new_read().to_arrow(plan.splits()).to_pylist(),
                   key=lambda row: row['k']),
            [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}],
        )
        self.assertTrue(builder.explain().native_planned)

    def test_data_evolution_blob_projection_filter_limit(self):
        schema = pa.schema([
            ('k', pa.int64()),
            ('v', pa.string()),
            ('media.camera', pa.large_binary()),
        ])
        self.cat.create_table('default.de_t', Schema.from_pyarrow_schema(
            schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }), False)
        table = self.cat.get_table('default.de_t')
        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        write.write_arrow(pa.Table.from_pylist([
            {'k': 1, 'v': 'a', 'media.camera': b'a'},
            {'k': 2, 'v': 'b', 'media.camera': b'b'},
            {'k': 3, 'v': 'c', 'media.camera': b'c'},
        ], schema=schema))
        write_builder.new_commit().commit(write.prepare_commit())
        write.close()

        update_builder = table.new_batch_write_builder()
        update = update_builder.new_update().with_update_type(['v'])
        messages = update.update_by_arrow_with_row_id(pa.Table.from_pydict({
            '_ROW_ID': pa.array([1], type=pa.int64()),
            'v': pa.array(['b2'], type=pa.string()),
        }))
        update_builder.new_commit().commit(messages)

        self._assert_matches('de_t')

        native_table = self.cat.get_table('default.de_t').copy(
            {'scan.native-plan.enabled': 'true'})
        predicate = native_table.new_read_builder().new_predicate_builder().equal(
            'v', 'b2')
        builder = (native_table.new_read_builder()
                   .with_projection(['k'])
                   .with_filter(predicate)
                   .with_limit(1))
        plan = builder.new_scan().plan()
        rows = builder.new_read().to_arrow(plan.splits()).to_pylist()

        self.assertEqual(rows, [{'k': 2}])
        self.assertTrue(builder.explain().native_planned)

        blob_builder = (native_table.new_read_builder()
                        .with_projection(['media.camera'])
                        .with_limit(1))
        blob_plan = blob_builder.new_scan().plan()
        blob_rows = blob_builder.new_read().to_arrow(
            blob_plan.splits()).to_pylist()
        self.assertEqual(blob_rows, [{'media.camera': b'a'}])
        self.assertTrue(any(
            data_file.file_name.endswith('.blob')
            for split in blob_plan.splits()
            for data_file in split.files
        ))

        descriptor_table = native_table.copy({'blob-as-descriptor': 'true'})
        descriptor_builder = (
            descriptor_table.new_read_builder()
            .with_projection(['media.camera'])
            .with_limit(1))
        descriptor_plan = descriptor_builder.new_scan().plan()
        descriptor_rows = descriptor_builder.new_read().to_arrow(
            descriptor_plan.splits()).to_pylist()
        self.assertEqual(len(descriptor_plan.splits()), 1)
        self.assertEqual(
            BlobDescriptor.deserialize(descriptor_rows[0]['media.camera']).length,
            1,
        )
        self.assertTrue(descriptor_builder.explain().native_planned)

    @unittest.skipUnless(_has_native_row_ranges(),
                         "pypaimon_rust row-range API not installed")
    def test_data_evolution_global_index_row_ranges(self):
        self.cat.create_table('default.de_range_t', Schema.from_pyarrow_schema(
            self.schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }), False)
        self._write('de_range_t', [
            {'k': 1, 'v': 'a'},
            {'k': 2, 'v': 'b'},
            {'k': 3, 'v': 'c'},
        ])
        table = self.cat.get_table('default.de_range_t').copy(
            {'scan.native-plan.enabled': 'true'})
        builder = table.new_read_builder()
        scan = builder.new_scan().with_global_index_result(
            GlobalIndexResult.from_range(Range(1, 1)))

        self.assertTrue(scan._native_plan_supported())
        with patch.object(
                scan.file_scanner, 'scan', side_effect=AssertionError("fallback")):
            plan = scan.plan()
        rows = builder.new_read().to_arrow(plan.splits()).to_pylist()

        self.assertEqual(rows, [{'k': 2, 'v': 'b'}])
        self.assertEqual(
            [(range_.from_, range_.to)
             for range_ in plan.splits()[0].row_ranges()],
            [(1, 1)],
        )

        empty_scan = builder.new_scan().with_global_index_result(
            GlobalIndexResult.create_empty())
        with patch.object(
                empty_scan.file_scanner, 'scan',
                side_effect=AssertionError("fallback")):
            empty_plan = empty_scan.plan()
        self.assertEqual(empty_plan.splits(), [])

    def test_filter_is_pushed_to_native_plan(self):
        options = {
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.filter_t', Schema.from_pyarrow_schema(
            self.schema, options=options), False)
        for k in range(1, 4):
            self._write('filter_t', [{'k': k, 'v': 'v%d' % k}])

        table = self.cat.get_table('default.filter_t')
        normal_builder = table.new_read_builder()
        predicate = normal_builder.new_predicate_builder().equal('k', 2)
        normal_builder.with_filter(predicate)
        normal_plan = normal_builder.new_scan().plan()

        native_builder = table.copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder()
        predicate = native_builder.new_predicate_builder().equal('k', 2)
        native_builder.with_filter(predicate)
        native_plan = native_builder.new_scan().plan()
        rows = native_builder.new_read().to_arrow(native_plan.splits()).to_pylist()

        self.assertEqual(rows, [{'k': 2, 'v': 'v2'}])
        self.assertEqual(len(native_plan.splits()), len(normal_plan.splits()))
        self.assertTrue(native_builder.explain().native_planned)

    def test_limit_is_pushed_to_native_plan(self):
        options = {
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.limit_t', Schema.from_pyarrow_schema(
            self.schema, options=options), False)
        for k in range(1, 4):
            self._write('limit_t', [{'k': k, 'v': 'v%d' % k}])

        table = self.cat.get_table('default.limit_t')
        normal = table.new_read_builder().with_limit(1).new_scan().plan()
        native_builder = table.copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder().with_limit(1)
        native = native_builder.new_scan().plan()
        rows = native_builder.new_read().to_arrow(native.splits()).to_pylist()

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(native.splits()), len(normal.splits()))
        self.assertEqual(len(native.splits()), 1)
        self.assertTrue(native_builder.explain().native_planned)

    def test_snapshot_time_travel_matches_normal_plan(self):
        self.cat.create_table(
            'default.travel_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('travel_t', [{'k': 1, 'v': 'a'}])
        self._write('travel_t', [{'k': 2, 'v': 'b'}])
        options = {'scan.snapshot-id': '1'}

        normal_table = self.cat.get_table('default.travel_t').copy(options)
        normal_builder = normal_table.new_read_builder()
        normal_plan = normal_builder.new_scan().plan()
        normal_rows = normal_builder.new_read().to_arrow(
            normal_plan.splits()).to_pylist()

        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})
        native_builder = native_table.new_read_builder()
        native_plan = native_builder.new_scan().plan()
        native_rows = native_builder.new_read().to_arrow(
            native_plan.splits()).to_pylist()

        self.assertEqual(native_plan.snapshot_id, 1)
        self.assertEqual(native_rows, normal_rows)
        self.assertEqual(native_rows, [{'k': 1, 'v': 'a'}])
        self.assertTrue(native_builder.explain().native_planned)

    def test_dynamic_split_target_size_matches_normal_plan(self):
        self.cat.create_table(
            'default.split_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('split_t', [{'k': 1, 'v': 'a'}])
        self._write('split_t', [{'k': 2, 'v': 'b'}])
        options = {'source.split.target-size': '1b'}
        normal_table = self.cat.get_table('default.split_t').copy(options)
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().explain()

        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, len(normal.splits()))
        self.assertGreater(native.split_count, 1)

    def test_dynamic_split_open_file_cost_matches_normal_plan(self):
        stored_options = {
            'source.split.target-size': '128mb',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.open_cost_t', Schema.from_pyarrow_schema(
            self.schema, options=stored_options), False)
        self._write('open_cost_t', [{'k': 1, 'v': 'a'}])
        self._write('open_cost_t', [{'k': 2, 'v': 'b'}])
        self._write('open_cost_t', [{'k': 3, 'v': 'c'}])
        base_table = self.cat.get_table('default.open_cost_t')
        normal_table = base_table.copy({'source.split.open-file-cost': '64mb'})
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        baseline = base_table.new_read_builder().new_scan().plan()
        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().explain()

        self.assertEqual(len(baseline.splits()), 1)
        self.assertGreater(len(normal.splits()), len(baseline.splits()))
        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, len(normal.splits()))

    def test_dynamic_split_option_reset_matches_normal_plan(self):
        stored_options = {
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.split_reset_t', Schema.from_pyarrow_schema(
            self.schema, options=stored_options), False)
        self._write('split_reset_t', [{'k': 1, 'v': 'a'}])
        self._write('split_reset_t', [{'k': 2, 'v': 'b'}])
        reset_options = {
            'source.split.target-size': None,
            'source.split.open-file-cost': None,
        }
        normal_table = self.cat.get_table('default.split_reset_t').copy(reset_options)
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().explain()

        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, len(normal.splits()))
        self.assertEqual(native.split_count, 1)

    def test_partitioned_table_matches_normal_plan(self):
        # Native decoding restores PyPaimon's legacy unescaped partition path.
        schema = pa.schema([('k', pa.int64()), ('p', pa.string())])
        self.cat.create_table('default.pt_t', Schema.from_pyarrow_schema(
            schema, partition_keys=['p']), False)
        t = self.cat.get_table('default.pt_t')
        wb = t.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(pa.Table.from_pylist(
            [{'k': 1, 'p': 'a/b'}, {'k': 2, 'p': 'a/b'}, {'k': 3, 'p': 'c'}],
            schema=schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

        self._assert_matches('pt_t')

    def test_explain_reflects_native_plan(self):
        self.cat.create_table(
            'default.ex_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('ex_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        normal = self.cat.get_table('default.ex_t').new_read_builder().explain()
        native = self.cat.get_table('default.ex_t').copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder().explain()
        self.assertFalse(normal.native_planned)
        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, normal.split_count)
        self.assertEqual(native.snapshot_id, normal.snapshot_id)
        self.assertIn('native', str(native))   # render shows the Planner line

    def test_empty_table_explain_reflects_python_fallback(self):
        self.cat.create_table(
            'default.empty_t', Schema.from_pyarrow_schema(self.schema), False)
        normal = self.cat.get_table('default.empty_t').new_read_builder().explain()
        fallback = self.cat.get_table('default.empty_t').copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder().explain()

        self.assertFalse(fallback.native_planned)
        self.assertEqual(fallback.snapshot_id, normal.snapshot_id)
        self.assertEqual(fallback.split_count, 0)
        self.assertNotIn('Planner:', str(fallback))


if __name__ == '__main__':
    unittest.main()
