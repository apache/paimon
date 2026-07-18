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

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


def _has_native_planner():
    try:
        from pypaimon_rust.datafusion import PaimonCatalog
    except Exception:
        return False
    return hasattr(PaimonCatalog, 'get_table')


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

    def _assert_matches(self, name):
        sid_n, rows_n = self._plan_and_read(name, native=False)
        sid_r, rows_r = self._plan_and_read(name, native=True)
        self.assertEqual(rows_r, rows_n)
        self.assertEqual(sid_r, sid_n)   # snapshot id preserved through native plan
        self.assertIsNotNone(sid_r)

    def test_primary_key_matches_normal_plan(self):
        self.cat.create_table('default.pk_t', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'], options={'bucket': '1'}), False)
        self._write('pk_t', [{'k': 1, 'v': 'a1'}, {'k': 2, 'v': 'b1'}])
        self._write('pk_t', [{'k': 2, 'v': 'b2'}, {'k': 3, 'v': 'c1'}])  # k=2 updated
        self._assert_matches('pk_t')

    def test_first_row_merge_engine_falls_back(self):
        self.cat.create_table('default.fr_t', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'],
            options={'bucket': '1', 'merge-engine': 'first-row'}), False)
        self._write('fr_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        self._write('fr_t', [{'k': 1, 'v': 'X'}, {'k': 3, 'v': 'c'}])  # k=1 stays 'a'
        self._assert_matches('fr_t')

    def test_append_matches_normal_plan(self):
        self.cat.create_table(
            'default.ap_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('ap_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        self._write('ap_t', [{'k': 3, 'v': 'c'}])
        self._assert_matches('ap_t')

    def test_dynamic_split_target_size_matches_normal_plan(self):
        self.cat.create_table(
            'default.split_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('split_t', [{'k': 1, 'v': 'a'}])
        self._write('split_t', [{'k': 2, 'v': 'b'}])
        options = {'source.split.target-size': '1b'}
        normal_table = self.cat.get_table('default.split_t').copy(options)
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().new_scan().plan()

        self.assertEqual(len(native.splits()), len(normal.splits()))
        self.assertGreater(len(native.splits()), 1)

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
        native = native_table.new_read_builder().new_scan().plan()

        self.assertEqual(len(baseline.splits()), 1)
        self.assertGreater(len(normal.splits()), len(baseline.splits()))
        self.assertEqual(len(native.splits()), len(normal.splits()))

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
        native = native_table.new_read_builder().new_scan().plan()

        self.assertEqual(len(native.splits()), len(normal.splits()))
        self.assertEqual(len(native.splits()), 1)

    def test_partitioned_table_and_partition_filter(self):
        schema = pa.schema([('k', pa.int64()), ('p', pa.string())])
        self.cat.create_table('default.pt_t', Schema.from_pyarrow_schema(
            schema, partition_keys=['p']), False)
        t = self.cat.get_table('default.pt_t')
        wb = t.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(pa.Table.from_pylist(
            [{'k': 1, 'p': 'a'}, {'k': 2, 'p': 'a'}, {'k': 3, 'p': 'b'}], schema=schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

        def read(native, filtered):
            tt = self.cat.get_table('default.pt_t')
            if native:
                tt = tt.copy({'scan.native-plan.enabled': 'true'})
            rb = tt.new_read_builder()
            if filtered:
                rb.with_filter(rb.new_predicate_builder().equal('p', 'a'))
            rows = rb.new_read().to_arrow(rb.new_scan().plan().splits()).to_pylist()
            return sorted((r['k'], r['p']) for r in rows)

        self.assertEqual(read(True, False), read(False, False))
        self.assertEqual(read(True, True), read(False, True))
        self.assertEqual(read(True, True), [(1, 'a'), (2, 'a')])

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
