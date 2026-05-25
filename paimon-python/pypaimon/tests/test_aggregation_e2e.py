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

"""End-to-end tests for the ``aggregation`` merge engine.

Each test creates a PK table with ``merge-engine=aggregation`` plus
per-field aggregator configuration, writes two or more commits against
the same PK, and reads back. The aggregation engine must reduce each
non-PK column independently using the configured aggregator (sum / max
/ last_value / ...). Disjoint PKs must remain unmerged. Default
behaviour when no aggregator is configured is ``last_non_null_value``,
matching the Java port.

Tests that pin down the unsupported-option guard (out-of-scope
aggregators, retract opt-ins) live in the corresponding follow-up
commit so this file stays focused on positive merge semantics.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class AggregationMergeEngineE2ETest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('total', pa.int64()),
            ('max_score', pa.int64()),
            ('label', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, table_name, field_aggs=None,
                         default_agg=None, extra_options=None):
        # bucket=1 forces all rows for a given PK to land in the same
        # bucket, which routes reads through SortMergeReader where the
        # aggregation merge function lives. Without it, fresh
        # single-snapshot tables take the raw_convertible fast path and
        # bypass the merge function entirely.
        options = {
            'bucket': '1',
            'merge-engine': 'aggregation',
        }
        if field_aggs:
            for field_name, agg_func in field_aggs.items():
                options['fields.{}.aggregate-function'.format(field_name)] = agg_func
        if default_agg:
            options['fields.default-aggregate-function'] = default_agg
        if extra_options:
            options.update(extra_options)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options=options,
        )
        full = 'default.{}'.format(table_name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def _write(self, table, rows):
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(rows, schema=self.pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    def _read(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        if not splits:
            return []
        return sorted(
            rb.new_read().to_arrow(splits).to_pylist(),
            key=lambda r: r['id'],
        )

    # -- aggregation happy path -----------------------------------------

    def test_sum_aggregator_across_commits(self):
        table = self._create_pk_table(
            'agg_sum',
            field_aggs={'total': 'sum'},
        )
        self._write(table, [{'id': 1, 'total': 10, 'max_score': 5, 'label': 'a'}])
        self._write(table, [{'id': 1, 'total': 20, 'max_score': 3, 'label': 'b'}])
        self._write(table, [{'id': 1, 'total': 30, 'max_score': 8, 'label': 'c'}])

        rows = self._read(table)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['id'], 1)
        # total: 10 + 20 + 30 = 60
        self.assertEqual(row['total'], 60)
        # max_score and label have no aggregator configured → default
        # last_non_null_value: latest non-null wins.
        self.assertEqual(row['max_score'], 8)
        self.assertEqual(row['label'], 'c')

    def test_multiple_aggregators_compose(self):
        table = self._create_pk_table(
            'agg_multi',
            field_aggs={
                'total': 'sum',
                'max_score': 'max',
                'label': 'last_value',
            },
        )
        self._write(table, [{'id': 1, 'total': 10, 'max_score': 5, 'label': 'a'}])
        self._write(table, [{'id': 1, 'total': 7, 'max_score': 12, 'label': 'b'}])
        self._write(table, [{'id': 1, 'total': 3, 'max_score': 1, 'label': 'c'}])

        row = self._read(table)[0]
        self.assertEqual(row['total'], 20)        # sum: 10+7+3
        self.assertEqual(row['max_score'], 12)     # max: max(5,12,1)
        self.assertEqual(row['label'], 'c')        # last_value

    def test_null_inputs_follow_aggregator_semantics(self):
        table = self._create_pk_table(
            'agg_nulls',
            field_aggs={
                'total': 'sum',
                'max_score': 'last_value',
            },
        )
        self._write(table, [{'id': 1, 'total': 5, 'max_score': 7, 'label': 'x'}])
        # null total is absorbed by sum; null max_score replaces under
        # last_value (mirroring Java semantics — last_value keeps the
        # last input verbatim, including None).
        self._write(table, [{'id': 1, 'total': None, 'max_score': None, 'label': None}])
        self._write(table, [{'id': 1, 'total': 4, 'max_score': 9, 'label': 'y'}])

        row = self._read(table)[0]
        self.assertEqual(row['total'], 9)          # 5 + 4 (None absorbed)
        self.assertEqual(row['max_score'], 9)      # last_value's last input
        # label: default last_non_null_value, intermediate None ignored,
        # the final 'y' wins.
        self.assertEqual(row['label'], 'y')

    def test_disjoint_keys_remain_unmerged(self):
        table = self._create_pk_table(
            'agg_disjoint',
            field_aggs={'total': 'sum'},
        )
        self._write(table, [
            {'id': 1, 'total': 10, 'max_score': 1, 'label': 'a'},
            {'id': 2, 'total': 20, 'max_score': 2, 'label': 'b'},
            {'id': 3, 'total': 30, 'max_score': 3, 'label': 'c'},
        ])
        # Second commit only touches id=2.
        self._write(table, [{'id': 2, 'total': 5, 'max_score': 7, 'label': 'B'}])

        rows = self._read(table)
        self.assertEqual(rows, [
            {'id': 1, 'total': 10, 'max_score': 1, 'label': 'a'},
            {'id': 2, 'total': 25, 'max_score': 7, 'label': 'B'},
            {'id': 3, 'total': 30, 'max_score': 3, 'label': 'c'},
        ])

    def test_default_aggregator_applies_to_unconfigured_fields(self):
        table = self._create_pk_table(
            'agg_default',
            default_agg='max',
        )
        self._write(table, [{'id': 1, 'total': 3, 'max_score': 5, 'label': 'm'}])
        self._write(table, [{'id': 1, 'total': 7, 'max_score': 2, 'label': 'a'}])
        self._write(table, [{'id': 1, 'total': 1, 'max_score': 9, 'label': 'z'}])

        row = self._read(table)[0]
        # All non-PK fields fall through to fields.default-aggregate-function=max.
        self.assertEqual(row['total'], 7)
        self.assertEqual(row['max_score'], 9)
        self.assertEqual(row['label'], 'z')  # 'z' > 'm' > 'a' lexicographically

    def test_default_behavior_is_last_non_null_value(self):
        # No field-level or default aggregator configured → every non-PK
        # field uses the system default last_non_null_value.
        table = self._create_pk_table('agg_implicit_default')
        self._write(table, [{'id': 1, 'total': 5, 'max_score': 9, 'label': 'a'}])
        self._write(table, [{'id': 1, 'total': None, 'max_score': 3, 'label': None}])
        self._write(table, [{'id': 1, 'total': 7, 'max_score': None, 'label': 'b'}])

        row = self._read(table)[0]
        self.assertEqual(row['total'], 7)       # latest non-null
        self.assertEqual(row['max_score'], 3)   # latest non-null
        self.assertEqual(row['label'], 'b')     # latest non-null


if __name__ == '__main__':
    unittest.main()
