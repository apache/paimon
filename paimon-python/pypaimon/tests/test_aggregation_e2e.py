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
behaviour when no aggregator is configured is ``last_non_null_value``.

The second half of the file exercises the merge-engine-support guard:
tables that configure aggregation with options pypaimon does not yet
implement (retract opt-ins, sequence-group, out-of-scope aggregator
identifiers) must raise ``NotImplementedError`` at TableRead
construction rather than silently fall back to a wrong answer.
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
        # last_value (last_value keeps the last input verbatim,
        # including None).
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

    # -- unsupported-option guards --------------------------------------
    #
    # Tables that opt into behaviour AggregateMergeFunction doesn't
    # implement must surface a NotImplementedError at TableRead
    # construction, not silently produce wrong results.

    def _create_and_expect_unsupported(self, table_name, extra_options,
                                       expected_substring,
                                       error_type=NotImplementedError):
        table = self._create_pk_table(
            table_name, extra_options=extra_options
        )
        # Writing is fine — the guard fires when a reader is built.
        self._write(table, [{'id': 1, 'total': 1, 'max_score': 1, 'label': 'a'}])
        rb = table.new_read_builder()
        with self.assertRaises(error_type) as cm:
            rb.new_read()
        msg = str(cm.exception)
        if error_type is NotImplementedError:
            self.assertIn('aggregation', msg)
        self.assertIn(expected_substring, msg)

    def test_remove_record_on_delete_rejected(self):
        self._create_and_expect_unsupported(
            'agg_reject_remove_on_delete',
            {'aggregation.remove-record-on-delete': 'true'},
            'aggregation.remove-record-on-delete',
        )

    def test_field_ignore_retract_rejected(self):
        self._create_and_expect_unsupported(
            'agg_reject_ignore_retract',
            {'fields.total.ignore-retract': 'true'},
            'fields.total.ignore-retract',
        )

    def test_sequence_field_supported(self):
        # Top-level sequence.field is honored by the aggregation engine:
        # aggregators fold in sequence-field order, not file order. Here
        # ``last_value`` must pick the value from the highest-``total`` row
        # even though it was written first.
        table = self._create_pk_table(
            'agg_sequence_field',
            field_aggs={'max_score': 'last_value', 'label': 'last_value'},
            extra_options={'sequence.field': 'total'},
        )
        self._write(table, [{'id': 1, 'total': 100, 'max_score': 9, 'label': 'hi'}])
        self._write(table, [{'id': 1, 'total': 50, 'max_score': 1, 'label': 'lo'}])
        self.assertEqual(
            self._read(table),
            [{'id': 1, 'total': 100, 'max_score': 9, 'label': 'hi'}],
        )

    def test_aggregate_function_on_sequence_field_rejected(self):
        # An explicit aggregator on the sequence column is invalid: Java
        # rejects fields.<seq>.aggregate-function in
        # SchemaValidation.validateSequenceField. Rather than silently
        # override 'sum' with last_value, the guard must reject it.
        self._create_and_expect_unsupported(
            'agg_reject_agg_on_seq',
            {'sequence.field': 'total',
             'fields.total.aggregate-function': 'sum'},
            'fields.total.aggregate-function',
            error_type=ValueError,
        )

    def test_field_sequence_group_rejected(self):
        self._create_and_expect_unsupported(
            'agg_reject_sequence_group',
            {'fields.max_score.sequence-group': 'label'},
            'fields.max_score.sequence-group',
        )

    def test_out_of_scope_field_aggregator_rejected(self):
        # collect is one of the aggregator identifiers this engine
        # doesn't support yet. The guard must reject the config rather
        # than let the per-field factory build a (silently wrong)
        # fallback.
        self._create_and_expect_unsupported(
            'agg_reject_collect',
            {'fields.label.aggregate-function': 'collect'},
            'fields.label.aggregate-function',
        )

    def test_out_of_scope_default_aggregator_rejected(self):
        self._create_and_expect_unsupported(
            'agg_reject_default_collect',
            {'fields.default-aggregate-function': 'product'},
            'fields.default-aggregate-function',
        )

    def test_supported_field_aggregator_passes_guard(self):
        # Sanity check: setting one of the supported aggregators does
        # NOT trip the guard introduced for out-of-scope identifiers.
        table = self._create_pk_table(
            'agg_supported_passes',
            field_aggs={'total': 'sum'},
        )
        self._write(table, [{'id': 1, 'total': 1, 'max_score': 1, 'label': 'a'}])
        # If the guard wrongly flagged 'sum', new_read() would raise.
        # Touch it explicitly so the test fails loudly otherwise.
        table.new_read_builder().new_read()

    # -- partition column that is also part of the primary key ----------

    def test_partition_pk_overlap_not_aggregated_by_default(self):
        # When a partition column is also part of the primary key and a
        # table-wide ``fields.default-aggregate-function`` is configured,
        # the partition-PK column must be treated as PK (identity) and
        # not run through the default aggregator. Regression for the
        # split_read bug where the trimmed PK list (which drops
        # partition columns) was passed to ``build_field_aggregators``.
        pa_schema = pa.schema([
            pa.field('p', pa.int64(), nullable=False),
            pa.field('id', pa.int64(), nullable=False),
            pa.field('v', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['p', 'id'],
            partition_keys=['p'],
            options={
                'bucket': '1',
                'merge-engine': 'aggregation',
                'fields.default-aggregate-function': 'sum',
            },
        )
        self.catalog.create_table(
            'default.agg_partition_pk_overlap', schema, False)
        table = self.catalog.get_table('default.agg_partition_pk_overlap')

        def write(rows):
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            c = wb.new_commit()
            try:
                w.write_arrow(pa.Table.from_pylist(rows, schema=pa_schema))
                c.commit(w.prepare_commit())
            finally:
                w.close()
                c.close()

        write([{'p': 1, 'id': 1, 'v': 10}])
        write([{'p': 1, 'id': 1, 'v': 20}])

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        rows = rb.new_read().to_arrow(splits).to_pylist()
        self.assertEqual(rows, [{'p': 1, 'id': 1, 'v': 30}])


if __name__ == '__main__':
    unittest.main()
