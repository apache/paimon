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

"""End-to-end tests for the ``sequence.field`` option on the read path.

``sequence.field`` lets the user pick an explicit column (or columns)
whose value -- not the file-level sequence number -- decides which record
is the "latest" for a primary key. The tricky case is when the
write/file order disagrees with the ``sequence.field`` order: a row
written *later* (higher file sequence number) carrying a *lower*
``sequence.field`` value must lose to the earlier-written row. The Java
merge path applies a ``UserDefinedSeqComparator`` on the value row before
falling back to the file sequence number; pypaimon mirrors that via
``builtin_seq_comparator`` wired into ``SortMergeReaderWithMinHeap``.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class SequenceFieldReadE2ETest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('ts', pa.int64()),
            ('ts2', pa.int64()),
            ('val', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, table_name, merge_engine='deduplicate',
                         extra_options=None, partition_keys=None):
        # bucket=1 forces all rows for a PK into one bucket so the read
        # goes through SortMergeReader (where sequence ordering matters)
        # instead of the raw-convertible fast path.
        options = {
            'bucket': '1',
            'merge-engine': merge_engine,
        }
        if extra_options:
            options.update(extra_options)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            partition_keys=partition_keys or [],
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

    def _read(self, table, projection=None, predicate=None):
        rb = table.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        if predicate is not None:
            rb = rb.with_filter(predicate)
        splits = rb.new_scan().plan().splits()
        if not splits:
            return []
        return sorted(
            rb.new_read().to_arrow(splits).to_pylist(),
            key=lambda r: r['id'],
        )

    # -- basic ordering --------------------------------------------------

    def test_later_write_with_lower_sequence_field_loses(self):
        """The row written second has a higher file sequence number but a
        lower ``sequence.field`` value, so the earlier (higher-ts) row
        must win.
        """
        table = self._create_pk_table(
            'seq_basic', extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}],
        )

    def test_later_write_with_higher_sequence_field_wins(self):
        """Sanity check the non-inverted case still works."""
        table = self._create_pk_table(
            'seq_basic_fwd', extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}],
        )

    # -- multiple sequence fields ----------------------------------------

    def test_multi_sequence_field_left_to_right(self):
        """When the first sequence field ties, the second breaks it."""
        table = self._create_pk_table(
            'seq_multi', extra_options={'sequence.field': 'ts,ts2'})
        # Same ts; ts2 decides. Write the ts2-winner first so file order
        # disagrees with the sequence-field order.
        self._write(table, [{'id': 1, 'ts': 10, 'ts2': 99, 'val': 'win'}])
        self._write(table, [{'id': 1, 'ts': 10, 'ts2': 1, 'val': 'lose'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 10, 'ts2': 99, 'val': 'win'}],
        )

    # -- sort order ------------------------------------------------------

    def test_descending_sort_order_lowest_wins(self):
        """With descending sort order, the lowest ``sequence.field`` value
        is considered the latest.
        """
        table = self._create_pk_table(
            'seq_desc',
            extra_options={'sequence.field': 'ts',
                           'sequence.field.sort-order': 'descending'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}],
        )

    def test_descending_sort_order_null_sequence_sorts_first(self):
        """Null ordering must stay independent of sort order: Java builds
        the sequence comparator with ``nullIsLast=false`` and applies
        descending only to non-null value comparisons, so a null
        ``sequence.field`` value always sorts first (loses) -- even under
        descending order. A non-null row must therefore beat a null-seq
        row regardless of write order.
        """
        table = self._create_pk_table(
            'seq_desc_null',
            extra_options={'sequence.field': 'ts',
                           'sequence.field.sort-order': 'descending'})
        # null-seq row written second (higher file sequence number). With
        # nulls-first ordering it still loses to the earlier non-null row.
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'real'}])
        self._write(table, [{'id': 1, 'ts': None, 'ts2': 0, 'val': 'null'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'real'}],
        )

    def test_ascending_sort_order_null_sequence_sorts_first(self):
        """Mirror of the descending case under the default ascending order:
        a null ``sequence.field`` value sorts first (loses) to a non-null
        row written earlier.
        """
        table = self._create_pk_table(
            'seq_asc_null', extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'real'}])
        self._write(table, [{'id': 1, 'ts': None, 'ts2': 0, 'val': 'null'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'real'}],
        )

    # -- projection drops the sequence field -----------------------------

    def test_projection_dropping_sequence_field(self):
        """Projecting columns that exclude the sequence field must still
        return the sequence-field-correct row, and the output schema must
        contain exactly the requested columns (no leaked ``ts``).
        """
        table = self._create_pk_table(
            'seq_proj', extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])

        rows = self._read(table, projection=['id', 'val'])
        self.assertEqual(rows, [{'id': 1, 'val': 'high'}])
        # No injected sequence column leaks into the output.
        self.assertEqual(set(rows[0].keys()), {'id', 'val'})

    def test_projection_dropping_sequence_field_with_predicate(self):
        """Projection drops the seq field AND a predicate filters on a
        kept column -- predicate coordinates must stay correct against the
        widened (seq-injected) read type.
        """
        table = self._create_pk_table(
            'seq_proj_pred', extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'keep'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'drop'}])
        self._write(table, [{'id': 2, 'ts': 5, 'ts2': 0, 'val': 'other'}])

        rb = table.new_read_builder().with_projection(['id', 'val'])
        pb = rb.new_predicate_builder()
        rows = self._read(table, projection=['id', 'val'],
                          predicate=pb.equal('val', 'keep'))
        self.assertEqual(rows, [{'id': 1, 'val': 'keep'}])

    # -- per merge engine ------------------------------------------------

    def test_partial_update_respects_sequence_field(self):
        """partial-update folds non-null fields in sequence-field order, so
        a later-written but lower-ts row must not overwrite a field set by
        the higher-ts row.
        """
        table = self._create_pk_table(
            'seq_pu', merge_engine='partial-update',
            extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}],
        )

    def test_first_row_with_sequence_field_rejected(self):
        """sequence.field on the first-row merge engine is an invalid
        configuration that Java rejects at schema validation
        (SchemaValidation.validateSequenceField). pypaimon has no
        schema-creation validation, so the read-builder guard must reject
        it rather than silently apply a sequence ordering first-row never
        honors on write.
        """
        table = self._create_pk_table(
            'seq_fr', merge_engine='first-row',
            extra_options={'sequence.field': 'ts'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        with self.assertRaises(ValueError) as ctx:
            table.new_read_builder().new_read()
        self.assertIn('FIRST_ROW', str(ctx.exception))

    def test_aggregation_last_value_respects_sequence_field(self):
        """``last_value`` must pick the value from the highest-sequence-field
        row, even when that row was written first.
        """
        table = self._create_pk_table(
            'seq_agg', merge_engine='aggregation',
            extra_options={
                'sequence.field': 'ts',
                'fields.val.aggregate-function': 'last_value',
                'fields.ts2.aggregate-function': 'last_value',
            })
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}],
        )

    # -- unsupported sub-features still rejected -------------------------

    def test_missing_sequence_field_rejected(self):
        """A sequence.field naming a column absent from the schema is
        invalid (Java SchemaValidation). The guard must reject it with a
        clear message before any read execution.
        """
        table = self._create_pk_table(
            'seq_missing', extra_options={'sequence.field': 'nope'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])
        with self.assertRaises(ValueError) as ctx:
            table.new_read_builder().new_read()
        self.assertIn('nope', str(ctx.exception))

    def test_duplicate_sequence_field_rejected(self):
        """A sequence.field listing the same column twice is invalid
        (Java SchemaValidation rejects repeated sequence fields).
        """
        table = self._create_pk_table(
            'seq_dup', extra_options={'sequence.field': 'ts,ts'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])
        with self.assertRaises(ValueError) as ctx:
            table.new_read_builder().new_read()
        self.assertIn('ts', str(ctx.exception))

    def test_empty_segment_sequence_field_rejected(self):
        """A malformed ``sequence.field`` with an empty segment (e.g.
        ``'ts,,ts2'``) leaves an empty field name after trimming -- matching
        Java ``CoreOptions.sequenceField()``, which trims but does not drop
        empty segments -- and must be rejected by validation rather than
        silently accepted as ``['ts', 'ts2']``.
        """
        table = self._create_pk_table(
            'seq_empty_seg', extra_options={'sequence.field': 'ts,,ts2'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])
        with self.assertRaises(ValueError) as ctx:
            table.new_read_builder().new_read()
        # The empty field name is the one that can't be found in the schema.
        self.assertIn('can not be found', str(ctx.exception))

    def test_cross_partition_update_with_sequence_field_rejected(self):
        """sequence.field is invalid under cross-partition update (the PK
        does not include all partition fields), matching Java
        SchemaValidation.
        """
        table = self._create_pk_table(
            'seq_xpart', extra_options={'sequence.field': 'ts'},
            partition_keys=['ts2'])
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])
        with self.assertRaises(ValueError) as ctx:
            table.new_read_builder().new_read()
        self.assertIn('cross partition', str(ctx.exception).lower())

    def test_aggregate_function_on_sequence_field_rejected(self):
        """Defining an aggregator on the sequence column is invalid: Java
        rejects fields.<seq>.aggregate-function outright in
        SchemaValidation.validateSequenceField. The read-builder guard
        must reject it rather than silently override the user's
        aggregator with last_value.
        """
        table = self._create_pk_table(
            'seq_agg_on_seq', merge_engine='aggregation',
            extra_options={'sequence.field': 'ts',
                           'fields.ts.aggregate-function': 'sum'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])
        with self.assertRaises(ValueError) as ctx:
            table.new_read_builder().new_read()
        self.assertIn('fields.ts.aggregate-function', str(ctx.exception))

    def test_sequence_group_still_rejected(self):
        """Top-level sequence.field is supported, but per-field
        sequence-group is not -- it must still be rejected. The shared
        merge-engine dispatch now rejects this combination fail-fast on
        the write path, so the write (not the read) is what raises.
        """
        table = self._create_pk_table(
            'seq_group', merge_engine='partial-update',
            extra_options={'sequence.field': 'ts',
                           'fields.ts2.sequence-group': 'val'})
        with self.assertRaises(NotImplementedError):
            self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])

    def test_nested_sequence_field_rejected(self):
        """nested-sequence-field is unimplemented and must be rejected
        rather than silently ignored by the top-level comparator.
        """
        table = self._create_pk_table(
            'seq_nested', merge_engine='deduplicate',
            extra_options={'sequence.field': 'ts',
                           'fields.val.nested-sequence-field': 'ts2'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'x'}])
        with self.assertRaises(NotImplementedError):
            self._read(table)

    def test_trailing_comma_sequence_field_tolerated(self):
        """A trailing comma (``'ts,'``) must be tolerated, matching Java
        ``String.split(',')`` which drops trailing empty segments. It
        behaves exactly like ``'ts'`` -- not rejected as an empty field.
        """
        table = self._create_pk_table(
            'seq_trailing', extra_options={'sequence.field': 'ts,'})
        self._write(table, [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}])
        self._write(table, [{'id': 1, 'ts': 50, 'ts2': 0, 'val': 'low'}])

        self.assertEqual(
            self._read(table),
            [{'id': 1, 'ts': 100, 'ts2': 0, 'val': 'high'}],
        )

    def test_complex_type_sequence_field_rejected(self):
        """A complex (non-atomic) sequence field is valid in Java (handled
        via RecordComparator) but unimplemented in pypaimon's atomic-only
        comparator. It must be rejected with a clear NotImplementedError
        rather than failing later with an obscure attribute error.
        """
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('seq', pa.list_(pa.int64())),
            ('val', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['id'],
            options={'bucket': '1', 'merge-engine': 'deduplicate',
                     'sequence.field': 'seq'})
        self.catalog.create_table('default.seq_complex', schema, False)
        table = self.catalog.get_table('default.seq_complex')
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(
                [{'id': 1, 'seq': [1, 2], 'val': 'x'}], schema=pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()
        with self.assertRaises(NotImplementedError) as ctx:
            table.new_read_builder().new_read()
        self.assertIn('seq', str(ctx.exception))


class SequenceFieldComparabilityUnitTest(unittest.TestCase):
    """Unit-level coverage of ``is_comparable_seq_field`` -- the predicate
    behind the read-builder guard. VARIANT in particular is an
    ``AtomicType`` but has no ordering, so it must be rejected like the
    complex types rather than slipping through an ``isinstance(AtomicType)``
    check.
    """

    def test_variant_sequence_field_not_comparable(self):
        from pypaimon.read.reader.sort_merge_reader import (
            is_comparable_seq_field)
        from pypaimon.schema.data_types import AtomicType, DataField

        variant = DataField(0, 'seq', AtomicType('VARIANT'))
        self.assertFalse(is_comparable_seq_field(variant))

    def test_atomic_types_are_comparable(self):
        from pypaimon.read.reader.sort_merge_reader import (
            is_comparable_seq_field)
        from pypaimon.schema.data_types import AtomicType, DataField

        for type_str in ('BIGINT', 'INT', 'TIMESTAMP(6)', 'DECIMAL(10, 2)',
                         'STRING', 'BIGINT NOT NULL'):
            field = DataField(0, 'seq', AtomicType(type_str))
            self.assertTrue(is_comparable_seq_field(field),
                            '{} should be comparable'.format(type_str))

    def test_complex_types_not_comparable(self):
        from pypaimon.read.reader.sort_merge_reader import (
            is_comparable_seq_field)
        from pypaimon.schema.data_types import (
            ArrayType, AtomicType, DataField)

        array = DataField(0, 'seq', ArrayType(True, AtomicType('INT')))
        self.assertFalse(is_comparable_seq_field(array))


class SequenceFieldParameterizedTypeTest(unittest.TestCase):
    """The comparability check must accept parameterized atomic types
    (TIMESTAMP(p), DECIMAL(p, s), TIME(p)) as sequence fields -- their
    type string carries ``(...)`` which must not be mistaken for a
    non-comparable type.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(cls.tempdir, 'warehouse')})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _run(self, table_name, pa_schema, rows_first, rows_second, expected):
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['id'],
            options={'bucket': '1', 'merge-engine': 'deduplicate',
                     'sequence.field': 'seq'})
        full = 'default.{}'.format(table_name)
        self.catalog.create_table(full, schema, False)
        table = self.catalog.get_table(full)
        for batch in (rows_first, rows_second):
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            c = wb.new_commit()
            try:
                w.write_arrow(pa.Table.from_pylist(batch, schema=pa_schema))
                c.commit(w.prepare_commit())
            finally:
                w.close()
                c.close()
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        rows = rb.new_read().to_arrow(splits).to_pylist()
        self.assertEqual(rows, expected)

    def test_timestamp_sequence_field(self):
        import datetime
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('seq', pa.timestamp('us')),
            ('val', pa.string()),
        ])
        hi = datetime.datetime(2020, 1, 2)
        lo = datetime.datetime(2020, 1, 1)
        # Later write has the lower timestamp -> earlier (higher-ts) wins.
        self._run('seq_ts',
                  pa_schema,
                  [{'id': 1, 'seq': hi, 'val': 'high'}],
                  [{'id': 1, 'seq': lo, 'val': 'low'}],
                  [{'id': 1, 'seq': hi, 'val': 'high'}])

    def test_decimal_sequence_field(self):
        from decimal import Decimal
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('seq', pa.decimal128(10, 2)),
            ('val', pa.string()),
        ])
        self._run('seq_dec',
                  pa_schema,
                  [{'id': 1, 'seq': Decimal('100.50'), 'val': 'high'}],
                  [{'id': 1, 'seq': Decimal('50.25'), 'val': 'low'}],
                  [{'id': 1, 'seq': Decimal('100.50'), 'val': 'high'}])


if __name__ == '__main__':
    unittest.main()
