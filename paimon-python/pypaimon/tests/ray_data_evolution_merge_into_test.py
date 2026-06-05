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
import uuid
from unittest.mock import Mock, patch

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import (
    WhenMatched, WhenNotMatched, merge_into,
    source_col, target_col, lit,
)

try:
    import datafusion  # noqa: F401
    _HAS_DATAFUSION = True
except ImportError:
    _HAS_DATAFUSION = False

_SKIP_CONDITION = not _HAS_DATAFUSION
_SKIP_REASON = "datafusion not installed"

_TEST_NUM_PARTITIONS = 2


class RayDataEvolutionMergeIntoTest(unittest.TestCase):

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
    ])

    de_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database('default', True)
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

    def _create_table(self, options=None):
        opts = options if options is not None else self.de_options
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(self.pa_schema, options=opts)
        self.catalog.create_table(name, s, False)
        return name

    def _source(self, ids=(1,)):
        return pa.Table.from_pydict(
            {
                'id': pa.array(list(ids), type=pa.int32()),
                'name': ['x'] * len(ids),
                'age': [10] * len(ids),
            },
            schema=self.pa_schema,
        )

    def _write(self, target, data):
        table = self.catalog.get_table(target)
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(data)
        wb.new_commit().commit(writer.prepare_commit())
        writer.close()

    def _read_sorted(self, target):
        table = self.catalog.get_table(target)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits).sort_by('id').to_pydict()

    def _snapshot_id(self, target):
        table = self.catalog.get_table(target)
        snap = table.snapshot_manager().get_latest_snapshot()
        return snap.id if snap is not None else None

    def test_paimon_source_table_pins_snapshot(self):
        from pypaimon.ray import data_evolution_merge_into as m

        target = self._create_table()
        source = self._create_table()
        self._write(source, self._source(ids=(1,)))
        expected_snapshot_id = self._snapshot_id(source)

        fake_ds = Mock()
        fake_ds.schema.return_value = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])

        with patch(
                'pypaimon.ray.ray_paimon.read_paimon',
                return_value=fake_ds,
        ) as mock_read_paimon:
            m._prepare(
                target, source, self.catalog_options,
                [WhenMatched(update='*')], [], ['id'],
            )

        mock_read_paimon.assert_called_once_with(
            source,
            self.catalog_options,
            snapshot_id=expected_snapshot_id,
        )

    def test_no_clause_raises(self):
        target = self._create_table()
        with self.assertRaises(ValueError):
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                num_partitions=_TEST_NUM_PARTITIONS,
            )

    def test_non_de_table_rejected(self):
        target = self._create_table(options={'row-tracking.enabled': 'true'})
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('data-evolution.enabled', str(ctx.exception))

    def test_no_row_tracking_rejected(self):
        target = self._create_table(options={'data-evolution.enabled': 'true'})
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('row-tracking.enabled', str(ctx.exception))

    def test_source_missing_on_col_raises(self):
        target = self._create_table()
        bad_source = pa.Table.from_pydict(
            {'name': ['x'], 'age': [10]},
            schema=pa.schema([('name', pa.string()), ('age', pa.int32())]),
        )
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=bad_source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn("'id'", str(ctx.exception))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_not_matched_condition_rejects_target_refs(self):
        target = self._create_table()
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_not_matched=[
                    WhenNotMatched(insert='*', condition='t.age > 10')
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('t.', str(ctx.exception))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_condition_unknown_source_col_rejected(self):
        target = self._create_table()
        self._write(target, self._source())
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[
                    WhenMatched(update='*', condition='s.nonexistent > 0')
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('nonexistent', str(ctx.exception))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_condition_unknown_target_col_rejected(self):
        target = self._create_table()
        self._write(target, self._source())
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[
                    WhenMatched(update='*', condition='s.age > t.nonexistent')
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('nonexistent', str(ctx.exception))

    def test_matched_update_star(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3, 4], type=pa.int32()),
                'name': ['b2', 'c2', 'd'],
                'age': pa.array([22, 33, 40], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c2'])
        self.assertEqual(out['age'], [10, 22, 33])

    def test_not_matched_insert_appends_unmatched(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3, 4], type=pa.int32()),
                'name': ['b2', 'c2', 'd'],
                'age': pa.array([22, 33, 40], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3, 4])
        self.assertEqual(out['name'], ['a', 'b', 'c', 'd'])
        self.assertEqual(out['age'], [10, 20, 30, 40])

    def test_combined_update_and_insert(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        metrics = merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c'])
        self.assertEqual(out['age'], [10, 22, 30])
        self.assertEqual(metrics, {
            'num_matched': 1, 'num_inserted': 1, 'num_unchanged': 0,
        })

    def test_on_with_renamed_columns_star(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source_schema = pa.schema([
            ('uid', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        source = pa.Table.from_pydict(
            {
                'uid': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=source_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on={'id': 'uid'},
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c'])
        self.assertEqual(out['age'], [10, 22, 30])

    def test_insert_into_empty_target(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['a', 'b', 'c'],
                'age': pa.array([10, 20, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b', 'c'])
        self.assertEqual(out['age'], [10, 20, 30])

    def test_multi_source_match_raises_by_default(self):
        # One target row matched by several source rows: the winning value is
        # undefined (Spark DE's checkCardinality=false), so we refuse by default.
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 1], type=pa.int32()),
                'name': ['x', 'y'],
                'age': pa.array([100, 200], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        with self.assertRaises(Exception) as ctx:
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn("multiple source rows", str(ctx.exception))

    def test_blob_columns_excluded(self):
        import types

        from pypaimon.ray.data_evolution_merge_into import _blob_col_names
        from pypaimon.schema.data_types import AtomicType, DataField

        fake_table = types.SimpleNamespace(
            table_schema=types.SimpleNamespace(
                fields=[
                    DataField(0, 'id', AtomicType('INT')),
                    DataField(1, 'payload', AtomicType('BLOB')),
                ]
            )
        )
        self.assertEqual({'payload'}, _blob_col_names(fake_table))

    def test_combined_writes_single_snapshot(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )
        before = self._snapshot_id(target)

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3], type=pa.int32()),
                'name': ['b2', 'c'],
                'age': pa.array([22, 30], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        after = self._snapshot_id(target)
        self.assertEqual(after, before + 1)

    def test_empty_target_matched_update_is_noop(self):
        target = self._create_table()
        before = self._snapshot_id(target)

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([10, 20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(self._snapshot_id(target), before)

    def test_partitioned_matched_update_rejected(self):
        pt_schema = pa.schema([
            ('pt', pa.string()),
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(
            pt_schema, partition_keys=['pt'], options=self.de_options,
        )
        self.catalog.create_table(name, s, False)

        source = pa.Table.from_pydict(
            {
                'pt': ['a'],
                'id': pa.array([1], type=pa.int32()),
                'name': ['x'],
            },
            schema=pt_schema,
        )

        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=name,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update='*')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('partitioned', str(ctx.exception))

    def test_partitioned_insert_allowed(self):
        pt_schema = pa.schema([
            ('pt', pa.string()),
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(
            pt_schema, partition_keys=['pt'], options=self.de_options,
        )
        self.catalog.create_table(name, s, False)

        source = pa.Table.from_pydict(
            {
                'pt': ['a', 'b'],
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['x', 'y'],
            },
            schema=pt_schema,
        )

        merge_into(
            target=name,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        table = self.catalog.get_table(name)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        out = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['pt'], ['a', 'b'])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_matched_update_with_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['a2', 'b2', 'c2'],
                'age': pa.array([15, 25, 45], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*', condition='s.age > t.age + 10')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b', 'c2'])
        self.assertEqual(out['age'], [10, 20, 45])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_matched_condition_with_source_on_key(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'name': ['a', 'b', 'c'],
                    'age': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['a2', 'b2', 'c2'],
                'age': pa.array([15, 25, 35], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*', condition='s.id >= 2')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c2'])
        self.assertEqual(out['age'], [10, 25, 35])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_not_matched_insert_with_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([2, 3, 4], type=pa.int32()),
                'name': ['b', 'c', 'd'],
                'age': pa.array([15, 25, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(insert='*', condition='s.age >= 10')
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b', 'c'])
        self.assertEqual(out['age'], [10, 15, 25])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_combined_with_conditions(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3, 4], type=pa.int32()),
                'name': ['a2', 'b2', 'c', 'd'],
                'age': pa.array([50, 5, 30, 8], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        metrics = merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*', condition='s.age > t.age')],
            when_not_matched=[
                WhenNotMatched(insert='*', condition='s.age > 10')
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a2', 'b', 'c'])
        self.assertEqual(out['age'], [50, 20, 30])
        self.assertEqual(metrics['num_matched'], 1)
        self.assertEqual(metrics['num_inserted'], 1)

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_condition_no_rows_match_is_noop(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a2', 'b2'],
                'age': pa.array([5, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update='*', condition='s.age > t.age')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])
        self.assertEqual(out['age'], [10, 20])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_duplicate_source_filtered_by_condition(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 1], type=pa.int32()),
                'name': ['x', 'y'],
                'age': pa.array([5, 20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched(update='*', condition='s.age > t.age')
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1])
        self.assertEqual(out['name'], ['y'])
        self.assertEqual(out['age'], [20])

    def test_matched_partial_update(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a2', 'b2'],
                'age': pa.array([99, 88], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={'age': 's.age'})],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])
        self.assertEqual(out['age'], [99, 88])

    def test_insert_partial_mapping(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([10, 20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(insert={'id': 's.id', 'name': 's.name'})
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])
        self.assertEqual(out['age'], [None, None])

    def test_update_with_literal(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['ignored'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={'name': 'updated'})],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['updated'])
        self.assertEqual(out['age'], [10])

    def test_invalid_target_column_rejected(self):
        target = self._create_table()
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update={'nonexistent': 's.id'})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('nonexistent', str(ctx.exception))

    def test_invalid_target_ref_rejected(self):
        target = self._create_table()
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update={'name': 't.nme'})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('nme', str(ctx.exception))

    def test_empty_mapping_rejected(self):
        target = self._create_table()
        with self.assertRaises(ValueError):
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update={})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )

    def test_insert_target_ref_rejected(self):
        target = self._create_table()
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_not_matched=[
                    WhenNotMatched(insert={'name': 't.name'})
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('t.', str(ctx.exception))

    def test_matched_update_with_target_ref(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['ignored'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={'age': 's.age', 'name': 't.name'})],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['old'])
        self.assertEqual(out['age'], [99])

    def test_callable_value_rejected(self):
        target = self._create_table()
        with self.assertRaises(TypeError):
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update={'name': lambda r: r})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )

    def test_source_missing_referenced_col(self):
        target = self._create_table()
        source = pa.Table.from_pydict(
            {'id': pa.array([1], type=pa.int32())},
            schema=pa.schema([('id', pa.int32())]),
        )
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched(update={'name': 's.name'})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('name', str(ctx.exception))

    def test_partial_insert_auto_fills_on_key(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([10, 20], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(insert={'name': 's.name'})
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])

    def test_partial_insert_renamed_on_key_auto_filled(self):
        target = self._create_table()

        source_schema = pa.schema([
            ('uid', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        source = pa.Table.from_pydict(
            {
                'uid': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([10, 20], type=pa.int32()),
            },
            schema=source_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on={'id': 'uid'},
            when_not_matched=[
                WhenNotMatched(insert={'name': 's.name'})
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['name'], ['a', 'b'])

    def test_explicit_source_ref_not_remapped_by_on_key(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source_schema = pa.schema([
            ('uid', pa.int32()),
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        source = pa.Table.from_pydict(
            {
                'uid': pa.array([1], type=pa.int32()),
                'id': pa.array([42], type=pa.int32()),
                'name': ['new'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=source_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on={'id': 'uid'},
            when_matched=[WhenMatched(update={
                'age': source_col('id'),
            })],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['age'], [42])
        self.assertEqual(out['name'], ['old'])

    def test_renamed_on_key_missing_source_col_rejected(self):
        target = self._create_table()
        source_schema = pa.schema([
            ('uid', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        source = pa.Table.from_pydict(
            {
                'uid': pa.array([1], type=pa.int32()),
                'name': ['a'],
                'age': pa.array([10], type=pa.int32()),
            },
            schema=source_schema,
        )
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on={'id': 'uid'},
                when_matched=[WhenMatched(update={
                    'id': source_col('id'),
                })],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('id', str(ctx.exception))

    def test_lit_prevents_column_ref_interpretation(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['ignored'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={
                'name': lit('s.active'),
            })],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['s.active'])
        self.assertEqual(out['age'], [10])

    def test_source_col_helper(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['old'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['new'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={
                'age': source_col('age'),
            })],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['old'])
        self.assertEqual(out['age'], [99])

    def test_target_col_helper(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['keep'],
                    'age': pa.array([10], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1], type=pa.int32()),
                'name': ['ignored'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched(update={
                'age': source_col('age'),
                'name': target_col('name'),
            })],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['keep'])
        self.assertEqual(out['age'], [99])


class TargetProjectionTest(unittest.TestCase):

    def _clause(self, spec, condition=None):
        from pypaimon.ray import data_evolution_merge_into as m
        return m._NormalizedClause(spec=spec, condition=condition)

    def test_unconditional_set_excludes_target_update_col(self):
        from pypaimon.ray import data_evolution_merge_into as m
        cols = m._resolve_target_projection(
            [self._clause({'feature': 's.feature'})],
            ['id'], ['feature'], ['id', 'feature', 'image'],
        )
        self.assertEqual(['id'], cols)

    def test_condition_adds_referenced_target_cols(self):
        from pypaimon.ray import data_evolution_merge_into as m
        cols = m._resolve_target_projection(
            [self._clause({'feature': 's.feature'}, condition='s.age > t.age')],
            ['id'], ['feature'], ['id', 'feature', 'age', 'image'],
        )
        self.assertIn('age', cols)
        self.assertIn('id', cols)


class MergeConditionUnitTest(unittest.TestCase):

    def test_rewrite_condition(self):
        from pypaimon.ray.merge_condition import rewrite_condition
        self.assertEqual(
            rewrite_condition('s.age > t.age + 10'),
            '"s.age" > "t.age" + 10',
        )

    def test_rewrite_condition_preserves_string_literals(self):
        from pypaimon.ray.merge_condition import rewrite_condition
        self.assertEqual(
            rewrite_condition("s.status = 't.active' AND s.age > t.age"),
            '"s.status" = \'t.active\' AND "s.age" > "t.age"',
        )

    def test_remap_source_on_keys(self):
        from pypaimon.ray.merge_condition import (
            remap_source_on_keys, rewrite_condition,
        )
        rewritten = rewrite_condition('s.id > 1 AND s.age > t.age')
        remapped = remap_source_on_keys(rewritten, {'id': 'id'})
        self.assertEqual(remapped, '"t.id" > 1 AND "s.age" > "t.age"')

    def test_remap_source_on_keys_renamed(self):
        from pypaimon.ray.merge_condition import (
            remap_source_on_keys, rewrite_condition,
        )
        rewritten = rewrite_condition('s.uid > 1')
        remapped = remap_source_on_keys(rewritten, {'uid': 'id'})
        self.assertEqual(remapped, '"t.id" > 1')

    def test_remap_preserves_string_literals(self):
        from pypaimon.ray.merge_condition import (
            remap_source_on_keys, rewrite_condition,
        )
        rewritten = rewrite_condition("s.note = '\"s.id\"' AND s.id = 1")
        remapped = remap_source_on_keys(rewritten, {'id': 'id'})
        self.assertEqual(
            remapped,
            '"s.note" = \'\"s.id\"\' AND "t.id" = 1',
        )

    def test_extract_target_columns(self):
        from pypaimon.ray.merge_condition import extract_target_columns
        self.assertEqual(
            extract_target_columns('s.name = t.name AND s.age > t.age'),
            {'name', 'age'},
        )

    def test_extract_target_columns_ignores_string_literals(self):
        from pypaimon.ray.merge_condition import extract_target_columns
        self.assertEqual(
            extract_target_columns("s.name = 't.fake' AND s.age > t.age"),
            {'age'},
        )

    def test_extract_columns(self):
        from pypaimon.ray.merge_condition import extract_columns
        self.assertEqual(
            extract_columns('s.id = t.id AND s.age > t.age'),
            {'s.id', 't.id', 's.age', 't.age'},
        )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_filter_batch(self):
        from pypaimon.ray.merge_condition import filter_batch
        batch = pa.table({
            's.id': pa.array([1, 2, 3], type=pa.int32()),
            's.age': pa.array([10, 25, 30], type=pa.int32()),
            't.age': pa.array([20, 20, 20], type=pa.int32()),
        })
        result = filter_batch(batch, 's.age > t.age')
        self.assertEqual(result.column('s.id').to_pylist(), [2, 3])


if __name__ == '__main__':
    unittest.main()
