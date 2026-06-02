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

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import WhenMatched, WhenNotMatched, merge_into

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

    def test_partitioned_update_rejects_partition_key_change(self):
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
        self.assertIn('partition key', str(ctx.exception))

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


class TargetProjectionTest(unittest.TestCase):

    def _clause(self, spec):
        from pypaimon.ray import data_evolution_merge_into as m
        return m._NormalizedClause(spec=spec)

    def test_unconditional_set_excludes_target_update_col(self):
        from pypaimon.ray import data_evolution_merge_into as m
        cols = m._resolve_target_projection(
            [self._clause({'feature': 's.feature'})],
            ['id'], ['feature'], ['id', 'feature', 'image'],
        )
        self.assertEqual(['id'], cols)


if __name__ == '__main__':
    unittest.main()
