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

import datetime
import os
import shutil
import tempfile
import unittest
import uuid
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.compute as pc
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import (
    WhenMatched, WhenNotMatched, merge_into, read_paimon,
    source_col, target_col, lit,
)

try:
    import datafusion  # noqa: F401
    _HAS_DATAFUSION = True
except ImportError:
    _HAS_DATAFUSION = False

_SKIP_CONDITION = not _HAS_DATAFUSION
_SKIP_REASON = "pypaimon[sql] is required for condition expressions"

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

    def _merge_and_capture_self_merge_plan(self, **kwargs):
        from pypaimon.ray.data_evolution_merge_join import (
            build_self_merge_update_plan as real_build_plan,
        )

        captured = {}

        def capture(**plan_kwargs):
            plan = real_build_plan(**plan_kwargs)
            captured['plan'] = plan
            return plan

        with patch(
                'pypaimon.ray.data_evolution_merge_into.'
                'build_self_merge_update_plan',
                side_effect=capture,
        ):
            result = merge_into(**kwargs)
        return result, captured['plan']

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
                [WhenMatched.update('*')], [], ['id'],
            )

        mock_read_paimon.assert_called_once_with(
            source,
            self.catalog_options,
            snapshot_id=expected_snapshot_id,
            projection=['id', 'name', 'age'],
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

    def test_unconditional_non_last_matched_rejected(self):
        target = self._create_table()
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[
                    WhenMatched.update('*'),
                    WhenMatched.update({'age': 's.age'}, condition='s.age > 10'),
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('when_matched', str(ctx.exception))
        self.assertIn('unreachable', str(ctx.exception))

    def test_unconditional_non_last_not_matched_rejected(self):
        target = self._create_table()
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_not_matched=[
                    WhenNotMatched(insert='*'),
                    WhenNotMatched(insert='*', condition='s.age > 10'),
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('when_not_matched', str(ctx.exception))
        self.assertIn('unreachable', str(ctx.exception))

    def test_non_de_table_rejected(self):
        target = self._create_table(options={'row-tracking.enabled': 'true'})
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=self._source(),
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched.update('*')],
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
                when_matched=[WhenMatched.update('*')],
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
                when_matched=[WhenMatched.update('*')],
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
                    WhenMatched.update('*', condition='s.nonexistent > 0')
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
                    WhenMatched.update('*', condition='s.age > t.nonexistent')
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
            when_matched=[WhenMatched.update('*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b2', 'c2'])
        self.assertEqual(out['age'], [10, 22, 33])

    def test_matched_delete(self):
        options = dict(self.de_options)
        options['deletion-vectors.enabled'] = 'true'
        target = self._create_table(options=options)
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

        metrics = merge_into(
            target=target,
            source=pa.Table.from_pydict(
                {
                    'id': pa.array([2, 3], type=pa.int32()),
                    'name': ['ignored', 'ignored'],
                    'age': pa.array([99, 99], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched.delete()],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(metrics, {
            'num_matched': 2, 'num_inserted': 0, 'num_unchanged': 0,
        })
        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1])
        self.assertEqual(out['name'], ['a'])
        self.assertEqual(out['age'], [10])

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
            when_matched=[WhenMatched.update('*')],
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
            when_matched=[WhenMatched.update('*')],
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
                when_matched=[WhenMatched.update('*')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn("multiple source rows", str(ctx.exception))

    def test_blob_table_merge_into_updates_and_inserts_blob_column(self):
        blob_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('payload', pa.large_binary()),
        ])
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            blob_schema, options=self.de_options)
        self.catalog.create_table(name, schema, False)
        self._write(
            name,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['Alice', 'Bob'],
                    'payload': pa.array(
                        [b'blob-1', b'blob-2'], type=pa.large_binary()),
                },
                schema=blob_schema,
            ),
        )

        metrics = merge_into(
            target=name,
            source=pa.Table.from_pydict(
                {
                    'id': pa.array([2, 3], type=pa.int32()),
                    'name': ['Bobby', 'Cindy'],
                    'payload': pa.array(
                        [b'blob-2-updated', b'blob-3'],
                        type=pa.large_binary()),
                },
                schema=blob_schema,
            ),
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched.update('*')],
            when_not_matched=[WhenNotMatched(insert='*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        table = self.catalog.get_table(name)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        out = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['Alice', 'Bobby', 'Cindy'])
        self.assertEqual(
            out['payload'], [b'blob-1', b'blob-2-updated', b'blob-3'])
        self.assertEqual(metrics, {
            'num_matched': 1, 'num_inserted': 1, 'num_unchanged': 0,
        })

    def test_blob_table_merge_into_inserts_null_for_unspecified_blob_column(self):
        blob_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('payload', pa.large_binary()),
        ])
        source_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            blob_schema, options=self.de_options)
        self.catalog.create_table(name, schema, False)

        metrics = merge_into(
            target=name,
            source=pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['Alice'],
                },
                schema=source_schema,
            ),
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[WhenNotMatched(insert={
                'id': source_col('id'),
                'name': source_col('name'),
            })],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        table = self.catalog.get_table(name)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        out = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(out['id'], [1])
        self.assertEqual(out['name'], ['Alice'])
        self.assertEqual(out['payload'], [None])
        self.assertEqual(metrics, {
            'num_matched': 0, 'num_inserted': 1, 'num_unchanged': 0,
        })

    def test_blob_table_feature_update(self):
        blob_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
            ('feature', pa.int32()),
        ])
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            blob_schema, options=self.de_options)
        self.catalog.create_table(name, schema, False)
        self._write(
            name,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'payload': [b'aa', b'bbb', b'cccc'],
                    'feature': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=blob_schema,
            ),
        )

        num_partitions = _TEST_NUM_PARTITIONS
        records_to_process = ray.data.from_arrow(pa.Table.from_pydict({
            'id': pa.array([1, 3], type=pa.int32()),
        }))
        target_rows = read_paimon(
            name,
            self.catalog_options,
            projection=['id', 'payload'],
        )
        selected = records_to_process.join(
            target_rows,
            join_type='inner',
            num_partitions=num_partitions,
            on=['id'],
        )

        def compute_feature(batch):
            payloads = batch['payload'].to_pylist()
            return pa.Table.from_pydict({
                'id': batch['id'],
                'new_feature': pa.array(
                    [len(v) if v is not None else 0 for v in payloads],
                    type=pa.int32(),
                ),
            })

        updates = selected.map_batches(compute_feature, batch_format='pyarrow')
        metrics = merge_into(
            target=name,
            source=updates,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched.update({'feature': source_col('new_feature')})
            ],
            num_partitions=num_partitions,
        )

        table = self.catalog.get_table(name)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        out = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['feature'], [2, 20, 4])
        self.assertEqual(out['payload'], [b'aa', b'bbb', b'cccc'])
        self.assertEqual(metrics, {
            'num_matched': 2, 'num_inserted': 0, 'num_unchanged': 0,
        })

    def test_blob_descriptor_resolve_and_merge(self):
        from pypaimon.table.row.blob import BlobDescriptor, Blob
        from pypaimon.common.uri_reader import UriReaderFactory

        blob_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
            ('feature', pa.int32()),
        ])
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            blob_schema, options=self.de_options)
        self.catalog.create_table(name, schema, False)
        self._write(
            name,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'payload': [b'aa', b'bbb', b'cccc'],
                    'feature': pa.array([10, 20, 30], type=pa.int32()),
                },
                schema=blob_schema,
            ),
        )

        num_partitions = _TEST_NUM_PARTITIONS
        input_ids = ray.data.from_arrow(pa.Table.from_pydict({
            'id': pa.array([1, 3], type=pa.int32()),
        }))

        target_rows = read_paimon(
            name,
            self.catalog_options,
            projection=['id', 'payload'],
            dynamic_options={'blob-as-descriptor': 'true'},
        )

        matched = input_ids.join(
            target_rows, join_type='inner',
            num_partitions=num_partitions, on=['id'],
        )

        uri_factory = UriReaderFactory(self.catalog_options)

        def resolve_and_compute(batch):
            features = []
            for desc_bytes in batch['payload'].to_pylist():
                desc = BlobDescriptor.deserialize(desc_bytes)
                reader = uri_factory.create(desc.uri)
                data = Blob.from_descriptor(reader, desc).to_data()
                features.append(len(data) * 100)
            return pa.Table.from_pydict({
                'id': batch['id'],
                'new_feature': pa.array(features, type=pa.int32()),
            })

        updates = matched.map_batches(
            resolve_and_compute, batch_format='pyarrow')
        metrics = merge_into(
            target=name,
            source=updates,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched.update({'feature': source_col('new_feature')})
            ],
            num_partitions=num_partitions,
        )

        table = self.catalog.get_table(name)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        out = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['feature'], [200, 20, 400])
        self.assertEqual(out['payload'], [b'aa', b'bbb', b'cccc'])
        self.assertEqual(metrics['num_matched'], 2)

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
            when_matched=[WhenMatched.update('*')],
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
            when_matched=[WhenMatched.update('*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(self._snapshot_id(target), before)

    def test_matched_update_with_no_matches_is_noop(self):
        target = self._create_table()
        self._write(target, self._source(ids=(1,)))
        before = self._snapshot_id(target)

        result = merge_into(
            target=target,
            source=self._source(ids=(2,)),
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched.update('*')],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 0)
        self.assertEqual(self._snapshot_id(target), before)
        self.assertEqual(self._read_sorted(target)['id'], [1])

    def test_matched_on_partitioned_table(self):
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

        table = self.catalog.get_table(name)
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(pa.Table.from_pydict(
            {
                'pt': ['a', 'a'],
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['old_1', 'old_2'],
            },
            schema=pt_schema,
        ))
        wb.new_commit().commit(writer.prepare_commit())
        writer.close()

        source = pa.Table.from_pydict(
            {
                'pt': ['a'],
                'id': pa.array([1], type=pa.int32()),
                'name': ['new_1'],
            },
            schema=pt_schema,
        )

        # Non-partition column update should succeed
        merge_into(
            target=name,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[WhenMatched.update({'name': source_col('name')})],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        out = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(out['name'], ['new_1', 'old_2'])
        self.assertEqual(out['pt'], ['a', 'a'])

        # Partition column update should be rejected
        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=name,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[WhenMatched.update({'pt': source_col('pt')})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('partition', str(ctx.exception))

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
            when_matched=[WhenMatched.update('*', condition='s.age > t.age + 10')],
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
            when_matched=[WhenMatched.update('*', condition='s.id >= 2')],
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
            when_matched=[WhenMatched.update('*', condition='s.age > t.age')],
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
            when_matched=[WhenMatched.update('*', condition='s.age > t.age')],
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
                WhenMatched.update('*', condition='s.age > t.age')
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
            when_matched=[WhenMatched.update({'age': 's.age'})],
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
            when_matched=[WhenMatched.update({'name': 'updated'})],
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
                when_matched=[WhenMatched.update({'nonexistent': 's.id'})],
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
                when_matched=[WhenMatched.update({'name': 't.nme'})],
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
                when_matched=[WhenMatched.update({})],
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
            when_matched=[WhenMatched.update({'age': 's.age', 'name': 't.name'})],
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
                when_matched=[WhenMatched.update({'name': lambda r: r})],
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
                when_matched=[WhenMatched.update({'name': 's.name'})],
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
            when_matched=[WhenMatched.update({
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
                when_matched=[WhenMatched.update({
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
            when_matched=[WhenMatched.update({
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
            when_matched=[WhenMatched.update({
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
            when_matched=[WhenMatched.update({
                'age': source_col('age'),
                'name': target_col('name'),
            })],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['keep'])
        self.assertEqual(out['age'], [99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_matched_clause_fall_through(self):
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
                'age': pa.array([99, 88, 77], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched.update('*', condition='s.age > 80'),
                WhenMatched.update('*'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a2', 'b2', 'c2'])
        self.assertEqual(out['age'], [99, 88, 77])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_not_matched_clause_fall_through(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2, 3], type=pa.int32()),
                'name': ['a', 'b', 'c'],
                'age': pa.array([25, 15, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(insert='*', condition='s.age >= 20'),
                WhenNotMatched(insert='*'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_matched_null_falls_through(self):
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
                'age': pa.array([None, 50, 60], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched.update('*', condition='s.age > 40'),
                WhenMatched.update('*'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a2', 'b2', 'c2'])
        self.assertEqual(out['age'], [None, 50, 60])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_not_matched_null_falls_through(self):
        target = self._create_table()

        source = pa.Table.from_pydict(
            {
                'id': pa.array([1, 2], type=pa.int32()),
                'name': ['a', 'b'],
                'age': pa.array([None, 25], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_not_matched=[
                WhenNotMatched(insert='*', condition='s.age > 20'),
                WhenNotMatched(insert='*'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2])
        self.assertEqual(out['age'], [None, 25])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_clause_no_match_skipped(self):
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
            when_matched=[
                WhenMatched.update('*', condition='s.age > 50'),
                WhenMatched.update('*', condition='s.age > 30'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['a', 'b'])
        self.assertEqual(out['age'], [10, 20])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_clause_first_wins(self):
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
                'name': ['first'],
                'age': pa.array([99], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched.update({'name': 's.name'},
                                   condition='s.age > 50'),
                WhenMatched.update({'age': 's.age'},
                                   condition='s.age > 10'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['first'])
        self.assertEqual(out['age'], [10])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_clause_duplicate_source_one_actionable(self):
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
                'age': pa.array([99, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        merge_into(
            target=target,
            source=source,
            catalog_options=self.catalog_options,
            on=['id'],
            when_matched=[
                WhenMatched.update('*', condition='s.age > 50'),
                WhenMatched.update('*', condition='s.age > 80'),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['x'])
        self.assertEqual(out['age'], [99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_clause_duplicate_both_actionable_raises(self):
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
                'age': pa.array([99, 50], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        with self.assertRaises(Exception) as ctx:
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[
                    WhenMatched.update('*', condition='s.age > 80'),
                    WhenMatched.update('*', condition='s.age > 30'),
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('multiple source rows', str(ctx.exception))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_multi_clause_duplicate_update_delete_raises(self):
        options = dict(self.de_options)
        options['deletion-vectors.enabled'] = 'true'
        target = self._create_table(options=options)
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
                'age': pa.array([99, 5], type=pa.int32()),
            },
            schema=self.pa_schema,
        )

        with self.assertRaises(Exception) as ctx:
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                when_matched=[
                    WhenMatched.update('*', condition='s.age > 50'),
                    WhenMatched.delete(condition='s.age < 10'),
                ],
                num_partitions=_TEST_NUM_PARTITIONS,
            )
        self.assertIn('multiple source rows', str(ctx.exception))

    def test_self_merge_update_literal(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update({'age': lit(99)})],
        )

        self.assertEqual(result['num_matched'], 3)
        out = self._read_sorted(target)
        self.assertEqual(out['age'], [99, 99, 99])
        self.assertEqual(out['name'], ['a', 'b', 'c'])

    def test_self_merge_update_bypasses_routing_shuffle(self):
        options = dict(self.de_options)
        options.update({
            'source.split.target-size': '1gb',
            'source.split.open-file-cost': '1b',
        })
        target = self._create_table(options=options)
        self._write(target, self._source(ids=(1, 2)))
        self._write(target, self._source(ids=(3, 4)))
        table = self.catalog.get_table(target)
        packed_splits = table.new_read_builder().new_scan().plan_for_write().splits()
        self.assertEqual(len(packed_splits), 1)

        with patch.object(
                ray.data.Dataset,
                'groupby',
                side_effect=AssertionError('routing shuffle is not allowed'),
        ):
            result, plan = self._merge_and_capture_self_merge_plan(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                when_matched=[WhenMatched.update({'age': lit(99)})],
                num_partitions=_TEST_NUM_PARTITIONS,
            )

        self.assertEqual(len(plan.file_groups), 2)
        self.assertEqual(result['num_matched'], 4)
        self.assertEqual(self._read_sorted(target)['age'], [99, 99, 99, 99])

    def test_self_merge_update_aborts_other_groups_after_failure(self):
        from pypaimon.ray import data_evolution_merge_into as merge_module
        from pypaimon.ray.data_evolution_merge_join import (
            distributed_self_merge_update_apply,
        )

        options = dict(self.de_options)
        options.update({
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        })
        target = self._create_table(options=options)
        self._write(target, self._source(ids=(1, 2)))
        self._write(target, self._source(ids=(3, 4)))

        table, source_ds, matched, not_matched, ctx = merge_module._prepare(
            target,
            target,
            self.catalog_options,
            [WhenMatched.update({'age': lit(99)})],
            [],
            ['_ROW_ID'],
        )
        snapshot = table.snapshot_manager().get_latest_snapshot()
        plan, _, _, _ = merge_module._build_datasets(
            table,
            target,
            source_ds,
            matched,
            not_matched,
            ctx,
            snapshot,
            _TEST_NUM_PARTITIONS,
            None,
        )
        self.assertGreaterEqual(len(plan.file_groups), 2)

        before = set()
        for root, _, files in os.walk(self.warehouse):
            before.update(os.path.join(root, name) for name in files)

        for data_file in plan.file_groups[-1].files:
            data_file.file_path += '.missing'

        with self.assertRaises(Exception):
            distributed_self_merge_update_apply(
                plan,
                num_partitions=_TEST_NUM_PARTITIONS,
            )

        after = set()
        for root, _, files in os.walk(self.warehouse):
            after.update(os.path.join(root, name) for name in files)
        self.assertEqual(before, after)

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_callable_assignment(self):
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

        def increment_age(rows):
            if rows.column_names != ['age', '_ROW_ID']:
                raise AssertionError(rows.column_names)
            return pc.add(rows['age'], 1)

        with patch.object(
                ray.data.Dataset,
                'groupby',
                side_effect=AssertionError('routing shuffle is not allowed'),
        ):
            result, plan = self._merge_and_capture_self_merge_plan(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                read_columns=['age'],
                when_matched=[WhenMatched.update(
                    {
                        'age': increment_age,
                        'name': lit('updated'),
                    },
                    condition='t.id IN (1, 3)',
                )],
                num_partitions=_TEST_NUM_PARTITIONS,
            )

        self.assertEqual(result['num_matched'], 2)
        self.assertEqual(
            self._read_sorted(target),
            {
                'id': [1, 2, 3],
                'name': ['updated', 'b', 'updated'],
                'age': [11, 20, 31],
            },
        )
        self.assertEqual(
            [field.name for field in plan.read_type],
            ['_ROW_ID', 'id', 'age'],
        )
        self.assertEqual(plan.callable_input_columns, ['age', '_ROW_ID'])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_callable_updates_variant_path(self):
        from pypaimon.data.generic_variant import GenericVariant
        from pypaimon.data.variant_path import variant_get, variant_replace

        variant_type = pa.struct([
            pa.field('value', pa.binary(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', variant_type),
        ])
        target = f'default.tbl_{uuid.uuid4().hex[:8]}'
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(pa_schema, options=self.de_options),
            False,
        )
        payload = GenericVariant.to_arrow_array([
            GenericVariant.from_python({'value': 1.5}),
            GenericVariant.from_python({'value': 2.5}),
        ])
        self._write(target, pa.table({
            'id': pa.array([1, 2], type=pa.int32()),
            'payload': payload,
        }, schema=pa_schema))

        def negate_value(rows):
            values = variant_get(
                rows['payload'], '$.value', pa.float64()
            )
            return variant_replace(
                rows['payload'], '$.value', pc.negate(values), strict=True
            )

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            read_columns=['payload'],
            when_matched=[WhenMatched.update(
                {'payload': negate_value}, condition='t.id = 2',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 1)
        output = self._read_sorted(target)['payload']
        decoded = [
            GenericVariant.from_arrow_struct(value).to_python()
            for value in output
        ]
        self.assertEqual(decoded, [{'value': 1.5}, {'value': -2.5}])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_no_match_does_not_invoke_callable(self):
        target = self._create_table()
        self._write(target, self._source())

        def should_not_run(_rows):
            raise AssertionError("Callable must not run without matched rows")

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            read_columns=['age'],
            when_matched=[WhenMatched.update(
                {'age': should_not_run}, condition='t.id = 99',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 0)
        self.assertEqual(self._read_sorted(target)['age'], [10])

    def test_self_merge_callable_validation(self):
        target = self._create_table()

        with self.assertRaisesRegex(
                ValueError, 'Callable SET values require read_columns'):
            merge_into(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                when_matched=[WhenMatched.update({
                    'age': lambda rows: rows['age'],
                })],
            )

        with self.assertRaisesRegex(
                ValueError, 'read_columns requires a callable SET value'):
            merge_into(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                read_columns=['age'],
                when_matched=[WhenMatched.update({'age': lit(99)})],
            )

        with self.assertRaisesRegex(
                ValueError, "Read column 'missing' is not in target"):
            merge_into(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                read_columns=['missing'],
                when_matched=[WhenMatched.update({
                    'age': lambda rows: rows['missing'],
                })],
            )

        source = self._create_table()
        with self.assertRaisesRegex(
                TypeError, 'callables are only supported for self-merge'):
            merge_into(
                target=target,
                source=source,
                catalog_options=self.catalog_options,
                on=['id'],
                read_columns=['age'],
                when_matched=[WhenMatched.update({
                    'age': lambda rows: rows['age'],
                })],
            )

    def test_self_merge_callable_rejects_invalid_result(self):
        from pypaimon.ray.data_evolution_merge_transform import (
            _resolve_spec_array,
        )

        batch = pa.table({'t.age': pa.array([10], type=pa.int32())})
        callable_input = pa.table({
            'age': pa.array([10], type=pa.int32()),
        })

        with self.assertRaisesRegex(
                ValueError, 'must return a pyarrow.Array'):
            _resolve_spec_array(
                lambda rows: rows['age'].to_pylist(),
                batch,
                set(batch.column_names),
                [],
                pa.int32(),
                callable_input=callable_input,
            )

        with self.assertRaisesRegex(
                ValueError, 'length must match matched row count'):
            _resolve_spec_array(
                lambda _rows: pa.array([], type=pa.int32()),
                batch,
                set(batch.column_names),
                [],
                pa.int32(),
                callable_input=callable_input,
            )

    def test_self_merge_callable_preserves_chunked_result(self):
        from pypaimon.ray.data_evolution_merge_transform import (
            _resolve_spec_array,
        )

        batch = pa.table({'t.age': pa.array([10, 20], type=pa.int32())})
        callable_input = pa.table({
            'age': pa.array([10, 20], type=pa.int32()),
        })

        def chunked_result(_rows):
            return pa.chunked_array([
                pa.array([10], type=pa.int64()),
                pa.array([20], type=pa.int64()),
            ])

        result = _resolve_spec_array(
            chunked_result,
            batch,
            set(batch.column_names),
            [],
            pa.int32(),
            callable_input=callable_input,
        )
        self.assertIsInstance(result, pa.ChunkedArray)
        self.assertEqual(result.num_chunks, 2)
        self.assertEqual(result.type, pa.int32())

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_condition_pushes_down_predicate(self):
        from pypaimon.common.options.core_options import (
            CoreOptions, GlobalIndexSearchMode,
        )

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
        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'age': lit(99)}, condition='t.id IN (1, 3)',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 2)
        self.assertEqual(
            self._read_sorted(target),
            {
                'id': [1, 2, 3],
                'name': ['a', 'b', 'c'],
                'age': [99, 20, 99],
            },
        )
        predicate = plan.predicate
        self.assertEqual(predicate.method, 'in')
        self.assertEqual(predicate.field, 'id')
        self.assertEqual(predicate.literals, [1, 3])
        self.assertEqual(
            plan.scan_table.table_schema.options.get(
                CoreOptions.SCALAR_INDEX_SEARCH_MODE.key()),
            GlobalIndexSearchMode.FULL.value,
        )
        self.assertEqual(
            plan.scan_table.table_schema.fields,
            plan.table.table_schema.fields,
        )
        self.assertEqual(
            plan.scan_table.table_schema.id,
            plan.table.table_schema.id,
        )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_pushdown_handles_evolved_file_groups(self):
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange

        options = dict(self.de_options)
        options.update({
            'global-index.enabled': 'true',
            'bucket': '-1',
        })
        target = self._create_table(options=options)
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

        table = self.catalog.get_table(target)
        self.assertGreater(table.create_global_index('id'), 0)

        # This append is intentionally not covered by the existing index.
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([3, 4], type=pa.int32()),
                    'name': ['c', 'd'],
                    'age': pa.array([30, 40], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        from pypaimon.index.index_file_handler import IndexFileHandler
        snapshot = table.snapshot_manager().get_latest_snapshot()
        indexed_ranges = {
            (
                entry.index_file.global_index_meta.row_range_start,
                entry.index_file.global_index_meta.row_range_end,
            )
            for entry in IndexFileHandler(table).scan(snapshot)
        }
        self.assertEqual({(0, 1)}, indexed_ranges)

        first_result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'age': lit(99)}, condition='t.id IN (2, 3)',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )
        self.assertEqual(first_result['num_matched'], 2)

        self.catalog.alter_table(
            target,
            [SchemaChange.add_column('note', AtomicType('STRING'))],
            False,
        )

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'name': lit('updated')},
                condition='t.id IN (1, 4) AND t.note IS NULL',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 2)
        self.assertEqual(
            self._read_sorted(target),
            {
                'id': [1, 2, 3, 4],
                'name': ['updated', 'b', 'c', 'updated'],
                'age': [10, 99, 99, 40],
                'note': [None, None, None, None],
            },
        )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_delete_condition_pushes_down_predicate(self):
        from pypaimon.ray.ray_paimon import read_paimon as real_read_paimon

        options = dict(self.de_options)
        options['deletion-vectors.enabled'] = 'true'
        target = self._create_table(options=options)
        self._write(target, self._source(ids=(1, 2, 3)))

        with patch(
                'pypaimon.ray.ray_paimon.read_paimon',
                wraps=real_read_paimon,
        ) as mock_read:
            result = merge_into(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                when_matched=[WhenMatched.delete(condition='s.id = 2')],
                num_partitions=_TEST_NUM_PARTITIONS,
            )

        self.assertEqual(result['num_matched'], 1)
        self.assertEqual(self._read_sorted(target)['id'], [1, 3])
        predicate = mock_read.call_args[1]['filter']
        self.assertEqual((predicate.method, predicate.field, predicate.literals),
                         ('equal', 'id', [2]))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_multiple_conditions_push_down_or(self):
        target = self._create_table()
        self._write(target, self._source(ids=(1, 2, 3)))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update(
                    {'age': lit(11)}, condition='t.id = 1',
                ),
                WhenMatched.update(
                    {'age': lit(33)}, condition='s.id = 3',
                ),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 2)
        predicate = plan.predicate
        self.assertEqual(predicate.method, 'or')
        self.assertEqual(
            [(p.field, p.literals) for p in predicate.literals],
            [('id', [1]), ('id', [3])],
        )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_unconditional_clause_disables_pushdown(self):
        target = self._create_table()
        self._write(target, self._source(ids=(1, 2, 3)))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update(
                    {'age': lit(11)}, condition='t.id = 1',
                ),
                WhenMatched.update({'age': lit(99)}),
            ],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 3)
        self.assertIsNone(plan.predicate)

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_column_comparison_fails_open(self):
        target = self._create_table()
        self._write(target, self._source(ids=(1, 2, 3)))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'name': lit('same')}, condition='t.age = s.age',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 3)
        self.assertIsNone(plan.predicate)
        self.assertEqual(self._read_sorted(target)['name'],
                         ['same', 'same', 'same'])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_pushdown_preserves_field_case(self):
        case_schema = pa.schema([
            ('UserID', pa.int32()),
            ('Value', pa.int32()),
        ])
        target = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            case_schema, options=self.de_options,
        )
        self.catalog.create_table(target, schema, False)
        self._write(target, pa.Table.from_pydict(
            {'UserID': [1, 2, 3], 'Value': [10, 20, 30]},
            schema=case_schema,
        ))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'Value': lit(99)},
                condition='t.UserID IN (1, 3)',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 2)
        predicate = plan.predicate
        self.assertEqual(predicate.field, 'UserID')
        table = self.catalog.get_table(target)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        out = read_builder.new_read().to_arrow(splits).sort_by('UserID')
        self.assertEqual(out['Value'].to_pylist(), [99, 20, 99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_date_condition_fails_open(self):
        date_schema = pa.schema([
            ('id', pa.int32()),
            ('event_date', pa.date32()),
            ('value', pa.int32()),
        ])
        target = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            date_schema, options=self.de_options,
        )
        self.catalog.create_table(target, schema, False)
        self._write(target, pa.Table.from_pydict({
            'id': [1, 2],
            'event_date': [
                datetime.date(2026, 1, 1),
                datetime.date(2026, 1, 2),
            ],
            'value': [10, 20],
        }, schema=date_schema))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'value': lit(99)},
                condition="t.event_date = '2026-01-01'",
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 1)
        self.assertIsNone(plan.predicate)
        table = self.catalog.get_table(target)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        out = read_builder.new_read().to_arrow(splits).sort_by('id')
        self.assertEqual(out['value'].to_pylist(), [99, 20])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_double_condition_fails_open(self):
        double_schema = pa.schema([
            ('id', pa.int32()),
            ('metric', pa.float64()),
            ('value', pa.int32()),
        ])
        target = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            double_schema, options=self.de_options,
        )
        self.catalog.create_table(target, schema, False)
        self._write(target, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'metric': [float('nan'), -1.0, 1.0],
            'value': [10, 20, 30],
        }, schema=double_schema))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'value': lit(99)}, condition='t.metric > 0',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 2)
        self.assertIsNone(plan.predicate)
        table = self.catalog.get_table(target)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        out = read_builder.new_read().to_arrow(splits).sort_by('id')
        self.assertEqual(out['value'].to_pylist(), [99, 20, 99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_like_condition_fails_open(self):
        string_schema = pa.schema([
            ('id', pa.int32()),
            ('text', pa.string()),
            ('value', pa.int32()),
        ])
        target = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            string_schema, options=self.de_options,
        )
        self.catalog.create_table(target, schema, False)
        self._write(target, pa.Table.from_pydict({
            'id': [0, 1, 2, 3, 4],
            'text': ['n', '\\n', '\n', 'line\nbreak', 'linebreak'],
            'value': [10, 20, 30, 40, 50],
        }, schema=string_schema))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'value': lit(99)}, condition=r"t.text LIKE '%\n%'",
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 4)
        self.assertIsNone(plan.predicate)
        table = self.catalog.get_table(target)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        out = read_builder.new_read().to_arrow(splits).sort_by('id')
        self.assertEqual(out['value'].to_pylist(), [99, 99, 30, 99, 99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_out_of_range_integer_fails_open(self):
        int_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.int32()),
        ])
        target = f'default.tbl_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(
            int_schema, options=self.de_options,
        )
        self.catalog.create_table(target, schema, False)
        self._write(target, pa.Table.from_pydict({
            'id': [-1, 0, 1],
            'value': [10, 20, 30],
        }, schema=int_schema))

        result, plan = self._merge_and_capture_self_merge_plan(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'value': lit(99)},
                condition='t.id < 9223372036854775808',
            )],
            num_partitions=_TEST_NUM_PARTITIONS,
        )

        self.assertEqual(result['num_matched'], 3)
        self.assertIsNone(plan.predicate)
        table = self.catalog.get_table(target)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        out = read_builder.new_read().to_arrow(splits).sort_by('id')
        self.assertEqual(out['value'].to_pylist(), [99, 99, 99])

    def test_self_merge_update_star(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update('*')],
        )

        self.assertEqual(result['num_matched'], 3)
        out = self._read_sorted(target)
        self.assertEqual(out['id'], [1, 2, 3])
        self.assertEqual(out['name'], ['a', 'b', 'c'])
        self.assertEqual(out['age'], [10, 20, 30])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_with_condition(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update({'age': lit(99)}, condition='t.age > 15')],
        )

        self.assertEqual(result['num_matched'], 2)
        out = self._read_sorted(target)
        self.assertEqual(out['age'], [10, 99, 99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_with_source_condition(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update(
                {'name': lit('updated')},
                condition='s.age > 15',
            )],
        )

        self.assertEqual(result['num_matched'], 2)
        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['a', 'updated', 'updated'])
        self.assertEqual(out['age'], [10, 20, 30])

    def test_self_merge_rejects_not_matched(self):
        target = self._create_table()
        self._write(target, self._source(ids=(1,)))

        with self.assertRaises(ValueError) as ctx:
            merge_into(
                target=target,
                source=target,
                catalog_options=self.catalog_options,
                on=['_ROW_ID'],
                when_matched=[WhenMatched.update('*')],
                when_not_matched=[WhenNotMatched(insert='*')],
            )
        self.assertIn('Self-merge', str(ctx.exception))

    def test_self_merge_partial_set(self):
        target = self._create_table()
        self._write(
            target,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['old_a', 'old_b'],
                    'age': pa.array([10, 20], type=pa.int32()),
                },
                schema=self.pa_schema,
            ),
        )

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update({'name': lit('updated')})],
        )

        self.assertEqual(result['num_matched'], 2)
        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['updated', 'updated'])
        self.assertEqual(out['age'], [10, 20])

    def test_self_merge_source_col_row_id(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[WhenMatched.update({'name': source_col('_ROW_ID')})],
        )

        self.assertEqual(result['num_matched'], 2)
        out = self._read_sorted(target)
        for v in out['name']:
            self.assertTrue(int(v) >= 0)

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_condition_on_row_id(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update(
                    {'age': lit(99)},
                    condition='s._ROW_ID >= 0',
                ),
            ],
        )

        self.assertEqual(result['num_matched'], 3)
        out = self._read_sorted(target)
        self.assertEqual(out['age'], [99, 99, 99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_condition_on_target_row_id(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update(
                    {'age': lit(99)},
                    condition='t._ROW_ID >= 0',
                ),
            ],
        )

        self.assertEqual(result['num_matched'], 3)
        out = self._read_sorted(target)
        self.assertEqual(out['age'], [99, 99, 99])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_multi_clause_fall_through(self):
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

        result = merge_into(
            target=target,
            source=target,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update({'name': lit('old')}, condition='s.age <= 10'),
                WhenMatched.update({'name': lit('young')}, condition='s.age <= 20'),
                WhenMatched.update({'name': lit('senior')}),
            ],
        )

        self.assertEqual(result['num_matched'], 3)
        out = self._read_sorted(target)
        self.assertEqual(out['name'], ['old', 'young', 'senior'])
        self.assertEqual(out['age'], [10, 20, 30])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_blob_source_condition(self):
        blob_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('picture', pa.large_binary()),
        ])
        tbl_name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(blob_schema, options=self.de_options)
        self.catalog.create_table(tbl_name, s, False)

        self._write(
            tbl_name,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1, 2], type=pa.int32()),
                    'name': ['a', 'b'],
                    'picture': [None, None],
                },
                schema=blob_schema,
            ),
        )

        result = merge_into(
            target=tbl_name,
            source=tbl_name,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update(
                    {'name': lit('updated')},
                    condition='s.picture IS NULL',
                ),
            ],
        )

        self.assertEqual(result['num_matched'], 2)
        out = self._read_sorted(tbl_name)
        self.assertEqual(out['name'], ['updated', 'updated'])

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_blob_target_condition_allowed(self):
        blob_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('picture', pa.large_binary()),
        ])
        tbl_name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(blob_schema, options=self.de_options)
        self.catalog.create_table(tbl_name, s, False)

        self._write(
            tbl_name,
            pa.Table.from_pydict(
                {
                    'id': pa.array([1], type=pa.int32()),
                    'name': ['a'],
                    'picture': [None],
                },
                schema=blob_schema,
            ),
        )

        result = merge_into(
            target=tbl_name,
            source=tbl_name,
            catalog_options=self.catalog_options,
            on=['_ROW_ID'],
            when_matched=[
                WhenMatched.update(
                    {'name': lit('x')},
                    condition='t.picture IS NOT NULL',
                ),
            ],
        )

        self.assertEqual(result['num_matched'], 0)
        out = self._read_sorted(tbl_name)
        self.assertEqual(out['name'], ['a'])


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

    def test_matched_source_projection_prunes_unneeded_cols(self):
        from pypaimon.ray.data_evolution_merge_join import (
            _resolve_source_projection,
        )
        from pypaimon.ray.data_evolution_merge_transform import (
            LiteralValue,
            SourceColumnRef,
            TargetColumnRef,
        )

        cols = _resolve_source_projection(
            [
                self._clause(
                    {
                        'age': SourceColumnRef('id'),
                        'name': TargetColumnRef('name'),
                        'note': LiteralValue('literal'),
                    },
                    condition="s.status = 't.fake' AND s.score > t.score",
                )
            ],
            ['uid'],
            ['uid', 'id', 'name', 'status', 'score', 'payload'],
        )
        self.assertEqual(['uid', 'id', 'status', 'score'], cols)

    def test_literal_update_source_projection_keeps_only_join_key(self):
        from pypaimon.ray.data_evolution_merge_join import (
            _resolve_source_projection,
        )
        from pypaimon.ray.data_evolution_merge_transform import LiteralValue

        cols = _resolve_source_projection(
            [self._clause({'name': LiteralValue('updated')})],
            ['id'],
            ['id', 'name', 'age', 'payload'],
        )
        self.assertEqual(['id'], cols)

    def test_matched_update_selects_needed_source_cols(self):
        from pypaimon.ray.data_evolution_merge_join import build_matched_update_ds
        from pypaimon.ray.data_evolution_merge_transform import SourceColumnRef

        source_ds = Mock()
        source_ds.schema.return_value = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('payload', pa.string()),
        ])
        selected_ds = Mock()
        source_renamed = Mock()
        source_ds.select_columns.return_value = selected_ds
        selected_ds.rename_columns.return_value = source_renamed

        target_ds = Mock()
        target_ds.schema.return_value = pa.schema([
            ('_ROW_ID', pa.int64()),
            ('id', pa.int32()),
        ])
        target_renamed = Mock()
        joined = Mock()
        result = object()
        target_ds.rename_columns.return_value = target_renamed
        target_renamed.join.return_value = joined
        joined.map_batches.return_value = result

        with patch(
                'pypaimon.ray.ray_paimon.read_paimon',
                return_value=target_ds,
        ):
            out = build_matched_update_ds(
                target_identifier='default.target',
                source_ds=source_ds,
                target_on=['id'],
                source_on=['id'],
                clauses=[self._clause({'name': SourceColumnRef('name')})],
                target_field_names=['id', 'name'],
                target_pa_schema=pa.schema([
                    ('id', pa.int32()),
                    ('name', pa.string()),
                ]),
                update_cols=['name'],
                catalog_options={'warehouse': '/tmp/warehouse'},
                num_partitions=1,
                resolve_target_projection=lambda *args: ['id'],
            )

        self.assertIs(out, result)
        source_ds.select_columns.assert_called_once_with(['id', 'name'])
        selected_ds.rename_columns.assert_called_once_with({
            'id': 's.id',
            'name': 's.name',
        })

    def test_not_matched_insert_selects_needed_source_cols(self):
        from pypaimon.ray.data_evolution_merge_join import (
            build_not_matched_insert_ds,
        )
        from pypaimon.ray.data_evolution_merge_transform import SourceColumnRef

        source_ds = Mock()
        source_ds.schema.return_value = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('payload', pa.string()),
        ])
        selected_ds = Mock()
        source_renamed = Mock()
        result = object()
        source_ds.select_columns.return_value = selected_ds
        selected_ds.rename_columns.return_value = source_renamed
        source_renamed.map_batches.return_value = result

        out = build_not_matched_insert_ds(
            target_identifier='default.target',
            source_ds=source_ds,
            target_on=['id'],
            source_on=['id'],
            clauses=[self._clause({'name': SourceColumnRef('name')})],
            target_field_names=['id', 'name'],
            target_pa_schema=pa.schema([
                ('id', pa.int32()),
                ('name', pa.string()),
            ]),
            catalog_options={'warehouse': '/tmp/warehouse'},
            num_partitions=1,
            target_empty=True,
        )

        self.assertIs(out, result)
        source_ds.select_columns.assert_called_once_with(['id', 'name'])
        selected_ds.rename_columns.assert_called_once_with({
            'id': 's.id',
            'name': 's.name',
        })


class MergeConditionUnitTest(unittest.TestCase):

    @staticmethod
    def _predicate_fields():
        return Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('MixedCase', pa.int32()),
            ('flag', pa.bool_()),
            ('event_date', pa.date32()),
            ('event_time', pa.timestamp('us')),
            ('amount', pa.decimal128(30, 2)),
            ('float_value', pa.float32()),
            ('double_value', pa.float64()),
        ])).fields

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
    def test_parse_simple_self_merge_predicate(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        predicate = try_parse_self_merge_predicate(
            't.id IN (1, 3) AND s.name = \'s.literal\'',
            self._predicate_fields(),
        )
        self.assertEqual(predicate.method, 'and')
        self.assertEqual(
            [(p.field, p.literals) for p in predicate.literals],
            [('id', [1, 3]), ('name', ['s.literal'])],
        )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_parse_self_merge_predicate_ast_subset(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        cases = [
            ('t.id != 1', 'notEqual', 'id', [1]),
            ('t.id NOT IN (1, 2)', 'notIn', 'id', [1, 2]),
            ('t.id BETWEEN 1 AND 2', 'between', 'id', [1, 2]),
            ('t.id NOT BETWEEN 1 AND 2', 'notBetween', 'id', [1, 2]),
            ('t.event_date IS NULL', 'isNull', 'event_date', None),
            ('t.event_date IS NOT NULL', 'isNotNull', 'event_date', None),
        ]
        for condition, method, field, literals in cases:
            with self.subTest(condition=condition):
                predicate = try_parse_self_merge_predicate(
                    condition, self._predicate_fields(),
                )
                self.assertEqual(
                    (predicate.method, predicate.field, predicate.literals),
                    (method, field, literals),
                )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_predicate_preserves_field_case(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        predicate = try_parse_self_merge_predicate(
            't.MixedCase = 1', self._predicate_fields(),
        )
        self.assertEqual(predicate.field, 'MixedCase')

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_column_comparison_is_not_pushed_down(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        self.assertIsNone(try_parse_self_merge_predicate(
            't.id = s.id', self._predicate_fields(),
        ))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_function_is_not_pushed_down(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        self.assertIsNone(try_parse_self_merge_predicate(
            'abs(t.id) > 1', self._predicate_fields(),
        ))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_predicate_parse_failure_is_logged(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        with self.assertLogs(
                'pypaimon.ray.merge_condition', level='DEBUG') as logs:
            self.assertIsNone(try_parse_self_merge_predicate(
                't.id =', self._predicate_fields(),
            ))
        self.assertIn('Unable to push down', '\n'.join(logs.output))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_unsafe_literal_types_fail_open(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        conditions = [
            "t.event_date = '2026-01-01'",
            "t.event_time = '2026-01-01 01:02:03'",
            't.amount = 12345678901234567890.12',
            't.float_value > 0',
            't.double_value > 0',
        ]
        for condition in conditions:
            with self.subTest(condition=condition):
                self.assertIsNone(try_parse_self_merge_predicate(
                    condition, self._predicate_fields(),
                ))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_invalid_literals_fail_open(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        conditions = [
            "t.flag = 'true'",
            't.name = bare_value',
            't.name IN (NULL)',
        ]
        for condition in conditions:
            with self.subTest(condition=condition):
                self.assertIsNone(try_parse_self_merge_predicate(
                    condition, self._predicate_fields(),
                ))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_boolean_literal_is_pushed_down(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        predicate = try_parse_self_merge_predicate(
            't.flag = TRUE', self._predicate_fields(),
        )
        self.assertEqual((predicate.field, predicate.literals),
                         ('flag', [True]))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_like_condition_is_not_pushed_down(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        self.assertIsNone(try_parse_self_merge_predicate(
            r"t.name LIKE '%\n%'", self._predicate_fields(),
        ))

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_self_merge_out_of_range_integers_fail_open(self):
        from pypaimon.ray.merge_condition import (
            try_parse_self_merge_predicate,
        )
        conditions = [
            't.id < 9223372036854775808',
            't.id > -9223372036854775809',
            't.id IN (1, 9223372036854775808)',
            't.id BETWEEN -9223372036854775809 AND 1',
        ]
        for condition in conditions:
            with self.subTest(condition=condition):
                self.assertIsNone(try_parse_self_merge_predicate(
                    condition, self._predicate_fields(),
                ))

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
