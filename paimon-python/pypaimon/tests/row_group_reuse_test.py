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

import base64
import os
import subprocess
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.execution.python.write.table_update_by_row_id import PythonTableUpdateByRowId
from pypaimon.tests.data_evolution_test_helpers import BatchModeMixin, DataEvolutionTestBase


@unittest.skipUnless(os.path.isfile(os.environ.get("PYPAIMON_ROW_GROUP_SPLICE_BIN", "")),
                     "row-group splice helper is not installed")
class RowGroupReuseTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):
    def _update(self, table, data, columns):
        builder = table.new_batch_write_builder()
        messages = (builder.new_update().with_update_type(columns)
                    .update_by_arrow_with_row_id(data))
        commit = builder.new_commit()
        commit.commit(messages)
        commit.close()
        return messages

    def test_sparse_field_update_reuses_untouched_groups(self):
        options = dict(self.table_options)
        options.update({
            'file.block-size': '1kb',
            'parquet.write-page-index.enabled': 'true',
            'data-evolution.row-group-reuse.enabled': 'true',
        })
        table = self._create_table(options=options)
        rows = 100
        self._write_arrow(table, pa.table({
            'id': pa.array(range(rows), pa.int32()),
            'name': pa.array(['name-{:03d}'.format(i) for i in range(rows)]),
            'age': pa.array(range(rows), pa.int32()),
            'city': pa.array(['city-{:03d}'.format(i) for i in range(rows)]),
        }, schema=self.pa_schema))
        all_values = pa.table({
            '_ROW_ID': pa.array(range(rows), pa.int64()),
            'name': ['field-{:03d}'.format(i) for i in range(rows)],
            'city': ['city-{:03d}'.format(i) for i in range(rows)],
        })
        self._update(table, all_values, ['name', 'city'])
        sparse = pa.table({
            '_ROW_ID': pa.array([3, 72], pa.int64()),
            'name': ['revised-3', 'revised-72'],
            'city': ['changed-3', 'changed-72'],
        })
        binary = os.environ['PYPAIMON_ROW_GROUP_SPLICE_BIN']
        with mock.patch.dict(os.environ, {'PYPAIMON_ROW_GROUP_SPLICE_BIN': binary}):
            with mock.patch(
                    'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                    wraps=subprocess.run) as spliced:
                messages = self._update(table, sparse, ['name', 'city'])
        self.assertEqual(spliced.call_count, 2 if len(messages) == 2 else 1)
        self.assertEqual(100, messages[0].new_files[0].row_count)
        output_file = LocalFileIO()._to_file(messages[0].new_files[0].file_path)
        metadata = pq.ParquetFile(output_file).metadata
        self.assertGreater(metadata.num_row_groups, 1)
        self.assertTrue(all(metadata.row_group(i).column(0).has_column_index
                            for i in range(metadata.num_row_groups)))
        actual = self._read_all(table).sort_by('id')
        self.assertEqual('revised-3', actual['name'][3].as_py())
        self.assertEqual('revised-72', actual['name'][72].as_py())
        self.assertEqual('changed-3', actual['city'][3].as_py())
        self.assertEqual('changed-72', actual['city'][72].as_py())
        self.assertEqual('field-004', actual['name'][4].as_py())
        self.assertEqual(4, actual['age'][4].as_py())

    def test_oversized_source_group_is_rewritten_in_batches(self):
        options = dict(self.table_options)
        options.update({
            'file.block-size': '128mb',
            'data-evolution.row-group-reuse.enabled': 'true',
        })
        table = self._create_table(options=options)
        rows = 1025
        payloads = [base64.b64encode(os.urandom(48 * 1024)).decode('ascii')
                    for _ in range(rows)]
        self._write_arrow(table, pa.table({
            'id': pa.array(range(rows), pa.int32()),
            'name': payloads,
            'age': pa.array(range(rows), pa.int32()),
            'city': ['city'] * rows,
        }, schema=self.pa_schema))
        baseline = self._update(table, pa.table({
            '_ROW_ID': pa.array(range(rows), pa.int64()),
            'name': payloads,
        }), ['name'])
        source_file = LocalFileIO()._to_file(baseline[0].new_files[0].file_path)
        source_meta = pq.read_metadata(source_file)
        self.assertEqual(1, source_meta.num_row_groups)
        self.assertGreater(source_meta.row_group(0).total_byte_size, 64 * 1024 ** 2)

        with mock.patch(
                'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                wraps=subprocess.run) as spliced:
            messages = self._update(table, pa.table({
                '_ROW_ID': pa.array([rows - 1, 0], pa.int64()),
                'name': ['last', 'first'],
            }), ['name'])
        self.assertEqual(1, spliced.call_count)
        output_file = LocalFileIO()._to_file(messages[0].new_files[0].file_path)
        self.assertGreater(pq.read_metadata(output_file).num_row_groups, 1)
        names = self._read_all(table).sort_by('id')['name']
        self.assertEqual('first', names[0].as_py())
        self.assertEqual(payloads[1], names[1].as_py())
        self.assertEqual('last', names[rows - 1].as_py())

    def test_multiple_column_sources_share_one_commit(self):
        options = dict(self.table_options)
        options['data-evolution.row-group-reuse.enabled'] = 'true'
        table = self._create_table(options=options)
        self._write_arrow(table, pa.table({
            'id': pa.array([1, 2], pa.int32()),
            'name': ['a', 'b'],
            'age': pa.array([10, 20], pa.int32()),
            'city': ['x', 'y'],
        }, schema=self.pa_schema))
        self._update(table, pa.table({'_ROW_ID': [0, 1], 'name': ['c', 'd']}), ['name'])
        self._update(table, pa.table({'_ROW_ID': [0, 1], 'city': ['z', 'w']}), ['city'])
        before_snapshot = table.snapshot_manager().get_latest_snapshot().id
        with mock.patch(
                'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                wraps=subprocess.run) as spliced:
            builder = table.new_batch_write_builder()
            messages = (builder.new_update().with_update_type(['name', 'city'])
                        .upsert_by_arrow_with_key(pa.table({
                            'id': pa.array([2], pa.int32()),
                            'name': ['final'], 'city': ['final-city'],
                        }), ['id']))
            commit = builder.new_commit()
            commit.commit(messages)
            commit.close()
        self.assertEqual(2, spliced.call_count)
        self.assertEqual(2, len(messages))
        self.assertEqual(before_snapshot + 1, table.snapshot_manager().get_latest_snapshot().id)
        actual = self._read_all(table).sort_by('id')
        self.assertEqual('final', actual['name'][1].as_py())
        self.assertEqual('final-city', actual['city'][1].as_py())

    def test_ten_field_columns_and_supplemental_share_one_commit(self):
        field = ['field_{}'.format(i) for i in range(10)]
        schema = pa.schema([('id', pa.int32())] +
                           [(name, pa.string()) for name in field + ['supplemental']])
        options = dict(self.table_options)
        options['data-evolution.row-group-reuse.enabled'] = 'true'
        table = self._create_table(pa_schema=schema, options=options)
        initial = {'id': pa.array(range(10), pa.int32())}
        initial.update({name: ['initial'] * 10 for name in field + ['supplemental']})
        self._write_arrow(table, pa.table(initial, schema=schema))
        field_data = {'_ROW_ID': pa.array(range(10), pa.int64())}
        field_data.update({name: ['old-{}'.format(name)] * 10 for name in field})
        self._update(table, pa.table(field_data), field)
        self._update(table, pa.table({
            '_ROW_ID': pa.array(range(10), pa.int64()),
            'supplemental': ['old-supplemental'] * 10,
        }), ['supplemental'])

        changed = {'id': pa.array([5], pa.int32()), 'supplemental': ['new-supplemental']}
        changed.update({name: ['new-{}'.format(name)] for name in field})
        before_snapshot = table.snapshot_manager().get_latest_snapshot().id
        with mock.patch(
                'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                wraps=subprocess.run) as spliced:
            builder = table.new_batch_write_builder()
            messages = (builder.new_update().with_update_type(field + ['supplemental'])
                        .upsert_by_arrow_with_key(pa.table(changed), ['id']))
            commit = builder.new_commit()
            commit.commit(messages)
            commit.close()
        self.assertEqual(2, spliced.call_count)
        self.assertEqual(2, len(messages))
        self.assertEqual(before_snapshot + 1, table.snapshot_manager().get_latest_snapshot().id)
        actual = self._read_all(table).sort_by('id')
        for name in field:
            self.assertEqual('new-{}'.format(name), actual[name][5].as_py())
            self.assertEqual('old-{}'.format(name), actual[name][4].as_py())
        self.assertEqual('new-supplemental', actual['supplemental'][5].as_py())
        self.assertEqual('old-supplemental', actual['supplemental'][4].as_py())

        owned_paths = []
        original = PythonTableUpdateByRowId._write_group_reusing_row_groups
        subgroup_calls = [0]

        def fail_second_group(updater, partition, first_row_id, data, columns):
            if len(columns) < len(field) + 1:
                subgroup_calls[0] += 1
                if subgroup_calls[0] == 2:
                    owned_paths.extend(file.physical_path()
                                       for message in updater.commit_messages
                                       for file in message.new_files)
                    raise RuntimeError('injected second-group failure')
            return original(updater, partition, first_row_id, data, columns)

        before_failure = table.snapshot_manager().get_latest_snapshot().id
        with mock.patch.object(PythonTableUpdateByRowId, '_write_group_reusing_row_groups',
                               fail_second_group):
            builder = table.new_batch_write_builder()
            with self.assertRaisesRegex(RuntimeError, 'second-group failure'):
                (builder.new_update().with_update_type(field + ['supplemental'])
                 .upsert_by_arrow_with_key(pa.table(changed), ['id']))
        self.assertEqual(before_failure, table.snapshot_manager().get_latest_snapshot().id)
        self.assertTrue(owned_paths)
        self.assertTrue(all(not os.path.exists(LocalFileIO()._to_file(path))
                            for path in owned_paths))

    def test_newer_full_column_update_keeps_latest_values(self):
        options = dict(self.table_options)
        options['data-evolution.row-group-reuse.enabled'] = 'true'
        table = self._create_table(options=options)
        self._write_arrow(table, pa.table({
            'id': pa.array([1, 2], pa.int32()), 'name': ['a', 'b'],
            'age': pa.array([10, 20], pa.int32()), 'city': ['x', 'y'],
        }, schema=self.pa_schema))
        self._update(table, pa.table({
            '_ROW_ID': [0, 1], 'name': ['first', 'first-2'],
            'city': ['first-city', 'first-city-2'],
        }), ['name', 'city'])
        self._update(table, pa.table({
            '_ROW_ID': [0, 1], 'id': pa.array([1, 2], pa.int32()),
            'name': ['newer', 'newer-2'], 'age': pa.array([10, 20], pa.int32()),
            'city': ['newer-city', 'newer-city-2'],
        }), ['id', 'name', 'age', 'city'])
        with mock.patch(
                'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                wraps=subprocess.run) as spliced:
            self._update(table, pa.table({
                '_ROW_ID': [1], 'name': ['final'], 'city': ['final-city'],
            }), ['name', 'city'])
        self.assertEqual(1, spliced.call_count)
        actual = self._read_all(table).sort_by('id')
        self.assertEqual('newer', actual['name'][0].as_py())
        self.assertEqual('final', actual['name'][1].as_py())

    def test_splice_failure_falls_back_to_streaming(self):
        options = dict(self.table_options)
        options['data-evolution.row-group-reuse.enabled'] = 'true'
        table = self._create_table(options=options)
        self._write_arrow(table, pa.table({
            'id': pa.array([1, 2], pa.int32()), 'name': ['a', 'b'],
            'age': pa.array([10, 20], pa.int32()), 'city': ['x', 'y'],
        }, schema=self.pa_schema))
        self._update(table, pa.table({'_ROW_ID': [0, 1], 'name': ['c', 'd']}), ['name'])
        with mock.patch(
                'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                side_effect=subprocess.CalledProcessError(1, 'splice')) as spliced:
            self._update(table, pa.table({'_ROW_ID': [1], 'name': ['final']}), ['name'])
        self.assertEqual(1, spliced.call_count)
        self.assertEqual('final', self._read_all(table).sort_by('id')['name'][1].as_py())

    def test_conflict_detection_is_unchanged(self):
        options = dict(self.table_options)
        options['data-evolution.row-group-reuse.enabled'] = 'true'
        table = self._create_table(options=options)
        self._write_arrow(table, pa.table({
            'id': pa.array([1, 2], pa.int32()), 'name': ['a', 'b'],
            'age': pa.array([10, 20], pa.int32()), 'city': ['x', 'y'],
        }, schema=self.pa_schema))
        self._update(table, pa.table({'_ROW_ID': [0, 1], 'name': ['c', 'd']}), ['name'])
        stale_builder = table.new_batch_write_builder()
        stale_messages = (stale_builder.new_update().with_update_type(['name'])
                          .update_by_arrow_with_row_id(pa.table({'_ROW_ID': [1], 'name': ['stale']})))
        self._update(table, pa.table({'_ROW_ID': [1], 'name': ['current']}), ['name'])
        stale_commit = stale_builder.new_commit()
        with self.assertRaisesRegex(RuntimeError, "multiple 'MERGE INTO'"):
            stale_commit.commit(stale_messages)
        stale_commit.close()
        self.assertEqual('current', self._read_all(table).sort_by('id')['name'][1].as_py())

    def test_null_and_map_values_survive_reuse(self):
        schema = pa.schema([
            ('id', pa.int32()), ('payload_text', pa.string()),
            ('payload_map', pa.map_(pa.string(), pa.string())),
            ('payload_time', pa.timestamp('ms')),
        ])
        options = dict(self.table_options)
        options.update({'file.block-size': '1kb',
                        'data-evolution.row-group-reuse.enabled': 'true'})
        table = self._create_table(pa_schema=schema, options=options)
        tags_type = schema.field('payload_map').type
        initial = pa.Table.from_arrays([
            pa.array(range(40), pa.int32()),
            pa.array(['initial-{}'.format(i) for i in range(40)]),
            pa.array([[('k', str(i))] for i in range(40)], type=tags_type),
            pa.array(range(40), type=pa.timestamp('ms')),
        ], schema=schema)
        self._write_arrow(table, initial)
        columns = ['payload_text', 'payload_map', 'payload_time']
        full = pa.table({
            '_ROW_ID': pa.array(range(40), pa.int64()),
            'payload_text': ['field-{}'.format(i) for i in range(40)],
            'payload_map': pa.array([[('v', str(i))] for i in range(40)], type=tags_type),
            'payload_time': pa.array(range(40), type=pa.timestamp('ms')),
        })
        self._update(table, full, columns)
        sparse = pa.table({
            '_ROW_ID': pa.array([2, 32], pa.int64()),
            'payload_text': pa.array([None, 'changed']),
            'payload_map': pa.array([[], None], type=tags_type),
            'payload_time': pa.array([2000, 32000], type=pa.timestamp('ms')),
        })
        with mock.patch(
                'pypaimon.execution.python.write.table_update_by_row_id.subprocess.run',
                wraps=subprocess.run) as spliced:
            self._update(table, sparse, columns)
        self.assertEqual(1, spliced.call_count)
        actual = self._read_all(table).sort_by('id')
        self.assertIsNone(actual['payload_text'][2].as_py())
        self.assertEqual([], actual['payload_map'][2].as_py())
        self.assertIsNone(actual['payload_map'][32].as_py())
        self.assertEqual('field-3', actual['payload_text'][3].as_py())
