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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.write.commit_callback import CommitCallback, CommitCallbackContext


class RecordingCallback(CommitCallback):
    """Test callback that records all invocations."""

    def __init__(self):
        self.contexts = []
        self.closed = False

    def call(self, context: CommitCallbackContext) -> None:
        self.contexts.append(context)

    def close(self) -> None:
        self.closed = True


class CommitCallbackTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('dt', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, table_name, partition_keys=None, options=None):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=partition_keys or [],
            options=options or {})
        self.catalog.create_table(f'default.{table_name}', schema, False)
        return self.catalog.get_table(f'default.{table_name}')

    def test_callback_invoked_on_commit(self):
        table = self._create_table('test_callback_invoked')
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        data = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())

        self.assertEqual(1, len(callback.contexts))
        ctx = callback.contexts[0]
        self.assertEqual(1, ctx.snapshot.id)
        self.assertEqual('APPEND', ctx.snapshot.commit_kind)
        self.assertGreater(len(ctx.commit_entries), 0)

        table_write.close()
        table_commit.close()

    def test_callback_receives_correct_snapshot_data(self):
        table = self._create_table('test_callback_snapshot_data', partition_keys=['dt'])
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'dt': ['p1', 'p1', 'p2'],
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())

        ctx = callback.contexts[0]
        self.assertEqual(3, ctx.snapshot.delta_record_count)
        self.assertEqual(3, ctx.snapshot.total_record_count)

        table_write.close()
        table_commit.close()

    def test_multiple_callbacks(self):
        table = self._create_table('test_multi_callbacks')
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        cb1 = RecordingCallback()
        cb2 = RecordingCallback()
        table_commit.add_commit_callback(cb1)
        table_commit.add_commit_callback(cb2)

        data = pa.Table.from_pydict({
            'id': [1],
            'name': ['a'],
            'dt': ['p1'],
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())

        self.assertEqual(1, len(cb1.contexts))
        self.assertEqual(1, len(cb2.contexts))
        self.assertEqual(cb1.contexts[0].snapshot.id, cb2.contexts[0].snapshot.id)

        table_write.close()
        table_commit.close()

    def test_callback_close_on_commit_close(self):
        table = self._create_table('test_callback_close')
        write_builder = table.new_batch_write_builder()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        self.assertFalse(callback.closed)
        table_commit.close()
        self.assertTrue(callback.closed)

    def test_callback_not_invoked_when_no_data(self):
        table = self._create_table('test_callback_no_data')
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        table_commit.commit(table_write.prepare_commit())

        self.assertEqual(0, len(callback.contexts))

        table_write.close()
        table_commit.close()

    def test_stream_commit_callback_multiple_rounds(self):
        table = self._create_table('test_stream_callback')
        write_builder = table.new_stream_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        for i in range(3):
            data = pa.Table.from_pydict({
                'id': [i],
                'name': [f'name_{i}'],
                'dt': ['p1'],
            }, schema=self.pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit(i), commit_identifier=i)

        self.assertEqual(3, len(callback.contexts))
        for i, ctx in enumerate(callback.contexts):
            self.assertEqual(i + 1, ctx.snapshot.id)

        table_write.close()
        table_commit.close()

    def test_data_evolution_callback_sees_row_id(self):
        table = self._create_table('test_de_row_id', options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'dt': ['p1', 'p2', 'p3'],
        }, schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())

        ctx = callback.contexts[0]
        self.assertIsNotNone(ctx.snapshot.next_row_id)
        for entry in ctx.commit_entries:
            self.assertIsNotNone(entry.file.first_row_id)

        total_rows = sum(e.file.row_count for e in ctx.commit_entries)
        self.assertEqual(3, total_rows)
        self.assertEqual(total_rows, ctx.snapshot.next_row_id)

        table_write.close()
        table_commit.close()

    def test_data_evolution_callback_row_id_increments_across_commits(self):
        table = self._create_table('test_de_row_id_incr', options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        write_builder = table.new_stream_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        callback = RecordingCallback()
        table_commit.add_commit_callback(callback)

        for i in range(3):
            data = pa.Table.from_pydict({
                'id': [i * 2, i * 2 + 1],
                'name': [f'a{i}', f'b{i}'],
                'dt': ['p1', 'p1'],
            }, schema=self.pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit(i), commit_identifier=i)

        self.assertEqual(3, len(callback.contexts))

        # Row IDs must be assigned and monotonically increasing across commits
        prev_next_row_id = 0
        for ctx in callback.contexts:
            for entry in ctx.commit_entries:
                self.assertIsNotNone(entry.file.first_row_id)
                self.assertGreaterEqual(entry.file.first_row_id, prev_next_row_id)
            self.assertGreater(ctx.snapshot.next_row_id, prev_next_row_id)
            prev_next_row_id = ctx.snapshot.next_row_id

        table_write.close()
        table_commit.close()


if __name__ == '__main__':
    unittest.main()
