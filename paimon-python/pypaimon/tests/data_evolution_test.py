"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class DataEvolutionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

    def test_basic(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema,
                                            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
        self.catalog.create_table('default.test_row_tracking', schema, False)
        table = self.catalog.get_table('default.test_row_tracking')

        # write 1
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            'f1': [-1001, 1002]
        }, schema=simple_pa_schema)
        table_write.write_arrow(expect_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # write 2
        table_write = write_builder.new_write().with_write_type(['f0'])
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'f0': [3, 4],
        }, schema=pa.schema([
            ('f0', pa.int8()),
        ]))
        table_write.write_arrow(data2)
        cmts = table_write.prepare_commit()
        cmts[0].new_files[0].first_row_id = 0
        table_commit.commit(cmts)
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        expect_data = pa.Table.from_pydict({
            'f0': [3, 4],
            'f1': [-1001, 1002]
        }, schema=pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
        ]))
        self.assertEqual(actual_data, expect_data)

    def test_multiple_appends(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_multiple_appends', schema, False)
        table = self.catalog.get_table('default.test_multiple_appends')

        write_builder = table.new_batch_write_builder()

        # write 100 rows: (1, "a", "b")
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        init_data = pa.Table.from_pydict({
            'f0': [1] * 100,
            'f1': ['a'] * 100,
            'f2': ['b'] * 100,
        }, schema=simple_pa_schema)
        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        # append：write (2, "x") and ("y"), set first_row_id = 100
        write0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        write1 = write_builder.new_write().with_write_type(['f2'])
        commit = write_builder.new_commit()
        data0 = pa.Table.from_pydict({'f0': [2], 'f1': ['x']},
                                     schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        data1 = pa.Table.from_pydict({'f2': ['y']}, schema=pa.schema([('f2', pa.string())]))
        write0.write_arrow(data0)
        write1.write_arrow(data1)
        cmts = write0.prepare_commit() + write1.prepare_commit()
        for c in cmts:
            for nf in c.new_files:
                nf.first_row_id = 100
        commit.commit(cmts)
        write0.close()
        write1.close()
        commit.close()

        # append：write (3, "c") and ("d"), set first_row_id = 101
        write0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        commit0 = write_builder.new_commit()
        data0 = pa.Table.from_pydict({'f0': [3], 'f1': ['c']},
                                     schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        write0.write_arrow(data0)
        cmts0 = write0.prepare_commit()
        for c in cmts0:
            for nf in c.new_files:
                nf.first_row_id = 101
        commit0.commit(cmts0)
        write0.close()
        commit0.close()

        write1 = write_builder.new_write().with_write_type(['f2'])
        commit1 = write_builder.new_commit()
        data1 = pa.Table.from_pydict({'f2': ['d']}, schema=pa.schema([('f2', pa.string())]))
        write1.write_arrow(data1)
        cmts1 = write1.prepare_commit()
        for c in cmts1:
            for nf in c.new_files:
                nf.first_row_id = 101
        commit1.commit(cmts1)
        write1.close()
        commit1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        self.assertEqual(actual.num_rows, 102)
        expect = pa.Table.from_pydict({
            'f0': [1] * 100 + [2] + [3],
            'f1': ['a'] * 100 + ['x'] + ['c'],
            'f2': ['b'] * 100 + ['y'] + ['d'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_disorder_cols_append(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_disorder_cols_append', schema, False)
        table = self.catalog.get_table('default.test_disorder_cols_append')

        write_builder = table.new_batch_write_builder()
        num_rows = 100
        # write 1 rows: (1, "a", "b")
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        init_data = pa.Table.from_pydict({
            'f0': [1] * num_rows,
            'f1': ['a'] * num_rows,
            'f2': ['b'] * num_rows,
        }, schema=simple_pa_schema)
        table_write.write_arrow(init_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # append：set first_row_id = 0 to modify the row with columns write
        write0 = write_builder.new_write().with_write_type(['f0', 'f2'])
        write1 = write_builder.new_write().with_write_type(['f1'])
        commit = write_builder.new_commit()
        data0 = pa.Table.from_pydict({'f0': [2] * num_rows, 'f2': ['y'] * num_rows},
                                     schema=pa.schema([('f0', pa.int32()), ('f2', pa.string())]))
        data1 = pa.Table.from_pydict({'f1': ['x'] * num_rows}, schema=pa.schema([('f1', pa.string())]))
        write0.write_arrow(data0)
        write1.write_arrow(data1)
        cmts = write0.prepare_commit() + write1.prepare_commit()
        for c in cmts:
            for nf in c.new_files:
                nf.first_row_id = 0
        commit.commit(cmts)
        write0.close()
        write1.close()
        commit.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        self.assertEqual(actual.num_rows, 100)
        expect = pa.Table.from_pydict({
            'f0': [2] * num_rows,
            'f1': ['x'] * num_rows,
            'f2': ['y'] * num_rows,
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_only_some_columns(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_only_some_columns', schema, False)
        table = self.catalog.get_table('default.test_only_some_columns')

        write_builder = table.new_batch_write_builder()

        # Commit 1: f0
        w0 = write_builder.new_write().with_write_type(['f0'])
        c0 = write_builder.new_commit()
        d0 = pa.Table.from_pydict({'f0': [1]}, schema=pa.schema([('f0', pa.int32())]))
        w0.write_arrow(d0)
        c0.commit(w0.prepare_commit())
        w0.close()
        c0.close()

        # Commit 2: f1, first_row_id = 0
        w1 = write_builder.new_write().with_write_type(['f1'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({'f1': ['a']}, schema=pa.schema([('f1', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        for c in cmts1:
            for nf in c.new_files:
                nf.first_row_id = 0
        c1.commit(cmts1)
        w1.close()
        c1.close()

        # Commit 3: f2, first_row_id = 0
        w2 = write_builder.new_write().with_write_type(['f2'])
        c2 = write_builder.new_commit()
        d2 = pa.Table.from_pydict({'f2': ['b']}, schema=pa.schema([('f2', pa.string())]))
        w2.write_arrow(d2)
        cmts2 = w2.prepare_commit()
        for c in cmts2:
            for nf in c.new_files:
                nf.first_row_id = 0
        c2.commit(cmts2)
        w2.close()
        c2.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expect = pa.Table.from_pydict({
            'f0': [1],
            'f1': ['a'],
            'f2': ['b'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_null_values(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_null_values', schema, False)
        table = self.catalog.get_table('default.test_null_values')

        write_builder = table.new_batch_write_builder()

        # Commit 1: some cols are null
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()

        d0 = pa.Table.from_pydict({'f0': [1], 'f1': [None]},
                                  schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        d1 = pa.Table.from_pydict({'f2': [None]}, schema=pa.schema([('f2', pa.string())]))
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # Commit 2
        w1 = write_builder.new_write().with_write_type(['f2'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({'f2': ['c']}, schema=pa.schema([('f2', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        for msg in cmts1:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c1.commit(cmts1)
        w1.close()
        c1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())
        expect = pa.Table.from_pydict({
            'f0': [1],
            'f1': [None],
            'f2': ['c'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    # different first_row_id append multiple times
    def test_multiple_appends_different_first_row_ids(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_multiple_appends_diff_rowid', schema, False)
        table = self.catalog.get_table('default.test_multiple_appends_diff_rowid')

        write_builder = table.new_batch_write_builder()

        # commit 1
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()
        d0 = pa.Table.from_pydict({'f0': [1], 'f1': ['a']},
                                  schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        d1 = pa.Table.from_pydict({'f2': ['b']}, schema=pa.schema([('f2', pa.string())]))
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # commit 2
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        c0 = write_builder.new_commit()
        d0 = pa.Table.from_pydict({'f0': [2], 'f1': ['c']},
                                  schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        w0.write_arrow(d0)
        cmts0 = w0.prepare_commit()
        for msg in cmts0:
            for nf in msg.new_files:
                nf.first_row_id = 1
        c0.commit(cmts0)
        w0.close()
        c0.close()

        # commit 3
        w1 = write_builder.new_write().with_write_type(['f2'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({'f2': ['d']}, schema=pa.schema([('f2', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        for msg in cmts1:
            for nf in msg.new_files:
                nf.first_row_id = 1
        c1.commit(cmts1)
        w1.close()
        c1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expect = pa.Table.from_pydict({
            'f0': [1, 2],
            'f1': ['a', 'c'],
            'f2': ['b', 'd'],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_more_data(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            simple_pa_schema,
            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
        )
        self.catalog.create_table('default.test_more_data', schema, False)
        table = self.catalog.get_table('default.test_more_data')

        write_builder = table.new_batch_write_builder()

        # first commit：100k rows
        w0 = write_builder.new_write().with_write_type(['f0', 'f1'])
        w1 = write_builder.new_write().with_write_type(['f2'])
        c = write_builder.new_commit()
        size = 100000
        d0 = pa.Table.from_pydict({
            'f0': list(range(size)),
            'f1': [f'a{i}' for i in range(size)],
        }, schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())]))
        d1 = pa.Table.from_pydict({
            'f2': [f'b{i}' for i in range(size)],
        }, schema=pa.schema([('f2', pa.string())]))
        w0.write_arrow(d0)
        w1.write_arrow(d1)
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for msg in cmts:
            for nf in msg.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # second commit：overwrite f2 to 'c{i}'
        w1 = write_builder.new_write().with_write_type(['f2'])
        c1 = write_builder.new_commit()
        d1 = pa.Table.from_pydict({
            'f2': [f'c{i}' for i in range(size)],
        }, schema=pa.schema([('f2', pa.string())]))
        w1.write_arrow(d1)
        cmts1 = w1.prepare_commit()
        c1.commit(cmts1)
        w1.close()
        c1.close()

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual = table_read.to_arrow(table_scan.plan().splits())

        expect = pa.Table.from_pydict({
            'f0': list(range(size)),
            'f1': [f'a{i}' for i in range(size)],
            'f2': [f'c{i}' for i in range(size)],
        }, schema=simple_pa_schema)
        self.assertEqual(actual, expect)

    def test_read_row_tracking_metadata(self):
        simple_pa_schema = pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
        ])
        schema = Schema.from_pyarrow_schema(simple_pa_schema,
                                            options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
        self.catalog.create_table('default.test_row_tracking_meta', schema, False)
        table = self.catalog.get_table('default.test_row_tracking_meta')

        # write 1
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            'f1': [-1001, 1002]
        }, schema=simple_pa_schema)
        table_write.write_arrow(expect_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        read_builder.with_projection(['f0', '_ROW_ID', 'f1', '_SEQUENCE_NUMBER'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        expect_data = pa.Table.from_pydict({
            'f0': [-1, 2],
            '_ROW_ID': [0, 1],
            'f1': [-1001, 1002],
            '_SEQUENCE_NUMBER': [1, 1],
        }, schema=pa.schema([
            ('f0', pa.int8()),
            ('_ROW_ID', pa.int64()),
            ('f1', pa.int16()),
            ('_SEQUENCE_NUMBER', pa.int64()),
        ]))
        self.assertEqual(actual_data, expect_data)

        # write 2
        table_write = write_builder.new_write().with_write_type(['f0'])
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'f0': [3, 4],
        }, schema=pa.schema([
            ('f0', pa.int8()),
        ]))
        table_write.write_arrow(data2)
        cmts = table_write.prepare_commit()
        cmts[0].new_files[0].first_row_id = 0
        table_commit.commit(cmts)
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        read_builder.with_projection(['f0', 'f1', '_ROW_ID', '_SEQUENCE_NUMBER'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        actual_data = table_read.to_arrow(table_scan.plan().splits())
        expect_data = pa.Table.from_pydict({
            'f0': [3, 4],
            'f1': [-1001, 1002],
            '_ROW_ID': [0, 1],
            '_SEQUENCE_NUMBER': [2, 2],
        }, schema=pa.schema([
            ('f0', pa.int8()),
            ('f1', pa.int16()),
            ('_ROW_ID', pa.int64()),
            ('_SEQUENCE_NUMBER', pa.int64()),
        ]))
        self.assertEqual(actual_data, expect_data)
