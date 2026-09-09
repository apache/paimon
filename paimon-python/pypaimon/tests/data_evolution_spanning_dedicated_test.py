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
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.utils.range import Range


class DataEvolutionSpanningDedicatedTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = CatalogFactory.create({
            'warehouse': os.path.join(self.tempdir.name, 'warehouse')})
        catalog = self.catalog
        catalog.create_database('default', True)
        schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
            ('embedding', pa.list_(pa.float32(), 2)),
        ])
        catalog.create_table('default.spanning', Schema.from_pyarrow_schema(
            schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'deletion-vectors.enabled': 'true',
                'vector.file.format': 'parquet',
                'read.batch-size': '2',
            }), False)
        self.table = catalog.get_table('default.spanning')
        self.data = pa.Table.from_pydict({
            'id': list(range(10)),
            'payload': [f'blob-{i}'.encode() for i in range(10)],
            'embedding': [[float(i), float(i + 1)] for i in range(10)],
        }, schema=schema)

        # Reproduce the persistent layout after splitting only normal files.
        # Readers must support it even with split-large-files absent or disabled.
        self._write(self.table.copy({'target-file-row-num': '3'}), ['id'], self.data)
        self._write(self.table, ['payload'], self.data.slice(0, 7), 0)
        self._write(self.table, ['payload'], self.data.slice(7, 3), 7)
        self._write(self.table, ['embedding'], self.data, 0)

    def tearDown(self):
        self.tempdir.cleanup()

    @staticmethod
    def _write(table, columns, data, first_row_id=None):
        builder = table.new_batch_write_builder()
        writer = builder.new_write().with_write_type(columns)
        commit = builder.new_commit()
        try:
            writer.write_arrow(data.select(columns))
            messages = writer.prepare_commit()
            if first_row_id is not None:
                for message in messages:
                    for file in message.new_files:
                        file.first_row_id = first_row_id
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    def _assert_rows(self, actual, expected_ids):
        actual = actual.sort_by([('_ROW_ID', 'ascending')])
        self.assertEqual(actual['_ROW_ID'].to_pylist(), expected_ids)
        self.assertEqual(actual['payload'].to_pylist(),
                         [f'blob-{i}'.encode() for i in expected_ids])
        self.assertEqual(actual['embedding'].to_pylist(),
                         [[float(i), float(i + 1)] for i in expected_ids])

    def test_full_scan_projection_and_row_ranges(self):
        builder = self.table.new_read_builder().with_projection(
            ['payload', '_ROW_ID', 'embedding'])
        splits = builder.new_scan().plan().splits()
        self.assertEqual(sum(split.merged_row_count() for split in splits), 10)
        self._assert_rows(builder.new_read().to_arrow(splits), list(range(10)))

        splits = builder.new_scan().with_row_ranges([Range(2, 7)]).plan().splits()
        self._assert_rows(builder.new_read().to_arrow(splits), list(range(2, 8)))

    def test_deletions_and_chunk_shuffle_use_each_normal_anchor(self):
        builder = self.table.new_batch_write_builder()
        messages = builder.new_update().delete_by_row_id([2, 4, 8])
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()

        expected = [0, 1, 3, 5, 6, 7, 9]
        read_builder = self.table.new_read_builder().with_projection(
            ['payload', '_ROW_ID', 'embedding'])
        splits = read_builder.new_scan().plan().splits()
        self.assertEqual(sum(split.merged_row_count() for split in splits), len(expected))
        self._assert_rows(read_builder.new_read().to_arrow(splits), expected)

        splits = read_builder.new_scan().with_row_ranges([Range(2, 7)]).plan().splits()
        self._assert_rows(read_builder.new_read().to_arrow(splits), [3, 5, 6, 7])

        chunks = read_builder.new_scan().with_chunk_shuffle(
            seed=17, chunk_size=4).plan().splits()
        self.assertEqual(sum(split.merged_row_count() for split in chunks), len(expected))
        for split in chunks:
            names = [file.file_name for file in split.files]
            self.assertEqual(len(names), len(set(names)))
        self._assert_rows(read_builder.new_read().to_arrow(chunks), expected)

    def test_vector_middle_update_survives_normal_merge_and_rename(self):
        self.catalog.alter_table('default.spanning', [
            SchemaChange.rename_column('embedding', 'renamed')], False)
        self.table = self.catalog.get_table('default.spanning')
        updated = self.data.slice(3, 3).set_column(
            2, self.data.schema.field('embedding'),
            pa.array([[103.0, 104.0], [104.0, 105.0], [105.0, 106.0]],
                     type=self.data.schema.field('embedding').type))
        updated = updated.rename_columns(['id', 'payload', 'renamed'])
        self._write(self.table, ['renamed'], updated, 3)
        expected = [[float(i), float(i + 1)] for i in range(10)]
        expected[3:6] = [[103.0, 104.0], [104.0, 105.0], [105.0, 106.0]]
        read_builder = self.table.new_read_builder()
        actual = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits()).sort_by([('id', 'ascending')])
        self.assertEqual(actual['renamed'].to_pylist(), expected)

        self._merge_normal_files(self.table, self.data)
        builder = self.table.new_read_builder().with_projection(['_ROW_ID', 'renamed'])
        for selected in [None, [Range(1, 7)]]:
            scan = builder.new_scan()
            if selected is not None:
                scan.with_row_ranges(selected)
            actual = builder.new_read().to_arrow(scan.plan().splits()).sort_by(
                [('_ROW_ID', 'ascending')])
            ids = list(range(10)) if selected is None else list(range(1, 8))
            self.assertEqual(actual['_ROW_ID'].to_pylist(), ids)
            self.assertEqual(actual['renamed'].to_pylist(), [expected[i] for i in ids])

    @staticmethod
    def _merge_normal_files(table, data):
        # Re-merge normal ranges while keeping every dedicated version unchanged.
        old_files = [file for split in table.new_read_builder().new_scan().plan().splits()
                     for file in split.files
                     if not DataFileMeta.is_blob_file(file.file_name)
                     and not DataFileMeta.is_vector_file(file.file_name)]
        builder = table.new_batch_write_builder()
        writer = builder.new_write().with_write_type(['id'])
        commit = builder.new_commit()
        try:
            writer.write_arrow(data.select(['id']))
            messages = writer.prepare_commit()
            messages[0].deleted_files = old_files
            for file in messages[0].new_files:
                file.first_row_id = 0
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    def test_multi_vector_file_keeps_untouched_column_during_partial_update(self):
        vector_type = pa.list_(pa.float32(), 2)
        schema = pa.schema([('id', pa.int32()), ('left', vector_type), ('right', vector_type)])
        data = pa.Table.from_pydict({
            'id': list(range(10)),
            'left': [[float(i), float(i + 1)] for i in range(10)],
            'right': [[float(i + 20), float(i + 21)] for i in range(10)],
        }, schema=schema)
        self.catalog.create_table('default.multi_vector', Schema.from_pyarrow_schema(
            schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'vector.file.format': 'parquet',
            }), False)
        table = self.catalog.get_table('default.multi_vector')
        self._write(table.copy({'target-file-row-num': '3'}), ['id'], data)
        self._write(table, ['left', 'right'], data, 0)
        vector_files = [file for split in table.new_read_builder().new_scan().plan().splits()
                        for file in split.files if DataFileMeta.is_vector_file(file.file_name)]
        self.assertEqual([file.write_cols for file in vector_files], [['left', 'right']])

        self.catalog.alter_table('default.multi_vector', [
            SchemaChange.rename_column('left', 'renamed')], False)
        table = self.catalog.get_table('default.multi_vector')
        updates = [[103.0, 104.0], [104.0, 105.0], [105.0, 106.0]]
        self._write(table, ['renamed'], pa.Table.from_pydict(
            {'renamed': updates}, schema=pa.schema([('renamed', vector_type)])), 3)
        expected = data['left'].to_pylist()
        expected[3:6] = updates

        for merge_normal_files in [False, True]:
            if merge_normal_files:
                self._merge_normal_files(table, data)
            builder = table.new_read_builder().with_projection(['_ROW_ID', 'renamed', 'right'])
            for selected in [None, [Range(1, 7)]]:
                scan = builder.new_scan()
                if selected is not None:
                    scan.with_row_ranges(selected)
                actual = builder.new_read().to_arrow(scan.plan().splits()).sort_by(
                    [('_ROW_ID', 'ascending')])
                ids = list(range(10)) if selected is None else list(range(1, 8))
                self.assertEqual(actual['_ROW_ID'].to_pylist(), ids)
                self.assertEqual(actual['renamed'].to_pylist(), [expected[i] for i in ids])
                self.assertEqual(actual['right'].to_pylist(),
                                 [[float(i + 20), float(i + 21)] for i in ids])

            # With DVs disabled, a vector-only projection may prune every normal file.
            vector_builder = table.new_read_builder().with_projection(['renamed', 'right'])
            splits = vector_builder.new_scan().plan().splits()
            projected_splits = [DataSplit(
                files=[file for file in split.files if DataFileMeta.is_vector_file(file.file_name)],
                partition=split.partition, bucket=split.bucket, raw_convertible=False)
                for split in splits]
            for input_splits in [splits, projected_splits]:
                actual = vector_builder.new_read().to_arrow(input_splits)
                rows = sorted(actual.to_pylist(), key=lambda row: row['right'][0])
                self.assertEqual([row['renamed'] for row in rows], expected)
                self.assertEqual([row['right'] for row in rows], data['right'].to_pylist())
