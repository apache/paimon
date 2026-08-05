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

import datetime
import glob
import os
import shutil
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

from pypaimon import CatalogFactory, Schema
import pyarrow as pa
from parameterized import parameterized

from pypaimon.common.json_util import JSON
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.write.table_write import TableWrite
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


class TableWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        cls.pk_pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False)
        ])
        cls.postpone_pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('dt', pa.string(), nullable=False),
            ('value', pa.string()),
        ])
        cls.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
            'behavior': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h', 'i', 'j'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2', 'p2', 'p1']
        }, schema=cls.pa_schema)
        cls.pk_expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
            'behavior': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h', 'i', 'j'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2', 'p2', 'p1']
        }, schema=cls.pk_pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @staticmethod
    def _commit_rows(table, rows):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        for row in rows:
            table_write.write_row(row)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @staticmethod
    def _read_sorted(table, sort_keys):
        read_builder = table.new_read_builder()
        return read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits()).sort_by(sort_keys)

    def _create_postpone_table(
            self, identifier, pa_schema=None, partition_keys=None,
            primary_keys=None, options=None):
        options = dict(options or {})
        options['bucket'] = -2
        schema = Schema.from_pyarrow_schema(
            pa_schema if pa_schema is not None else self.pk_pa_schema,
            partition_keys=partition_keys or [],
            primary_keys=primary_keys or [],
            options=options,
        )
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier)

    @staticmethod
    def _commit_arrow(table, data, fixed_bucket=False):
        builder = (
            table.new_postpone_fixed_bucket_write_builder()
            if fixed_bucket else table.new_batch_write_builder()
        )
        write = builder.new_write()
        commit = builder.new_commit()
        try:
            write.write_arrow(data)
            messages = write.prepare_commit()
            commit.commit(messages)
            return messages
        finally:
            write.close()
            commit.close()

    @staticmethod
    @contextmanager
    def _postpone_write(table, overwrite=None):
        builder = table.new_postpone_fixed_bucket_write_builder()
        if overwrite is not None:
            builder.overwrite(overwrite)
        write = builder.new_write()
        commit = builder.new_commit()
        try:
            yield write, commit
        finally:
            write.close()
            commit.close()

    @staticmethod
    def _mock_table_write(partitions, buckets):
        table_write = object.__new__(TableWrite)
        table_write._validate_pyarrow_schema = Mock()
        table_write.row_key_extractor = Mock()
        table_write.file_store_write = Mock()
        table_write.row_key_extractor.extract_partition_bucket_batch.return_value = (
            partitions, buckets)
        return table_write

    def test_write_arrow_batch_reuses_full_batch(self):
        data = pa.RecordBatch.from_pydict({
            'id': [0, 1],
            'payload': [b'a', b'b'],
        })
        table_write = self._mock_table_write(
            [('p1',), ('p1',)], [0, 0])

        with patch.object(pa.compute, 'take', wraps=pa.compute.take) as take:
            table_write.write_arrow_batch(data)

        take.assert_not_called()
        written = table_write.file_store_write.write.call_args[0][2]
        self.assertIs(data, written)

    def test_write_arrow_batch_uses_zero_copy_for_contiguous_groups(self):
        data = pa.RecordBatch.from_pydict({
            'id': [0, 1, 2, 3],
            'payload': [b'a', b'b', b'c', b'd'],
        })
        table_write = self._mock_table_write(
            [('p1',), ('p1',), ('p2',), ('p2',)],
            [0, 0, 1, 1])
        with patch.object(pa.compute, 'take', wraps=pa.compute.take) as take:
            table_write.write_arrow_batch(data)

        take.assert_not_called()
        calls = table_write.file_store_write.write.call_args_list
        self.assertEqual(2, len(calls))
        self.assertEqual({'id': [0, 1], 'payload': [b'a', b'b']},
                         calls[0][0][2].to_pydict())
        self.assertEqual({'id': [2, 3], 'payload': [b'c', b'd']},
                         calls[1][0][2].to_pydict())
        self.assertEqual(
            data.column(1).buffers()[2].address,
            calls[0][0][2].column(1).buffers()[2].address)

    def test_write_arrow_batch_uses_take_for_non_contiguous_groups(self):
        data = pa.RecordBatch.from_pydict({
            'id': [0, 1, 2, 3],
            'payload': [b'a', b'b', b'c', b'd'],
        })
        table_write = self._mock_table_write(
            [('p1',), ('p2',), ('p1',), ('p2',)],
            [0, 1, 0, 1])

        with patch.object(pa.compute, 'take', wraps=pa.compute.take) as take:
            table_write.write_arrow_batch(data)

        self.assertEqual(2, take.call_count)
        calls = table_write.file_store_write.write.call_args_list
        self.assertEqual({'id': [0, 2], 'payload': [b'a', b'c']},
                         calls[0][0][2].to_pydict())
        self.assertEqual({'id': [1, 3], 'payload': [b'b', b'd']},
                         calls[1][0][2].to_pydict())

    def test_write_snapshot(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_write_snapshot', schema, False)
        table = self.catalog.get_table('default.test_write_snapshot')
        write_builder = table.new_batch_write_builder()

        # write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(self.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # read
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        self.assertEqual(self.expected, actual)

        # snapshot
        snapshot_json: str = JSON.to_json(table.snapshot_manager().get_latest_snapshot())
        self.assertEqual(True, snapshot_json.__contains__("baseManifestList"))
        self.assertEqual(False, snapshot_json.__contains__("nextRowId"))

    def test_write_row_append_only_partitioned_table(self):
        from pypaimon.table.row.generic_row import GenericRow

        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table(
            'default.test_write_row_append_only_partitioned', schema, False)
        table = self.catalog.get_table(
            'default.test_write_row_append_only_partitioned')

        reordered_fields = [
            table.field_dict['dt'],
            table.field_dict['behavior'],
            table.field_dict['item_id'],
            table.field_dict['user_id'],
        ]
        rows = [
            GenericRow(['p1', 'a', 1001, 1], reordered_fields),
            GenericRow(['p2', 'b', 1002, 2], reordered_fields),
        ]
        self._commit_rows(table, rows)

        expected = pa.Table.from_pydict({
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p2'],
        }, schema=self.pa_schema)
        actual = self._read_sorted(table, 'user_id')
        self.assertEqual(expected, actual)

    def test_write_row_fixed_bucket_primary_key_table(self):
        from pypaimon.table.row.generic_row import GenericRow

        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'bucket': '2'},
        )
        self.catalog.create_table(
            'default.test_write_row_fixed_bucket_pk', schema, False)
        table = self.catalog.get_table(
            'default.test_write_row_fixed_bucket_pk')

        rows = [
            GenericRow([1, 1001, 'a', 'p1'], table.fields),
            GenericRow([2, 1002, 'b', 'p2'], table.fields),
        ]
        self._commit_rows(table, rows)

        expected = pa.Table.from_pydict({
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p2'],
        }, schema=self.pk_pa_schema)
        sort_keys = [('user_id', 'ascending'), ('dt', 'ascending')]
        self.assertEqual(
            expected.sort_by(sort_keys), self._read_sorted(table, sort_keys))

    def test_write_row_dynamic_bucket_primary_key_table(self):
        from pypaimon.table.row.generic_row import GenericRow

        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'bucket': '-1'},
        )
        self.catalog.create_table(
            'default.test_write_row_dynamic_bucket_pk', schema, False)
        table = self.catalog.get_table(
            'default.test_write_row_dynamic_bucket_pk')

        rows = [
            GenericRow([1, 1001, 'a', 'p1'], table.fields),
            GenericRow([2, 1002, 'b', 'p2'], table.fields),
        ]
        self._commit_rows(table, rows)

        expected = pa.Table.from_pydict({
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p2'],
        }, schema=self.pk_pa_schema)
        sort_keys = [('user_id', 'ascending'), ('dt', 'ascending')]
        self.assertEqual(
            expected.sort_by(sort_keys), self._read_sorted(table, sort_keys))

    def test_multi_prepare_commit_ao(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.catalog.get_table('default.test_append_only_parquet')
        write_builder = table.new_stream_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        # write 1
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(0)
        # write 2
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(1)
        # write 3
        data3 = {
            'user_id': [9, 10],
            'item_id': [1009, 1010],
            'behavior': ['i', 'j'],
            'dt': ['p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data3, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        cm = table_write.prepare_commit(2)
        # commit
        table_commit.commit(cm, 2)
        table_write.close()
        table_commit.close()
        self.assertEqual(2, table_write.file_store_write.commit_identifier)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        self.assertEqual(self.expected, actual)

    def test_commit_minor_compacts_manifest_files(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={'manifest.merge-min-count': '2'},
        )
        self.catalog.create_table('default.test_minor_manifest_compaction', schema, False)
        table = self.catalog.get_table('default.test_minor_manifest_compaction')

        expected_data = {
            'user_id': [],
            'item_id': [],
            'behavior': [],
            'dt': [],
        }
        for i in range(3):
            row = {
                'user_id': [i + 1],
                'item_id': [1000 + i],
                'behavior': ['click'],
                'dt': ['p1'],
            }
            for key, values in row.items():
                expected_data[key].extend(values)

            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            table_write.write_arrow(pa.Table.from_pydict(row, schema=self.pa_schema))
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        snapshot = table.snapshot_manager().get_latest_snapshot()
        manifest_list_manager = ManifestListManager(table)
        base_manifests = manifest_list_manager.read(snapshot.base_manifest_list)
        delta_manifests = manifest_list_manager.read(snapshot.delta_manifest_list)

        self.assertEqual(len(base_manifests), 1)
        self.assertEqual(base_manifests[0].num_added_files, 2)
        self.assertEqual(base_manifests[0].num_deleted_files, 0)
        self.assertEqual(len(delta_manifests), 1)

        expected = pa.Table.from_pydict(expected_data, schema=self.pa_schema)
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        self.assertEqual(expected, actual)

    def test_multi_prepare_commit_pk(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': '2'})
        self.catalog.create_table('default.test_primary_key_parquet', schema, False)
        table = self.catalog.get_table('default.test_primary_key_parquet')
        write_builder = table.new_stream_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        # write 1
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pk_pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(0)
        # write 2
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pk_pa_schema)
        table_write.write_arrow(pa_table)
        table_write.prepare_commit(1)
        # write 3
        data3 = {
            'user_id': [9, 10],
            'item_id': [1009, 1010],
            'behavior': ['i', 'j'],
            'dt': ['p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data3, schema=self.pk_pa_schema)
        table_write.write_arrow(pa_table)
        cm = table_write.prepare_commit(2)
        # commit
        table_commit.commit(cm, 2)
        table_write.close()
        table_commit.close()
        self.assertEqual(2, table_write.file_store_write.commit_identifier)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits).sort_by('user_id')
        self.assertEqual(self.pk_expected, actual)

    def test_postpone_read_write(self):
        table = self._create_postpone_table(
            'default.test_postpone',
            pa_schema=self.pa_schema,
            partition_keys=['user_id'],
            primary_keys=['user_id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '1 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 2,
            },
        )
        data = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        expect = pa.Table.from_pydict(data, schema=self.pk_pa_schema)

        write_builder = table.new_postpone_fixed_bucket_write_builder()
        table_write = write_builder.new_write()
        from pypaimon.write.postpone_batch_table_write import (
            PostponeFixedBucketBatchTableWrite,
            PostponeFixedBucketWriteBuilder,
        )
        self.assertIsInstance(
            write_builder, PostponeFixedBucketWriteBuilder)
        self.assertIsInstance(
            table_write, PostponeFixedBucketBatchTableWrite)
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        self.assertTrue(os.path.exists(self.warehouse + "/default.db/test_postpone/snapshot/LATEST"))
        self.assertTrue(os.path.exists(self.warehouse + "/default.db/test_postpone/snapshot/snapshot-1"))
        self.assertTrue(os.path.exists(self.warehouse + "/default.db/test_postpone/manifest"))
        self.assertEqual(len(glob.glob(self.warehouse + "/default.db/test_postpone/manifest/*")), 3)
        self.assertEqual({2}, {message.total_buckets for message in commit_messages})
        self.assertEqual(
            1,
            len(glob.glob(
                self.warehouse
                + "/default.db/test_postpone/user_id=2/bucket-[01]/*.parquet"
            )),
        )
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertEqual(expect, actual)

    def test_postpone_file_store_write_validates_runtime_bucket_count(self):
        from pypaimon.write.file_store_write import (
            PostponeFixedBucketFileStoreWrite,
        )

        table = self._create_postpone_table(
            'default.test_postpone_runtime_bucket_validation',
            pa_schema=self.pa_schema,
            partition_keys=['user_id'],
            primary_keys=['user_id', 'dt'],
        )
        write = PostponeFixedBucketFileStoreWrite(table, 'test-user')
        try:
            with self.assertRaisesRegex(ValueError, 'must be positive'):
                write.write((1,), 0, self.pk_expected.to_batches()[0], 0)
            with self.assertRaisesRegex(ValueError, 'out of range'):
                write.write((1,), 2, self.pk_expected.to_batches()[0], 2)

            write._check_runtime_bucket((1,), 0, 2)
            with self.assertRaisesRegex(RuntimeError, 'new bucket num 3'):
                write._check_runtime_bucket((1,), 0, 3)
        finally:
            write.abort()

    def test_postpone_batch_write_builder_keeps_postpone_mode(self):
        table = self._create_postpone_table(
            'default.test_postpone_default_builder',
            pa_schema=self.pa_schema,
            partition_keys=['user_id'],
            primary_keys=['user_id', 'dt'],
        )
        expected = pa.Table.from_pydict({
            'user_id': [1],
            'item_id': [1001],
            'behavior': ['a'],
            'dt': ['p1'],
        }, schema=self.pk_pa_schema)

        self._commit_arrow(table, expected)

        self.assertEqual(
            1,
            len(glob.glob(
                self.warehouse
                + "/default.db/test_postpone_default_builder/user_id=1/"
                + "bucket-postpone/*.avro"
            )),
        )
        splits = table.new_read_builder().new_scan().plan().splits()
        self.assertTrue(not table.new_read_builder().new_read().to_arrow(splits))

    def test_postpone_batch_infers_bucket_num_from_input_size(self):
        table = self._create_postpone_table(
            'default.test_postpone_size_inference',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '100 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 3,
            },
        )
        data = pa.Table.from_pydict({
            'id': [1, 2],
            'dt': ['small', 'large'],
            'value': ['x', 'x' * 500],
        }, schema=self.postpone_pa_schema)

        with self._postpone_write(table) as (write, commit):
            write.write_arrow(data)
            messages = write.prepare_commit()
            total_buckets = {
                tuple(message.partition): message.total_buckets
                for message in messages
            }
            self.assertEqual(1, total_buckets[('small',)])
            self.assertEqual(3, total_buckets[('large',)])
            commit.commit(messages)

        self.assertEqual(
            [1, 2], self._read_sorted(table, 'id').column('id').to_pylist()
        )

    def test_postpone_size_inference_matches_java_binary_row(self):
        from pypaimon.write.postpone_bucket import PostponeBucketPlanner

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('key', pa.string(), nullable=False),
            pa.field('value', pa.string(), nullable=False),
        ])
        table = self._create_postpone_table(
            'default.test_postpone_java_size_fixture',
            pa_schema,
            primary_keys=['id'],
            options={
                'postpone.target-size-per-bucket': '20 kb',
                'postpone.batch-write-fixed-bucket.max-parallelism': 3,
            },
        )
        data = pa.RecordBatch.from_pydict({
            'id': list(range(1000)),
            'key': ['k'] * 1000,
            'value': ['v'] * 1000,
        }, schema=pa_schema)

        planner = PostponeBucketPlanner(
            table, known_num_buckets={}, postpone_row_counts={})
        stats = planner.input_partition_stats(data)
        self.assertEqual((1000, 32000), stats[()])
        self.assertEqual(2, planner.plan(stats).num_buckets(()))

    def test_postpone_stats_skip_known_partitions(self):
        from pypaimon.write.postpone_bucket import PostponeBucketPlanner

        table = self._create_postpone_table(
            'default.test_postpone_skip_known_stats',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
        )
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2],
            'dt': ['known', 'new'],
            'value': ['x', 'y'],
        }, schema=self.postpone_pa_schema)
        planner = PostponeBucketPlanner(
            table,
            known_num_buckets={('known',): 2},
            postpone_row_counts={},
        )

        self.assertEqual(
            {('new',)}, set(planner.input_partition_stats(data)))

    def test_postpone_size_inference_supports_java_type_surface(self):
        from pypaimon.write.postpone_bucket import PostponeBucketPlanner

        variant_type = pa.struct([
            pa.field('value', pa.binary(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('items', pa.list_(pa.int32())),
            ('attributes', pa.map_(pa.string(), pa.int32())),
            ('nested', pa.struct([('label', pa.string())])),
            ('embedding', pa.list_(pa.float32(), 2)),
            ('payload', variant_type),
            ('event_time', pa.timestamp('us', tz='UTC')),
        ])
        table = self._create_postpone_table(
            'default.test_postpone_java_type_surface',
            pa_schema,
            primary_keys=['id'],
        )
        data = pa.RecordBatch.from_pydict({
            'id': [1],
            'items': [[1, 2, 3]],
            'attributes': [[('a', 1)]],
            'nested': [{'label': 'x'}],
            'embedding': [[1.0, 2.0]],
            'payload': [{'value': b'\x00', 'metadata': b'\x01'}],
            'event_time': [datetime.datetime(
                2026, 1, 1, tzinfo=datetime.timezone.utc
            )],
        }, schema=pa_schema)

        planner = PostponeBucketPlanner(
            table, known_num_buckets={}, postpone_row_counts={})
        self.assertEqual((1, 176), planner.input_partition_stats(data)[()])

    def test_postpone_size_inference_supports_nested_vector(self):
        from pypaimon.write.postpone_bucket import PostponeBucketPlanner

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('embeddings', pa.list_(pa.list_(pa.float32(), 2))),
        ])
        table = self._create_postpone_table(
            'default.test_postpone_nested_vector_size',
            pa_schema,
            primary_keys=['id'],
        )
        data = pa.RecordBatch.from_pydict({
            'id': [1],
            'embeddings': [[[1.0, 2.0], [3.0, 4.0]]],
        }, schema=pa_schema)

        planner = PostponeBucketPlanner(
            table, known_num_buckets={}, postpone_row_counts={})
        self.assertEqual((1, 80), planner.input_partition_stats(data)[()])

    def test_postpone_default_bucket_function_matches_java(self):
        from pypaimon.write.postpone_bucket import PostponeBucketPlan
        from pypaimon.write.row_key_extractor import (
            PostponeFixedBucketRowKeyExtractor,
        )

        pa_schema = pa.schema([
            pa.field('key', pa.string(), nullable=False),
            pa.field('value', pa.string(), nullable=False),
        ])
        table = self._create_postpone_table(
            'default.test_postpone_java_bucket_fixture',
            pa_schema,
            primary_keys=['key'],
        )
        extractor = PostponeFixedBucketRowKeyExtractor(
            table, PostponeBucketPlan({(): 4}))
        data = pa.RecordBatch.from_pydict({
            'key': ['hello-java'],
            'value': ['v'],
        }, schema=pa_schema)

        # Java BinaryRow hash -201703277 maps to bucket 1 of 4.
        self.assertEqual([1], extractor.extract_partition_bucket_batch(data)[1])

    def test_postpone_bucket_key_hashes_match_java(self):
        from pypaimon.schema.data_types import (
            AtomicType,
            DataField,
        )
        from pypaimon.write.row_key_extractor import RowKeyExtractor

        cases = [
            (
                'timestamp_ltz',
                datetime.datetime(
                    2026, 1, 2, 11, 4, 5, 123456,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=8))),
                AtomicType('TIMESTAMP_LTZ(6)'),
                1245041971,
            ),
            (
                'variant',
                {'value': b'\x00', 'metadata': b'\x01'},
                AtomicType('VARIANT'),
                -1501111295,
            ),
        ]

        for name, value, data_type, expected in cases:
            with self.subTest(name=name):
                actual = RowKeyExtractor._binary_row_hash_code(
                    (value,), [DataField(0, 'key', data_type)])
                if actual >= 0x80000000:
                    actual -= 0x100000000
                self.assertEqual(expected, actual)

    @parameterized.expand([
        ('compact', 'ms', 123000),
        ('non_compact', 'us', 123456),
    ])
    def test_postpone_ltz_key_reopens_and_filters(
            self, name, unit, microsecond):
        identifier = 'default.test_postpone_ltz_' + name
        pa_schema = pa.schema([
            pa.field('key', pa.timestamp(unit, tz='UTC'), nullable=False),
            pa.field('value', pa.string()),
        ])
        table = self._create_postpone_table(
            identifier, pa_schema, primary_keys=['key'])
        key = datetime.datetime(
            2026, 1, 2, 3, 4, 5, microsecond,
            tzinfo=datetime.timezone.utc)
        self._commit_arrow(table, pa.Table.from_pydict({
            'key': [key], 'value': ['v'],
        }, schema=pa_schema), fixed_bucket=True)

        reopened = self.catalog.get_table(identifier)
        read_builder = reopened.new_read_builder()
        read_builder.with_filter(
            read_builder.new_predicate_builder().equal('key', key))
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(1, result.num_rows)

    def test_postpone_variant_key_reopens_and_scans(self):
        from pypaimon.data.generic_variant import GenericVariant

        identifier = 'default.test_postpone_variant_key'
        variant = GenericVariant.from_python(-1)
        variant_array = GenericVariant.to_arrow_array([variant])
        pa_schema = pa.schema([
            pa.field('key', variant_array.type, nullable=False),
            pa.field('value', pa.string()),
        ])
        table = self._create_postpone_table(
            identifier, pa_schema, primary_keys=['key'])
        data = pa.Table.from_arrays(
            [variant_array, pa.array(['v'])], schema=pa_schema)
        self._commit_arrow(table, data, fixed_bucket=True)

        reopened = self.catalog.get_table(identifier)
        read_builder = reopened.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(1, result.num_rows)

    @parameterized.expand([('mod',), ('hive',)])
    def test_postpone_rejects_unsupported_bucket_function(
            self, bucket_function):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('value', pa.string(), nullable=False),
        ])
        table = self._create_postpone_table(
            'default.test_postpone_bucket_function_' + bucket_function,
            pa_schema,
            primary_keys=['id'],
            options={'bucket-function.type': bucket_function},
        )

        with self.assertRaisesRegex(
                ValueError, 'only support bucket-function.type=default'):
            table.new_postpone_fixed_bucket_write_builder().new_write()

    def test_postpone_batch_plans_all_record_batches(self):
        table = self._create_postpone_table(
            'default.test_postpone_multi_batch_plan',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '100 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 3,
            },
        )

        with self._postpone_write(table) as (write, commit):
            write.write_arrow_batch(pa.RecordBatch.from_pydict({
                'id': [1],
                'dt': ['p'],
                'value': ['x'],
            }, schema=self.postpone_pa_schema))
            write.write_arrow_batch(pa.RecordBatch.from_pydict({
                'id': [2],
                'dt': ['p'],
                'value': ['x' * 500],
            }, schema=self.postpone_pa_schema))
            messages = write.prepare_commit()
            self.assertEqual({3}, {message.total_buckets for message in messages})
            commit.commit(messages)

        self.assertEqual(
            [1, 2], self._read_sorted(table, 'id').column('id').to_pylist()
        )

    def test_postpone_batch_prefers_target_row_num(self):
        table = self._create_postpone_table(
            'default.test_postpone_row_num_plan',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-row-num-per-bucket': 1,
                'postpone.target-size-per-bucket': '1 gb',
                'postpone.batch-write-fixed-bucket.max-parallelism': 8,
            },
        )

        with self._postpone_write(table) as (write, commit):
            write.write_arrow(pa.Table.from_pydict({
                'id': list(range(8)),
                'dt': ['p'] * 8,
                'value': ['x'] * 8,
            }, schema=self.postpone_pa_schema))
            messages = write.prepare_commit()
            self.assertEqual({8}, {message.total_buckets for message in messages})
            commit.commit(messages)

        self.assertEqual(
            list(range(8)),
            self._read_sorted(table, 'id').column('id').to_pylist(),
        )

    def test_postpone_batch_plans_write_rows_with_arrow_input(self):
        from pypaimon.table.row.generic_row import GenericRow

        table = self._create_postpone_table(
            'default.test_postpone_write_row_plan',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-row-num-per-bucket': 1,
                'postpone.target-size-per-bucket': '1 gb',
                'postpone.batch-write-fixed-bucket.max-parallelism': 8,
            },
        )

        with self._postpone_write(table) as (write, commit):
            for row_id in range(8):
                write.write_row(
                    GenericRow([row_id, 'rows', 'x'], table.fields)
                )
            write.write_row(GenericRow([8, 'mixed', 'x'], table.fields))
            write.write_arrow(pa.Table.from_pydict({
                'id': list(range(9, 16)),
                'dt': ['mixed'] * 7,
                'value': ['x'] * 7,
            }, schema=self.postpone_pa_schema))

            messages = write.prepare_commit()
            total_buckets = {
                tuple(message.partition): message.total_buckets
                for message in messages
            }
            self.assertEqual(8, total_buckets[('rows',)])
            self.assertEqual(8, total_buckets[('mixed',)])
            commit.commit(messages)

        self.assertEqual(16, self._read_sorted(table, 'id').num_rows)

    def test_postpone_target_row_num_counts_existing_postpone_rows(self):
        from pypaimon.write.postpone_bucket import (
            PostponeBucketPlanner,
        )

        table = self._create_postpone_table(
            'default.test_postpone_existing_row_count',
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={
                'postpone.target-row-num-per-bucket': 2,
                'postpone.batch-write-fixed-bucket.max-parallelism': 8,
            },
        )
        append_planner = PostponeBucketPlanner(
            table,
            known_num_buckets={},
            postpone_row_counts={('p',): 3},
        )
        append_plan = append_planner.plan({('p',): (1, 10)})
        self.assertEqual(2, append_plan.num_buckets(('p',)))

        overwrite_planner = PostponeBucketPlanner(
            table,
            known_num_buckets={},
            postpone_row_counts={('p',): 3},
        )
        overwrite_plan = overwrite_planner.plan(
            {('p',): (1, 10)}, include_postpone_rows=False
        )
        self.assertEqual(1, overwrite_plan.num_buckets(('p',)))

    def test_postpone_worker_bucket_plan_mismatch_fails_commit(self):
        from pypaimon.write.commit.conflict_detection import CommitConflictError

        table = self._create_postpone_table(
            'default.test_postpone_worker_plan_mismatch',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '100 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 3,
            },
        )
        small_builder = table.new_postpone_fixed_bucket_write_builder()
        large_builder = table.new_postpone_fixed_bucket_write_builder()
        small_write = small_builder.new_write()
        large_write = large_builder.new_write()
        commit = small_builder.new_commit()
        try:
            small_write.write_arrow(pa.Table.from_pydict({
                'id': [1], 'dt': ['p'], 'value': ['x'],
            }, schema=self.postpone_pa_schema))
            large_write.write_arrow(pa.Table.from_pydict({
                'id': [2], 'dt': ['p'], 'value': ['x' * 500],
            }, schema=self.postpone_pa_schema))
            small_messages = small_write.prepare_commit()
            large_messages = large_write.prepare_commit()
            self.assertEqual({1}, {m.total_buckets for m in small_messages})
            self.assertEqual({3}, {m.total_buckets for m in large_messages})
            with self.assertRaisesRegex(CommitConflictError, 'Total buckets'):
                commit.commit(small_messages + large_messages)
        finally:
            small_write.close()
            large_write.close()
            commit.close()

    def test_postpone_overwrite_bucket_plan_mismatch_fails_commit(self):
        from pypaimon.write.commit.conflict_detection import CommitConflictError

        table = self._create_postpone_table(
            'default.test_postpone_overwrite_plan_mismatch',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '100 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 3,
            },
        )
        small_builder = table.new_postpone_fixed_bucket_write_builder()
        large_builder = table.new_postpone_fixed_bucket_write_builder()
        small_builder.overwrite({'dt': 'p'})
        large_builder.overwrite({'dt': 'p'})
        small_write = small_builder.new_write()
        large_write = large_builder.new_write()
        commit = small_builder.new_commit()
        try:
            small_write.write_arrow(pa.Table.from_pydict({
                'id': [1], 'dt': ['p'], 'value': ['x'],
            }, schema=self.postpone_pa_schema))
            large_write.write_arrow(pa.Table.from_pydict({
                'id': [2], 'dt': ['p'], 'value': ['x' * 500],
            }, schema=self.postpone_pa_schema))
            messages = (
                small_write.prepare_commit() + large_write.prepare_commit())
            self.assertEqual({1, 3}, {m.total_buckets for m in messages})
            paths = [
                file.external_path or file.file_path
                for message in messages
                for file in message.new_files
            ]

            with self.assertRaisesRegex(CommitConflictError, 'Total buckets'):
                commit.commit(messages)
            self.assertTrue(all(
                not table.file_io.exists(path) for path in paths))
        finally:
            small_write.close()
            large_write.close()
            commit.close()

    def test_postpone_overwrite_allows_bucket_rescale(self):
        from pypaimon.write.postpone_bucket import PostponeBucketPlan

        table = self._create_postpone_table(
            'default.test_postpone_overwrite_rescale',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '1 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 2,
            },
        )
        self._commit_arrow(table, pa.Table.from_pydict({
            'id': [1], 'dt': ['p'], 'value': ['old'],
        }, schema=self.postpone_pa_schema), fixed_bucket=True)

        builder = table.new_postpone_fixed_bucket_write_builder()
        builder.with_bucket_plan(PostponeBucketPlan({('p',): 3}))
        builder.overwrite({'dt': 'p'})
        write = builder.new_write()
        commit = builder.new_commit()
        try:
            write.write_arrow(pa.Table.from_pydict({
                'id': [2], 'dt': ['p'], 'value': ['new'],
            }, schema=self.postpone_pa_schema))
            messages = write.prepare_commit()
            self.assertEqual({3}, {m.total_buckets for m in messages})
            commit.commit(messages)
        finally:
            write.close()
            commit.close()

        self.assertEqual(
            [2], self._read_sorted(table, 'id').column('id').to_pylist())

    def test_postpone_batch_fixed_bucket_reuses_existing_bucket_num(self):
        table = self._create_postpone_table(
            'default.test_postpone_reuse',
            pa_schema=self.pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '1 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 2,
            },
        )
        expected = pa.Table.from_pydict({
            'user_id': [1],
            'item_id': [1001],
            'behavior': ['a'],
            'dt': ['p1'],
        }, schema=self.pk_pa_schema)

        self._commit_arrow(table, expected, fixed_bucket=True)

        copied_table = table.copy({
            'postpone.batch-write-fixed-bucket.max-parallelism': 3,
        })
        from pypaimon.write.postpone_bucket import PostponeBucketPlanner

        planner = PostponeBucketPlanner(copied_table)
        plan = planner.plan({('p2',): (1, 10)})
        copied_write = (
            copied_table.new_postpone_fixed_bucket_write_builder()
            .with_bucket_plan(plan)
            .new_write()
        )
        self.assertEqual(2, copied_write.row_key_extractor.num_buckets(('p1',)))
        self.assertEqual(3, copied_write.row_key_extractor.num_buckets(('p2',)))
        copied_write.close()

    def test_postpone_reuses_bucket_num_for_int_date_partition(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('part', pa.int32(), nullable=False),
            pa.field('day', pa.date32(), nullable=False),
            ('value', pa.string()),
        ])
        table = self._create_postpone_table(
            'default.test_postpone_typed_partition_reuse',
            pa_schema,
            partition_keys=['part', 'day'],
            primary_keys=['id', 'part', 'day'],
            options={
                'postpone.target-size-per-bucket': '1 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 2,
            },
        )
        identifier = 'default.test_postpone_typed_partition_reuse'
        day = datetime.date(2026, 8, 1)

        self._commit_arrow(table, pa.Table.from_pydict({
            'id': [1],
            'part': [7],
            'day': [day],
            'value': ['first'],
        }, schema=pa_schema), fixed_bucket=True)

        # Reopen to load the existing bucket count from manifests.
        reopened = self.catalog.get_table(identifier).copy({
            'postpone.batch-write-fixed-bucket.max-parallelism': 3,
        })
        with self._postpone_write(reopened) as (second_write, second_commit):
            second_write.write_arrow(pa.Table.from_pydict({
                'id': [2, 3],
                'part': [7, 8],
                'day': [day, day],
                'value': ['existing', 'new'],
            }, schema=pa_schema))
            messages = second_write.prepare_commit()
            total_buckets = {
                tuple(message.partition): message.total_buckets
                for message in messages
            }
            self.assertEqual(2, total_buckets[(7, day)])
            self.assertEqual(3, total_buckets[(8, day)])
            second_commit.commit(messages)

        actual = self._read_sorted(reopened, 'id')
        self.assertEqual([1, 2, 3], actual.column('id').to_pylist())

    def test_postpone_legacy_partition_migrates_to_fixed_bucket(self):
        identifier = 'default.test_postpone_legacy_partition_migration'
        legacy = self._create_postpone_table(
            identifier,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'postpone.batch-write-fixed-bucket': False},
        )
        self._commit_arrow(legacy, pa.Table.from_pydict({
            'user_id': [1],
            'item_id': [1001],
            'behavior': ['legacy'],
            'dt': ['p1'],
        }, schema=self.pk_pa_schema))

        fixed = self.catalog.get_table(identifier).copy({
            'postpone.batch-write-fixed-bucket': True,
            'postpone.target-size-per-bucket': '1 b',
            'postpone.batch-write-fixed-bucket.max-parallelism': 2,
        })
        with self._postpone_write(fixed) as (fixed_write, fixed_commit):
            fixed_write.write_arrow(pa.Table.from_pydict({
                'user_id': [2],
                'item_id': [1002],
                'behavior': ['fixed'],
                'dt': ['p1'],
            }, schema=self.pk_pa_schema))
            messages = fixed_write.prepare_commit()
            # Legacy -2 files do not define a real bucket count.
            self.assertEqual({2}, {m.total_buckets for m in messages})
            fixed_commit.commit(messages)

        scanner = fixed.new_read_builder().new_scan().file_scanner
        manifests, _ = scanner.manifest_scanner()
        entries = scanner.manifest_file_manager.read_entries_parallel(
            manifests, drop_stats=False
        )
        partition_entries = [
            entry for entry in entries
            if tuple(entry.partition.values) == ('p1',)
        ]
        self.assertEqual({-2, 2}, {
            entry.total_buckets for entry in partition_entries
        })
        self.assertIn(-2, {entry.bucket for entry in partition_entries})
        self.assertTrue(any(entry.bucket >= 0 for entry in partition_entries))

        actual = self._read_sorted(fixed, 'user_id')
        self.assertEqual([2], actual.column('user_id').to_pylist())

    def test_postpone_overwrite_updates_catalog_bucket_count(self):
        table = self._create_postpone_table(
            'default.test_postpone_overwrite_bucket_statistics',
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
        )
        self._commit_arrow(table, pa.Table.from_pydict({
            'user_id': [1],
            'item_id': [1001],
            'behavior': ['legacy'],
            'dt': ['p1'],
        }, schema=self.pk_pa_schema))

        fixed = table.copy({
            'postpone.target-size-per-bucket': '1 b',
            'postpone.batch-write-fixed-bucket.max-parallelism': 2,
        })
        captured_statistics = []
        with self._postpone_write(
                fixed, overwrite={'dt': 'p1'}) as (write, commit):
            write.write_arrow(pa.Table.from_pydict({
                'user_id': [2],
                'item_id': [1002],
                'behavior': ['fixed'],
                'dt': ['p1'],
            }, schema=self.pk_pa_schema))
            messages = write.prepare_commit()
            real_commit = commit.file_store_commit.snapshot_commit.commit

            def capture_statistics(base_snapshot_uuid, snapshot, statistics):
                captured_statistics.extend(statistics)
                return real_commit(base_snapshot_uuid, snapshot, statistics)

            commit.file_store_commit.snapshot_commit.commit = capture_statistics
            commit.commit(messages)

        self.assertEqual(2, captured_statistics[0].total_buckets)
        self.assertEqual(
            [2], self._read_sorted(fixed, 'user_id').column('user_id').to_pylist()
        )

    def test_postpone_concurrent_new_partition_bucket_num_conflict(self):
        from pypaimon.write.commit.conflict_detection import CommitConflictError

        table_two_buckets = self._create_postpone_table(
            'default.test_postpone_concurrent_bucket_num',
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={
                'postpone.target-size-per-bucket': '1 b',
                'postpone.batch-write-fixed-bucket.max-parallelism': 2,
            },
        )
        table_three_buckets = table_two_buckets.copy({
            'postpone.batch-write-fixed-bucket.max-parallelism': 3,
        })

        builder_two = (
            table_two_buckets.new_postpone_fixed_bucket_write_builder())
        builder_three = (
            table_three_buckets.new_postpone_fixed_bucket_write_builder())
        write_two = builder_two.new_write()
        write_three = builder_three.new_write()
        commit_two = builder_two.new_commit()
        commit_three = builder_three.new_commit()
        try:
            write_two.write_arrow(pa.Table.from_pydict({
                'user_id': [1],
                'item_id': [1001],
                'behavior': ['a'],
                'dt': ['new-partition'],
            }, schema=self.pk_pa_schema))
            write_three.write_arrow(pa.Table.from_pydict({
                'user_id': [2],
                'item_id': [1002],
                'behavior': ['b'],
                'dt': ['new-partition'],
            }, schema=self.pk_pa_schema))

            messages_two = write_two.prepare_commit()
            messages_three = write_three.prepare_commit()
            self.assertEqual({2}, {m.total_buckets for m in messages_two})
            self.assertEqual({3}, {m.total_buckets for m in messages_three})

            losing_paths = [
                file.external_path or file.file_path
                for message in messages_three
                for file in message.new_files
            ]
            self.assertTrue(all(
                table_three_buckets.file_io.exists(path)
                for path in losing_paths
            ))
            concurrent_commit = {'done': False}

            def fail_cas_after_concurrent_commit(*_):
                if not concurrent_commit['done']:
                    concurrent_commit['done'] = True
                    commit_two.commit(messages_two)
                    return False
                raise AssertionError('Bucket conflict should precede another CAS')

            commit_three.file_store_commit.snapshot_commit.commit = (
                fail_cas_after_concurrent_commit)
            with self.assertRaisesRegex(CommitConflictError, "Total buckets"):
                commit_three.commit(messages_three)
            self.assertTrue(concurrent_commit['done'])
            self.assertTrue(all(
                not table_three_buckets.file_io.exists(path)
                for path in losing_paths
            ))
        finally:
            write_two.close()
            write_three.close()
            commit_two.close()
            commit_three.close()

    def test_uncertain_commit_then_cas_failure_keeps_files(self):
        table = self._create_postpone_table(
            'default.test_uncertain_commit_then_cas_failure',
            pa_schema=self.postpone_pa_schema,
            partition_keys=['dt'],
            primary_keys=['id', 'dt'],
        )
        builder = table.new_postpone_fixed_bucket_write_builder()
        write = builder.new_write()
        commit = builder.new_commit()
        try:
            write.write_arrow(pa.Table.from_pydict({
                'id': [1], 'dt': ['p'], 'value': ['v'],
            }, schema=self.postpone_pa_schema))
            messages = write.prepare_commit()
            data_paths = [
                file.external_path or file.file_path
                for message in messages
                for file in message.new_files
            ]
            uncertain_error = TimeoutError('lost commit response')
            file_store_commit = commit.file_store_commit
            file_store_commit.commit_max_retries = 1
            snapshot_commit = file_store_commit.snapshot_commit
            real_commit = snapshot_commit.commit
            attempts = 0

            def uncertain_then_cas_failure(base_uuid, snapshot, statistics):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    self.assertTrue(real_commit(base_uuid, snapshot, statistics))
                    self._commit_arrow(
                        table,
                        pa.Table.from_pydict({
                            'id': [2], 'dt': ['p'], 'value': ['v2'],
                        }, schema=self.postpone_pa_schema),
                        fixed_bucket=True,
                    )
                    raise uncertain_error
                return False

            real_get_snapshot = file_store_commit.snapshot_manager.get_snapshot_by_id

            def hide_first_snapshot(snapshot_id):
                return None if snapshot_id == 1 else real_get_snapshot(snapshot_id)

            # Keep the retry on the CAS path after snapshot 1 becomes unavailable.
            with patch.object(
                snapshot_commit,
                'commit',
                side_effect=uncertain_then_cas_failure,
            ), patch.object(
                file_store_commit.snapshot_manager,
                'get_snapshot_by_id',
                side_effect=hide_first_snapshot,
            ), patch.object(
                file_store_commit.conflict_detection,
                'check_conflicts',
                return_value=None,
            ), patch.object(file_store_commit, '_commit_retry_wait'):
                with self.assertRaises(RuntimeError) as context:
                    commit.commit(messages)

            self.assertIs(uncertain_error, context.exception.__cause__)
            self.assertEqual(2, attempts)
            self.assertTrue(all(table.file_io.exists(path) for path in data_paths))
            self.assertEqual(
                [1, 2], self._read_sorted(table, 'id').column('id').to_pylist()
            )
        finally:
            write.close()
            commit.close()

    def test_data_file_prefix_postpone(self):
        """Test that generated data file names follow the expected prefix format."""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'], primary_keys=['user_id', 'dt'],
                                            options={'bucket': -2, 'postpone.batch-write-fixed-bucket': False})
        self.catalog.create_table('default.test_file_prefix_postpone', schema, False)
        table = self.catalog.get_table('default.test_file_prefix_postpone')

        # Write some data to generate files
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pk_pa_schema)
        table_write.write_arrow(pa_table)

        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # Find generated data files
        table_path = os.path.join(self.warehouse, 'default.db', 'test_file_prefix_postpone')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.avro') or file.endswith('.orc'):
                    data_files.append(file)

        # Verify at least one data file was created
        self.assertGreater(len(data_files), 0, "No data files were generated")

        # Verify file name format: {table_prefix}-u-{commit_user}-s-{random_number}-w--{uuid}-0.{format}
        # Expected pattern: data--u-{user}-s-{random}-w--{uuid}-0.{format}
        expected_pattern = r'^data--u-.+-s-\d+-w-.+-0\.avro$'

        for file_name in data_files:
            self.assertRegex(file_name, expected_pattern,
                             f"File name '{file_name}' does not match expected prefix format")

            # Additional checks for specific components
            parts = file_name.split('-')
            self.assertEqual('data', parts[0], f"File prefix should start with 'data', got '{parts[0]}'")
            self.assertEqual('u', parts[2], f"Second part should be 'u', got '{parts[2]}'")
            self.assertEqual('s', parts[8], f"Fourth part should be 's', got '{parts[8]}'")
            self.assertEqual('w', parts[10], f"Sixth part should be 'w', got '{parts[10]}'")

    def test_data_file_prefix_default(self):
        """Test that generated data file names follow the expected prefix format."""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'])
        self.catalog.create_table('default.test_file_prefix_default', schema, False)
        table = self.catalog.get_table('default.test_file_prefix_default')

        # Write some data to generate files
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)

        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # Find generated data files
        table_path = os.path.join(self.warehouse, 'default.db', 'test_file_prefix_default')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.avro') or file.endswith('.orc'):
                    data_files.append(file)

        # Verify at least one data file was created
        self.assertGreater(len(data_files), 0, "No data files were generated")

        expected_pattern = r'^data-.+-0\.parquet$'

        for file_name in data_files:
            self.assertRegex(file_name, expected_pattern,
                             f"File name '{file_name}' does not match expected prefix format")

            # Additional checks for specific components
            parts = file_name.split('-')
            self.assertEqual('data', parts[0], f"File prefix should start with 'data', got '{parts[0]}'")

    def test_data_file_prefix(self):
        """Test that generated data file names follow the expected prefix format."""
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'],
                                            options={'data-file.prefix': 'test_prefix'})
        self.catalog.create_table('default.test_file_prefix', schema, False)
        table = self.catalog.get_table('default.test_file_prefix')

        # Write some data to generate files
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'user_id': [1, 2],
            'item_id': [1001, 1002],
            'behavior': ['a', 'b'],
            'dt': ['p1', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)

        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # Find generated data files
        table_path = os.path.join(self.warehouse, 'default.db', 'test_file_prefix')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for file in files:
                if file.endswith('.parquet') or file.endswith('.avro') or file.endswith('.orc'):
                    data_files.append(file)

        # Verify at least one data file was created
        self.assertGreater(len(data_files), 0, "No data files were generated")

        expected_pattern = r'^test_prefix.+-0\.parquet$'

        for file_name in data_files:
            self.assertRegex(file_name, expected_pattern,
                             f"File name '{file_name}' does not match expected prefix format")

    def test_dynamic_bucket_write(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'bucket': '-1'}
        )
        self.catalog.create_table(
            'default.test_dynamic_bucket', schema, False)
        table = self.catalog.get_table(
            'default.test_dynamic_bucket')
        expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
            'behavior': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h', 'i', 'j'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2', 'p2', 'p1']
        }, schema=self.pk_pa_schema)
        write_builder = table.new_batch_write_builder()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        sort_keys = [('user_id', 'ascending'), ('dt', 'ascending')]
        self.assertEqual(
            self.pk_expected.sort_by(sort_keys),
            actual.sort_by(sort_keys),
        )

    def test_column_subset_write_rejects_int64_for_int32(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(
            'default.test_column_subset_reject_int64', schema, False)
        table = self.catalog.get_table(
            'default.test_column_subset_reject_int64')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write().with_write_type(['id'])
        with self.assertRaises(ValueError) as e:
            table_write.write_arrow(pa.Table.from_pydict(
                {'id': [1]}))
        self.assertTrue(str(e.exception).startswith(
            "Input schema isn't consistent with table schema and write cols."))

    def test_write_pandas_respects_write_cols(self):
        import pandas as pd

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('score', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(
            'default.test_write_pandas_write_cols', schema, False)
        table = self.catalog.get_table(
            'default.test_write_pandas_write_cols')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write().with_write_type(['id', 'name'])
        table_commit = write_builder.new_commit()
        # DataFrame only carries the written subset; missing ``score`` is
        # padded with null on read.
        table_write.write_pandas(pd.DataFrame({
            'id': [1, 2],
            'name': ['a', 'b'],
        }))
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        expected = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['a', 'b'],
            'score': [None, None],
        }, schema=pa_schema)
        actual = self._read_sorted(table, 'id')
        self.assertEqual(expected, actual)

    def test_write_pandas_full_columns_unchanged(self):
        import pandas as pd

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(
            'default.test_write_pandas_full', schema, False)
        table = self.catalog.get_table('default.test_write_pandas_full')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_pandas(pd.DataFrame({
            'id': [1, 2],
            'name': ['a', 'b'],
        }))
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        expected = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['a', 'b'],
        }, schema=pa_schema)
        actual = self._read_sorted(table, 'id')
        self.assertEqual(expected, actual)

    def test_validate_schema_allows_binary_family_for_write_cols(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(
            'default.test_validate_binary_family', schema, False)
        table = self.catalog.get_table('default.test_validate_binary_family')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_write._validate_pyarrow_schema(pa.schema([
            ('id', pa.int32()),
            ('payload', pa.binary(4)),
        ]))

        table_write.with_write_type(['payload'])
        table_write._validate_pyarrow_schema(pa.schema([
            ('payload', pa.binary(4)),
        ]))

    @parameterized.expand([('parquet',), ('orc',), ('avro',)])
    def test_write_time_type(self, file_format):
        time_schema = pa.schema([
            ('id', pa.int32()),
            ('t', pa.time32('ms'))
        ])
        expected = pa.Table.from_pydict({
            'id': [1, 2, 3],
            't': [datetime.time(0, 0, 1), datetime.time(0, 0, 2), datetime.time(0, 0, 3)]
        }, schema=time_schema)

        table_name = 'default.test_write_time_' + file_format
        schema = Schema.from_pyarrow_schema(time_schema, options={'file.format': file_format})
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertEqual(expected, actual)

    def test_rolling(self):
        pa_schema = pa.schema([('name', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=[])
        self.catalog.create_table('default.test_rolling_recursion', schema, True)
        table = self.catalog.get_table('default.test_rolling_recursion')

        row_value = 'x' * 100
        sample = pa.Table.from_batches([
            pa.RecordBatch.from_pydict({'name': pa.array([row_value], type=pa.string())})
        ])
        # Set target just above single chunk nbytes so best_split=1 every time
        target = sample.nbytes + 1

        options = CoreOptions.copy(table.options)
        options.set(CoreOptions.TARGET_FILE_SIZE, str(target))
        writer = AppendOnlyDataWriter(
            table=table, partition=(), bucket=0,
            max_seq_number=0, options=options,
        )

        num_rows = 1500
        big_batch = pa.RecordBatch.from_pydict(
            {'name': pa.array([row_value] * num_rows, type=pa.string())}
        )
        writer.write(big_batch)

        pending_rows = writer.pending_data.num_rows if writer.pending_data is not None else 0
        committed_rows = sum(f.row_count for f in writer.committed_files)
        self.assertEqual(committed_rows + pending_rows, num_rows)
        self.assertGreater(len(writer.committed_files), 0)
        if writer.pending_data is not None:
            self.assertLessEqual(writer.pending_data.nbytes, target)
