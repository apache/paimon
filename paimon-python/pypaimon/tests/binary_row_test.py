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
import random
import tempfile
import unittest
from typing import List

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer


def _random_format():
    return random.choice(['parquet', 'avro', 'orc'])


class BinaryRowTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)
        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string()),
            ('f2', pa.int64()),
        ])
        cls.catalog.create_table('default.test_append', Schema.from_pyarrow_schema(
            pa_schema, partition_keys=['f0'], options={'file.format': _random_format()}), False)
        cls.catalog.create_table('default.test_pk', Schema.from_pyarrow_schema(
            pa_schema, partition_keys=['f2'], primary_keys=['f0'],
            options={'bucket': '1', 'file.format': _random_format()}), False)
        cls.data = pa.Table.from_pydict({
            'f0': [1, 2, 3, 4, 5],
            'f1': ['abc', 'abbc', 'bc', 'd', None],
            'f2': [6, 7, 8, 9, 10],
        }, schema=pa_schema)

        append_table = cls.catalog.get_table('default.test_append')
        write_builder = append_table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        write.write_arrow(cls.data)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

        pk_table = cls.catalog.get_table('default.test_pk')
        write_builder = pk_table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        write.write_arrow(cls.data)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

    def test_not_equal_append(self):
        table = self.catalog.get_table('default.test_append')
        self._overwrite_manifest_entry(table)
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.not_equal('f2', 6)  # test stats filter when filtering ManifestEntry
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(1, 4)
        self.assertEqual(expected, actual)
        self.assertEqual(len(expected), len(splits))

    def test_less_than_append(self):
        table = self.catalog.get_table('default.test_append')

        self._overwrite_manifest_entry(table)

        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.less_than('f2', 8)
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(0, 2)
        self.assertEqual(actual, expected)
        self.assertEqual(len(expected), len(splits))  # test stats filter when filtering ManifestEntry

    def test_is_null_append(self):
        table = self.catalog.get_table('default.test_append')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.is_null('f1')  # value_stats_cols=None
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(4, 1)
        self.assertEqual(expected, actual)
        self.assertEqual(len(expected), len(splits))

    def test_is_not_null_append(self):
        table = self.catalog.get_table('default.test_append')
        file_scanner = FileScanner(table, lambda: [])
        latest_snapshot = file_scanner.snapshot_manager.get_latest_snapshot()
        manifest_files = file_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = file_scanner.manifest_file_manager.read(manifest_files[0].file_name)
        self._transform_manifest_entries(manifest_entries, [])
        l = ['abc', 'abbc', 'bc', 'd', None]
        for i, entry in enumerate(manifest_entries):
            entry.file.value_stats_cols = ['f1']
            entry.file.value_stats = SimpleStats(
                GenericRow([l[i]], [table.fields[1]]),
                GenericRow([l[i]], [table.fields[1]]),
                [1 if l[i] is None else 0],
            )
        file_scanner.manifest_file_manager.write(manifest_files[0].file_name, manifest_entries)

        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.is_not_null('f1')
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(0, 4)
        self.assertEqual(expected, actual)
        self.assertEqual(len(expected), len(splits))

    def test_is_in_append(self):
        table = self.catalog.get_table('default.test_append')
        self._overwrite_manifest_entry(table)
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.is_in('f2', [6, 8])
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.take([0, 2])
        self.assertEqual(expected, actual)
        self.assertEqual(len(expected), len(splits))

    def test_equal_pk(self):
        table = self.catalog.get_table('default.test_pk')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('f2', 6)
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(0, 1)
        self.assertEqual(expected, actual)
        self.assertEqual(len(splits), len(expected))  # test partition filter when filtering ManifestEntry

    def test_not_equal_pk(self):
        table = self.catalog.get_table('default.test_pk')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.not_equal('f2', 6)
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(1, 4)
        self.assertEqual(actual, expected)
        self.assertEqual(len(splits), len(expected))  # test partition filter when filtering ManifestEntry

    def test_less_than_pk(self):
        table = self.catalog.get_table('default.test_pk')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.less_than('f0', 3)
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(0, 2)
        self.assertEqual(expected, actual)
        self.assertEqual(len(expected), len(splits))  # test key stats filter when filtering ManifestEntry

    def test_is_null_pk(self):
        table = self.catalog.get_table('default.test_pk')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.is_null('f1')
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(4, 1)
        self.assertEqual(actual, expected)

    def test_is_not_null_pk(self):
        table = self.catalog.get_table('default.test_pk')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.is_not_null('f1')
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        expected = self.data.slice(0, 4)
        self.assertEqual(actual, expected)

    def test_is_in_pk(self):
        table = self.catalog.get_table('default.test_pk')
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.is_in('f0', [1, 5])
        splits, actual = self._read_result(read_builder.with_filter(predicate))
        # expected rows: indices [0, 3]
        expected = self.data.take([0, 4])
        self.assertEqual(actual, expected)
        self.assertEqual(len(splits), len(expected))  # test key stats filter when filtering ManifestEntry

    def test_append_multi_cols(self):
        # Create a 10-column append table and write 10 rows
        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string()),
            ('f2', pa.int64()),
            ('f3', pa.string()),
            ('f4', pa.int64()),
            ('f5', pa.string()),
            ('f6', pa.int64()),
            ('f7', pa.string()),
            ('f8', pa.int64()),
            ('f9', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['f0'],
            options={'file.format': _random_format()}
        )
        self.catalog.create_table('default.test_append_10cols', schema, False)
        table = self.catalog.get_table('default.test_append_10cols')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {
            'f0': list(range(1, 11)),  # 0..9
            'f1': ['a0', 'bb', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9'],  # contains 'bb' at index 1
            'f2': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            'f3': ['x0', 'x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8', 'x9'],
            'f4': [0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
            'f5': ['y0', 'y1', 'y2', 'y3', 'y4', 'y5', 'y6', 'y7', 'y8', 'y9'],
            'f6': [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            'f7': ['z0', 'z1', 'z2', 'z3', 'z4', 'z5', 'z6', 'z7', 'z8', 'z9'],
            'f8': [5, 4, 3, 2, 1, 0, -1, -2, -3, -4],
            'f9': ['w0', 'w1', 'w2', 'w3', 'w4', 'w5', 'w6', 'w7', 'w8', 'w9'],
        }
        pa_table = pa.Table.from_pydict(data, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        file_scanner = FileScanner(table, lambda: [])
        latest_snapshot = file_scanner.snapshot_manager.get_latest_snapshot()
        manifest_files = file_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = file_scanner.manifest_file_manager.read(manifest_files[0].file_name)
        self._transform_manifest_entries(manifest_entries, [])
        for i, entry in enumerate(manifest_entries):
            entry.file.value_stats_cols = ['f2', 'f6', 'f8']
            entry.file.value_stats = SimpleStats(
                GenericRow([10 * (i + 1), 100 * (i + 1), 5 - i], [table.fields[2], table.fields[6], table.fields[8]]),
                GenericRow([10 * (i + 1), 100 * (i + 1), 5 - i], [table.fields[2], table.fields[6], table.fields[8]]),
                [0, 0, 0],
            )
        file_scanner.manifest_file_manager.write(manifest_files[0].file_name, manifest_entries)
        # Build multiple predicates and combine them
        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        p_in = predicate_builder.is_in('f6', [100, 600, 1000])
        p_contains = predicate_builder.less_or_equal('f8', -3)
        p_not_null = predicate_builder.is_not_null('f2')
        p_ge = predicate_builder.greater_or_equal('f2', 50)
        p_or = predicate_builder.or_predicates([p_in, p_contains])
        combined = predicate_builder.and_predicates([p_or, p_not_null, p_ge])

        splits, actual = self._read_result(read_builder.with_filter(combined))

        # Expected rows after filter: indices 5 and 9
        expected_data = {'f0': [6, 9, 10],
                         'f1': ['a5', 'a8', 'a9'],
                         'f2': [60, 90, 100],
                         'f3': ['x5', 'x8', 'x9'],
                         'f4': [1, 0, 1],
                         'f5': ['y5', 'y8', 'y9'],
                         'f6': [600, 900, 1000],
                         'f7': ['z5', 'z8', 'z9'],
                         'f8': [0, -3, -4],
                         'f9': ['w5', 'w8', 'w9']
                         }
        self.assertEqual(expected_data, actual.to_pydict())

        file_scanner = FileScanner(table, lambda: [])
        latest_snapshot = file_scanner.snapshot_manager.get_latest_snapshot()
        manifest_files = file_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = file_scanner.manifest_file_manager.read(manifest_files[0].file_name)
        self._transform_manifest_entries(manifest_entries, [])
        for i, entry in enumerate(manifest_entries):
            entry.file.value_stats_cols = ['f2', 'f6', 'f8']
            entry.file.value_stats = SimpleStats(
                GenericRow([0, 100 * (i + 1), 5 - i], [table.fields[2], table.fields[6], table.fields[8]]),
                GenericRow([0, 100 * (i + 1), 5 - i], [table.fields[2], table.fields[6], table.fields[8]]),
                [0, 0, 0],
            )
        file_scanner.manifest_file_manager.write(manifest_files[0].file_name, manifest_entries)
        splits, actual = self._read_result(read_builder.with_filter(combined))
        self.assertFalse(actual)

    def _read_result(self, read_builder):
        scan = read_builder.new_scan()
        read = read_builder.new_read()
        splits = scan.plan().splits()
        actual = read.to_arrow(splits)
        return splits, actual

    def _transform_manifest_entries(self, manifest_entries: List[ManifestEntry], trimmed_pk_fields):
        for entry in manifest_entries:
            entry.file.key_stats.min_values = GenericRowDeserializer.from_bytes(entry.file.key_stats.min_values.data,
                                                                                trimmed_pk_fields)
            entry.file.key_stats.max_values = GenericRowDeserializer.from_bytes(entry.file.key_stats.max_values.data,
                                                                                trimmed_pk_fields)

    def _overwrite_manifest_entry(self, table):
        file_scanner = FileScanner(table, lambda: [])
        latest_snapshot = file_scanner.snapshot_manager.get_latest_snapshot()
        manifest_files = file_scanner.manifest_list_manager.read_all(latest_snapshot)
        manifest_entries = file_scanner.manifest_file_manager.read(manifest_files[0].file_name)
        self._transform_manifest_entries(manifest_entries, [])
        for i, entry in enumerate(manifest_entries):
            entry.file.value_stats_cols = ['f2']
            entry.file.value_stats = SimpleStats(
                GenericRow([6 + i], [table.fields[2]]),
                GenericRow([6 + i], [table.fields[2]]),
                [0],
            )
        file_scanner.manifest_file_manager.write(manifest_files[0].file_name, manifest_entries)
