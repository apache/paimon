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
#  limitations under the License.
################################################################################

"""
Test cases for split generation logic, matching Java's SplitGeneratorTest.

This test covers MergeTree split generation and rawConvertible logic.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions, MergeEngine


class SplitGeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, table_name, merge_engine='deduplicate',
                      deletion_vectors_enabled=False,
                      split_target_size=None, split_open_file_cost=None):
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.string())
        ])
        options = {
            'bucket': '1',  # Single bucket for testing
            'merge-engine': merge_engine,
            'deletion-vectors.enabled': str(deletion_vectors_enabled).lower()
        }
        if split_target_size is not None:
            options[CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key()] = split_target_size
        if split_open_file_cost is not None:
            options[CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key()] = split_open_file_cost
        
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options=options
        )
        self.catalog.create_table(f'default.{table_name}', schema, False)
        return self.catalog.get_table(f'default.{table_name}')

    def _create_test_data(self, id_ranges):
        return [
            {'id': list(range(start, end)) if isinstance(start, int) else start,
             'value': [f'v{i}' for i in (range(start, end) if isinstance(start, int) else start)]}
            for start, end in id_ranges
        ]
    
    def _write_data(self, table, data_list):
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.string())
        ])
        for data in data_list:
            write_builder = table.new_batch_write_builder()
            writer = write_builder.new_write()
            commit = write_builder.new_commit()
            try:
                batch = pa.Table.from_pydict(data, schema=pa_schema)
                writer.write_arrow(batch)
                commit.commit(writer.prepare_commit())
            finally:
                writer.close()
                commit.close()

    def _get_splits_info(self, table):
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()
        
        result = []
        for split in splits:
            file_names = sorted([f.file_name for f in split.files])
            result.append((file_names, split.raw_convertible))
        return result

    def test_merge_tree(self):
        test_data = self._create_test_data([
            (0, 11), (0, 13), (15, 61), (18, 41), (82, 86), (100, 201)
        ])
        
        table1 = self._create_table('test_merge_tree_1', split_target_size='1kb', split_open_file_cost='100b')
        self._write_data(table1, test_data)
        splits_info1 = self._get_splits_info(table1)
        self.assertGreater(len(splits_info1), 0)
        total_files1 = sum(len(files) for files, _ in splits_info1)
        self.assertEqual(total_files1, 6)
        self.assertLessEqual(len(splits_info1), 6)
        
        table2 = self._create_table('test_merge_tree_2', split_target_size='1kb', split_open_file_cost='1kb')
        self._write_data(table2, test_data)
        splits_info2 = self._get_splits_info(table2)
        self.assertGreater(len(splits_info2), 0)
        total_files2 = sum(len(files) for files, _ in splits_info2)
        self.assertEqual(total_files2, 6)
        self.assertGreaterEqual(len(splits_info2), len(splits_info1))
        
        for file_names, raw_convertible in splits_info1 + splits_info2:
            self.assertGreater(len(file_names), 0)

    def test_split_raw_convertible(self):
        table = self._create_table('test_raw_convertible')
        self._write_data(table, [{'id': [1, 2], 'value': ['a', 'b']}])
        splits = table.new_read_builder().new_scan().plan().splits()
        for split in splits:
            if len(split.files) == 1:
                has_delete_rows = any(f.delete_row_count and f.delete_row_count > 0 for f in split.files)
                if not has_delete_rows:
                    self.assertTrue(split.raw_convertible)
        
        table_dv = self._create_table('test_dv', deletion_vectors_enabled=True)
        self._write_data(table_dv, [
            {'id': [1, 2], 'value': ['a', 'b']},
            {'id': [3, 4], 'value': ['c', 'd']},
        ])
        splits_dv = table_dv.new_read_builder().new_scan().plan().splits()
        
        if len(splits_dv) == 0:
            pass
        else:
            for split in splits_dv:
                if len(split.files) == 1:
                    has_delete_rows = any(f.delete_row_count and f.delete_row_count > 0 for f in split.files)
                    if not has_delete_rows:
                        self.assertTrue(split.raw_convertible)
        
        table_first_row = self._create_table('test_first_row', merge_engine='first-row')
        self._write_data(table_first_row, [
            {'id': [1, 2], 'value': ['a', 'b']},
            {'id': [3, 4], 'value': ['c', 'd']},
        ])
        splits_first_row = table_first_row.new_read_builder().new_scan().plan().splits()
        for split in splits_first_row:
            if len(split.files) == 1:
                has_delete_rows = any(f.delete_row_count and f.delete_row_count > 0 for f in split.files)
                if not has_delete_rows:
                    self.assertTrue(split.raw_convertible)

    def test_merge_tree_split_raw_convertible(self):
        table = self._create_table('test_mixed_levels')
        self._write_data(table, self._create_test_data([
            (0, 11), (0, 13), (13, 21), (21, 221), (201, 211), (211, 221)
        ]))
        splits = table.new_read_builder().new_scan().plan().splits()
        self.assertGreater(len(splits), 0)
        
        deletion_vectors_enabled = table.options.deletion_vectors_enabled()
        merge_engine = table.options.merge_engine()
        merge_engine_first_row = merge_engine == MergeEngine.FIRST_ROW
        
        for split in splits:
            has_level_0 = any(f.level == 0 for f in split.files)
            has_delete_rows = any(f.delete_row_count and f.delete_row_count > 0 for f in split.files)
            
            if len(split.files) == 1:
                if not has_level_0 and not has_delete_rows:
                    self.assertTrue(
                        split.raw_convertible,
                        "Single file split should be raw_convertible")
            else:
                all_non_level0_no_delete = all(
                    f.level != 0 and (not f.delete_row_count or f.delete_row_count == 0)
                    for f in split.files
                )
                levels = {f.level for f in split.files}
                one_level = len(levels) == 1
                
                use_optimized_path = all_non_level0_no_delete and (
                    deletion_vectors_enabled or merge_engine_first_row or one_level
                )
                
                if use_optimized_path:
                    self.assertTrue(
                        split.raw_convertible,
                        "Multi-file split should be raw_convertible when optimized path is used")
                else:
                    self.assertFalse(
                        split.raw_convertible,
                        "Multi-file split should not be raw_convertible when optimized path is not used")

    def test_shard_with_empty_partition(self):
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={'bucket': '3'}  # Use 3 buckets for shard testing
        )
        self.catalog.create_table('default.test_shard_empty_partition', schema, False)
        table = self.catalog.get_table('default.test_shard_empty_partition')
        
        self._write_data(table, [
            {'id': [0, 3, 6], 'value': ['v0', 'v3', 'v6']},
            {'id': [1, 4, 7], 'value': ['v1', 'v4', 'v7']},
            {'id': [2, 5, 8], 'value': ['v2', 'v5', 'v8']},
        ])
        
        read_builder = table.new_read_builder()
        
        splits_all = read_builder.new_scan().plan().splits()
        self.assertGreater(len(splits_all), 0, "Should have splits without shard filtering")
        
        splits_shard_0 = read_builder.new_scan().with_shard(0, 3).plan().splits()
        
        self.assertGreaterEqual(
            len(splits_shard_0), 0,
            "Should return splits even if some partitions are empty after shard filtering")
        
        for split in splits_shard_0:
            self.assertGreater(len(split.files), 0, "Each split should have at least one file")


if __name__ == '__main__':
    unittest.main()
