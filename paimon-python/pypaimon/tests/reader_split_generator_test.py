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
            pa.field('id', pa.int64(), nullable=False),
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
            pa.field('id', pa.int64(), nullable=False),
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
            
            merged_count = split.merged_row_count()
            if merged_count is not None:
                self.assertGreaterEqual(merged_count, 0, "merged_row_count should be non-negative")
                self.assertLessEqual(
                    merged_count, split.row_count,
                    "merged_row_count should be <= row_count")

    def test_shard_with_empty_partition(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
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
            
            merged_count = split.merged_row_count()
            if merged_count is not None:
                self.assertGreaterEqual(merged_count, 0, "merged_row_count should be non-negative")
                self.assertLessEqual(
                    merged_count, split.row_count,
                    "merged_row_count should be <= row_count")
            
            from pypaimon.read.sliced_split import SlicedSplit
            if isinstance(split, SlicedSplit):
                sliced_merged = split.merged_row_count()
                if split.shard_file_idx_map():
                    self.assertEqual(
                        sliced_merged, split.row_count,
                        "SlicedSplit with shard_file_idx_map should return row_count as merged_row_count")
                else:
                    underlying_merged = split.data_split().merged_row_count()
                    self.assertEqual(
                        sliced_merged, underlying_merged,
                        "SlicedSplit without shard_file_idx_map should delegate to underlying split")

    def test_sliced_split_merged_row_count(self):
        """Test merged_row_count() for SlicedSplit with explicit slicing."""
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('value', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={'bucket': '1'}  # Single bucket to ensure splits can be sliced
        )
        self.catalog.create_table('default.test_sliced_split', schema, False)
        table = self.catalog.get_table('default.test_sliced_split')
        
        # Write enough data to create multiple files/splits
        # Write in multiple batches to potentially create multiple files
        for i in range(3):
            self._write_data(table, [{'id': list(range(i * 10, (i + 1) * 10)),
                                     'value': [f'v{j}' for j in range(i * 10, (i + 1) * 10)]}])
        
        read_builder = table.new_read_builder()
        splits_all = read_builder.new_scan().plan().splits()
        self.assertGreater(len(splits_all), 0, "Should have splits")
        
        # Use with_shard to potentially create SlicedSplit
        # Using multiple shards increases chance of creating SlicedSplit
        from pypaimon.read.sliced_split import SlicedSplit
        
        for shard_idx in range(3):
            splits_shard = read_builder.new_scan().with_shard(shard_idx, 3).plan().splits()
            for split in splits_shard:
                # Test merged_row_count for all splits
                merged_count = split.merged_row_count()
                if merged_count is not None:
                    self.assertGreaterEqual(merged_count, 0, "merged_row_count should be non-negative")
                    self.assertLessEqual(
                        merged_count, split.row_count,
                        "merged_row_count should be <= row_count")
                
                # Explicitly test SlicedSplit if present
                if isinstance(split, SlicedSplit):
                    sliced_merged = split.merged_row_count()
                    shard_map = split.shard_file_idx_map()
                    if shard_map:
                        # When shard_file_idx_map is present, merged_row_count should equal row_count
                        self.assertEqual(
                            sliced_merged, split.row_count,
                            "SlicedSplit with shard_file_idx_map should return row_count as merged_row_count")
                    else:
                        # When shard_file_idx_map is empty, should delegate to underlying split
                        underlying_merged = split.data_split().merged_row_count()
                        self.assertEqual(
                            sliced_merged, underlying_merged,
                            "SlicedSplit without shard_file_idx_map should delegate to underlying split")
        
        # Note: SlicedSplit may or may not be created depending on data distribution
        # This test ensures that if SlicedSplit is created, merged_row_count() works correctly


class ApplyPushDownLimitUnitTest(unittest.TestCase):
    """Direct, mock-driven coverage of ``FileScanner._apply_push_down_limit``.

    Pypaimon's writer doesn't compact L0 → L1+, and the DV-enabled
    PK-table read path skips L0 files, so a true DV-aware
    ``raw_convertible`` split (where ``merged_row_count < row_count``)
    is hard to produce from a pure-Python end-to-end fixture. The
    accumulator semantics, however, are a simple loop on the splits
    list — exercise it directly with synthetic split stand-ins.

    These cases pin down the correctness contract without depending on
    storage layout: the accumulator must use ``merged_row_count``
    (matching Java's ``partialMergedRowCount``) and must keep every
    split it has visited up to and including the one that meets the
    budget.
    """

    @staticmethod
    def _apply(splits, limit):
        from pypaimon.read.scanner.file_scanner import FileScanner

        # Stand in for ``self`` — only ``self.limit`` is read by the method.
        class _FakeScanner:
            pass

        scanner = _FakeScanner()
        scanner.limit = limit
        return FileScanner._apply_push_down_limit(scanner, splits)

    @staticmethod
    def _split(raw_convertible, row_count, merged_row_count):
        class _FakeSplit:
            pass

        s = _FakeSplit()
        s.raw_convertible = raw_convertible
        s.row_count = row_count
        s._merged = merged_row_count

        def _merged_fn():
            return s._merged

        s.merged_row_count = _merged_fn
        return s

    def test_dv_aware_accumulator_uses_merged_row_count(self):
        """[raw(row_count=10, dv→merged=4), non-raw, non-raw] + limit=5.

        Pre-fix accumulator (``+= row_count``): the raw split's pre-DV
        count of 10 already meets ``limit=5``, the loop early-returns
        with just ``[raw]`` and the two non-raw splits are dropped. The
        reader can then only see 4 rows from the DV split — silently
        less than ``limit``.

        Post-fix accumulator (``+= merged_row_count``): only 4 rows of
        budget after the raw split, so 4 < 5; the loop keeps walking,
        adds the two non-raw splits without changing the accumulator,
        and falls through to ``return splits`` with all three. The
        reader then has enough material across three splits to produce
        ``limit`` rows.
        """
        s_raw = self._split(raw_convertible=True, row_count=10, merged_row_count=4)
        s_nr1 = self._split(raw_convertible=False, row_count=10, merged_row_count=None)
        s_nr2 = self._split(raw_convertible=False, row_count=10, merged_row_count=None)

        result = self._apply([s_raw, s_nr1, s_nr2], limit=5)
        self.assertEqual(
            len(result), 3,
            "merged_row_count accumulator must NOT early-return after a "
            "DV-aware raw split whose post-DV count is below the limit; "
            "got {}".format([id(s) for s in result]),
        )

    def test_accumulator_skips_splits_with_unknown_merged_count(self):
        """``merged_row_count`` returns ``None`` for layouts where the
        DV cardinality / data-evolution range isn't recorded yet (e.g.
        a non-raw split, or a raw split missing DV cardinality in the
        manifest). Such splits cannot meaningfully contribute to the
        budget — Java's ``applyPushDownLimit`` skips them in the
        accumulator loop and falls through to the full split list when
        the loop completes without reaching the limit. We mirror that:
        with a single split whose ``merged_row_count`` is unavailable,
        the loop never accumulates anything and we return the input
        unchanged."""
        s = self._split(raw_convertible=True, row_count=10, merged_row_count=None)
        result = self._apply([s], limit=5)
        self.assertEqual(len(result), 1)
        self.assertIs(result[0], s)

    def test_no_raw_splits_falls_through_to_full_list(self):
        """No raw splits → accumulator never moves → loop completes →
        fallback returns the full list (matching Java's behaviour for
        the all-non-raw case)."""
        s1 = self._split(raw_convertible=False, row_count=10, merged_row_count=None)
        s2 = self._split(raw_convertible=False, row_count=10, merged_row_count=None)
        result = self._apply([s1, s2], limit=5)
        self.assertEqual(result, [s1, s2])

    def test_empty_splits_returns_empty(self):
        self.assertEqual(self._apply([], limit=5), [])

    def test_no_limit_returns_input_unchanged(self):
        s = self._split(raw_convertible=True, row_count=10, merged_row_count=10)
        result = self._apply([s], limit=None)
        self.assertEqual(result, [s])


if __name__ == '__main__':
    unittest.main()
