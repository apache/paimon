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

import unittest
from datetime import timedelta
from typing import Dict, Tuple

from pypaimon.compact import (
    CompactBuilder,
    AppendCompactTask,
    FullCompactStrategy,
    MinorCompactStrategy,
    CompactionStrategyFactory,
    PartitionBinaryPredicate,
    PartitionAndPredicate,
    PartitionOrPredicate,
    PartitionPredicateConverter,
)


class MockFileStoretable:
    """Mock FileStoreTable for testing."""

    def __init__(self, is_primary_key: bool = False):
        self.is_primary_key_table = is_primary_key
        self.options = {
            'target-file-size': '256mb',
            'compaction.min.file-num': '5',
            'compaction.delete-ratio-threshold': '0.2',
        }
        self.partition_keys = ['dt', 'hour']
        self.identifier = 'test.table'

    def bucket_mode(self):
        """Mock bucket mode."""
        from pypaimon.table.bucket_mode import BucketMode
        return BucketMode.BUCKET_UNAWARE

    def snapshot_manager(self):
        """Mock snapshot manager."""
        return MockSnapshotManager()

    def copy(self, options: Dict[str, str]) -> 'MockFileStoretable':
        """Create a copy with modified options."""
        new_table = MockFileStoretable(self.is_primary_key_table)
        new_table.options.update(options)
        return new_table


class MockSnapshotManager:
    """Mock SnapshotManager for testing."""

    def latest_snapshot(self):
        """Return a mock snapshot."""
        return MockSnapshot()


class MockSnapshot:
    """Mock Snapshot for testing."""

    def file_iterator(self):
        """Return an iterator of mock file entries."""
        return iter([
            MockFileEntry(('2024-01-01', '00'), 0, 100),
            MockFileEntry(('2024-01-01', '00'), 0, 150),
            MockFileEntry(('2024-01-01', '01'), 0, 200),
        ])


class MockFileEntry:
    """Mock file entry from snapshot."""

    def __init__(self, partition: Tuple, bucket: int, size: int):
        self._partition = partition
        self._bucket = bucket
        self._size = size

    def partition(self):
        """Return partition key."""
        return self._partition

    def bucket(self):
        """Return bucket id."""
        return self._bucket

    def file_size(self):
        """Return file size."""
        return self._size


class TestCompactBuilder(unittest.TestCase):
    """Test cases for CompactBuilder."""

    def setUp(self):
        """Set up test fixtures."""
        self.table = MockFileStoretable(is_primary_key=False)

    def test_compact_builder_creation(self):
        """Test creating a compact builder."""
        builder = CompactBuilder(self.table)
        self.assertIsNotNone(builder)
        self.assertEqual(builder._strategy, CompactBuilder.MINOR_COMPACTION)

    def test_with_strategy_full(self):
        """Test setting strategy to full."""
        builder = CompactBuilder(self.table)
        result = builder.with_strategy(CompactBuilder.FULL_COMPACTION)
        self.assertIs(result, builder)
        self.assertEqual(builder._strategy, CompactBuilder.FULL_COMPACTION)
        self.assertTrue(builder._full_compaction)

    def test_with_strategy_minor(self):
        """Test setting strategy to minor."""
        builder = CompactBuilder(self.table)
        result = builder.with_strategy(CompactBuilder.MINOR_COMPACTION)
        self.assertIs(result, builder)
        self.assertEqual(builder._strategy, CompactBuilder.MINOR_COMPACTION)
        self.assertFalse(builder._full_compaction)

    def test_with_strategy_invalid(self):
        """Test setting invalid strategy."""
        builder = CompactBuilder(self.table)
        with self.assertRaises(ValueError):
            builder.with_strategy('invalid')

    def test_with_partitions(self):
        """Test specifying partitions."""
        builder = CompactBuilder(self.table)
        partitions = [
            {'dt': '2024-01-01', 'hour': '00'},
            {'dt': '2024-01-01', 'hour': '01'},
        ]
        result = builder.with_partitions(partitions)
        self.assertIs(result, builder)
        self.assertEqual(builder._partitions, partitions)
        self.assertIsNone(builder._where_sql)

    def test_with_where(self):
        """Test specifying WHERE condition."""
        builder = CompactBuilder(self.table)
        where_sql = "dt > '2024-01-01'"
        result = builder.with_where(where_sql)
        self.assertIs(result, builder)
        self.assertEqual(builder._where_sql, where_sql)
        self.assertIsNone(builder._partitions)

    def test_with_partition_idle_time_string(self):
        """Test specifying partition idle time as string."""
        builder = CompactBuilder(self.table)
        result = builder.with_partition_idle_time('1d')
        self.assertIs(result, builder)
        self.assertEqual(builder._partition_idle_time, timedelta(days=1))

    def test_with_partition_idle_time_timedelta(self):
        """Test specifying partition idle time as timedelta."""
        builder = CompactBuilder(self.table)
        idle_time = timedelta(hours=2)
        result = builder.with_partition_idle_time(idle_time)
        self.assertIs(result, builder)
        self.assertEqual(builder._partition_idle_time, idle_time)

    def test_with_options(self):
        """Test specifying options."""
        builder = CompactBuilder(self.table)
        options = {'sink.parallelism': '4'}
        result = builder.with_options(options)
        self.assertIs(result, builder)
        self.assertEqual(builder._options, options)

    def test_method_chaining(self):
        """Test method chaining."""
        builder = CompactBuilder(self.table)
        result = (
            builder
            .with_strategy(CompactBuilder.FULL_COMPACTION)
            .with_options({'sink.parallelism': '8'})
        )
        self.assertIs(result, builder)
        self.assertEqual(builder._strategy, CompactBuilder.FULL_COMPACTION)
        self.assertEqual(builder._options, {'sink.parallelism': '8'})

    def test_validate_configuration_no_conflict(self):
        """Test configuration validation with no conflicts."""
        builder = CompactBuilder(self.table)
        builder.with_strategy(CompactBuilder.FULL_COMPACTION)
        # Should not raise exception
        builder._validate_configuration()

    def test_validate_configuration_partition_methods_conflict(self):
        """Test configuration validation with conflicting partition methods."""
        builder = CompactBuilder(self.table)
        builder.with_partitions([{'dt': '2024-01-01'}])
        builder._where_sql = "dt > '2024-01-01'"
        # Should raise ValueError
        with self.assertRaises(ValueError):
            builder._validate_configuration()

    def test_parse_duration_days(self):
        """Test parsing duration with days."""
        duration = CompactBuilder._parse_duration('1d')
        self.assertEqual(duration, timedelta(days=1))

    def test_parse_duration_hours(self):
        """Test parsing duration with hours."""
        duration = CompactBuilder._parse_duration('2h')
        self.assertEqual(duration, timedelta(hours=2))

    def test_parse_duration_minutes(self):
        """Test parsing duration with minutes."""
        duration = CompactBuilder._parse_duration('30m')
        self.assertEqual(duration, timedelta(minutes=30))

    def test_parse_duration_seconds(self):
        """Test parsing duration with seconds."""
        duration = CompactBuilder._parse_duration('60s')
        self.assertEqual(duration, timedelta(seconds=60))

    def test_parse_duration_invalid(self):
        """Test parsing invalid duration."""
        with self.assertRaises(ValueError):
            CompactBuilder._parse_duration('invalid')


class TestCompactStrategy(unittest.TestCase):
    """Test cases for compaction strategies."""

    def test_full_compact_strategy(self):
        """Test full compaction strategy."""
        strategy = FullCompactStrategy()
        files = [1, 2, 3, 4, 5]
        selected = strategy.select_files(files)
        self.assertEqual(selected, files)

    def test_minor_compact_strategy(self):
        """Test minor compaction strategy."""
        strategy = MinorCompactStrategy(target_file_size=200)
        files = [
            MockFileEntry(('2024-01-01', '00'), 0, 100),
            MockFileEntry(('2024-01-01', '00'), 0, 150),
            MockFileEntry(('2024-01-01', '00'), 0, 300),
        ]
        selected = strategy.select_files(files)
        # Should select files smaller than 200
        self.assertEqual(len(selected), 2)

    def test_compact_strategy_factory_full(self):
        """Test strategy factory creating full strategy."""
        strategy = CompactionStrategyFactory.create_strategy('full')
        self.assertIsInstance(strategy, FullCompactStrategy)

    def test_compact_strategy_factory_minor(self):
        """Test strategy factory creating minor strategy."""
        strategy = CompactionStrategyFactory.create_strategy('minor')
        self.assertIsInstance(strategy, MinorCompactStrategy)

    def test_compact_strategy_factory_invalid(self):
        """Test strategy factory with invalid strategy."""
        with self.assertRaises(ValueError):
            CompactionStrategyFactory.create_strategy('invalid')


class TestPartitionPredicate(unittest.TestCase):
    """Test cases for partition predicates."""

    def test_binary_predicate_equals(self):
        """Test binary predicate with equality."""
        pred = PartitionBinaryPredicate('dt', '=', '2024-01-01')
        self.assertTrue(pred.matches({'dt': '2024-01-01'}))
        self.assertFalse(pred.matches({'dt': '2024-01-02'}))

    def test_binary_predicate_greater_than(self):
        """Test binary predicate with greater than."""
        pred = PartitionBinaryPredicate('dt', '>', '2024-01-01')
        self.assertTrue(pred.matches({'dt': '2024-01-02'}))
        self.assertFalse(pred.matches({'dt': '2024-01-01'}))

    def test_binary_predicate_less_than(self):
        """Test binary predicate with less than."""
        pred = PartitionBinaryPredicate('hour', '<', 12)
        self.assertTrue(pred.matches({'hour': 10}))
        self.assertFalse(pred.matches({'hour': 12}))

    def test_and_predicate(self):
        """Test AND predicate."""
        pred1 = PartitionBinaryPredicate('dt', '=', '2024-01-01')
        pred2 = PartitionBinaryPredicate('hour', '<', 12)
        and_pred = PartitionAndPredicate([pred1, pred2])

        self.assertTrue(and_pred.matches({'dt': '2024-01-01', 'hour': 10}))
        self.assertFalse(and_pred.matches({'dt': '2024-01-02', 'hour': 10}))
        self.assertFalse(and_pred.matches({'dt': '2024-01-01', 'hour': 12}))

    def test_or_predicate(self):
        """Test OR predicate."""
        pred1 = PartitionBinaryPredicate('dt', '=', '2024-01-01')
        pred2 = PartitionBinaryPredicate('dt', '=', '2024-01-02')
        or_pred = PartitionOrPredicate([pred1, pred2])

        self.assertTrue(or_pred.matches({'dt': '2024-01-01'}))
        self.assertTrue(or_pred.matches({'dt': '2024-01-02'}))
        self.assertFalse(or_pred.matches({'dt': '2024-01-03'}))


class TestPartitionPredicateConverter(unittest.TestCase):
    """Test cases for partition predicate converter."""

    def test_parse_binary_predicate_equals(self):
        """Test parsing binary predicate with equals."""
        pred = PartitionPredicateConverter.from_sql_where(
            "dt = '2024-01-01'",
            ['dt', 'hour']
        )
        self.assertIsNotNone(pred)
        if pred is not None:
            self.assertTrue(pred.matches({'dt': '2024-01-01'}))

    def test_parse_and_predicate(self):
        """Test parsing AND predicate."""
        pred = PartitionPredicateConverter.from_sql_where(
            "dt = '2024-01-01' and hour < 12",
            ['dt', 'hour']
        )
        self.assertIsNotNone(pred)
        if pred is not None:
            self.assertTrue(pred.matches({'dt': '2024-01-01', 'hour': 10}))
            self.assertFalse(pred.matches({'dt': '2024-01-02', 'hour': 10}))

    def test_parse_empty_where(self):
        """Test parsing empty WHERE clause."""
        pred = PartitionPredicateConverter.from_sql_where(
            "",
            ['dt', 'hour']
        )
        self.assertIsNone(pred)


class TestCompactTask(unittest.TestCase):
    """Test cases for compaction tasks."""

    def test_append_compact_task_creation(self):
        """Test creating append compact task."""
        partition = ('2024-01-01', '00')
        files = [1, 2, 3]
        task = AppendCompactTask(partition, 0, files)

        self.assertEqual(task.get_partition(), partition)
        self.assertEqual(task.get_bucket(), 0)
        self.assertEqual(task.get_files(), files)

    def test_append_compact_task_execute(self):
        """Test executing append compact task."""
        partition = ('2024-01-01', '00')
        files = [1, 2, 3]
        task = AppendCompactTask(partition, 0, files)
        # Should not raise exception
        task.execute()

    def test_append_compact_task_execute_no_files(self):
        """Test executing append compact task with no files."""
        partition = ('2024-01-01', '00')
        task = AppendCompactTask(partition, 0, [])
        # Should raise exception
        with self.assertRaises(ValueError):
            task.execute()


if __name__ == '__main__':
    unittest.main()
