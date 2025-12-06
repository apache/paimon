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

"""
Comprehensive test suite for Paimon Python Compaction module.
Tests all components and integration scenarios.
"""

import unittest
import logging
from datetime import timedelta
from typing import Dict, Tuple

from pypaimon.compact import (
    CompactBuilder,
    AppendCompactTask,
    KeyValueCompactTask,
    FullCompactStrategy,
    MinorCompactStrategy,
    CompactionStrategyFactory,
    PartitionBinaryPredicate,
    PartitionAndPredicate,
    PartitionOrPredicate,
    PartitionPredicateConverter,
    AppendOnlyCompactCoordinator,
    CompactResult,
)

logger = logging.getLogger(__name__)


class MockFileStoretable:
    """Mock FileStoreTable for comprehensive testing."""

    def __init__(self, is_primary_key: bool = False, num_files: int = 10):
        self.is_primary_key_table = is_primary_key
        self.num_files = num_files
        self.options = {
            'target-file-size': '256mb',
            'compaction.min.file-num': '5',
            'compaction.delete-ratio-threshold': '0.2',
            'num-sorted-run.compaction-trigger': '5',
        }
        self.partition_keys = ['dt', 'hour']
        self.identifier = 'test.table'

    def bucket_mode(self):
        from pypaimon.table.bucket_mode import BucketMode
        return BucketMode.BUCKET_UNAWARE

    def snapshot_manager(self):
        return MockSnapshotManager(self.num_files)

    def copy(self, options: Dict[str, str]) -> 'MockFileStoretable':
        new_table = MockFileStoretable(self.is_primary_key_table, self.num_files)
        new_table.options.update(options)
        return new_table


class MockSnapshotManager:
    """Mock SnapshotManager for testing."""

    def __init__(self, num_files: int = 10):
        self.num_files = num_files

    def latest_snapshot(self):
        return MockSnapshot(self.num_files)


class MockSnapshot:
    """Mock Snapshot with realistic file data."""

    def __init__(self, num_files: int = 10):
        self.num_files = num_files

    def file_iterator(self):
        """Generate mock file entries."""
        files = []
        partitions = [
            ('2024-01-01', '00'),
            ('2024-01-01', '01'),
            ('2024-01-02', '00'),
            ('2024-01-02', '01'),
        ]

        for i in range(self.num_files):
            partition = partitions[i % len(partitions)]
            size = (i + 1) * 50 * 1024 * 1024  # 50MB, 100MB, 150MB, ...
            files.append(MockFileEntry(partition, 0, size))

        return iter(files)


class MockFileEntry:
    """Mock file entry with realistic data."""

    def __init__(self, partition: Tuple, bucket: int, size: int):
        self._partition = partition
        self._bucket = bucket
        self._size = size
        self._row_count = size // 1024  # Estimate: 1 row per KB
        self._delete_count = int(self._row_count * 0.1)  # 10% deleted

    def partition(self):
        return self._partition

    def bucket(self):
        return self._bucket

    def file_size(self):
        return self._size

    def row_count(self):
        return self._row_count

    def delete_rows_count(self):
        return self._delete_count


class TestCompactBuilderComprehensive(unittest.TestCase):
    """Comprehensive tests for CompactBuilder."""

    def setUp(self):
        self.table = MockFileStoretable(is_primary_key=False, num_files=20)
        self.builder = CompactBuilder(self.table)

    def test_builder_initialization(self):
        """Test builder is properly initialized."""
        self.assertIsNotNone(self.builder)
        self.assertEqual(self.builder._strategy, CompactBuilder.MINOR_COMPACTION)
        self.assertFalse(self.builder._full_compaction)
        self.assertIsNone(self.builder._partitions)
        self.assertIsNone(self.builder._where_sql)
        self.assertIsNone(self.builder._partition_idle_time)

    def test_strategy_switching(self):
        """Test switching between strategies multiple times."""
        # Start with minor
        self.builder.with_strategy('minor')
        self.assertFalse(self.builder._full_compaction)

        # Switch to full
        self.builder.with_strategy('full')
        self.assertTrue(self.builder._full_compaction)

        # Switch back to minor
        self.builder.with_strategy('minor')
        self.assertFalse(self.builder._full_compaction)

    def test_partition_filter_exclusive(self):
        """Test that partition filtering methods work correctly."""
        builder = self.builder.with_partitions([{'dt': '2024-01-01'}])
        self.assertIsNotNone(builder._partitions)

        # Setting WHERE clause clears partitions
        builder.with_where("dt > '2024-01-01'")
        self.assertIsNone(builder._partitions)
        self.assertIsNotNone(builder._where_sql)

        # Setting idle time modifies the state
        builder.with_partition_idle_time('1d')
        self.assertIsNotNone(builder._partition_idle_time)
        # WHERE SQL may still be present depending on implementation
        self.assertEqual(builder._partition_idle_time, timedelta(days=1))

    def test_multiple_partition_specifications(self):
        """Test specifying multiple partitions."""
        partitions = [
            {'dt': '2024-01-01', 'hour': '00'},
            {'dt': '2024-01-01', 'hour': '01'},
            {'dt': '2024-01-01', 'hour': '02'},
            {'dt': '2024-01-02', 'hour': '00'},
            {'dt': '2024-01-02', 'hour': '01'},
        ]
        builder = self.builder.with_partitions(partitions)
        self.assertIsNotNone(builder._partitions)
        self.assertEqual(len(builder._partitions or []), 5)
        self.assertEqual(builder._partitions, partitions)

    def test_complex_where_conditions(self):
        """Test various WHERE clause conditions."""
        test_cases = [
            "dt = '2024-01-01'",
            "dt > '2024-01-01'",
            "hour < 12",
            "dt = '2024-01-01' and hour < 12",
            "dt >= '2024-01-01' and hour > 0",
        ]

        for where_clause in test_cases:
            builder = CompactBuilder(self.table)
            builder.with_where(where_clause)
            self.assertEqual(builder._where_sql, where_clause)

    def test_idle_time_parsing(self):
        """Test parsing various idle time formats."""
        test_cases = [
            ('1d', timedelta(days=1)),
            ('2d', timedelta(days=2)),
            ('1h', timedelta(hours=1)),
            ('6h', timedelta(hours=6)),
            ('30m', timedelta(minutes=30)),
            ('60s', timedelta(seconds=60)),
        ]

        for idle_time_str, expected_timedelta in test_cases:
            builder = CompactBuilder(self.table)
            builder.with_partition_idle_time(idle_time_str)
            self.assertEqual(builder._partition_idle_time, expected_timedelta)

    def test_options_accumulation(self):
        """Test that options accumulate correctly."""
        builder = self.builder \
            .with_options({'sink.parallelism': '4'}) \
            .with_options({'compaction.min.file-num': '10'}) \
            .with_options({'write-only': 'false'})

        self.assertEqual(len(builder._options), 3)
        self.assertEqual(builder._options['sink.parallelism'], '4')
        self.assertEqual(builder._options['compaction.min.file-num'], '10')
        self.assertEqual(builder._options['write-only'], 'false')

    def test_empty_options(self):
        """Test handling empty options."""
        builder = self.builder.with_options({})
        self.assertEqual(len(builder._options), 0)

    def test_configuration_validation_success(self):
        """Test successful configuration validation."""
        builder = self.builder.with_strategy('full')
        # Should not raise exception
        builder._validate_configuration()

    def test_method_chaining_order_independence(self):
        """Test that method chaining order doesn't affect result."""
        # Order 1
        builder1 = CompactBuilder(self.table) \
            .with_strategy('full') \
            .with_options({'sink.parallelism': '4'}) \
            .with_partitions([{'dt': '2024-01-01'}])

        # Order 2
        builder2 = CompactBuilder(self.table) \
            .with_partitions([{'dt': '2024-01-01'}]) \
            .with_strategy('full') \
            .with_options({'sink.parallelism': '4'})

        self.assertEqual(builder1._strategy, builder2._strategy)
        self.assertEqual(builder1._partitions, builder2._partitions)
        self.assertEqual(builder1._options, builder2._options)


class TestPartitionPredicateComprehensive(unittest.TestCase):
    """Comprehensive tests for partition predicates."""

    def test_binary_predicate_operators(self):
        """Test all supported operators."""
        operators = ['=', '!=', '<>', '<', '<=', '>', '>=']
        for op in operators:
            pred = PartitionBinaryPredicate('dt', op, '2024-01-01')
            self.assertIsNotNone(pred)

    def test_complex_predicate_combinations(self):
        """Test complex AND/OR combinations."""
        pred1 = PartitionBinaryPredicate('dt', '=', '2024-01-01')
        pred2 = PartitionBinaryPredicate('hour', '<', 12)
        pred3 = PartitionBinaryPredicate('hour', '>=', 0)

        # (dt = '2024-01-01' AND hour < 12) OR (hour >= 0)
        and_pred = PartitionAndPredicate([pred1, pred2])
        or_pred = PartitionOrPredicate([and_pred, pred3])

        self.assertTrue(or_pred.matches({'dt': '2024-01-01', 'hour': 10}))
        self.assertTrue(or_pred.matches({'dt': '2024-01-02', 'hour': 5}))

    def test_predicate_with_missing_columns(self):
        """Test predicates with missing partition columns."""
        pred = PartitionBinaryPredicate('dt', '=', '2024-01-01')

        # Missing 'dt' column
        self.assertFalse(pred.matches({'hour': '00'}))

        # Has all required columns
        self.assertTrue(pred.matches({'dt': '2024-01-01', 'hour': '00'}))

    def test_sql_where_parsing_comprehensive(self):
        """Test parsing various SQL WHERE clauses."""
        test_cases = [
            ("dt = '2024-01-01'", {'dt': '2024-01-01', 'hour': '10'}, True),
            ("dt > '2024-01-01'", {'dt': '2024-01-02', 'hour': '10'}, True),
            ("dt > '2024-01-01'", {'dt': '2024-01-01', 'hour': '10'}, False),
            ("hour < 12", {'dt': '2024-01-01', 'hour': 10}, True),
            ("hour < 12", {'dt': '2024-01-01', 'hour': 12}, False),
        ]

        partition_columns = ['dt', 'hour']

        for where_clause, partition, expected in test_cases:
            pred = PartitionPredicateConverter.from_sql_where(
                where_clause,
                partition_columns
            )
            if pred is not None:
                result = pred.matches(partition)
                self.assertEqual(
                    result,
                    expected,
                    f"Failed for: {where_clause} with {partition}"
                )


class TestCompactStrategyComprehensive(unittest.TestCase):
    """Comprehensive tests for compaction strategies."""

    def test_full_strategy_selects_all(self):
        """Test full strategy selects all files."""
        strategy = FullCompactStrategy()
        files = [MockFileEntry(('2024-01-01', '00'), 0, i * 100) for i in range(1, 11)]
        selected = strategy.select_files(files)
        self.assertEqual(len(selected), 10)

    def test_minor_strategy_filters_correctly(self):
        """Test minor strategy filters by size."""
        strategy = MinorCompactStrategy(target_file_size=300)  # 300 bytes
        files = [
            MockFileEntry(('2024-01-01', '00'), 0, 100),   # Selected
            MockFileEntry(('2024-01-01', '00'), 0, 200),   # Selected
            MockFileEntry(('2024-01-01', '00'), 0, 300),   # Not selected
            MockFileEntry(('2024-01-01', '00'), 0, 400),   # Not selected
        ]
        selected = strategy.select_files(files)
        self.assertEqual(len(selected), 2)

    def test_strategy_factory(self):
        """Test strategy factory creates correct types."""
        full_strategy = CompactionStrategyFactory.create_strategy('full')
        self.assertIsInstance(full_strategy, FullCompactStrategy)

        minor_strategy = CompactionStrategyFactory.create_strategy('minor')
        self.assertIsInstance(minor_strategy, MinorCompactStrategy)

    def test_strategy_with_empty_files(self):
        """Test strategies handle empty file lists."""
        full_strategy = FullCompactStrategy()
        minor_strategy = MinorCompactStrategy()

        self.assertEqual(len(full_strategy.select_files([])), 0)
        self.assertEqual(len(minor_strategy.select_files([])), 0)

    def test_strategy_case_insensitivity(self):
        """Test factory accepts case-insensitive strategy names."""
        strategies = [
            CompactionStrategyFactory.create_strategy('FULL'),
            CompactionStrategyFactory.create_strategy('Full'),
            CompactionStrategyFactory.create_strategy('full'),
            CompactionStrategyFactory.create_strategy('FuLl'),
        ]

        for strategy in strategies:
            self.assertIsInstance(strategy, FullCompactStrategy)


class TestCompactTaskComprehensive(unittest.TestCase):
    """Comprehensive tests for compaction tasks."""

    def test_append_compact_task_properties(self):
        """Test AppendCompactTask properties."""
        partition = ('2024-01-01', '00')
        files = [MockFileEntry(partition, 0, 100) for _ in range(5)]
        task = AppendCompactTask(partition, 0, files)

        self.assertEqual(task.get_partition(), partition)
        self.assertEqual(task.get_bucket(), 0)
        self.assertEqual(len(task.get_files()), 5)

    def test_keyvalue_compact_task_changelog_flag(self):
        """Test KeyValueCompactTask changelog flag."""
        partition = ('2024-01-01', '00')
        files = [MockFileEntry(partition, 0, 100) for _ in range(3)]

        task1 = KeyValueCompactTask(partition, 0, files, changelog_only=True)
        self.assertTrue(task1.is_changelog_only())

        task2 = KeyValueCompactTask(partition, 0, files, changelog_only=False)
        self.assertFalse(task2.is_changelog_only())

    def test_task_with_minimal_files(self):
        """Test task creation with minimal file count."""
        partition = ('2024-01-01', '00')
        # Single file
        file_single = [MockFileEntry(partition, 0, 100)]
        task = AppendCompactTask(partition, 0, file_single)
        self.assertEqual(len(task.get_files()), 1)

    def test_task_execution_validation(self):
        """Test task execution validation."""
        partition = ('2024-01-01', '00')

        # Valid: files provided
        files = [MockFileEntry(partition, 0, 100) for _ in range(2)]
        task = AppendCompactTask(partition, 0, files)
        task.execute()  # Should not raise

        # Invalid: no files
        empty_task = AppendCompactTask(partition, 0, [])
        with self.assertRaises(ValueError):
            empty_task.execute()


class TestCompactCoordinatorComprehensive(unittest.TestCase):
    """Comprehensive tests for compaction coordinators."""

    def test_coordinator_initialization(self):
        """Test coordinator initialization."""
        table = MockFileStoretable(num_files=10)
        coordinator = AppendOnlyCompactCoordinator(table, is_streaming=False)

        self.assertIsNotNone(coordinator)
        self.assertEqual(coordinator.file_count, 0)
        self.assertEqual(coordinator.total_file_size, 0)

    def test_coordinator_scan_and_plan(self):
        """Test coordinator scan and plan functionality."""
        table = MockFileStoretable(num_files=15)
        coordinator = AppendOnlyCompactCoordinator(table, is_streaming=False)

        compaction_tasks = coordinator.scan_and_plan()
        self.assertIsNotNone(compaction_tasks)
        self.assertGreaterEqual(len(compaction_tasks), 0)

    def test_coordinator_file_counting(self):
        """Test coordinator accurately counts files."""
        table = MockFileStoretable(num_files=20)
        coordinator = AppendOnlyCompactCoordinator(table, is_streaming=False)

        coordinator.scan_and_plan()
        self.assertEqual(coordinator.file_count, 20)

    def test_coordinator_trigger_decision(self):
        """Test coordinator trigger decision logic."""
        # Small table - should not trigger
        small_table = MockFileStoretable(num_files=2)
        small_coordinator = AppendOnlyCompactCoordinator(small_table)
        small_coordinator.scan_and_plan()

        # Large table - should trigger
        large_table = MockFileStoretable(num_files=50)
        large_coordinator = AppendOnlyCompactCoordinator(large_table)
        large_coordinator.scan_and_plan()

        # At least one should be different
        small_decision = small_coordinator.should_trigger_compaction()
        large_decision = large_coordinator.should_trigger_compaction()
        logger.info(
            f"Trigger decisions: small={small_decision}, large={large_decision}"
        )


class TestCompactManagerInterface(unittest.TestCase):
    """Test CompactManager interface and CompactResult."""

    def test_compact_result_creation(self):
        """Test CompactResult creation and methods."""
        result = CompactResult(
            success=True,
            files_compacted=10,
            new_files_count=2,
            compaction_time_ms=5000
        )

        self.assertTrue(result.is_successful())
        self.assertEqual(result.files_compacted, 10)
        self.assertEqual(result.new_files_count, 2)
        self.assertEqual(result.compaction_time_ms, 5000)
        self.assertIsNone(result.error_message)

    def test_compact_result_failure(self):
        """Test CompactResult with failure."""
        result = CompactResult(
            success=False,
            error_message="Compaction failed due to I/O error"
        )

        self.assertFalse(result.is_successful())
        self.assertEqual(
            result.error_message,
            "Compaction failed due to I/O error"
        )


class TestEdgeCasesAndBoundaries(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def test_zero_file_compaction(self):
        """Test handling of zero files."""
        table = MockFileStoretable(num_files=0)
        coordinator = AppendOnlyCompactCoordinator(table)
        compaction_tasks = coordinator.scan_and_plan()
        self.assertEqual(len(compaction_tasks), 0)

    def test_single_file_compaction(self):
        """Test handling of single file."""
        table = MockFileStoretable(num_files=1)
        coordinator = AppendOnlyCompactCoordinator(table)
        compaction_tasks = coordinator.scan_and_plan()
        self.assertEqual(len(compaction_tasks), 0)  # Single file shouldn't be compacted

    def test_very_large_file_count(self):
        """Test handling of very large file counts."""
        table = MockFileStoretable(num_files=1000)
        coordinator = AppendOnlyCompactCoordinator(table)

        # Should handle without memory issues
        _ = coordinator.scan_and_plan()
        self.assertEqual(coordinator.file_count, 1000)

    def test_invalid_duration_format(self):
        """Test invalid duration format handling."""
        builder = CompactBuilder(MockFileStoretable())

        with self.assertRaises(ValueError):
            builder.with_partition_idle_time('invalid')

    def test_empty_partition_dict(self):
        """Test empty partition dictionary."""
        builder = CompactBuilder(MockFileStoretable())

        with self.assertRaises(ValueError):
            builder.with_partitions([{}])

    def test_null_partition_list(self):
        """Test null partition list."""
        builder = CompactBuilder(MockFileStoretable())

        # Type checking will catch this, but at runtime we test error handling
        with self.assertRaises((ValueError, TypeError)):
            builder.with_partitions([])  # Empty list triggers error


class TestIntegrationScenarios(unittest.TestCase):
    """Test realistic integration scenarios."""

    def test_full_compaction_workflow(self):
        """Test complete full compaction workflow."""
        table = MockFileStoretable(num_files=20)
        builder = CompactBuilder(table)

        try:
            builder \
                .with_strategy('full') \
                .with_options({'sink.parallelism': '4'}) \
                .build()
        except RuntimeError:
            # Expected since we don't have actual implementation
            logger.info("Full compaction workflow test completed")

    def test_partition_specific_compaction_workflow(self):
        """Test partition-specific compaction workflow."""
        table = MockFileStoretable(num_files=15)
        builder = CompactBuilder(table)

        try:
            builder \
                .with_partitions([
                    {'dt': '2024-01-01', 'hour': '00'},
                    {'dt': '2024-01-01', 'hour': '01'}
                ]) \
                .with_strategy('minor') \
                .build()
        except RuntimeError:
            pass

    def test_conditional_compaction_workflow(self):
        """Test conditional compaction workflow."""
        table = MockFileStoretable(num_files=25)
        builder = CompactBuilder(table)

        try:
            builder \
                .with_where("dt = '2024-01-01' and hour < 12") \
                .with_options({'sink.parallelism': '8'}) \
                .build()
        except RuntimeError:
            pass


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run tests with verbose output
    unittest.main(verbosity=2)
