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

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta

from pypaimon.compact.table_level_file_scanner import (
    FileStatistics,
    PartitionFileStatistics,
    TableLevelFileScanner,
)
from pypaimon.compact.table_level_task_generator import (
    TableLevelTaskGenerator,
    CompactionTaskGenerationResult,
)
from pypaimon.compact.compact_action import (
    CompactAction,
    CompactionActionResult,
)
from pypaimon.compact.compact_strategy import (
    FullCompactStrategy,
    MinorCompactStrategy,
)


class TestFileStatistics:
    """Test FileStatistics class."""

    def test_init(self):
        """Test FileStatistics initialization."""
        file_stat = FileStatistics(
            file_name='file_1.parquet',
            file_size=1024 * 1024,
            row_count=1000,
            delete_row_count=100,
        )

        assert file_stat.file_name == 'file_1.parquet'
        assert file_stat.file_size == 1024 * 1024
        assert file_stat.row_count == 1000
        assert file_stat.delete_row_count == 100

    def test_delete_ratio(self):
        """Test delete ratio calculation."""
        file_stat = FileStatistics(
            file_name='file_1.parquet',
            file_size=1024 * 1024,
            row_count=1000,
            delete_row_count=200,
        )

        assert file_stat.delete_ratio == pytest.approx(0.2)

    def test_delete_ratio_no_rows(self):
        """Test delete ratio when no rows."""
        file_stat = FileStatistics(
            file_name='file_1.parquet',
            file_size=1024,
            row_count=0,
            delete_row_count=0,
        )

        assert file_stat.delete_ratio == 0.0

    def test_repr(self):
        """Test string representation."""
        file_stat = FileStatistics(
            file_name='file_1.parquet',
            file_size=1024,
            row_count=100,
            delete_row_count=10,
        )

        repr_str = repr(file_stat)
        assert 'file_1.parquet' in repr_str
        assert '1024' in repr_str


class TestPartitionFileStatistics:
    """Test PartitionFileStatistics class."""

    def test_init(self):
        """Test PartitionFileStatistics initialization."""
        partition_key = ('2024-01-01', '00')
        stats = PartitionFileStatistics(partition_key)

        assert stats.partition_key == partition_key
        assert stats.file_count == 0
        assert stats.total_size == 0

    def test_add_file(self):
        """Test adding files."""
        partition_key = ('2024-01-01', '00')
        stats = PartitionFileStatistics(partition_key)

        file1 = FileStatistics('file1.parquet', 1000, 100, 10)
        file2 = FileStatistics('file2.parquet', 2000, 200, 20)

        stats.add_file(file1)
        stats.add_file(file2)

        assert stats.file_count == 2
        assert stats.total_size == 3000
        assert stats.total_row_count == 300
        assert stats.total_delete_count == 30

    def test_delete_ratio(self):
        """Test partition delete ratio."""
        stats = PartitionFileStatistics(('2024-01-01',))
        file1 = FileStatistics('file1.parquet', 1000, 100, 25)
        file2 = FileStatistics('file2.parquet', 2000, 200, 75)

        stats.add_file(file1)
        stats.add_file(file2)

        # Total: 300 rows, 100 deletes = 0.333...
        assert stats.delete_ratio == pytest.approx(0.3333, rel=1e-3)

    def test_average_file_size(self):
        """Test average file size calculation."""
        stats = PartitionFileStatistics(('2024-01-01',))
        stats.add_file(FileStatistics('file1.parquet', 1000, 100, 0))
        stats.add_file(FileStatistics('file2.parquet', 2000, 200, 0))
        stats.add_file(FileStatistics('file3.parquet', 3000, 300, 0))

        assert stats.average_file_size == pytest.approx(2000.0)


class TestTableLevelFileScanner:
    """Test TableLevelFileScanner class."""

    def _create_mock_table(self):
        """Create a mock table."""
        table = Mock()
        table.identifier = 'test_db.test_table'
        table.options = {'target-file-size': '256mb'}

        # Mock schema
        schema = Mock()
        schema.partition_keys.return_value = ['dt', 'hour']
        table.schema.return_value = schema

        # Mock snapshot manager
        snapshot_manager = Mock()
        table.snapshot_manager.return_value = snapshot_manager

        return table, snapshot_manager

    def test_init(self):
        """Test scanner initialization."""
        table, _ = self._create_mock_table()
        scanner = TableLevelFileScanner(table)

        assert scanner.table == table
        assert scanner.partition_filter is None
        assert scanner.total_file_count == 0

    def test_parse_size(self):
        """Test size parsing."""
        assert TableLevelFileScanner._parse_size('1024b') == 1024
        assert TableLevelFileScanner._parse_size('1kb') == 1024
        assert TableLevelFileScanner._parse_size('1mb') == 1024 * 1024
        assert TableLevelFileScanner._parse_size('1gb') == 1024 * 1024 * 1024
        assert TableLevelFileScanner._parse_size('2.5mb') == int(2.5 * 1024 * 1024)

    def test_parse_size_invalid(self):
        """Test invalid size parsing."""
        with pytest.raises(ValueError):
            TableLevelFileScanner._parse_size('invalid')

    def test_format_size(self):
        """Test size formatting."""
        assert 'B' in TableLevelFileScanner._format_size(512)
        assert 'KB' in TableLevelFileScanner._format_size(2048)
        assert 'MB' in TableLevelFileScanner._format_size(2 * 1024 * 1024)
        assert 'GB' in TableLevelFileScanner._format_size(2 * 1024 * 1024 * 1024)

    def test_scan_no_snapshot(self):
        """Test scan with no snapshot."""
        table, snapshot_manager = self._create_mock_table()
        snapshot_manager.latest_snapshot.return_value = None

        scanner = TableLevelFileScanner(table)
        result = scanner.scan()

        assert result == {}
        assert scanner.total_file_count == 0

    def test_get_small_file_partitions(self):
        """Test finding partitions with small files."""
        table, snapshot_manager = self._create_mock_table()
        snapshot = Mock()
        snapshot_manager.latest_snapshot.return_value = snapshot

        # Mock file iterator
        file1 = Mock()
        file1.partition.return_value = ['2024-01-01', '00']
        file1.bucket.return_value = 0
        file1.fileName.return_value = 'file1.parquet'
        file1.file_size.return_value = 10 * 1024 * 1024  # 10 MB
        file1.row_count.return_value = 100
        file1.delete_rows_count.return_value = 0

        file2 = Mock()
        file2.partition.return_value = ['2024-01-01', '01']
        file2.bucket.return_value = 0
        file2.fileName.return_value = 'file2.parquet'
        file2.file_size.return_value = 200 * 1024 * 1024  # 200 MB
        file2.row_count.return_value = 2000
        file2.delete_rows_count.return_value = 0

        snapshot.file_iterator.return_value = [file1, file2]

        scanner = TableLevelFileScanner(table)
        scanner.scan()

        # Find small files (< 50% of 256 MB = 128 MB)
        small_partitions = scanner.get_small_file_partitions()
        assert ('2024-01-01', '00') in small_partitions

    def test_get_high_delete_partitions(self):
        """Test finding partitions with high delete ratio."""
        table, snapshot_manager = self._create_mock_table()
        snapshot = Mock()
        snapshot_manager.latest_snapshot.return_value = snapshot

        # Mock file with high deletes
        file1 = Mock()
        file1.partition.return_value = ['2024-01-01', '00']
        file1.bucket.return_value = 0
        file1.fileName.return_value = 'file1.parquet'
        file1.file_size.return_value = 100 * 1024 * 1024
        file1.row_count.return_value = 1000
        file1.delete_rows_count.return_value = 300  # 30% delete

        snapshot.file_iterator.return_value = [file1]

        scanner = TableLevelFileScanner(table)
        scanner.scan()

        high_delete = scanner.get_high_delete_partitions(
            delete_ratio_threshold=0.2
        )
        assert ('2024-01-01', '00') in high_delete

    def test_summary(self):
        """Test summary generation."""
        table, snapshot_manager = self._create_mock_table()
        snapshot = Mock()
        snapshot_manager.latest_snapshot.return_value = snapshot

        file1 = Mock()
        file1.partition.return_value = ['2024-01-01', '00']
        file1.bucket.return_value = 0
        file1.fileName.return_value = 'file1.parquet'
        file1.file_size.return_value = 100 * 1024 * 1024
        file1.row_count.return_value = 1000
        file1.delete_rows_count.return_value = 0

        snapshot.file_iterator.return_value = [file1]

        scanner = TableLevelFileScanner(table)
        scanner.scan()

        summary = scanner.summary()
        assert 'Total files' in summary
        assert 'Total rows' in summary
        assert 'Partitions' in summary


class TestTableLevelTaskGenerator:
    """Test TableLevelTaskGenerator class."""

    def _create_mock_table(self):
        """Create a mock table."""
        table = Mock()
        table.identifier = 'test_db.test_table'
        table.options = {'target-file-size': '256mb'}
        return table

    def _create_mock_scanner(self):
        """Create a mock scanner with partition stats."""
        scanner = Mock()
        scanner.partition_stats = {}

        # Add some test data
        partition_key = ('2024-01-01', '00')
        stats = PartitionFileStatistics(partition_key)

        file1 = FileStatistics('file1.parquet', 10 * 1024 * 1024, 100, 10)
        file2 = FileStatistics('file2.parquet', 20 * 1024 * 1024, 200, 20)
        file3 = FileStatistics('file3.parquet', 30 * 1024 * 1024, 300, 30)

        stats.add_file(file1)
        stats.add_file(file2)
        stats.add_file(file3)

        scanner.partition_stats[partition_key] = stats
        scanner.get_partition_stats.return_value = stats

        return scanner

    def test_init(self):
        """Test generator initialization."""
        table = self._create_mock_table()
        strategy = FullCompactStrategy()

        generator = TableLevelTaskGenerator(table, strategy)
        assert generator.table == table
        assert generator.strategy == strategy
        assert generator.min_file_num == 5

    def test_generate_tasks(self):
        """Test task generation."""
        table = self._create_mock_table()
        strategy = FullCompactStrategy()
        scanner = self._create_mock_scanner()

        generator = TableLevelTaskGenerator(table, strategy, min_file_num=2)
        result = generator.generate_tasks(scanner)

        assert isinstance(result, CompactionTaskGenerationResult)
        assert len(result.tasks) > 0
        assert result.partitions_selected >= 0

    def test_should_compact_partition_by_file_count(self):
        """Test compaction trigger by file count."""
        table = self._create_mock_table()
        strategy = FullCompactStrategy()
        generator = TableLevelTaskGenerator(table, strategy, min_file_num=2)

        stats = PartitionFileStatistics(('2024-01-01',))
        stats.add_file(FileStatistics('file1.parquet', 1000, 100, 0))
        stats.add_file(FileStatistics('file2.parquet', 1000, 100, 0))

        should_compact = generator._should_compact_partition(
            stats, 256 * 1024 * 1024
        )
        assert should_compact

    def test_should_compact_partition_by_delete_ratio(self):
        """Test compaction trigger by delete ratio."""
        table = self._create_mock_table()
        strategy = FullCompactStrategy()
        generator = TableLevelTaskGenerator(
            table, strategy, delete_ratio_threshold=0.2
        )

        stats = PartitionFileStatistics(('2024-01-01',))
        stats.add_file(FileStatistics('file1.parquet', 1000, 100, 30))

        should_compact = generator._should_compact_partition(
            stats, 256 * 1024 * 1024
        )
        assert should_compact

    def test_generate_idle_partition_tasks(self):
        """Test idle partition task generation."""
        table = self._create_mock_table()
        strategy = FullCompactStrategy()
        scanner = self._create_mock_scanner()

        generator = TableLevelTaskGenerator(table, strategy)
        idle_time = timedelta(days=7)

        result = generator.generate_idle_partition_tasks(
            scanner, idle_time
        )

        assert isinstance(result, CompactionTaskGenerationResult)

    def test_summary(self):
        """Test summary generation."""
        table = self._create_mock_table()
        strategy = FullCompactStrategy()
        generator = TableLevelTaskGenerator(table, strategy)

        result = CompactionTaskGenerationResult(
            tasks=[], total_files_selected=0, partitions_selected=0
        )

        summary = generator.summary(result)
        assert 'Total tasks' in summary
        assert 'Files selected' in summary


class TestCompactAction:
    """Test CompactAction class."""

    def _create_mock_table(self):
        """Create a mock table."""
        table = Mock()
        table.identifier = 'test_db.test_table'
        table.options = {'target-file-size': '256mb'}
        schema = Mock()
        schema.partition_keys.return_value = ['dt', 'hour']
        table.schema.return_value = schema
        return table

    def test_init(self):
        """Test CompactAction initialization."""
        table = self._create_mock_table()
        action = CompactAction(table)

        assert action.table == table
        assert action.identifier == 'test_db.test_table'

    def test_validate_partitions(self):
        """Test partition validation."""
        table = self._create_mock_table()
        action = CompactAction(table)

        partitions = [
            {'dt': '2024-01-01', 'hour': '00'},
            {'dt': '2024-01-01', 'hour': '01'},
        ]

        partition_tuples = action._validate_partitions(partitions)

        assert len(partition_tuples) == 2
        assert partition_tuples[0] == ('2024-01-01', '00')
        assert partition_tuples[1] == ('2024-01-01', '01')

    def test_validate_partitions_missing_key(self):
        """Test validation with missing partition key."""
        table = self._create_mock_table()
        action = CompactAction(table)

        partitions = [{'dt': '2024-01-01'}]  # Missing 'hour'

        with pytest.raises(ValueError):
            action._validate_partitions(partitions)

    def test_validate_partitions_empty(self):
        """Test validation with empty list."""
        table = self._create_mock_table()
        action = CompactAction(table)

        with pytest.raises(ValueError):
            action._validate_partitions([])

    def test_execute_partition_compaction_result(self):
        """Test partition compaction execution result."""
        table = self._create_mock_table()
        action = CompactAction(table)

        # Mock the execution
        with patch.object(
            action, '_validate_partitions'
        ) as mock_validate, patch(
            'pypaimon.compact.compact_action.TableLevelFileScanner'
        ) as MockScanner, patch(
            'pypaimon.compact.compact_action.TableLevelTaskGenerator'
        ) as MockGenerator:

            mock_validate.return_value = [
                ('2024-01-01', '00')
            ]

            mock_scanner = Mock()
            MockScanner.return_value = mock_scanner

            mock_generator = Mock()
            mock_result = CompactionTaskGenerationResult(
                tasks=[], total_files_selected=0, partitions_selected=0
            )
            mock_generator.generate_selective_tasks.return_value = (
                mock_result
            )
            MockGenerator.return_value = mock_generator

            partitions = [{'dt': '2024-01-01', 'hour': '00'}]
            result = action.execute_partition_compaction(partitions)

            assert isinstance(result, CompactionActionResult)

    def test_get_compaction_stats(self):
        """Test getting compaction statistics."""
        table = self._create_mock_table()

        # Mock snapshot manager
        snapshot_manager = Mock()
        snapshot = Mock()
        snapshot_manager.latest_snapshot.return_value = snapshot

        file1 = Mock()
        file1.partition.return_value = ['2024-01-01', '00']
        file1.bucket.return_value = 0
        file1.fileName.return_value = 'file1.parquet'
        file1.file_size.return_value = 100 * 1024 * 1024
        file1.row_count.return_value = 1000
        file1.delete_rows_count.return_value = 0

        snapshot.file_iterator.return_value = [file1]
        table.snapshot_manager.return_value = snapshot_manager

        action = CompactAction(table)
        stats = action.get_compaction_stats()

        assert 'table' in stats
        assert stats['total_files'] == 1
        assert 'total_size_bytes' in stats
        assert 'total_rows' in stats


class TestCompactionActionResult:
    """Test CompactionActionResult class."""

    def test_init_success(self):
        """Test result initialization for success."""
        result = CompactionActionResult(
            success=True,
            tasks_generated=10,
            files_compacted=50,
            partitions_processed=5,
        )

        assert result.success is True
        assert result.tasks_generated == 10
        assert result.files_compacted == 50
        assert result.error_message is None

    def test_init_failure(self):
        """Test result initialization for failure."""
        error = "Compaction failed"
        result = CompactionActionResult(success=False, error_message=error)

        assert result.success is False
        assert result.error_message == error

    def test_repr(self):
        """Test string representation."""
        result = CompactionActionResult(
            success=True,
            tasks_generated=10,
            files_compacted=50,
            partitions_processed=5,
        )

        repr_str = repr(result)
        assert 'success=True' in repr_str
        assert 'tasks=10' in repr_str
