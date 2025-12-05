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
from datetime import datetime
from unittest.mock import Mock, patch

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit


@patch('pypaimon.write.file_store_commit.SnapshotManager')
@patch('pypaimon.write.file_store_commit.ManifestFileManager')
@patch('pypaimon.write.file_store_commit.ManifestListManager')
class TestFileStoreCommit(unittest.TestCase):
    """Test cases for FileStoreCommit class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock table with required attributes
        self.mock_table = Mock()
        self.mock_table.partition_keys = ['dt', 'region']
        self.mock_table.current_branch.return_value = 'main'
        self.mock_table.table_path = '/test/table/path'
        self.mock_table.file_io = Mock()

        # Mock snapshot commit
        self.mock_snapshot_commit = Mock()

    def _create_file_store_commit(self):
        """Helper method to create FileStoreCommit instance."""
        return FileStoreCommit(
            snapshot_commit=self.mock_snapshot_commit,
            table=self.mock_table,
            commit_user='test_user'
        )

    def test_generate_partition_statistics_single_partition_single_file(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation with single partition and single file."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        # Create test data
        creation_time_dt = datetime(2024, 1, 15, 10, 30, 0)
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time = Timestamp.from_local_date_time(creation_time_dt)
        file_meta = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,  # 1MB
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        self.assertIsInstance(stat, PartitionStatistics)
        self.assertEqual(stat.spec, {'dt': '2024-01-15', 'region': 'us-east-1'})
        self.assertEqual(stat.record_count, 10000)
        self.assertEqual(stat.file_count, 1)
        self.assertEqual(stat.file_size_in_bytes, 1024 * 1024)
        expected_time = file_meta.creation_time_epoch_millis()
        self.assertEqual(stat.last_file_creation_time, expected_time)

    def test_generate_partition_statistics_multiple_files_same_partition(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation with multiple files in same partition."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time_1 = Timestamp.from_local_date_time(datetime(2024, 1, 15, 10, 30, 0))
        creation_time_2 = Timestamp.from_local_date_time(datetime(2024, 1, 15, 11, 30, 0))  # Later time

        file_meta_1 = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,  # 1MB
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time_1
        )

        file_meta_2 = DataFileMeta.create(
            file_name="test_file_2.parquet",
            file_size=2 * 1024 * 1024,  # 2MB
            row_count=15000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time_2
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta_1, file_meta_2]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        self.assertEqual(stat.spec, {'dt': '2024-01-15', 'region': 'us-east-1'})
        self.assertEqual(stat.record_count, 25000)  # 10000 + 15000
        self.assertEqual(stat.file_count, 2)
        self.assertEqual(stat.file_size_in_bytes, 3 * 1024 * 1024)  # 1MB + 2MB
        expected_time = file_meta_2.creation_time_epoch_millis()
        self.assertEqual(stat.last_file_creation_time, expected_time)

    def test_generate_partition_statistics_multiple_partitions(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation with multiple different partitions."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        creation_time_dt = datetime(2024, 1, 15, 10, 30, 0)
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time = Timestamp.from_local_date_time(creation_time_dt)

        # File for partition 1
        file_meta_1 = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        # File for partition 2
        file_meta_2 = DataFileMeta.create(
            file_name="test_file_2.parquet",
            file_size=2 * 1024 * 1024,
            row_count=20000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=101,
            max_sequence_number=200,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        commit_message_1 = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta_1]
        )

        commit_message_2 = CommitMessage(
            partition=('2024-01-15', 'us-west-2'),
            bucket=0,
            new_files=[file_meta_2]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(
            self._to_entries([commit_message_1, commit_message_2]))

        # Verify results
        self.assertEqual(len(statistics), 2)

        # Sort statistics by partition spec for consistent testing
        statistics.sort(key=lambda s: s.spec['region'])

        # Check first partition (us-east-1)
        stat_1 = statistics[0]
        self.assertEqual(stat_1.spec, {'dt': '2024-01-15', 'region': 'us-east-1'})
        self.assertEqual(stat_1.record_count, 10000)
        self.assertEqual(stat_1.file_count, 1)
        self.assertEqual(stat_1.file_size_in_bytes, 1024 * 1024)

        # Check second partition (us-west-2)
        stat_2 = statistics[1]
        self.assertEqual(stat_2.spec, {'dt': '2024-01-15', 'region': 'us-west-2'})
        self.assertEqual(stat_2.record_count, 20000)
        self.assertEqual(stat_2.file_count, 1)
        self.assertEqual(stat_2.file_size_in_bytes, 2 * 1024 * 1024)

    def test_generate_partition_statistics_unpartitioned_table(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation for unpartitioned table."""
        # Update mock table to have no partition keys
        self.mock_table.partition_keys = []

        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        creation_time_dt = datetime(2024, 1, 15, 10, 30, 0)
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        creation_time = Timestamp.from_local_date_time(creation_time_dt)
        file_meta = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=creation_time,
            external_path=None,
            first_row_id=None,
            write_cols=None
        )

        commit_message = CommitMessage(
            partition=(),  # Empty partition for unpartitioned table
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        self.assertEqual(stat.spec, {})  # Empty spec for unpartitioned table
        self.assertEqual(stat.record_count, 10000)
        self.assertEqual(stat.file_count, 1)
        self.assertEqual(stat.file_size_in_bytes, 1024 * 1024)

    def test_generate_partition_statistics_no_creation_time(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation when file has no creation time."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        file_meta = DataFileMeta(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=None,
            max_key=None,
            key_stats=None,
            value_stats=None,
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=None,
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1'),
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        # Should have a valid timestamp (current time)
        self.assertGreater(stat.last_file_creation_time, 0)

    def test_generate_partition_statistics_mismatched_partition_keys(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation when partition tuple doesn't match partition keys."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        # Table has 2 partition keys but partition tuple has 3 values
        from pypaimon.data.timestamp import Timestamp
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        file_meta = DataFileMeta.create(
            file_name="test_file_1.parquet",
            file_size=1024 * 1024,
            row_count=10000,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=1,
            max_sequence_number=100,
            schema_id=0,
            level=0,
            extra_files=[],
            creation_time=Timestamp.from_local_date_time(datetime(2024, 1, 15, 10, 30, 0))
        )

        commit_message = CommitMessage(
            partition=('2024-01-15', 'us-east-1', 'extra-value'),  # 3 values but table has 2 keys
            bucket=0,
            new_files=[file_meta]
        )

        # Test method
        statistics = file_store_commit._generate_partition_statistics(self._to_entries([commit_message]))

        # Verify results - should fallback to index-based naming
        self.assertEqual(len(statistics), 1)

        stat = statistics[0]
        expected_spec = {
            'partition_0': '2024-01-15',
            'partition_1': 'us-east-1',
            'partition_2': 'extra-value'
        }
        self.assertEqual(stat.spec, expected_spec)

    def test_generate_partition_statistics_empty_commit_messages(
            self, mock_manifest_list_manager, mock_manifest_file_manager, mock_snapshot_manager):
        """Test partition statistics generation with empty commit messages list."""
        # Create FileStoreCommit instance
        file_store_commit = self._create_file_store_commit()

        # Test method
        statistics = file_store_commit._generate_partition_statistics([])

        # Verify results
        self.assertEqual(len(statistics), 0)

    @staticmethod
    def _to_entries(commit_messages):
        commit_entries = []
        for msg in commit_messages:
            partition = GenericRow(list(msg.partition), None)
            for file in msg.new_files:
                commit_entries.append(ManifestEntry(
                    kind=0,
                    partition=partition,
                    bucket=msg.bucket,
                    total_buckets=None,
                    file=file
                ))
        return commit_entries
