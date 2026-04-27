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
from unittest.mock import Mock, patch

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.write.commit.commit_scanner import CommitScanner
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit

PARTITION_FIELDS = [
    DataField(0, 'dt', AtomicType('STRING')),
    DataField(1, 'region', AtomicType('STRING')),
]

TABLE_FIELDS = [
    DataField(0, 'dt', AtomicType('STRING')),
    DataField(1, 'id', AtomicType('INT')),
    DataField(2, 'name', AtomicType('STRING')),
    DataField(3, 'region', AtomicType('STRING')),
]

_partition_builder = PredicateBuilder(PARTITION_FIELDS)


def _mock_table():
    table = Mock()
    table.field_names = ['dt', 'id', 'name', 'region']
    table.fields = TABLE_FIELDS
    table.partition_keys = ['dt', 'region']
    table.partition_keys_fields = PARTITION_FIELDS
    return table


def _mock_scanner_table():
    table = _mock_table()
    table.trimmed_primary_keys = []
    table.is_primary_key_table = False
    table.options.source_split_target_size.return_value = 128 * 1024 * 1024
    table.options.source_split_open_file_cost.return_value = 4 * 1024 * 1024
    table.options.bucket.return_value = -1
    table.options.data_evolution_enabled.return_value = False
    table.options.deletion_vectors_enabled.return_value = False
    table.options.scan_manifest_parallelism.return_value = 1
    table.table_schema = Mock(id=0)
    table.schema_manager = Mock()
    table.schema_manager.get_schema.return_value = Mock(fields=TABLE_FIELDS)
    return table


def _manifest_file_meta(partition_min, partition_max):
    return ManifestFileMeta(
        file_name='manifest-test',
        file_size=1024,
        num_added_files=1,
        num_deleted_files=0,
        partition_stats=SimpleStats(
            min_values=GenericRow(partition_min, PARTITION_FIELDS),
            max_values=GenericRow(partition_max, PARTITION_FIELDS),
            null_counts=[0, 0],
        ),
        schema_id=0,
    )


def _manifest_entry(partition_values):
    return ManifestEntry(
        kind=0,
        partition=GenericRow(partition_values, PARTITION_FIELDS),
        bucket=0,
        total_buckets=1,
        file=Mock(),
    )


@patch('pypaimon.read.scanner.file_scanner.SnapshotManager')
@patch('pypaimon.read.scanner.file_scanner.ManifestFileManager')
@patch('pypaimon.read.scanner.file_scanner.ManifestListManager')
class TestFileScannerPartitionPredicate(unittest.TestCase):

    def _scanner(self, predicate=None, partition_predicate=None):
        return FileScanner(
            _mock_scanner_table(), lambda: ([], None),
            predicate=predicate, partition_predicate=partition_predicate,
        )

    def test_partition_predicate_used_directly(self, *_):
        pred = _partition_builder.equal('dt', '2024-01-15')
        scanner = self._scanner(partition_predicate=pred)

        self.assertIs(scanner.partition_key_predicate, pred)
        self.assertIsNone(scanner.predicate)
        self.assertIsNone(scanner.predicate_for_stats)
        self.assertIsNone(scanner.primary_key_predicate)

    def test_no_partition_predicate_derives_from_predicate(self, *_):
        full_pred = PredicateBuilder(TABLE_FIELDS).equal('dt', '2024-01-15')
        scanner = self._scanner(predicate=full_pred)

        self.assertIsNotNone(scanner.partition_key_predicate)
        self.assertEqual(scanner.partition_key_predicate.field, 'dt')

    def test_neither_predicate_means_no_filtering(self, *_):
        scanner = self._scanner()

        self.assertIsNone(scanner.partition_key_predicate)
        self.assertTrue(scanner._filter_manifest_entry(
            _manifest_entry(['2024-01-15', 'us-east-1'])))

    def test_filters_manifest_file_by_stats(self, *_):
        scanner = self._scanner(
            partition_predicate=_partition_builder.equal('dt', '2024-01-15'))

        self.assertTrue(scanner._filter_manifest_file(
            _manifest_file_meta(['2024-01-15', 'us-east-1'], ['2024-01-15', 'us-west-2'])))
        self.assertFalse(scanner._filter_manifest_file(
            _manifest_file_meta(['2024-01-16', 'us-east-1'], ['2024-01-16', 'us-west-2'])))

    def test_filters_manifest_entry_by_partition(self, *_):
        scanner = self._scanner(
            partition_predicate=_partition_builder.and_predicates([
                _partition_builder.equal('dt', '2024-01-15'),
                _partition_builder.equal('region', 'us-east-1'),
            ]))

        self.assertTrue(scanner._filter_manifest_entry(
            _manifest_entry(['2024-01-15', 'us-east-1'])))
        self.assertFalse(scanner._filter_manifest_entry(
            _manifest_entry(['2024-01-16', 'us-east-1'])))
        self.assertFalse(scanner._filter_manifest_entry(
            _manifest_entry(['2024-01-15', 'us-west-2'])))


@patch('pypaimon.write.file_store_commit.SnapshotManager')
@patch('pypaimon.write.file_store_commit.ManifestFileManager')
@patch('pypaimon.write.file_store_commit.ManifestListManager')
class TestOverwritePartitionPredicate(unittest.TestCase):

    _TARGET = {'dt': '2024-01-15', 'region': 'us-east-1'}

    def setUp(self):
        self.table = _mock_table()

    def _create_commit(self, stub_commit=True):
        commit = FileStoreCommit(Mock(), self.table, 'test_user')
        if stub_commit:
            commit._try_commit = Mock()
        return commit

    @staticmethod
    def _msg(partition):
        return CommitMessage(partition=partition, bucket=0, new_files=[Mock(row_count=10)])

    def _extract_partition_predicate(self, commit):
        entries_plan = commit._try_commit.call_args[1]['commit_entries_plan']
        with patch('pypaimon.write.file_store_commit.FileScanner') as mock_cls:
            mock_cls.return_value.read_manifest_entries.return_value = []
            commit.manifest_list_manager.read_all.return_value = []
            entries_plan(Mock(id=1))
            return mock_cls.call_args[1]['partition_predicate']

    def test_overwrite_rejects_mismatched_partition(self, *_):
        commit = self._create_commit(stub_commit=False)
        with self.assertRaises(RuntimeError) as ctx:
            commit.overwrite(self._TARGET, [self._msg(('2024-01-15', 'us-west-2'))], 1)
        self.assertIn('does not belong to this partition', str(ctx.exception))

    def test_overwrite_passes_partition_scoped_predicate(self, *_):
        commit = self._create_commit()
        commit.overwrite(self._TARGET, [self._msg(('2024-01-15', 'us-east-1'))], 1)

        pred = self._extract_partition_predicate(commit)
        self.assertTrue(pred.test(OffsetRow(('2024-01-15', 'us-east-1'), 0, 2)))
        self.assertFalse(pred.test(OffsetRow(('2024-01-15', 'us-west-2'), 0, 2)))

    def test_drop_partitions_passes_or_predicate(self, *_):
        commit = self._create_commit()
        commit.drop_partitions([
            {'dt': '2024-01-15', 'region': 'us-east-1'},
            {'dt': '2024-01-16', 'region': 'us-west-2'},
        ], 1)

        pred = self._extract_partition_predicate(commit)
        self.assertTrue(pred.test(OffsetRow(('2024-01-15', 'us-east-1'), 0, 2)))
        self.assertTrue(pred.test(OffsetRow(('2024-01-16', 'us-west-2'), 0, 2)))
        self.assertFalse(pred.test(OffsetRow(('2024-01-17', 'eu-west-1'), 0, 2)))


class TestCommitScannerPartitionPredicate(unittest.TestCase):

    def _scanner(self):
        return CommitScanner(_mock_table(), Mock())

    def test_filter_uses_partition_key_index(self):
        scanner = self._scanner()
        pred = scanner._build_partition_filter_from_entries([
            _manifest_entry(['2024-01-15', 'us-east-1']),
            _manifest_entry(['2024-01-16', 'us-west-2']),
        ])

        self.assertTrue(pred.test(GenericRow(['2024-01-15', 'us-east-1'], PARTITION_FIELDS)))
        self.assertTrue(pred.test(GenericRow(['2024-01-16', 'us-west-2'], PARTITION_FIELDS)))
        self.assertFalse(pred.test(GenericRow(['2024-01-17', 'eu-west-1'], PARTITION_FIELDS)))

    def test_filter_none_without_partition_keys(self):
        scanner = CommitScanner(Mock(partition_keys=[]), Mock())

        pred = scanner._build_partition_filter_from_entries(
            [_manifest_entry(['2024-01-15', 'us-east-1'])])
        self.assertIsNone(pred)

    @patch('pypaimon.write.commit.commit_scanner.FileScanner')
    def test_passes_partition_predicate_to_file_scanner(self, mock_scanner_cls):
        mock_scanner_cls.return_value.read_manifest_entries.return_value = []
        entries = [_manifest_entry(['2024-01-15', 'us-east-1'])]

        cases = [
            ('read_all_entries_from_changed_partitions', 'read_all', []),
            ('read_incremental_entries_from_changed_partitions', 'read_delta', [Mock()]),
        ]
        for method, setup_attr, setup_return in cases:
            with self.subTest(method=method):
                mock_scanner_cls.reset_mock()
                scanner = self._scanner()
                getattr(scanner.manifest_list_manager, setup_attr).return_value = setup_return
                getattr(scanner, method)(Mock(), entries)

                kwargs = mock_scanner_cls.call_args[1]
                self.assertIn('partition_predicate', kwargs)
                self.assertIsNotNone(kwargs['partition_predicate'])
                self.assertNotIn('predicate', kwargs)
