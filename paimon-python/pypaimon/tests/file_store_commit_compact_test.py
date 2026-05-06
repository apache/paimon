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

from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.compact_increment import CompactIncrement
from pypaimon.write.data_increment import DataIncrement
from pypaimon.write.file_store_commit import FileStoreCommit


def _make_file(name: str, *, first_row_id=None) -> DataFileMeta:
    return DataFileMeta.create(
        file_name=name,
        file_size=4096,
        row_count=10,
        min_key=GenericRow([], []),
        max_key=GenericRow([], []),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=1,
        max_sequence_number=10,
        schema_id=0,
        level=0,
        extra_files=[],
        creation_time=Timestamp.from_local_date_time(datetime(2024, 1, 15, 10, 30, 0)),
        first_row_id=first_row_id,
    )


@patch('pypaimon.write.file_store_commit.SnapshotManager')
@patch('pypaimon.write.file_store_commit.ManifestFileManager')
@patch('pypaimon.write.file_store_commit.ManifestListManager')
class TestFileStoreCommitCompact(unittest.TestCase):
    """Phase 1 protocol-level tests: verify compact_before/after entries flow correctly through commit().

    These tests stub _try_commit so we only verify the entry-construction and commit_kind selection.
    Full e2e (with real manifest writes / scans) is covered in Phase 2 once the rewriter exists.
    """

    def setUp(self):
        self.mock_table = Mock()
        self.mock_table.partition_keys = ['dt']
        self.mock_table.partition_keys_fields = [DataField(0, 'dt', AtomicType('STRING'))]
        self.mock_table.total_buckets = 4
        self.mock_table.current_branch.return_value = 'main'
        self.mock_table.identifier = 'default.t'
        self.mock_snapshot_commit = Mock()

    def _create_commit(self):
        return FileStoreCommit(
            snapshot_commit=self.mock_snapshot_commit,
            table=self.mock_table,
            commit_user='test_user',
        )

    def test_build_entries_emits_add_for_new_files(self, *_):
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=2,
            data_increment=DataIncrement(new_files=[_make_file('a.parquet')]),
        )

        entries = commit._build_commit_entries([msg])

        self.assertEqual(1, len(entries))
        self.assertEqual(0, entries[0].kind)
        self.assertEqual(2, entries[0].bucket)
        self.assertEqual('a.parquet', entries[0].file.file_name)
        self.assertEqual(['2024-01-15'], list(entries[0].partition.values))

    def test_build_entries_emits_delete_for_compact_before_and_add_for_compact_after(self, *_):
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=1,
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old-1.parquet'), _make_file('old-2.parquet')],
                compact_after=[_make_file('merged.parquet')],
            ),
        )

        entries = commit._build_commit_entries([msg])

        kinds = [e.kind for e in entries]
        names = [e.file.file_name for e in entries]
        self.assertEqual([1, 1, 0], kinds)
        self.assertEqual(['old-1.parquet', 'old-2.parquet', 'merged.parquet'], names)
        self.assertTrue(all(e.bucket == 1 for e in entries))

    def test_commit_with_only_compact_messages_uses_compact_kind(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('new.parquet')],
            ),
        )

        commit.commit([msg], commit_identifier=100)

        commit._try_commit.assert_called_once()
        call_kwargs = commit._try_commit.call_args.kwargs
        self.assertEqual('COMPACT', call_kwargs['commit_kind'])
        self.assertEqual(100, call_kwargs['commit_identifier'])

    def test_commit_with_new_files_keeps_append_kind_even_when_compact_fields_present(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(new_files=[_make_file('new.parquet')]),
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('merged.parquet')],
            ),
        )

        commit.commit([msg], commit_identifier=200)

        call_kwargs = commit._try_commit.call_args.kwargs
        self.assertEqual('APPEND', call_kwargs['commit_kind'])

    def test_commit_compact_uses_compact_kind_and_no_conflict_detection(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('p1',),
            bucket=3,
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('new.parquet')],
            ),
        )

        commit.commit_compact([msg], commit_identifier=300)

        commit._try_commit.assert_called_once()
        kwargs = commit._try_commit.call_args.kwargs
        self.assertEqual('COMPACT', kwargs['commit_kind'])
        self.assertEqual(300, kwargs['commit_identifier'])
        self.assertFalse(kwargs['detect_conflicts'])
        self.assertFalse(kwargs['allow_rollback'])

    def test_commit_compact_rejects_messages_with_new_files(self, *_):
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(new_files=[_make_file('append.parquet')]),
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('new.parquet')],
            ),
        )

        with self.assertRaises(ValueError):
            commit.commit_compact([msg], commit_identifier=400)

    def test_commit_compact_skips_when_no_messages(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()

        commit.commit_compact([], commit_identifier=500)

        commit._try_commit.assert_not_called()

    def test_commit_compact_skips_when_messages_have_no_files(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        empty_msg = CommitMessage(partition=('p1',), bucket=0)

        commit.commit_compact([empty_msg], commit_identifier=600)

        commit._try_commit.assert_not_called()


if __name__ == '__main__':
    unittest.main()
