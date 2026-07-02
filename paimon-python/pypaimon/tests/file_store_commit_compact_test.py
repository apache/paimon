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
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.compact_increment import CompactIncrement
from pypaimon.write.data_increment import DataIncrement
from pypaimon.write.file_store_commit import FileStoreCommit


def _make_index_delete(name: str) -> IndexManifestEntry:
    return IndexManifestEntry(
        kind=1,  # DELETE
        partition=GenericRow([], []),
        bucket=0,
        index_file=IndexFileMeta(
            index_type='HASH',
            file_name=name,
            file_size=64,
            row_count=1,
        ),
    )


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


@patch('pypaimon.write.file_store_commit.ManifestFileManager')
@patch('pypaimon.write.file_store_commit.ManifestListManager')
class TestFileStoreCommitCompact(unittest.TestCase):
    """Phase 1 protocol-level tests: verify data / compaction increments flow
    correctly through the single public commit() entry.

    commit() splits each batch into an APPEND (or OVERWRITE) snapshot for the
    data increments and a separate COMPACT snapshot for the compact increments,
    mirroring Java's FileStoreCommitImpl (one commit may produce up to two
    snapshots). These tests stub _try_commit so we only verify entry
    construction and commit_kind selection. Full e2e (with real manifest writes
    / scans) is covered in Phase 2 once the rewriter exists.
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

    # ---- entry construction -------------------------------------------------

    def test_build_data_entries_emits_add_for_new_files(self, *_):
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=2,
            data_increment=DataIncrement(new_files=[_make_file('a.parquet')]),
        )

        entries = commit._build_data_entries([msg])

        self.assertEqual(1, len(entries))
        self.assertEqual(0, entries[0].kind)
        self.assertEqual(2, entries[0].bucket)
        self.assertEqual('a.parquet', entries[0].file.file_name)
        self.assertEqual(['2024-01-15'], list(entries[0].partition.values))

    def test_build_compact_entries_emits_delete_before_and_add_after(self, *_):
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=1,
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old-1.parquet'), _make_file('old-2.parquet')],
                compact_after=[_make_file('merged.parquet')],
            ),
        )

        entries = commit._build_compact_entries([msg])

        kinds = [e.kind for e in entries]
        names = [e.file.file_name for e in entries]
        self.assertEqual([1, 1, 0], kinds)
        self.assertEqual(['old-1.parquet', 'old-2.parquet', 'merged.parquet'], names)
        self.assertTrue(all(e.bucket == 1 for e in entries))

    def test_build_data_entries_ignores_compact_increment(self, *_):
        # _build_data_entries only reads new_files; compact files belong to the
        # compact side and must not leak into the APPEND snapshot.
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=0,
            compact_increment=CompactIncrement(compact_after=[_make_file('m.parquet')]),
        )
        self.assertEqual([], commit._build_data_entries([msg]))

    def test_build_data_entries_ignores_changelog_files(self, *_):
        # changelog_files are committed to the changelog manifest via
        # _collect_changelog_entries, not as delta entries.
        commit = self._create_commit()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=0,
            data_increment=DataIncrement(changelog_files=[_make_file('c.parquet')]),
        )
        self.assertEqual([], commit._build_data_entries([msg]))

    def test_build_data_entries_uses_message_total_buckets_when_set(self, *_):
        """A stale plan whose bucket count has been rescaled should keep the
        message's own total_buckets, not be silently overwritten with the
        table's current value.
        """
        commit = self._create_commit()
        # Table claims 4 buckets; message was planned when there were 8.
        self.mock_table.total_buckets = 4
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=0,
            total_buckets=8,
            data_increment=DataIncrement(new_files=[_make_file('a.parquet')]),
        )

        entries = commit._build_data_entries([msg])

        self.assertEqual(1, len(entries))
        self.assertEqual(8, entries[0].total_buckets)

    def test_build_data_entries_falls_back_to_table_total_buckets(self, *_):
        commit = self._create_commit()
        self.mock_table.total_buckets = 4
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=0,
            total_buckets=None,
            data_increment=DataIncrement(new_files=[_make_file('a.parquet')]),
        )

        entries = commit._build_data_entries([msg])

        self.assertEqual(4, entries[0].total_buckets)

    # ---- unwired-slot rejection --------------------------------------------

    def test_commit_emits_delete_entry_for_data_increment_deleted_files(self, *_):
        # deleted_files (e.g. a partition/row DELETE in data-evolution tables)
        # become kind=1 DELETE entries on the data side.
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=0,
            data_increment=DataIncrement(deleted_files=[_make_file('d.parquet')]),
        )

        commit.commit([msg], commit_identifier=100)

        commit._try_commit.assert_called_once()
        entries = commit._try_commit.call_args.kwargs['commit_entries_plan'](None)
        self.assertEqual([1], [e.kind for e in entries])
        self.assertEqual('d.parquet', entries[0].file.file_name)

    def test_commit_rejects_compact_increment_changelog_files(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('2024-01-15',),
            bucket=0,
            compact_increment=CompactIncrement(changelog_files=[_make_file('c.parquet')]),
        )
        with self.assertRaises(NotImplementedError):
            commit.commit([msg], commit_identifier=110)
        commit._try_commit.assert_not_called()

    # ---- compaction through the public commit() ----------------------------

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

        commit.commit([msg], commit_identifier=300)

        # Data side is empty, so only the COMPACT snapshot is committed.
        commit._try_commit.assert_called_once()
        kwargs = commit._try_commit.call_args.kwargs
        self.assertEqual('COMPACT', kwargs['commit_kind'])
        self.assertEqual(300, kwargs['commit_identifier'])
        self.assertFalse(kwargs['detect_conflicts'])
        self.assertFalse(kwargs['allow_rollback'])

    def test_commit_skips_when_no_messages(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()

        commit.commit([], commit_identifier=500)

        commit._try_commit.assert_not_called()

    def test_commit_skips_when_messages_have_no_files(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        empty_msg = CommitMessage(partition=('p1',), bucket=0)

        commit.commit([empty_msg], commit_identifier=600)

        commit._try_commit.assert_not_called()

    def test_commit_changelog_only_message_produces_no_snapshot(self, *_):
        # A message whose only data content is a changelog file produces no delta
        # entry and no index deletes, so neither side commits a snapshot.
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(changelog_files=[_make_file('c.parquet')]),
        )

        commit.commit([msg], commit_identifier=650)

        commit._try_commit.assert_not_called()

    def test_commit_compact_forwards_index_deletes(self, *_):
        # A compaction that also removes index manifest entries (e.g. a
        # global-index update) must forward those deletes to _try_commit;
        # otherwise the old index entries are left behind.
        commit = self._create_commit()
        commit._try_commit = Mock()
        deletes = [_make_index_delete('idx-old-1'), _make_index_delete('idx-old-2')]
        msg = CommitMessage(
            partition=('p1',),
            bucket=3,
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('new.parquet')],
            ),
            index_deletes=deletes,
        )

        commit.commit([msg], commit_identifier=610)

        commit._try_commit.assert_called_once()
        self.assertEqual(deletes, commit._try_commit.call_args.kwargs['index_deletes'])

    def test_commit_message_with_only_index_deletes(self, *_):
        # A message whose only content is index_deletes (no data, no compact
        # files) still carries meaningful state. With no compact increment it
        # rides the APPEND snapshot, matching how pure global-index update
        # messages have always committed.
        commit = self._create_commit()
        commit._try_commit = Mock()
        deletes = [_make_index_delete('idx-only')]
        msg = CommitMessage(partition=('p1',), bucket=0, index_deletes=deletes)

        commit.commit([msg], commit_identifier=620)

        commit._try_commit.assert_called_once()
        kwargs = commit._try_commit.call_args.kwargs
        self.assertEqual('APPEND', kwargs['commit_kind'])
        self.assertEqual(deletes, kwargs['index_deletes'])

    # ---- data + compaction in one batch: up to two snapshots ----------------

    def test_mixed_batch_produces_append_then_compact_snapshots(self, *_):
        # A batch mixing a data-only message and a compact-only message produces
        # two snapshots: APPEND for the writes, then COMPACT for the compaction.
        commit = self._create_commit()
        commit._try_commit = Mock()
        data_msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(new_files=[_make_file('append.parquet')]),
        )
        compact_msg = CommitMessage(
            partition=('p1',),
            bucket=1,
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('merged.parquet')],
            ),
        )

        commit.commit([data_msg, compact_msg], commit_identifier=700)

        self.assertEqual(2, commit._try_commit.call_count)
        kinds = [c.kwargs['commit_kind'] for c in commit._try_commit.call_args_list]
        self.assertEqual(['APPEND', 'COMPACT'], kinds)

    def test_single_message_with_both_increments_splits_into_two_snapshots(self, *_):
        # A single message carrying both a data increment and a compact increment
        # is split: new_files → APPEND, compact_before/after → COMPACT.
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(new_files=[_make_file('append.parquet')]),
            compact_increment=CompactIncrement(
                compact_before=[_make_file('old.parquet')],
                compact_after=[_make_file('merged.parquet')],
            ),
        )

        commit.commit([msg], commit_identifier=710)

        self.assertEqual(2, commit._try_commit.call_count)
        append_kwargs, compact_kwargs = [c.kwargs for c in commit._try_commit.call_args_list]
        self.assertEqual('APPEND', append_kwargs['commit_kind'])
        self.assertEqual('COMPACT', compact_kwargs['commit_kind'])
        append_names = [e.file.file_name for e in append_kwargs['commit_entries_plan'](None)]
        compact_names = [e.file.file_name for e in compact_kwargs['commit_entries_plan'](None)]
        self.assertEqual(['append.parquet'], append_names)
        self.assertEqual(['old.parquet', 'merged.parquet'], compact_names)

    def test_data_only_batch_produces_single_append_snapshot(self, *_):
        commit = self._create_commit()
        commit._try_commit = Mock()
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(new_files=[_make_file('append.parquet')]),
        )

        commit.commit([msg], commit_identifier=720)

        commit._try_commit.assert_called_once()
        self.assertEqual('APPEND', commit._try_commit.call_args.kwargs['commit_kind'])

    # ---- abort --------------------------------------------------------------

    def test_abort_deletes_compact_after_and_changelog_but_not_compact_before(self, *_):
        """A failed/cancelled compaction must clean every file it produced:
        compact_after and compact changelog files, alongside the data-side
        new_files/changelog. compact_before are pre-existing inputs and must be
        left untouched."""
        commit = self._create_commit()

        def _with_path(name):
            f = _make_file(name)
            f.file_path = name
            return f

        before = _with_path('before.parquet')
        after = _with_path('after.parquet')
        compact_changelog = _with_path('compact-changelog.parquet')
        new_file = _with_path('new.parquet')
        data_changelog = _with_path('data-changelog.parquet')
        msg = CommitMessage(
            partition=('p1',),
            bucket=0,
            data_increment=DataIncrement(
                new_files=[new_file],
                changelog_files=[data_changelog],
            ),
            compact_increment=CompactIncrement(
                compact_before=[before],
                compact_after=[after],
                changelog_files=[compact_changelog],
            ),
        )

        commit.abort([msg])

        deleted = {c.args[0] for c in self.mock_table.file_io.delete_quietly.call_args_list}
        self.assertEqual(
            {'new.parquet', 'data-changelog.parquet', 'after.parquet', 'compact-changelog.parquet'},
            deleted,
        )
        self.assertNotIn('before.parquet', deleted)


if __name__ == '__main__':
    unittest.main()
