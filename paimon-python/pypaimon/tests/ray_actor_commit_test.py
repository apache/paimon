# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import glob
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.write.file_store_commit import FileStoreCommit
from pypaimon.write.ray_datasink import (PaimonActorDatasink,
                                         PaimonCommitterActor,
                                         write_paimon_dataset)

SCHEMA = pa.schema([
    ('id', pa.int32()),
    ('value', pa.string()),
])


class _FakeTaskContext:
    """Stand-in for ray.data's TaskContext.

    PaimonActorDatasink.write only reads ``task_idx`` off it (to build the
    commit tag and the stable per-task commit_user), so driving the datasink
    directly needs nothing more than this.
    """

    def __init__(self, task_idx):
        self.task_idx = task_idx


class RayActorCommitTest(unittest.TestCase):
    """Tests for Ray actor commit mode.

    Actor mode is scoped to append-only tables. Primary-key tables assign
    sequence numbers from a snapshot-derived baseline read when each writer is
    created, so parallel Ray tasks writing the same bucket can assign
    overlapping sequence numbers no matter how the commits are ordered.
    Serializing commits through the actor does not fix that, so those tables
    are rejected up front -- see the rejection tests below.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        try:
            shutil.rmtree(cls.tempdir)
        except OSError:
            pass

    def _create_append_table(self, name, options=None):
        """Create an append-only (BUCKET_UNAWARE) table and return it."""
        schema = Schema.from_pyarrow_schema(SCHEMA, options=options)
        self.catalog.create_table(f'default.{name}', schema, False)
        return self.catalog.get_table(f'default.{name}')

    @staticmethod
    def _read_all(table):
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        return read_builder.new_read().to_arrow(table_scan.plan().splits())

    @staticmethod
    def _commit_users(table):
        """commit_user of every snapshot, in id order."""
        snapshot_manager = table.snapshot_manager()
        latest = snapshot_manager.get_latest_snapshot()
        if latest is None:
            return []
        users = []
        for snapshot_id in range(1, latest.id + 1):
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is not None:
                users.append(snapshot.commit_user)
        return users

    # ------------------------------------------------------------------
    # Happy paths
    # ------------------------------------------------------------------

    def test_actor_mode_basic_write(self):
        table = self._create_append_table('test_actor_basic')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict(
                {'id': [1, 2, 3, 4, 5], 'value': ['a', 'b', 'c', 'd', 'e']},
                schema=SCHEMA
            )
        )

        result = write_paimon_dataset(ds, table, commit_mode='actor')

        self.assertIsNotNone(result)
        self.assertEqual(result.row_count, 5)

        arrow = self._read_all(table)
        rows = sorted(zip(arrow.column('id').to_pylist(),
                          arrow.column('value').to_pylist()))
        self.assertEqual(rows, [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')])

    def test_actor_mode_multiple_batches(self):
        """Several write tasks commit through the actor without losing or
        duplicating rows."""
        table = self._create_append_table('test_actor_multi')

        tables = [
            pa.Table.from_pydict(
                {'id': list(range(i * 10, (i + 1) * 10)),
                 'value': [f'v{j}' for j in range(i * 10, (i + 1) * 10)]},
                schema=SCHEMA
            )
            for i in range(5)
        ]
        ds = ray.data.from_arrow(tables)

        result = write_paimon_dataset(ds, table, commit_mode='actor')

        self.assertIsNotNone(result)
        self.assertEqual(result.row_count, 50)

        arrow = self._read_all(table)
        ids = sorted(arrow.column('id').to_pylist())
        # Exactly once: no row lost, none committed twice.
        self.assertEqual(ids, list(range(50)))

    def test_actor_mode_commits_incrementally(self):
        """Each write task commits its own snapshot rather than waiting for a
        single coordinator commit -- this is what actor mode buys over
        two_phase."""
        table = self._create_append_table('test_actor_incremental')

        tables = [
            pa.Table.from_pydict(
                {'id': [i], 'value': [f'v{i}']}, schema=SCHEMA
            )
            for i in range(4)
        ]
        ds = ray.data.from_arrow(tables).repartition(4)

        result = write_paimon_dataset(ds, table, commit_mode='actor')
        self.assertEqual(result.row_count, 4)

        latest = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual(result.snapshot_id, latest.id)
        self.assertEqual(len(self._read_all(table)), 4)

        # two_phase produces exactly one snapshot no matter how many tasks ran,
        # so "more than one snapshot, each from a different commit_user" is the
        # assertion that actually distinguishes the two modes. `>= 1` does not:
        # two_phase satisfies it too.
        commit_users = self._commit_users(table)
        if len(commit_users) == 1:
            self.skipTest(
                'Ray fused the whole dataset into one write task, so this run '
                'cannot distinguish actor from two_phase'
            )
        self.assertEqual(latest.id, len(commit_users))
        # One commit per task: no task committed twice, and no single
        # coordinator committed on everyone's behalf.
        self.assertEqual(len(set(commit_users)), len(commit_users))

    def test_actor_mode_data_is_visible_before_the_write_finishes(self):
        """Actor mode's point is that a task's rows are queryable as soon as
        that task returns, without waiting for the rest of the write.

        Driving the datasink directly is what makes this observable: under
        write_paimon_dataset the only moment available to look is after every
        task has already finished, where two_phase looks identical.
        """
        name = 'test_actor_visible_midwrite'
        table = self._create_append_table(name)

        datasink = PaimonActorDatasink(
            table,
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        datasink.on_write_start()
        try:
            first = pa.Table.from_pydict(
                {'id': [1], 'value': ['a']}, schema=SCHEMA)
            datasink.write([first], _FakeTaskContext(0))

            # on_write_complete has NOT run, so the write is still in progress.
            self.assertEqual(
                sorted(self._read_all(table).column('id').to_pylist()), [1],
                'task 0 returned but its row is not readable yet',
            )

            second = pa.Table.from_pydict(
                {'id': [2], 'value': ['b']}, schema=SCHEMA)
            datasink.write([second], _FakeTaskContext(1))
            self.assertEqual(
                sorted(self._read_all(table).column('id').to_pylist()), [1, 2]
            )
            # Two independent snapshots, not one batched commit at the end.
            self.assertEqual(
                table.snapshot_manager().get_latest_snapshot().id, 2)
        finally:
            datasink._shutdown_actor()

    def test_actor_mode_applies_options(self):
        """`options` must reach the write tasks, not just be accepted and
        dropped. file.format is observable in the committed file names."""
        table = self._create_append_table('test_actor_options')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict(
                {'id': [1, 2, 3], 'value': ['a', 'b', 'c']}, schema=SCHEMA
            )
        )

        result = write_paimon_dataset(
            ds, table,
            commit_mode='actor',
            options={'file.format': 'orc'},
        )

        self.assertEqual(result.row_count, 3)

        table_dir = os.path.join(self.warehouse, 'default.db', 'test_actor_options')
        orc_files = glob.glob(os.path.join(table_dir, '**', '*.orc'), recursive=True)
        parquet_files = glob.glob(
            os.path.join(table_dir, '**', '*.parquet'), recursive=True
        )
        self.assertTrue(orc_files, f'no .orc data files under {table_dir}')
        self.assertFalse(parquet_files, 'file.format=orc was not applied')

        self.assertEqual(len(self._read_all(table)), 3)

    def test_two_phase_mode_applies_options(self):
        """The two_phase path also has to forward `options` to the task-side
        writer builders instead of accepting and dropping them."""
        table = self._create_append_table('test_two_phase_options')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict(
                {'id': [1, 2, 3], 'value': ['a', 'b', 'c']}, schema=SCHEMA
            )
        )

        result = write_paimon_dataset(
            ds, table,
            commit_mode='two_phase',
            options={'file.format': 'orc'},
        )

        self.assertEqual(result.row_count, 3)

        table_dir = os.path.join(
            self.warehouse, 'default.db', 'test_two_phase_options'
        )
        self.assertTrue(
            glob.glob(os.path.join(table_dir, '**', '*.orc'), recursive=True),
            f'no .orc data files under {table_dir}',
        )
        self.assertFalse(
            glob.glob(os.path.join(table_dir, '**', '*.parquet'), recursive=True),
            'file.format=orc was not applied',
        )
        self.assertEqual(len(self._read_all(table)), 3)

    # ------------------------------------------------------------------
    # Commit dedup (actor replay / whole-task retry)
    # ------------------------------------------------------------------

    def _prepare_commit_messages(self, table, data):
        table_write = table.new_batch_write_builder().new_write()
        try:
            table_write.write_arrow(pa.Table.from_pydict(data, schema=SCHEMA))
            return table_write.prepare_commit()
        finally:
            table_write.close()

    def test_committer_actor_skips_replayed_commit(self):
        """A replayed commit -- same commit_user and base_snapshot_id, as Ray
        resends after the actor dies mid-commit or a write task is retried --
        must be recognised and skipped, not committed twice."""
        table = self._create_append_table('test_actor_replay')
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier='default.test_actor_replay',
        )
        messages = self._prepare_commit_messages(
            table, {'id': [1, 2, 3], 'value': ['a', 'b', 'c']}
        )

        committer.commit(messages, commit_user='stable-user', dedup_base={'snapshot_id': 0, 'file_millis': None})
        first_snapshot = table.snapshot_manager().get_latest_snapshot().id

        # Replay with identical args.
        committer.commit(messages, commit_user='stable-user', dedup_base={'snapshot_id': 0, 'file_millis': None})

        self.assertEqual(
            table.snapshot_manager().get_latest_snapshot().id,
            first_snapshot,
            'replayed commit produced a second snapshot',
        )
        self.assertEqual(len(self._read_all(table)), 3)

    def test_committer_actor_does_not_skip_distinct_commits(self):
        """Negative control: the dedup scan must only match a replay of the
        *same* task, not suppress a different task's commit."""
        table = self._create_append_table('test_actor_distinct')
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier='default.test_actor_distinct',
        )

        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='task-0', dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        committer.commit(
            self._prepare_commit_messages(table, {'id': [2], 'value': ['b']}),
            commit_user='task-1', dedup_base={'snapshot_id': 0, 'file_millis': None},
        )

        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)
        arrow = self._read_all(table)
        self.assertEqual(sorted(arrow.column('id').to_pylist()), [1, 2])

    def test_committer_actor_dedup_survives_intervening_commits(self):
        """The pinned base is below the task's own first commit, so the replay
        is still found after other tasks have committed on top of it. This is
        the case a per-attempt base read would get wrong."""
        table = self._create_append_table('test_actor_replay_interleaved')
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier='default.test_actor_replay_interleaved',
        )
        messages = self._prepare_commit_messages(
            table, {'id': [1], 'value': ['a']}
        )

        # Our task commits, then two other tasks commit on top of it.
        committer.commit(messages, commit_user='task-0', dedup_base={'snapshot_id': 0, 'file_millis': None})
        for i, user in enumerate(('task-1', 'task-2'), start=2):
            committer.commit(
                self._prepare_commit_messages(
                    table, {'id': [i], 'value': [f'v{i}']}
                ),
                commit_user=user, dedup_base={'snapshot_id': 0, 'file_millis': None},
            )
        snapshot_before_replay = table.snapshot_manager().get_latest_snapshot().id
        self.assertEqual(snapshot_before_replay, 3)

        # task-0 is retried. base_snapshot_id is the driver-pinned 0, not a
        # re-read of the now-current snapshot 3.
        committer.commit(messages, commit_user='task-0', dedup_base={'snapshot_id': 0, 'file_millis': None})

        self.assertEqual(
            table.snapshot_manager().get_latest_snapshot().id,
            snapshot_before_replay,
            'replay was committed again instead of being deduped',
        )
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1, 2, 3]
        )

    def test_actor_datasink_pins_dedup_base_on_driver(self):
        """on_write_start pins the dedup base once, before any task runs.

        Reading it per attempt inside write() instead would place the base
        above the task's own earlier commit, so the scan range (base, latest]
        would exclude it, the replay would go undetected and the task's files
        would be committed twice.
        """
        name = 'test_actor_pinned_base'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )

        def seed(row_id, user):
            committer.commit(
                self._prepare_commit_messages(
                    table, {'id': [row_id], 'value': [f'v{row_id}']}
                ),
                commit_user=user, dedup_base={'snapshot_id': 0, 'file_millis': None},
            )

        seed(1, 'seed-0')
        seed(2, 'seed-1')
        pinned = table.snapshot_manager().get_latest_snapshot().id
        pinned_millis = table.snapshot_manager().snapshot_file_millis(pinned)

        datasink = PaimonActorDatasink(
            table,
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        self.assertIsNone(datasink._dedup_base)

        datasink.on_write_start()
        try:
            self.assertEqual(
                datasink._dedup_base,
                {'snapshot_id': pinned, 'file_millis': pinned_millis},
            )

            # Snapshots keep landing while write tasks run. The pinned base
            # must not follow them.
            seed(3, 'seed-2')
            self.assertGreater(
                table.snapshot_manager().get_latest_snapshot().id, pinned
            )
            self.assertEqual(datasink._dedup_base['snapshot_id'], pinned)
        finally:
            datasink._shutdown_actor()

    def test_dedup_base_and_scan_follow_the_configured_branch(self):
        """With ``branch`` in options, commits go to that branch, so the dedup
        bound and the scan have to be taken there too.

        Taking them on the main-branch table instead yields a bound from a
        different id line: here main is at 3 while the branch is empty, so the
        scan range (3, 1] is empty, a replay goes undetected and the task's
        files are committed twice.
        """
        name = 'test_actor_branch'
        table = self._create_append_table(name)
        main_committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        for i in (1, 2, 3):
            main_committer.commit(
                self._prepare_commit_messages(
                    table, {'id': [i], 'value': [f'v{i}']}
                ),
                commit_user=f'main-{i}',
                dedup_base={'snapshot_id': 0, 'file_millis': None},
            )
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 3)

        # A branch created without a tag copies only the schema, so it starts
        # with no snapshots at all -- its ids are a separate line from main's.
        self.catalog.create_branch(f'default.{name}', 'dev')
        branch_table = self.catalog.get_table(f'default.{name}$branch_dev')
        self.assertIsNone(branch_table.snapshot_manager().get_latest_snapshot())

        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
            options={'branch': 'dev'},
        )
        self.assertEqual(
            committer.dedup_base(),
            {'snapshot_id': 0, 'file_millis': None},
            'the bound describes main instead of the branch being written',
        )

        messages = self._prepare_commit_messages(
            branch_table, {'id': [9], 'value': ['dev']}
        )
        base = committer.dedup_base()
        committer.commit(messages, commit_user='dev-0', dedup_base=base)
        self.assertEqual(
            branch_table.snapshot_manager().get_latest_snapshot().id, 1)

        committer.commit(messages, commit_user='dev-0', dedup_base=base)
        self.assertEqual(
            branch_table.snapshot_manager().get_latest_snapshot().id, 1,
            'the replay was committed again instead of being deduped',
        )
        # The branch write left main alone.
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 3)
        self.assertEqual(
            sorted(self._read_all(branch_table).column('id').to_pylist()), [9])

    def test_actor_datasink_reports_the_branch_it_wrote(self):
        """on_write_complete reports the snapshot of the branch written, not
        whatever main happens to be at."""
        name = 'test_actor_branch_result'
        table = self._create_append_table(name)
        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )
        write_paimon_dataset(ds, table, commit_mode='actor')
        write_paimon_dataset(
            ray.data.from_arrow(
                pa.Table.from_pydict({'id': [2], 'value': ['b']}, schema=SCHEMA)
            ),
            table, commit_mode='actor',
        )
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)

        self.catalog.create_branch(f'default.{name}', 'dev')
        branch_table = self.catalog.get_table(f'default.{name}$branch_dev')

        result = write_paimon_dataset(
            ray.data.from_arrow(
                pa.Table.from_pydict({'id': [9], 'value': ['z']}, schema=SCHEMA)
            ),
            table, commit_mode='actor', options={'branch': 'dev'},
        )

        self.assertEqual(
            result.snapshot_id,
            branch_table.snapshot_manager().get_latest_snapshot().id,
        )
        self.assertEqual(result.snapshot_id, 1)
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)

    def _rollback_to(self, table, snapshot_id):
        """Drop every snapshot above ``snapshot_id`` and re-point LATEST.

        A crude stand-in for rollback, but the part that matters to dedup is
        exactly what it reproduces: ids above the rollback point are freed and
        handed out again to different commits.
        """
        snapshot_manager = table.snapshot_manager()
        latest = snapshot_manager.get_latest_snapshot().id
        for dropped in range(snapshot_id + 1, latest + 1):
            table.file_io.delete(snapshot_manager.get_snapshot_path(dropped))
        table.file_io.overwrite_file_utf8(
            snapshot_manager.latest_file, str(snapshot_id))

    def test_dedup_does_not_trust_a_bound_whose_snapshot_was_rolled_back(self):
        """A bound is an id *and* the mtime of that id's file, because a
        rollback hands the id out again.

        Bound at snapshot 3, rolled back to 1, a different commit then takes id
        2. Trusting the bare id 3 makes the scan range (3, 2] empty, so the
        replay is not found and lands a second time. The mtime no longer
        matching is what tells the scan the id line changed under it.
        """
        name = 'test_actor_rollback_bound'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        for i in (1, 2, 3):
            committer.commit(
                self._prepare_commit_messages(
                    table, {'id': [i], 'value': [f'v{i}']}
                ),
                commit_user=f'seed-{i}',
                dedup_base={'snapshot_id': 0, 'file_millis': None},
            )
        stale_base = committer.dedup_base()
        self.assertEqual(stale_base['snapshot_id'], 3)
        self.assertIsNotNone(stale_base['file_millis'])

        self._rollback_to(table, 1)
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 1)

        # A task of the bounded write commits, taking the freed id 2.
        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']}
        )
        committer.commit(messages, commit_user='task-0', dedup_base=stale_base)
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)

        committer.commit(messages, commit_user='task-0', dedup_base=stale_base)
        self.assertEqual(
            table.snapshot_manager().get_latest_snapshot().id, 2,
            'the replay was committed again: the bound was trusted on its '
            'snapshot id alone, after a rollback re-used that id',
        )
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1, 42])

    def test_dedup_tolerates_an_unreadable_unrelated_snapshot(self):
        """An id the scan cannot read is only fatal when the answer depends on
        it.

        Here our own commit is found at snapshot 2, so snapshot 1 being gone
        (expired, or mid-write by another client) decides nothing -- giving up
        with "evidence missing" would fail a write that is provably a replay.
        """
        name = 'test_actor_hole_in_window'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        base = committer.dedup_base()
        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='other-0', dedup_base=base,
        )
        messages = self._prepare_commit_messages(
            table, {'id': [2], 'value': ['b']}
        )
        committer.commit(messages, commit_user='task-0', dedup_base=base)
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)

        snapshot_manager = table.snapshot_manager()
        table.file_io.delete(snapshot_manager.get_snapshot_path(1))

        committer.commit(messages, commit_user='task-0', dedup_base=base)
        self.assertEqual(
            table.snapshot_manager().get_latest_snapshot().id, 2,
            'the replay was committed again despite its own snapshot still '
            'being there to find',
        )

    def test_vouching_actor_skips_the_scan_only_for_unseen_commit_users(self):
        """An actor alive since before the write's first commit knows a
        commit_user it has never attempted has nothing to be a replay of, so
        the scan can be skipped -- which is what keeps a write's dedup cost
        linear rather than quadratic in its number of commits.

        The converse is deliberately NOT taken from memory: once the actor has
        attempted that commit_user, the filesystem decides.
        """
        name = 'test_actor_vouch_gate'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        # Outside a Ray actor there is no incarnation to check, so vouching is
        # off by default; this is the state an original actor instance is in.
        committer._sees_every_commit = True

        # Another task commits first. Without an existing snapshot the scan is
        # skipped for a different reason entirely (an empty table cannot hold a
        # duplicate), which would make the assertions below vacuous.
        committer.commit(
            self._prepare_commit_messages(table, {'id': [0], 'value': ['x']}),
            commit_user='other-0',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        base = committer.dedup_base()
        self.assertEqual(base['snapshot_id'], 1)
        messages = self._prepare_commit_messages(
            table, {'id': [1], 'value': ['a']}
        )

        scans = []
        original = FileStoreCommit._find_own_commit

        def spy(commit_self, *args, **kwargs):
            scans.append(args[:2])
            return original(commit_self, *args, **kwargs)

        with patch.object(FileStoreCommit, '_find_own_commit', spy):
            committer.commit(messages, commit_user='task-0', dedup_base=base)
            self.assertEqual(
                scans, [], 'scanned for a commit_user the actor can vouch for')

            committer.commit(messages, commit_user='task-0', dedup_base=base)
            self.assertTrue(
                scans, 'took "I committed this already" from memory instead '
                       'of checking the filesystem')

        self.assertEqual(
            table.snapshot_manager().get_latest_snapshot().id, 2,
            'the replay was committed again',
        )

    def test_commit_metadata_options_land_in_snapshot_properties(self):
        """``commit.metadata.*`` tags one commit with the caller's own metadata,
        re-prefixed so it cannot collide with Paimon's own snapshot keys."""
        name = 'test_actor_commit_metadata'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
            options={'commit.metadata.job': 'nightly-42'},
        )
        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='meta-0',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )

        snapshot = table.snapshot_manager().get_snapshot_by_id(1)
        self.assertEqual(
            snapshot.properties.get('paimon.commit.metadata.job'),
            'nightly-42',
        )

    def test_commit_without_metadata_options_has_no_extra_properties(self):
        """Negative control: the metadata plumbing must not invent properties
        for a commit that asked for none."""
        name = 'test_actor_no_commit_metadata'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='nometa-0',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )

        snapshot = table.snapshot_manager().get_snapshot_by_id(1)
        properties = snapshot.properties or {}
        self.assertEqual(
            [key for key in properties if 'commit.metadata' in key], [])

    def test_commit_survives_the_latest_snapshot_expiring_mid_read(self):
        """The interleaving that matters: the snapshot a read resolved is
        expired before its file is read, while the snapshot that replaced it is
        intact and readable. Every read on the commit path has to relocate past
        it -- bounding the dedup scan is only one of them, and protecting just
        that one leaves the commit itself failing on a recoverable race."""
        name = 'test_latest_expired_mid_read'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        # Two snapshots, then drop the older one the way retention would.
        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='seed-0',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        committer.commit(
            self._prepare_commit_messages(table, {'id': [2], 'value': ['b']}),
            commit_user='seed-1',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        manager = table.snapshot_manager()
        manager.file_io.delete(manager.get_snapshot_path(1))

        # Bound the dedup scan above the snapshot that is really gone, so this
        # test isolates the mid-read race. A window that spans a permanently
        # expired id is a genuine evidence hole, and refusing it is the
        # behaviour the unreadable-snapshot test covers.
        base = committer.dedup_base()
        self.assertEqual(base['snapshot_id'], 2)

        messages = self._prepare_commit_messages(
            table, {'id': [3], 'value': ['c']})

        # One-shot: the first snapshot-file read raises as if that file had been
        # expired at exactly that instant. Every read after it behaves normally,
        # so the replacement snapshot is there to be found -- which is what
        # makes failing the write the wrong answer here.
        file_io = manager.file_io
        original_read = type(file_io).read_file_utf8
        injected = []

        def read_with_one_expiry(self, path):
            if not injected and '/snapshot-' in str(path):
                injected.append(str(path))
                raise FileNotFoundError(path)
            return original_read(self, path)

        with patch.object(type(file_io), 'read_file_utf8', read_with_one_expiry):
            committer.commit(
                messages,
                commit_user='after-race-0',
                dedup_base=base,
            )

        self.assertTrue(injected, 'the race was never injected')
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 3)
        # Dropping snapshot 1's metadata does not drop its rows: snapshot 3's
        # base manifest still references that data file.
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1, 2, 3])

    # ------------------------------------------------------------------
    # Rejections
    # ------------------------------------------------------------------

    def test_actor_mode_rejects_overwrite(self):
        table = self._create_append_table('test_actor_overwrite')
        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(ValueError) as ctx:
            write_paimon_dataset(ds, table, commit_mode='actor', overwrite=True)

        self.assertIn('actor', str(ctx.exception).lower())
        self.assertIn('overwrite', str(ctx.exception).lower())

    def test_actor_mode_rejects_map_groups(self):
        """map_groups introduces a barrier, which defeats incremental
        visibility."""
        table = self._create_append_table('test_actor_shuffle')
        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(ValueError) as ctx:
            write_paimon_dataset(
                ds, table,
                commit_mode='actor',
                hash_fixed_precluster='map_groups'
            )

        self.assertIn('actor', str(ctx.exception).lower())
        self.assertIn('map_groups', str(ctx.exception).lower())

    def test_actor_mode_rejects_hash_fixed_primary_key(self):
        """Serializing commits does not prevent overlapping sequence numbers:
        they are baked into the data files before the commit is issued, from a
        baseline each writer reads independently."""
        schema = Schema.from_pyarrow_schema(
            SCHEMA, primary_keys=['id'], options={'bucket': '2'}
        )
        self.catalog.create_table('default.test_actor_pk', schema, False)
        table = self.catalog.get_table('default.test_actor_pk')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(ValueError) as ctx:
            write_paimon_dataset(ds, table, commit_mode='actor')

        self.assertIn('sequence number', str(ctx.exception).lower())

    def test_actor_mode_rejects_postpone_bucket(self):
        """POSTPONE tables are committed by a driver-side coordinator, so an
        actor request would be silently downgraded to two-phase."""
        schema = Schema.from_pyarrow_schema(
            SCHEMA, primary_keys=['id'], options={'bucket': '-2'}
        )
        self.catalog.create_table('default.test_actor_postpone', schema, False)
        table = self.catalog.get_table('default.test_actor_postpone')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(ValueError) as ctx:
            write_paimon_dataset(ds, table, commit_mode='actor')

        self.assertIn('postpone', str(ctx.exception).lower())
        self.assertIn('two_phase', str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()
