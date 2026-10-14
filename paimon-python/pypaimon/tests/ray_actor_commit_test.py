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
from pypaimon.utils.file_store_path_factory import canonical_data_file_path
from pypaimon.write.file_store_commit import (CommitDedup,
                                              DedupEvidenceMissing,
                                              FileStoreCommit)
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

    def test_actor_mode_empty_write_to_populated_table_returns_none(self):
        """No task committed, so the write reports None.

        The table already holding a snapshot is not evidence that *this* write
        committed anything: reporting that snapshot as the write's result
        violates the write_paimon contract (None when no data/snapshot was
        committed) and attributes another write's snapshot to this one.
        """
        table = self._create_append_table('test_actor_empty_write')
        seeded = write_paimon_dataset(
            ray.data.from_arrow(
                pa.Table.from_pydict(
                    {'id': [1], 'value': ['a']}, schema=SCHEMA)
            ),
            table, commit_mode='actor',
        )
        self.assertIsNotNone(seeded)

        empty = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [], 'value': []}, schema=SCHEMA))
        result = write_paimon_dataset(empty, table, commit_mode='actor')

        self.assertIsNone(result)
        # The seeded snapshot is untouched.
        self.assertEqual(
            table.snapshot_manager().get_latest_snapshot().id,
            seeded.snapshot_id)

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

    def test_public_write_paimon_reaches_actor_mode(self):
        """``commit_mode`` and ``options`` must be reachable through the
        documented public entry point, not only through the internal
        ``write_paimon_dataset``. ``file.format`` doubles as proof that
        both kwargs really traversed the wrapper rather than being
        accepted and dropped.
        """
        from pypaimon.ray.ray_paimon import write_paimon

        table = self._create_append_table('test_public_actor')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict(
                {'id': [1, 2, 3], 'value': ['a', 'b', 'c']}, schema=SCHEMA
            )
        )

        result = write_paimon(
            ds, 'default.test_public_actor', {'warehouse': self.warehouse},
            commit_mode='actor',
            options={'file.format': 'orc'},
        )

        self.assertEqual(result.row_count, 3)

        table_dir = os.path.join(
            self.warehouse, 'default.db', 'test_public_actor'
        )
        self.assertTrue(
            glob.glob(os.path.join(table_dir, '**', '*.orc'), recursive=True),
            f'no .orc data files under {table_dir}',
        )
        self.assertFalse(
            glob.glob(os.path.join(table_dir, '**', '*.parquet'), recursive=True),
            'file.format=orc did not survive the public wrapper',
        )
        self.assertEqual(len(self._read_all(table)), 3)

    def test_public_write_paimon_rejects_unknown_commit_mode(self):
        """The wrapper must forward ``commit_mode`` to the validation that
        rejects it, rather than silently defaulting to two_phase.
        """
        from pypaimon.ray.ray_paimon import write_paimon

        self._create_append_table('test_public_bad_mode')

        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(ValueError) as ctx:
            write_paimon(
                ds, 'default.test_public_bad_mode',
                {'warehouse': self.warehouse},
                commit_mode='nonsense',
            )
        self.assertIn('nonsense', str(ctx.exception))

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

    def test_commit_refuses_a_bound_whose_snapshot_was_rolled_back(self):
        """A rollback under an in-flight write must fail the commit outright.

        A bound is an id *and* the mtime of that id's file, because a rollback
        hands the id out again. Bound at snapshot 3, rolled back to 1: the
        bound's file is gone and the id line now runs below it, so the files
        this write (or its replays) rely on may have been deleted. Probing for
        a new scan window would let the write continue over a history that
        changed under it; instead the commit is refused, consistent with the
        documented recovery procedure (stop writers before rolling back).
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

        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']}
        )
        with self.assertRaises(RuntimeError) as ctx:
            committer.commit(
                messages, commit_user='task-0', dedup_base=stale_base)
        self.assertIn('rollback', str(ctx.exception).lower())

        # Nothing landed: the table still holds only the rolled-back state.
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 1)
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1])

    def test_commit_refuses_a_pinned_bound_after_table_reset(self):
        """A reset to empty under a pinned bound must fail, not re-publish.

        The duplicate scan is skipped on an empty table -- nothing on it can
        be a duplicate -- but a bound pinned at id >= 1 proves the table was
        NOT empty when this write started, so emptiness here means the
        history was reset. The replayed messages reference files a reset
        deletes; committing them publishes a snapshot whose reads then fail
        with FileNotFoundError. The bound's validity must be checked on every
        commit, independent of whether a duplicate scan is needed.
        """
        name = 'test_actor_reset_bound'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        messages = self._prepare_commit_messages(
            table, {'id': [1], 'value': ['a']}
        )
        committer.commit(
            messages, commit_user='task-0',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        stale_base = committer.dedup_base()
        self.assertEqual(stale_base['snapshot_id'], 1)

        # Reset: every snapshot and the LATEST pointer are gone, as after
        # dropping and recreating the table in place.
        snapshot_manager = table.snapshot_manager()
        table.file_io.delete(snapshot_manager.get_snapshot_path(1))
        table.file_io.delete(snapshot_manager.latest_file)
        self.assertIsNone(snapshot_manager.get_latest_snapshot())

        with self.assertRaises(RuntimeError) as ctx:
            committer.commit(
                messages, commit_user='task-0', dedup_base=stale_base)
        self.assertIn('rollback', str(ctx.exception).lower())
        # Nothing was published over the reset history.
        self.assertIsNone(snapshot_manager.get_latest_snapshot())

    def test_vouched_commit_still_validates_the_pinned_bound(self):
        """Vouching skips the duplicate scan, never the history check.

        An actor that has seen every commit of this write may skip the scan
        for a commit_user it has never attempted -- there is nothing to find.
        But the pinned bound names a history, and a rollback below it means
        the files this write relies on may be gone. That check must run even
        when the scan is skipped.
        """
        name = 'test_actor_vouch_rollback'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        committer._sees_every_commit = True
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

        self._rollback_to(table, 1)

        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']}
        )
        with self.assertRaises(RuntimeError) as ctx:
            committer.commit(
                messages, commit_user='task-9', dedup_base=stale_base)
        self.assertIn('rollback', str(ctx.exception).lower())
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 1)
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1])

    def test_cas_retry_refuses_an_observed_latest_regression(self):
        """A retry must notice the latest id falling below what it saw.

        The pinned base only proves history down at the base; the failed
        attempt's own observation of the latest snapshot is a second, higher
        boundary. Expiry removes the oldest snapshots, never the latest, so
        the latest id regressing below an id this very process observed is
        unambiguous rollback -- even when the rollback stops above the base,
        leaving the base's file and mtime intact. Without this check the
        retry treats (observed, current] as a trustworthy empty dedup window
        and re-publishes messages whose files the rollback deleted.
        """
        name = 'test_actor_cas_retry_regression'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='seed-1',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        # Base pinned at 1 -- the rollback below will stop here, so the
        # anchor's file and mtime survive unchanged.
        pinned_base = committer.dedup_base()
        self.assertEqual(pinned_base['snapshot_id'], 1)
        for i in (2, 3):
            committer.commit(
                self._prepare_commit_messages(
                    table, {'id': [i], 'value': [f'v{i}']}
                ),
                commit_user=f'seed-{i}', dedup_base=pinned_base,
            )
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 3)

        builder = table.new_batch_write_builder()
        builder.commit_user = 'task-0'
        commit = builder.new_commit(dedup=CommitDedup(
            on_first_attempt=True,
            base_snapshot_id=pinned_base['snapshot_id'],
            base_file_millis=pinned_base['file_millis'],
        ))
        fsc = commit.file_store_commit
        original_cas = fsc.snapshot_commit.commit
        state = {'failed_once': False}
        test = self

        def cas_publishes_then_loses_the_ack(base_uuid, snapshot, statistics):
            ok = original_cas(base_uuid, snapshot, statistics)
            if not state['failed_once']:
                state['failed_once'] = True
                # The commit landed (snapshot 4), but before the ack reaches
                # the committer the table is rolled back to 1 and the ack is
                # lost, so the committer retries with an uncertain result.
                test._rollback_to(table, 1)
                raise RuntimeError('ack lost')
            return ok

        fsc.snapshot_commit.commit = cas_publishes_then_loses_the_ack
        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']}
        )
        try:
            with self.assertRaises(RuntimeError) as ctx:
                commit.commit(messages)
        finally:
            commit.close()
        self.assertIn('rollback', str(ctx.exception).lower())
        # Nothing was re-published over the rolled-back history.
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 1)
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1])

    def test_cas_retry_refuses_a_regrown_history(self):
        """A regrown id line is a different history, not a recovered one.

        Same lost-ACK scenario as above, except other writers push the
        latest id back to -- or past -- what the failed attempt observed
        before the retry runs. Every numeric comparison then passes: the
        pinned base's file is untouched and current >= observed. What
        changed is the *identity* of the observed snapshot: the file at that
        id now holds a different snapshot (different uuid) than the one this
        process read moments ago, which only a rollback-and-regrow can
        produce. The retry must compare identity, not ids, and refuse.
        """
        for regrow in (2, 3):  # latest becomes 3 (== observed) and 4 (>)
            with self.subTest(regrow=regrow):
                name = f'test_actor_cas_regrow_{regrow}'
                table = self._create_append_table(name)
                committer = PaimonCommitterActor(
                    catalog_options={'warehouse': self.warehouse},
                    table_identifier=f'default.{name}',
                )
                committer.commit(
                    self._prepare_commit_messages(
                        table, {'id': [1], 'value': ['a']}),
                    commit_user='seed-1',
                    dedup_base={'snapshot_id': 0, 'file_millis': None},
                )
                pinned_base = committer.dedup_base()
                for i in (2, 3):
                    committer.commit(
                        self._prepare_commit_messages(
                            table, {'id': [i], 'value': [f'v{i}']}
                        ),
                        commit_user=f'seed-{i}', dedup_base=pinned_base,
                    )

                builder = table.new_batch_write_builder()
                builder.commit_user = 'task-0'
                commit = builder.new_commit(dedup=CommitDedup(
                    on_first_attempt=True,
                    base_snapshot_id=pinned_base['snapshot_id'],
                    base_file_millis=pinned_base['file_millis'],
                ))
                fsc = commit.file_store_commit
                original_cas = fsc.snapshot_commit.commit
                state = {'failed_once': False}
                test = self

                def cas_rolls_back_and_regrows(base_uuid, snapshot,
                                               statistics,
                                               _original=original_cas,
                                               _state=state,
                                               _table=table,
                                               _regrow=regrow):
                    ok = _original(base_uuid, snapshot, statistics)
                    if not _state['failed_once']:
                        _state['failed_once'] = True
                        # Snapshot 4 lands, the table is rolled back to 1,
                        # then other writers regrow the id line before the
                        # lost ACK forces a retry here.
                        test._rollback_to(_table, 1)
                        regrower = PaimonCommitterActor(
                            catalog_options={
                                'warehouse': test.warehouse},
                            table_identifier=(
                                f'default.{_table.identifier.object}'),
                        )
                        for j in range(_regrow):
                            regrower.commit(
                                test._prepare_commit_messages(
                                    _table,
                                    {'id': [90 + j], 'value': [f'r{j}']}
                                ),
                                commit_user=f'regrow-{j}',
                                dedup_base={'snapshot_id': 0,
                                            'file_millis': None},
                            )
                        raise RuntimeError('ack lost')
                    return ok

                fsc.snapshot_commit.commit = cas_rolls_back_and_regrows
                messages = self._prepare_commit_messages(
                    table, {'id': [42], 'value': ['task']}
                )
                try:
                    with self.assertRaises(RuntimeError) as ctx:
                        commit.commit(messages)
                finally:
                    commit.close()
                self.assertIn('rollback', str(ctx.exception).lower())
                # Only the regrown history remains; nothing was re-published
                # over it.
                self.assertEqual(
                    table.snapshot_manager().get_latest_snapshot().id,
                    1 + regrow)
                ids = sorted(self._read_all(table).column('id').to_pylist())
                self.assertNotIn(42, ids)

    def test_dedup_rescan_refuses_a_history_replaced_mid_scan(self):
        """A rescan that reads a replaced history must not keep the old one.

        The scan hits a transient FileNotFoundError on the observed latest;
        during that window the table is rolled back below it and other
        writers regrow the id line. The rescan then *reads* the new snapshot
        at the observed id -- same id, different uuid -- so the replacement
        is observed, not hypothetical. Keeping only the numeric upper bound
        and declaring "not a duplicate" lets _try_commit_once build the
        commit on the stale snapshot and publish over the regrown history,
        referencing files the rollback deleted. The identity the rescan read
        must be honored: refuse instead.
        """
        name = 'test_actor_rescan_race'
        table = self._create_append_table(name)
        committer = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        committer.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['a']}),
            commit_user='seed-1',
            dedup_base={'snapshot_id': 0, 'file_millis': None},
        )
        pinned_base = committer.dedup_base()
        for i in (2, 3):
            committer.commit(
                self._prepare_commit_messages(
                    table, {'id': [i], 'value': [f'v{i}']}
                ),
                commit_user=f'seed-{i}', dedup_base=pinned_base,
            )

        builder = table.new_batch_write_builder()
        builder.commit_user = 'task-0'
        commit = builder.new_commit(dedup=CommitDedup(
            on_first_attempt=True,
            base_snapshot_id=pinned_base['snapshot_id'],
            base_file_millis=pinned_base['file_millis'],
        ))
        fsc = commit.file_store_commit
        manager = fsc.snapshot_manager
        original_get = manager.get_snapshot_by_id
        state = {'raced': False}
        test = self

        def get_races_with_rollback_and_regrow(snapshot_id):
            if snapshot_id == 3 and not state['raced']:
                state['raced'] = True
                test._rollback_to(table, 1)
                regrower = PaimonCommitterActor(
                    catalog_options={'warehouse': test.warehouse},
                    table_identifier=f'default.{name}',
                )
                for j in (2, 3):
                    regrower.commit(
                        test._prepare_commit_messages(
                            table, {'id': [90 + j], 'value': [f'r{j}']}
                        ),
                        commit_user=f'regrow-{j}',
                        dedup_base={'snapshot_id': 0, 'file_millis': None},
                    )
                raise FileNotFoundError('expired mid-read')
            return original_get(snapshot_id)

        manager.get_snapshot_by_id = get_races_with_rollback_and_regrow
        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']}
        )
        try:
            with self.assertRaises(RuntimeError) as ctx:
                commit.commit(messages)
        finally:
            commit.close()
        self.assertIn('rollback', str(ctx.exception).lower())
        # Only the regrown history remains; nothing was published on top of
        # the stale snapshot.
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 3)
        ids = sorted(self._read_all(table).column('id').to_pylist())
        self.assertNotIn(42, ids)

    def test_authorization_rechecks_a_missing_latest(self):
        for permanent in (True, False):
            with self.subTest(permanent=permanent):
                name = 'authorization_missing_' + str(permanent).lower()
                table = self._create_append_table(name)
                actor = PaimonCommitterActor(
                    {'warehouse': self.warehouse}, 'default.' + name)
                actor.commit(
                    self._prepare_commit_messages(
                        table, {'id': [1], 'value': ['seed']}),
                    commit_user='seed',
                    dedup_base={'snapshot_id': 0, 'file_millis': None})
                pinned = actor.dedup_base()
                builder = table.new_batch_write_builder()
                builder.commit_user = 'task'
                commit = builder.new_commit(dedup=CommitDedup(
                    on_first_attempt=True,
                    base_snapshot_id=pinned['snapshot_id'],
                    base_file_millis=pinned['file_millis']))
                manager = commit.file_store_commit.snapshot_manager
                original_read = manager.file_io.read_file_utf8
                snapshot_path = manager.get_snapshot_path(1)
                reads = []

                def read_with_disappearing_snapshot(path):
                    if path == snapshot_path:
                        reads.append(path)
                        if len(reads) == 2:
                            if permanent:
                                table.file_io.delete(snapshot_path)
                                table.file_io.delete(manager.latest_file)
                            raise FileNotFoundError('snapshot disappeared')
                    return original_read(path)

                messages = self._prepare_commit_messages(
                    table, {'id': [42], 'value': ['task']})
                try:
                    with patch.object(manager.file_io, 'read_file_utf8',
                                      side_effect=read_with_disappearing_snapshot):
                        if permanent:
                            with self.assertRaises(RuntimeError):
                                commit.commit(messages)
                        else:
                            commit.commit(messages)
                finally:
                    commit.close()
                if permanent:
                    self.assertIsNone(manager.get_latest_snapshot())
                else:
                    self.assertGreaterEqual(len(reads), 3)
                    self.assertEqual(
                        sorted(self._read_all(table).column('id').to_pylist()),
                        [1, 42])

    def test_missing_authorization_observation_never_reaches_publication(self):
        table = self._create_append_table('authorization_expired_observation')
        actor = PaimonCommitterActor(
            {'warehouse': self.warehouse},
            'default.authorization_expired_observation')
        actor.commit(
            self._prepare_commit_messages(table, {'id': [1], 'value': ['seed']}),
            commit_user='seed',
            dedup_base={'snapshot_id': 0, 'file_millis': None})
        builder = table.new_batch_write_builder()
        builder.commit_user = 'task'
        commit = builder.new_commit(dedup=CommitDedup(on_first_attempt=True))
        manager = commit.file_store_commit.snapshot_manager
        original_read = manager.file_io.read_file_utf8
        original_cas = commit.file_store_commit.snapshot_commit.commit
        snapshot_path = manager.get_snapshot_path(1)
        reads = []

        def expire_observation_before_authorization(path):
            if path == snapshot_path:
                reads.append(path)
                if len(reads) == 3:
                    actor.commit(
                        self._prepare_commit_messages(
                            table, {'id': [99], 'value': ['other']}),
                        commit_user='other',
                        dedup_base={'snapshot_id': 0, 'file_millis': None})
                    table.file_io.delete(snapshot_path)
                    raise FileNotFoundError('latest advanced and observation expired')
            return original_read(path)

        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']})
        try:
            with patch.object(manager.file_io, 'read_file_utf8',
                              side_effect=expire_observation_before_authorization):
                with patch.object(commit.file_store_commit.snapshot_commit,
                                  'commit', wraps=original_cas) as publish:
                    with self.assertRaises(DedupEvidenceMissing):
                        commit.commit(messages)
                    publish.assert_not_called()
        finally:
            commit.close()
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()), [1, 99])

    def test_recovery_checks_files_after_duplicate_filtering(self):
        for already_committed in (True, False):
            with self.subTest(already_committed=already_committed):
                name = 'recovery_files_' + str(already_committed).lower()
                table = self._create_append_table(name)
                actor = PaimonCommitterActor(
                    {'warehouse': self.warehouse}, 'default.' + name)
                actor._sees_every_commit = False
                pinned = actor.dedup_base()
                messages = self._prepare_commit_messages(
                    table, {'id': [42], 'value': ['task']})
                if already_committed:
                    actor.commit(messages, commit_user='task', dedup_base=pinned)
                for message in messages:
                    for data_file in message.new_files:
                        table.file_io.delete(canonical_data_file_path(
                            table, message.partition, message.bucket,
                            data_file.file_name))
                if already_committed:
                    actor.commit(messages, commit_user='task', dedup_base=pinned)
                    self.assertEqual(
                        table.snapshot_manager().get_latest_snapshot().id, 1)
                else:
                    with self.assertRaisesRegex(RuntimeError, 'missing|deleted'):
                        actor.commit(messages, commit_user='task', dedup_base=pinned)
                    self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_cas_retry_checks_uncommitted_files(self):
        table = self._create_append_table('retry_missing_files')
        messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']})
        commit = table.new_batch_write_builder().new_commit()
        original_cas = commit.file_store_commit.snapshot_commit.commit
        attempts = []

        def fail_before_publication(base_uuid, snapshot, statistics):
            attempts.append(snapshot.id)
            if len(attempts) == 1:
                for message in messages:
                    for data_file in message.new_files:
                        table.file_io.delete(canonical_data_file_path(
                            table, message.partition, message.bucket,
                            data_file.file_name))
                raise OSError('publication response unavailable')
            return original_cas(base_uuid, snapshot, statistics)

        try:
            with patch.object(commit.file_store_commit.snapshot_commit, 'commit',
                              side_effect=fail_before_publication):
                with self.assertRaisesRegex(RuntimeError, 'missing|deleted'):
                    commit.commit(messages)
        finally:
            commit.close()
        self.assertEqual(attempts, [1])
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_expired_retry_observation_requires_positive_commit_evidence(self):
        for landed in (True, False):
            with self.subTest(landed=landed):
                name = 'expired_retry_observation_' + str(landed).lower()
                table = self._create_append_table(name)
                actor = PaimonCommitterActor(
                    {'warehouse': self.warehouse}, 'default.' + name)
                for identifier in (1, 2):
                    actor.commit(
                        self._prepare_commit_messages(
                            table, {'id': [identifier], 'value': ['seed']}),
                        commit_user='seed-%d' % identifier,
                        dedup_base={'snapshot_id': 0, 'file_millis': None})
                builder = table.new_batch_write_builder()
                builder.commit_user = 'task'
                commit = builder.new_commit(dedup=CommitDedup(on_first_attempt=True))
                original_cas = commit.file_store_commit.snapshot_commit.commit
                attempts = []

                def lose_ack_after_expiry(base_uuid, snapshot, statistics):
                    attempts.append(snapshot.id)
                    self.assertEqual(len(attempts), 1)
                    if landed:
                        self.assertTrue(original_cas(base_uuid, snapshot, statistics))
                    else:
                        actor.commit(
                            self._prepare_commit_messages(
                                table, {'id': [99], 'value': ['other']}),
                            commit_user='other',
                            dedup_base={'snapshot_id': 0, 'file_millis': None})
                    manager = table.snapshot_manager()
                    for expired_id in (1, 2):
                        table.file_io.delete(manager.get_snapshot_path(expired_id))
                    raise OSError('ack lost after expiry')

                messages = self._prepare_commit_messages(
                    table, {'id': [42], 'value': ['task']})
                try:
                    with patch.object(commit.file_store_commit.snapshot_commit, 'commit',
                                      side_effect=lose_ack_after_expiry):
                        if landed:
                            commit.commit(messages)
                        else:
                            with self.assertRaises(DedupEvidenceMissing):
                                commit.commit(messages)
                finally:
                    commit.close()
                self.assertEqual(attempts, [3])
                self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 3)
                self.assertEqual(
                    sorted(self._read_all(table).column('id').to_pylist()),
                    [1, 2, 42] if landed else [1, 2, 99])

    def test_dedup_treats_a_missing_anchor_as_unknown_history(self):
        """A missing anchor with the id line above it proves nothing.

        Benign expiry of the bound's snapshot and a rollback below the bound
        whose recreated anchor file was *itself* expired leave identical
        on-disk state: anchor file gone, latest id at or above the bound. The
        mtime comparison can only catch a recreated file that still exists.
        Under the rolled-back history, ids above the bound were reused by
        other commits, so a scan of (bound, latest] that finds nothing does
        not prove this replay never landed -- its commit can sit at a reused
        id *below* the bound with its files still live. The scan may still
        succeed by finding its own commit, but an empty scan must refuse
        rather than publish the same rows a second time.
        """
        name = 'test_actor_missing_anchor'
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

        # History changes under the pinned bound: rollback to 1, then a
        # post-rollback job legitimately re-pins its own bound and commits
        # the task's rows at reused id 2, with unrelated commits regrowing
        # the id line to 4.
        self._rollback_to(table, 1)
        post_rollback = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        fresh_base = post_rollback.dedup_base()
        self.assertEqual(fresh_base['snapshot_id'], 1)
        post_rollback.commit(
            self._prepare_commit_messages(
                table, {'id': [42], 'value': ['task']}
            ),
            commit_user='task-0', dedup_base=fresh_base,
        )
        for i in (3, 4):
            post_rollback.commit(
                self._prepare_commit_messages(
                    table, {'id': [i], 'value': [f'post{i}']}
                ),
                commit_user=f'other-{i}', dedup_base=fresh_base,
            )
        snapshot_manager = table.snapshot_manager()
        self.assertEqual(snapshot_manager.get_latest_snapshot().id, 4)

        # Retention trims snapshots 1..3 -- including the recreated anchor at
        # id 3 -- while the task's files stay live through snapshot 4's
        # manifests. The stale bound now sees: anchor gone, latest above it.
        for snapshot_id in (1, 2, 3):
            table.file_io.delete(
                snapshot_manager.get_snapshot_path(snapshot_id))

        # A zombie replay of the pre-rollback job arrives with fresh files,
        # the same commit_user and the stale bound. Its rows are already live
        # at reused id 2, which lies outside (3, 4] -- the scan cannot find
        # it, so it must refuse instead of committing a second copy.
        replay = PaimonCommitterActor(
            catalog_options={'warehouse': self.warehouse},
            table_identifier=f'default.{name}',
        )
        replay_messages = self._prepare_commit_messages(
            table, {'id': [42], 'value': ['task']}
        )
        with self.assertRaises(DedupEvidenceMissing):
            replay.commit(
                replay_messages, commit_user='task-0', dedup_base=stale_base)

        # Row 42 appears exactly once.
        self.assertEqual(snapshot_manager.get_latest_snapshot().id, 4)
        self.assertEqual(
            sorted(self._read_all(table).column('id').to_pylist()),
            [1, 3, 4, 42])

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

    def test_actor_mode_rejects_native_commit_table_option(self):
        """The native committer has no duplicate-commit scan, so actor mode
        must refuse it at initialization -- before any task writes or
        commits -- not merely when a commit happens to request the scan."""
        table = self._create_append_table(
            'test_actor_native_table_opt',
            options={'commit.native.enabled': 'true'})
        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(NotImplementedError) as ctx:
            write_paimon_dataset(ds, table, commit_mode='actor')
        self.assertIn('commit.native.enabled', str(ctx.exception))
        # Nothing was committed.
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_actor_mode_rejects_native_commit_dynamic_option(self):
        """The rejection reads the final effective configuration, so a
        dynamic option enabling native commit is caught too."""
        table = self._create_append_table('test_actor_native_dyn_opt')
        ds = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1], 'value': ['a']}, schema=SCHEMA)
        )

        with self.assertRaises(NotImplementedError) as ctx:
            write_paimon_dataset(
                ds, table, commit_mode='actor',
                options={'commit.native.enabled': 'true'})
        self.assertIn('commit.native.enabled', str(ctx.exception))
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_committer_actor_rejects_native_commit(self):
        """The actor itself refuses a native-commit table at construction:
        it reloads the table from the catalog, so the datasink-side check
        cannot see configuration that only exists there."""
        self._create_append_table(
            'test_actor_native_committer',
            options={'commit.native.enabled': 'true'})

        with self.assertRaises(NotImplementedError) as ctx:
            PaimonCommitterActor(
                catalog_options={'warehouse': self.warehouse},
                table_identifier='default.test_actor_native_committer',
            )
        self.assertIn('commit.native.enabled', str(ctx.exception))

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
