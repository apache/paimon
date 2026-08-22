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

import importlib.util
import unittest
from unittest.mock import Mock, patch

from pypaimon.ray.data_evolution_merge_into import _reraise_inner
from pypaimon.ray.row_id_conflict_rewriter import (
    commit_self_merge_with_compaction_retry,
)
from pypaimon.write.commit.conflict_detection import RowIdExistenceConflict
from pypaimon.write.file_store_commit import CommitResultUncertainError


class RayRowIdConflictRewriterTest(unittest.TestCase):

    @staticmethod
    def _message(snapshot_id, name):
        return Mock(check_from_snapshot=snapshot_id, name=name)

    @staticmethod
    def _conflict(name):
        entry = Mock(bucket=0)
        entry.file = Mock(
            file_name=name,
            first_row_id=0,
            row_count=1,
        )
        return RowIdExistenceConflict(entry)

    @staticmethod
    def _table(commits, snapshots, max_retries=3):
        builder = Mock()
        builder.new_commit.side_effect = commits
        commit_table = Mock()
        commit_table.new_batch_write_builder.return_value = builder
        table = Mock()
        table.copy_without_time_travel.return_value = commit_table
        table.snapshot_manager.return_value.get_latest_snapshot.side_effect = (
            snapshots
        )
        table.options.commit_timeout.return_value = 60_000
        table.options.commit_max_retries.return_value = max_retries
        return table

    def test_uncertain_commit_is_not_rebased_as_row_id_conflict(self):
        entry = Mock(bucket=0)
        entry.file = Mock(
            file_name='data-file.parquet',
            first_row_id=0,
            row_count=1,
        )
        conflict = RowIdExistenceConflict(entry)
        uncertain = CommitResultUncertainError(
            'The snapshot commit result is uncertain.')
        uncertain.__cause__ = conflict

        commit = Mock()
        commit.commit.side_effect = [uncertain, None]
        commit_table = Mock()
        commit_table.new_batch_write_builder.return_value.new_commit.return_value = (
            commit)
        table = Mock()
        table.copy_without_time_travel.return_value = commit_table
        table.snapshot_manager.return_value.get_latest_snapshot.return_value = (
            Mock(id=2))

        with patch(
            'pypaimon.ray.row_id_conflict_rewriter._find_row_id_conflict',
            return_value=conflict,
        ) as find_conflict, patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            return_value=Mock(
                update_messages=[],
                rewritten_file_count=1,
            ),
        ) as rewrite_updates, patch(
            'pypaimon.write.file_store_commit._abort_commit_messages',
        ) as abort_messages:
            with self.assertRaises(CommitResultUncertainError) as context:
                commit_self_merge_with_compaction_retry(
                    table,
                    [],
                    [],
                    num_partitions=1,
                )

        self.assertIs(uncertain, context.exception)
        find_conflict.assert_not_called()
        rewrite_updates.assert_not_called()
        abort_messages.assert_not_called()
        commit.close.assert_called_once_with()

    def test_public_error_preserves_uncertain_commit(self):
        conflict = RuntimeError('inner conflict')
        uncertain = CommitResultUncertainError('uncertain commit')
        uncertain.__cause__ = conflict

        with self.assertRaises(CommitResultUncertainError) as context:
            _reraise_inner(uncertain)

        self.assertIs(uncertain, context.exception)

    def test_public_error_does_not_unwrap_regular_cause(self):
        timeout = TimeoutError('commit timeout')
        outer = RuntimeError('commit result is uncertain')
        outer.__cause__ = timeout

        with self.assertRaises(RuntimeError) as context:
            _reraise_inner(outer)

        self.assertIs(outer, context.exception)

    @unittest.skipUnless(
        importlib.util.find_spec('ray') is not None,
        'Ray is not installed.',
    )
    def test_public_error_unwraps_ray_task_error(self):
        from ray.exceptions import RayTaskError

        cause = ValueError('worker failure')
        ray_error = RayTaskError(
            'worker',
            'Traceback (most recent call last):\nValueError: worker failure',
            cause,
            proctitle='ray::worker',
            pid=1,
            ip='127.0.0.1',
        ).as_instanceof_cause()

        with self.assertRaises(ValueError) as context:
            _reraise_inner(ray_error)

        self.assertIs(cause, context.exception)

    @unittest.skipUnless(
        importlib.util.find_spec('ray') is not None,
        'Ray is not installed.',
    )
    def test_ray_task_error_stops_at_uncertain_commit(self):
        from ray.exceptions import RayTaskError

        timeout = TimeoutError('commit timeout')
        uncertain = CommitResultUncertainError('uncertain commit')
        uncertain.__cause__ = timeout
        ray_error = RayTaskError(
            'worker',
            'Traceback (most recent call last):\n'
            'CommitResultUncertainError: uncertain commit',
            uncertain,
            proctitle='ray::worker',
            pid=1,
            ip='127.0.0.1',
        ).as_instanceof_cause()

        with self.assertRaises(CommitResultUncertainError) as context:
            _reraise_inner(ray_error)

        self.assertIs(uncertain, context.exception)
        self.assertIs(timeout, context.exception.__cause__)

    def test_uncertain_final_generation_does_not_abort_messages(self):
        generation_0 = self._message(1, 'generation-0')
        generation_1 = self._message(2, 'generation-1')
        generation_2 = self._message(3, 'generation-2')
        other = self._message(-1, 'insert')
        snapshot_1 = Mock(id=1, uuid='uuid-1')
        snapshot_2 = Mock(id=2, uuid='uuid-2')
        snapshot_3 = Mock(id=3, uuid='uuid-3')
        commits = [Mock(), Mock(), Mock()]
        commits[0].commit.side_effect = self._conflict('compact-1')
        commits[1].commit.side_effect = self._conflict('compact-2')
        uncertain = CommitResultUncertainError('uncertain final attempt')
        commits[2].commit.side_effect = uncertain
        table = self._table(
            commits,
            [snapshot_1, snapshot_2, snapshot_3],
        )

        with patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            side_effect=[
                Mock(
                    update_messages=[generation_1],
                    rewritten_file_count=1,
                ),
                Mock(
                    update_messages=[generation_2],
                    rewritten_file_count=1,
                ),
            ],
        ), patch(
            'pypaimon.ray.row_id_conflict_rewriter._retry_wait',
        ), patch(
            'pypaimon.write.file_store_commit._abort_commit_messages',
        ) as abort_messages, self.assertRaises(
            CommitResultUncertainError,
        ) as context:
            commit_self_merge_with_compaction_retry(
                table,
                [generation_0],
                [other],
                num_partitions=1,
            )

        self.assertIs(uncertain, context.exception)
        abort_messages.assert_not_called()

    def test_exhausted_deterministic_conflict_does_not_abort_messages(self):
        generation_0 = self._message(1, 'generation-0')
        generation_1 = self._message(2, 'generation-1')
        other = self._message(-1, 'insert')
        snapshot_1 = Mock(id=1, uuid='uuid-1')
        snapshot_2 = Mock(id=2, uuid='uuid-2')
        commits = [Mock(), Mock()]
        commits[0].commit.side_effect = self._conflict('compact-1')
        terminal = self._conflict('compact-2')
        commits[1].commit.side_effect = terminal
        table = self._table(
            commits,
            [snapshot_1, snapshot_2],
            max_retries=1,
        )

        with patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            return_value=Mock(
                update_messages=[generation_1],
                rewritten_file_count=1,
            ),
        ), patch(
            'pypaimon.ray.row_id_conflict_rewriter._retry_wait',
        ), patch(
            'pypaimon.write.file_store_commit._abort_commit_messages',
        ) as abort_messages, self.assertRaises(
            RowIdExistenceConflict,
        ) as context:
            commit_self_merge_with_compaction_retry(
                table,
                [generation_0],
                [other],
                num_partitions=1,
            )

        self.assertIs(terminal, context.exception)
        abort_messages.assert_not_called()

    def test_unknown_final_error_does_not_abort_messages(self):
        generation_0 = self._message(1, 'generation-0')
        generation_1 = self._message(2, 'generation-1')
        other = self._message(-1, 'insert')
        snapshot_1 = Mock(id=1, uuid='uuid-1')
        snapshot_2 = Mock(id=2, uuid='uuid-2')
        commits = [Mock(), Mock()]
        commits[0].commit.side_effect = self._conflict('compact')
        unknown = RuntimeError('callback failed after commit')
        commits[1].commit.side_effect = unknown
        table = self._table(commits, [snapshot_1, snapshot_2])

        with patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            return_value=Mock(
                update_messages=[generation_1],
                rewritten_file_count=1,
            ),
        ), patch(
            'pypaimon.ray.row_id_conflict_rewriter._retry_wait',
        ), patch(
            'pypaimon.write.file_store_commit._abort_commit_messages',
        ) as abort_messages, self.assertRaises(RuntimeError) as context:
            commit_self_merge_with_compaction_retry(
                table,
                [generation_0],
                [other],
                num_partitions=1,
            )

        self.assertIs(unknown, context.exception)
        abort_messages.assert_not_called()


if __name__ == '__main__':
    unittest.main()
