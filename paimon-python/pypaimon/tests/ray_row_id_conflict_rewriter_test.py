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
    _partial_file_write_cols,
    _validate_no_logical_conflict,
    commit_updates_with_compaction_retry,
)
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.schema.table_schema import TableSchema
from pypaimon.write.row_id_conflict import RowIdExistenceConflict


class RayRowIdConflictRewriterTest(unittest.TestCase):

    def test_cross_schema_check_selects_python_before_dispatch(self):
        table = Mock()
        table.options.native_commit_enabled.return_value = True
        base = Mock(id=1, schema_id=0)
        latest = Mock(id=2, schema_id=1)
        messages = [self._message(1, 'staged')]
        with patch('pypaimon.ray.row_id_conflict_rewriter.check_row_id_conflicts') as check:
            _validate_no_logical_conflict(table, messages, latest, base)
        table.copy.assert_called_once_with({'commit.native.enabled': 'false'})
        check.assert_called_once_with(table.copy.return_value, messages, 2, 1)

    def test_same_schema_native_check_failure_is_not_replayed(self):
        table = Mock()
        table.options.native_commit_enabled.return_value = True
        base = Mock(id=1, schema_id=0)
        latest = Mock(id=2, schema_id=0)
        failure = PermissionError('native authorization failed')
        with patch('pypaimon.ray.row_id_conflict_rewriter.check_row_id_conflicts',
                   side_effect=failure) as check:
            with self.assertRaises(PermissionError) as raised:
                _validate_no_logical_conflict(table, [], latest, base)
        self.assertIs(raised.exception, failure)
        table.copy.assert_not_called()
        check.assert_called_once_with(table, [], 2, 1)

    def test_resolve_compact_null_write_cols_for_rewrite(self):
        schema = TableSchema(
            id=1,
            fields=[
                DataField(0, 'id', AtomicType('INT')),
                DataField(1, 'blob', AtomicType('BLOB')),
                DataField(2, 'name', AtomicType('STRING')),
            ],
            options={
                'data-evolution.enabled': 'true',
                'data-evolution.write-cols-optimization.enabled': 'true',
            },
        )
        table = Mock(table_schema=schema)
        file = Mock(schema_id=1, write_cols=None)

        self.assertEqual(
            ['id', 'name'], _partial_file_write_cols(table, file)
        )

    def test_rewrite_preserves_row_descendant_identity_scope(self):
        for nested in (False, True):
            for added_child in (False, True):
                with self.subTest(nested=nested, added_child=added_child):
                    old_type = RowType(True, [DataField(10, 'a', AtomicType('INT'))])
                    children = [DataField(10, 'renamed', AtomicType('BIGINT'))]
                    if added_child:
                        # Reusing the historical name still introduces a new field ID.
                        children.append(DataField(11, 'a', AtomicType('INT')))
                    current_type = RowType(True, children)
                    if nested:
                        old_type = RowType(True, [DataField(5, 'inner', old_type)])
                        current_type = RowType(True, [DataField(5, 'inner', current_type)])
                    historical = TableSchema(id=0, fields=[DataField(1, 'nest', old_type)])
                    current = TableSchema(id=1, fields=[DataField(1, 'renamed_nest', current_type)])
                    table = Mock(table_schema=current)
                    table.schema_manager.get_schema.return_value = historical
                    file = Mock(schema_id=0, write_cols=['nest'], first_row_id=0)
                    if added_child:
                        with self.assertRaisesRegex(RuntimeError, 'cannot expand ROW field'):
                            _partial_file_write_cols(table, file)
                    else:
                        self.assertEqual(_partial_file_write_cols(table, file), ['renamed_nest'])

    @staticmethod
    def _message(snapshot_id, name):
        return Mock(check_from_snapshot=snapshot_id, name=name, new_files=[])

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
        schema = TableSchema(id=0, fields=[])
        commit_table.table_schema = schema
        commit_table.schema_manager.latest.return_value = schema
        commit_table.copy_without_time_travel.return_value = commit_table
        commit_table.new_batch_write_builder.return_value = builder
        table = Mock()
        table.copy_without_time_travel.return_value = commit_table
        table.snapshot_manager.return_value.get_latest_snapshot.side_effect = (
            snapshots
        )
        table.options.commit_timeout.return_value = 60_000
        table.options.commit_max_retries.return_value = max_retries
        table.options.data_evolution_row_id_conflict_rewrite_max_size.return_value = 256 * 1024 * 1024
        return table

    def test_nested_row_id_conflict_is_rebased(self):
        entry = Mock(bucket=0)
        entry.file = Mock(
            file_name='data-file.parquet',
            first_row_id=0,
            row_count=1,
        )
        conflict = RowIdExistenceConflict(entry)
        commit_error = RuntimeError('commit failed')
        commit_error.__cause__ = conflict

        commit = Mock()
        commit.commit.side_effect = [commit_error, None]
        table = self._table(
            [commit, commit],
            [Mock(id=1), Mock(id=2)],
        )
        rewrite_result = Mock(
            update_messages=[],
            rewritten_file_count=1,
        )

        with patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            return_value=rewrite_result,
        ) as rewrite_updates, patch(
            'pypaimon.ray.row_id_conflict_rewriter._retry_wait',
        ):
            commit_updates_with_compaction_retry(
                table,
                [],
                [],
                num_partitions=1,
            )

        rewrite_updates.assert_called_once()
        self.assertEqual(2, commit.commit.call_count)
        self.assertEqual(2, commit.close.call_count)

    def test_rewrite_error_keeps_row_id_conflict_as_cause(self):
        conflict = self._conflict('compact')
        rewrite_error = ValueError('rewrite failed')
        update = self._message(1, 'update')
        commit = Mock()
        commit.commit.side_effect = conflict
        table = self._table(
            [commit],
            [Mock(id=1), Mock(id=2)],
        )

        with patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            side_effect=rewrite_error,
        ), self.assertRaises(RuntimeError) as context:
            commit_updates_with_compaction_retry(
                table,
                [update],
                [],
                num_partitions=1,
            )

        self.assertIs(conflict, context.exception.__cause__)

    def test_timeout_after_eager_rewrite_or_retry_wait_does_not_open_publisher(self):
        for eager in (False, True):
            with self.subTest(eager=eager):
                update = self._message(1, 'staged')
                commit = Mock()
                commit.commit.side_effect = self._conflict('compact')
                table = self._table([commit, Mock()], [Mock(id=2)] if eager else [Mock(id=1), Mock(id=2)])
                table.options.commit_timeout.return_value = 100
                clock = Mock()
                clock.monotonic.return_value = 0.0
                clock.time.side_effect = clock.monotonic
                result = Mock(update_messages=[self._message(2, 'rewritten')], rewritten_file_count=1)

                def rewrite(*args, **kwargs):
                    if eager:
                        clock.monotonic.return_value = 0.25
                    return result

                def wait(*args):
                    clock.monotonic.return_value = 0.25

                with patch('pypaimon.ray.row_id_conflict_rewriter.time', clock), patch(
                    'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates', side_effect=rewrite,
                ), patch('pypaimon.ray.row_id_conflict_rewriter._retry_wait', side_effect=wait), patch(
                    'pypaimon.execution.python.write.file_store_commit.FileStoreCommit.abort',
                ) as abort, self.assertRaisesRegex(TimeoutError, 'before snapshot publication'):
                    commit_updates_with_compaction_retry(table, [update], [], num_partitions=1)
                builder = table.copy_without_time_travel.return_value.new_batch_write_builder.return_value
                self.assertEqual(builder.new_commit.call_count, 0 if eager else 1)
                self.assertEqual(commit.commit.call_count, 0 if eager else 1)
                abort.assert_not_called()

    def test_successful_publication_past_deadline_is_not_replayed(self):
        commit = Mock()
        table = self._table([commit], [Mock(id=2)])
        table.options.commit_timeout.return_value = 100
        clock = Mock()
        clock.monotonic.return_value = 0.0
        clock.time.side_effect = clock.monotonic

        def publish(*args):
            clock.monotonic.return_value = 0.25

        commit.commit.side_effect = publish
        with patch('pypaimon.ray.row_id_conflict_rewriter.time', clock), patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates',
            return_value=Mock(update_messages=[], rewritten_file_count=1),
        ) as rewrite:
            commit_updates_with_compaction_retry(table, [self._message(1, 'staged')], [], num_partitions=1)
        rewrite.assert_called_once()
        commit.commit.assert_called_once()
        commit.close.assert_called_once()

    def test_publisher_preparation_consumes_remaining_timeout(self):
        commit = Mock()
        table = self._table([commit], [Mock(id=2)])
        table.options.commit_timeout.return_value = 100
        commit_table = table.copy_without_time_travel.return_value
        builder = commit_table.new_batch_write_builder.return_value
        clock = Mock()
        clock.monotonic.return_value = 0.0
        clock.time.side_effect = clock.monotonic

        def rewrite(*args, **kwargs):
            clock.monotonic.return_value = 0.04
            return Mock(update_messages=[], rewritten_file_count=1)

        def open_publisher():
            clock.monotonic.return_value = 0.25
            return commit

        builder.new_commit.side_effect = open_publisher
        with patch('pypaimon.ray.row_id_conflict_rewriter.time', clock), patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_updates', side_effect=rewrite,
        ), self.assertRaisesRegex(TimeoutError, 'before snapshot publication'):
            commit_updates_with_compaction_retry(table, [self._message(1, 'staged')], [], num_partitions=1)
        commit_table.copy_without_time_travel.assert_called_once_with({'commit.timeout': '60 ms'})
        commit.commit.assert_not_called()
        commit.close.assert_called_once()

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

    def test_final_error_after_multiple_rebases_does_not_abort_messages(self):
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
        terminal = RuntimeError('final attempt failed')
        commits[2].commit.side_effect = terminal
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
            'pypaimon.execution.python.write.file_store_commit.FileStoreCommit.abort',
        ) as abort_messages, self.assertRaises(RuntimeError) as context:
            commit_updates_with_compaction_retry(
                table,
                [generation_0],
                [other],
                num_partitions=1,
            )

        self.assertIs(terminal, context.exception)
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
            'pypaimon.execution.python.write.file_store_commit.FileStoreCommit.abort',
        ) as abort_messages, self.assertRaises(
            RowIdExistenceConflict,
        ) as context:
            commit_updates_with_compaction_retry(
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
            'pypaimon.execution.python.write.file_store_commit.FileStoreCommit.abort',
        ) as abort_messages, self.assertRaises(RuntimeError) as context:
            commit_updates_with_compaction_retry(
                table,
                [generation_0],
                [other],
                num_partitions=1,
            )

        self.assertIs(unknown, context.exception)
        abort_messages.assert_not_called()


if __name__ == '__main__':
    unittest.main()
