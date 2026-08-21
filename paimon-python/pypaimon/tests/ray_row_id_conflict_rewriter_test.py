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

import unittest
from unittest.mock import Mock, patch

from pypaimon.ray.data_evolution_merge_into import _reraise_inner
from pypaimon.ray.row_id_conflict_rewriter import (
    commit_self_merge_with_compaction_retry,
)
from pypaimon.write.commit.conflict_detection import RowIdExistenceConflict
from pypaimon.write.file_store_commit import CommitResultUncertainError


class RayRowIdConflictRewriterTest(unittest.TestCase):

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
                superseded_messages=[object()],
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


if __name__ == '__main__':
    unittest.main()
