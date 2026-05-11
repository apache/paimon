"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import unittest
from unittest.mock import Mock

from parameterized import parameterized

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_commit import BatchTableCommit, StreamTableCommit


class TestTableCommitEmptyOverwrite(unittest.TestCase):
    """Tests for TableCommit._commit handling of empty commit messages in overwrite mode."""

    def _create_commit(self, cls, overwrite_partition=None):
        commit = cls.__new__(cls)
        commit.table = Mock()
        commit.table.identifier = 'default.test_table'
        commit.commit_user = 'test_user'
        commit.overwrite_partition = overwrite_partition
        commit.file_store_commit = Mock()
        commit.batch_committed = False
        return commit, commit.file_store_commit

    # -- Overwrite mode: should always call overwrite(), even with empty messages --

    @parameterized.expand([
        ("no_messages", []),
        ("all_empty", [False]),
        ("non_empty", [True]),
        ("mixed", [False, True]),
    ])
    def test_overwrite_forwards_filtered_messages(self, name, msg_flags):
        """Overwrite mode should always call overwrite(), filtering out empty messages."""
        commit, mock_fsc = self._create_commit(BatchTableCommit, overwrite_partition={'f0': 1})

        messages = [
            CommitMessage(partition=(1,), bucket=0, new_files=[Mock()] if has_files else [])
            for has_files in msg_flags
        ]
        commit.commit(messages)

        mock_fsc.overwrite.assert_called_once_with(
            overwrite_partition={'f0': 1},
            commit_messages=[m for m in messages if not m.is_empty()],
            commit_identifier=BATCH_COMMIT_IDENTIFIER,
        )

    # -- Append mode: should only call commit() when there are non-empty messages --

    @parameterized.expand([
        ("no_messages", []),
        ("all_empty", [False]),
        ("non_empty", [True]),
    ])
    def test_append_forwards_non_empty_messages(self, name, msg_flags):
        """Append mode should only call commit() when there are non-empty messages."""
        commit, mock_fsc = self._create_commit(BatchTableCommit, overwrite_partition=None)

        messages = [
            CommitMessage(partition=(), bucket=0, new_files=[Mock()] if has_files else [])
            for has_files in msg_flags
        ]
        commit.commit(messages)

        if any(msg_flags):
            mock_fsc.commit.assert_called_once_with(
                commit_messages=[m for m in messages if not m.is_empty()],
                commit_identifier=BATCH_COMMIT_IDENTIFIER,
            )
        else:
            mock_fsc.commit.assert_not_called()
            mock_fsc.overwrite.assert_not_called()

    # -- StreamTableCommit overwrite should also reach overwrite() with empty messages --

    def test_stream_commit_overwrite_empty_messages(self):
        commit, mock_fsc = self._create_commit(StreamTableCommit, overwrite_partition={'dt': '2024-01-15'})

        commit.commit([], commit_identifier=42)

        mock_fsc.overwrite.assert_called_once_with(
            overwrite_partition={'dt': '2024-01-15'},
            commit_messages=[],
            commit_identifier=42,
        )
