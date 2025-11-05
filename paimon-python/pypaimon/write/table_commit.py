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

from typing import List, Optional

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit


class TableCommit:
    """Python implementation of BatchTableCommit for batch writing scenarios."""

    def __init__(self, table, commit_user: str, static_partition: Optional[dict]):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.overwrite_partition = static_partition

        # Get SnapshotCommit from table's catalog environment
        snapshot_commit = table.new_snapshot_commit()
        if snapshot_commit is None:
            raise RuntimeError("Table does not provide a SnapshotCommit instance")

        self.file_store_commit = FileStoreCommit(snapshot_commit, table, commit_user)
        self.batch_committed = False

    def _commit(self, commit_messages: List[CommitMessage], commit_identifier: int = BATCH_COMMIT_IDENTIFIER):
        self._check_committed()

        non_empty_messages = [msg for msg in commit_messages if not msg.is_empty()]
        if not non_empty_messages:
            return

        try:
            if self.overwrite_partition is not None:
                self.file_store_commit.overwrite(
                    overwrite_partition=self.overwrite_partition,
                    commit_messages=non_empty_messages,
                    commit_identifier=commit_identifier
                )
            else:
                self.file_store_commit.commit(
                    commit_messages=non_empty_messages,
                    commit_identifier=commit_identifier
                )
        except Exception as e:
            self.file_store_commit.abort(commit_messages)
            raise RuntimeError(f"Failed to commit: {str(e)}") from e

    def abort(self, commit_messages: List[CommitMessage]):
        self.file_store_commit.abort(commit_messages)

    def close(self):
        self.file_store_commit.close()

    def _check_committed(self):
        if self.batch_committed:
            raise RuntimeError("BatchTableCommit only supports one-time committing.")
        self.batch_committed = True


class BatchTableCommit(TableCommit):
    def commit(self, commit_messages: List[CommitMessage]):
        self._commit(commit_messages, BATCH_COMMIT_IDENTIFIER)


class StreamTableCommit(TableCommit):

    def commit(self, commit_messages: List[CommitMessage], commit_identifier: int = BATCH_COMMIT_IDENTIFIER):
        self._commit(commit_messages, commit_identifier)
