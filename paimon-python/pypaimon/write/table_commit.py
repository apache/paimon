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

import logging
from typing import Dict, List, Optional

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER

logger = logging.getLogger(__name__)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit


class TableCommit:
    """Common base for batch and stream table commits.

    Owns the underlying :class:`FileStoreCommit` and provides the shared
    :meth:`_commit` implementation. The concrete subclasses differ only in
    their public ``commit`` signature and lifecycle constraints:

    * :class:`BatchTableCommit` accepts no ``commit_identifier`` and may be
      committed at most once.
    * :class:`StreamTableCommit` requires an explicit ``commit_identifier``
      on every call and may be reused for many commits.
    """

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

    def _commit(self, commit_messages: List[CommitMessage], commit_identifier: int = BATCH_COMMIT_IDENTIFIER):
        non_empty_messages = [msg for msg in commit_messages if not msg.is_empty()]

        if self.overwrite_partition is not None:
            # Always call overwrite() even with empty messages, so that
            # FileStoreCommit.overwrite can handle the empty case properly
            # (e.g. static overwrite with empty data should delete the partition).
            logger.info(
                "Committing overwrite to table %s, %d non-empty messages",
                self.table.identifier, len(non_empty_messages)
            )
            self.file_store_commit.overwrite(
                overwrite_partition=self.overwrite_partition,
                commit_messages=non_empty_messages,
                commit_identifier=commit_identifier
            )
        else:
            if not non_empty_messages:
                return
            logger.info(
                "Committing table %s, %d non-empty messages",
                self.table.identifier, len(non_empty_messages)
            )
            self.file_store_commit.commit(
                commit_messages=non_empty_messages,
                commit_identifier=commit_identifier
            )

    def abort(self, commit_messages: List[CommitMessage]):
        self.file_store_commit.abort(commit_messages)

    def close(self):
        self.file_store_commit.close()


class BatchTableCommit(TableCommit):
    """Batch-mode commit; supports at most one commit per instance."""

    def __init__(self, table, commit_user: str, static_partition: Optional[dict]):
        super().__init__(table, commit_user, static_partition)
        self.batch_committed = False

    def commit(self, commit_messages: List[CommitMessage]):
        self._check_committed()
        self._commit(commit_messages, BATCH_COMMIT_IDENTIFIER)

    def truncate_table(self) -> None:
        """Truncate the entire table, deleting all data."""
        self._check_committed()
        self.file_store_commit.truncate_table(BATCH_COMMIT_IDENTIFIER)

    def truncate_partitions(self, partitions: List[Dict[str, str]]) -> None:
        self._check_committed()
        self.file_store_commit.drop_partitions(partitions, BATCH_COMMIT_IDENTIFIER)

    def _check_committed(self):
        if self.batch_committed:
            raise RuntimeError("BatchTableCommit only supports one-time committing.")
        self.batch_committed = True


class StreamTableCommit(TableCommit):
    """Stream-mode commit; reusable across many commit rounds.

    Each call must be tagged with a monotonically increasing
    ``commit_identifier`` — analogous to
    :meth:`StreamTableWrite.prepare_commit`.
    """

    def commit(self, commit_messages: List[CommitMessage], commit_identifier: int):
        self._commit(commit_messages, commit_identifier)
