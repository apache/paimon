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

from typing import Dict, List, Optional

from pypaimon.write.backend_dispatch import (
    abort_table_commit, close_table_commit, commit_table_messages, prepare_native_commit,
    python_commit, truncate_partitions_commit, truncate_table_commit,
)
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_callback import CommitCallback
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_options import validate_commit_options


class TableCommit:
    """Common base for batch and stream table commits.

    Coordinates prepared commit messages through an execution backend.
    The concrete subclasses differ only in
    their public ``commit`` signature and lifecycle constraints:

    * :class:`BatchTableCommit` accepts no ``commit_identifier`` and may be
      committed at most once.
    * :class:`StreamTableCommit` requires an explicit ``commit_identifier``
      on every call and may be reused for many commits.
    """

    def __init__(self, table, commit_user: str, static_partition: Optional[dict]):
        from pypaimon.table.file_store_table import FileStoreTable

        validate_commit_options(table.options)
        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.overwrite_partition = static_partition

        self._commit_callbacks: List[CommitCallback] = []
        self._native_commit = None
        self._python_commit = None
        self._closed = False

    def add_commit_callback(self, callback: CommitCallback) -> None:
        """Register a callback to be invoked after each successful commit."""
        self._commit_callbacks.append(callback)

    def _commit(
            self,
            commit_messages: List[CommitMessage],
            commit_identifier: int = BATCH_COMMIT_IDENTIFIER,
            snapshot_properties: Optional[Dict[str, str]] = None):
        """Publish prepared messages while preserving files on any failure."""
        return commit_table_messages(self, commit_messages, commit_identifier, snapshot_properties)

    def _prepare_native_commit(self, messages, *, batch=False):
        return prepare_native_commit(self, messages, batch=batch)

    def abort(self, commit_messages: List[CommitMessage]):
        """Delete explicitly abandoned, known-uncommitted files.

        Never call after a commit whose outcome is unknown. Publication may
        have succeeded before its response failed. Internal failure paths
        must preserve prepared files instead of calling this method.
        """
        return abort_table_commit(self, commit_messages)

    def close(self):
        return close_table_commit(self)

    @property
    def file_store_commit(self):
        """Access the Python committer explicitly, creating it on first use."""
        return python_commit(self).file_store_commit

    @file_store_commit.setter
    def file_store_commit(self, commit):
        from pypaimon.execution.python.write.table_commit import PythonTableCommit

        self._python_commit = (None if commit is None else
                               PythonTableCommit(commit, self.overwrite_partition))


class BatchTableCommit(TableCommit):
    """Batch-mode commit; supports at most one commit per instance."""

    def __init__(self, table, commit_user: str, static_partition: Optional[dict]):
        super().__init__(table, commit_user, static_partition)
        self.batch_committed = False

    def commit(
            self,
            commit_messages: List[CommitMessage],
            snapshot_properties: Optional[Dict[str, str]] = None):
        """Commit once, attaching optional properties to the snapshot."""
        self._check_committed()
        self._commit(
            commit_messages,
            BATCH_COMMIT_IDENTIFIER,
            snapshot_properties=snapshot_properties)

    def truncate_table(self) -> None:
        """Truncate the entire table, deleting all data."""
        self._check_committed()
        return truncate_table_commit(self, BATCH_COMMIT_IDENTIFIER)

    def truncate_partitions(self, partitions: List[Dict[str, str]]) -> None:
        return truncate_partitions_commit(self, partitions, BATCH_COMMIT_IDENTIFIER)

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

    def __init__(self, table, commit_user: str):
        super().__init__(table, commit_user, None)

    def commit(
            self,
            commit_messages: List[CommitMessage],
            commit_identifier: int,
            snapshot_properties: Optional[Dict[str, str]] = None):
        """Commit a stream checkpoint with optional snapshot properties."""
        self._commit(
            commit_messages,
            commit_identifier,
            snapshot_properties=snapshot_properties)
