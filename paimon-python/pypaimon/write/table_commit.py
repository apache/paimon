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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.write.backend_dispatch import (
    abort_table_commit, close_table_commit, commit_table_messages, prepare_native_commit,
    truncate_partitions_commit, truncate_table_commit,
)
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_callback import CommitCallback
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_options import CommitDedup, validate_commit_options


def native_commit_unsupported_message(table_identifier) -> str:
    """Why actor mode and native commit cannot be combined.

    Shared by every place that rejects the combination (the Ray datasink and
    committer actor at initialization, TableCommit as defense in depth), so
    the user sees the same explanation wherever the rejection fires.
    """
    key = CoreOptions.COMMIT_NATIVE_ENABLED.key()
    return (
        f"commit_mode='actor' is not supported with {key}=true on table "
        f"{table_identifier}. The native committer does not implement the "
        f"duplicate-commit scan that actor mode relies on for replay "
        f"safety. Disable {key} or use commit_mode='two_phase'."
    )


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

    def __init__(self, table, commit_user: str, static_partition: Optional[dict],
                 *, dedup: Optional[CommitDedup] = None):
        from pypaimon.table.file_store_table import FileStoreTable

        validate_commit_options(table.options)
        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.overwrite_partition = static_partition

        self._commit_callbacks: List[CommitCallback] = []
        self._native_commit = None
        self._python_commit = None
        self._closed = False
        self._dedup = dedup
        if (dedup is not None and dedup.on_first_attempt
                and table.options.native_commit_enabled()):
            # The native committer has no duplicate-commit scan. A re-issued
            # actor commit with the same commit_user would publish the same
            # files a second time, so the combination is unsupported. Fail
            # early rather than silently allowing data duplication.
            raise NotImplementedError(
                native_commit_unsupported_message(table.identifier))

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


class BatchTableCommit(TableCommit):
    """Batch-mode commit; supports at most one commit per instance."""

    def __init__(self, table, commit_user: str, static_partition: Optional[dict],
                 *, dedup: Optional[CommitDedup] = None):
        super().__init__(table, commit_user, static_partition, dedup=dedup)
        self._batch_committed = False

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
        if self._batch_committed:
            raise RuntimeError("BatchTableCommit only supports one-time committing.")
        self._batch_committed = True


class StreamTableCommit(TableCommit):
    """Stream-mode commit; reusable across many commit rounds.

    Each call must be tagged with a monotonically increasing
    ``commit_identifier`` — analogous to
    :meth:`StreamTableWrite.prepare_commit`.
    """

    def __init__(self, table, commit_user: str,
                 *, dedup: Optional[CommitDedup] = None):
        super().__init__(table, commit_user, None, dedup=dedup)

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
