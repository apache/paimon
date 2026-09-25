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

import logging
from typing import Dict, List, Optional

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_callback import CommitCallback
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit

logger = logging.getLogger(__name__)


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

        self._commit_callbacks: List[CommitCallback] = []
        self._native_commit = None
        self.file_store_commit = FileStoreCommit(
            snapshot_commit, table, commit_user,
            commit_callbacks=self._commit_callbacks)

    def add_commit_callback(self, callback: CommitCallback) -> None:
        """Register a callback to be invoked after each successful commit."""
        self._commit_callbacks.append(callback)

    def _commit(
            self,
            commit_messages: List[CommitMessage],
            commit_identifier: int = BATCH_COMMIT_IDENTIFIER,
            snapshot_properties: Optional[Dict[str, str]] = None):
        non_empty_messages = [msg for msg in commit_messages if not msg.is_empty()]
        commit_kwargs = {
            "commit_messages": non_empty_messages,
            "commit_identifier": commit_identifier,
        }
        if snapshot_properties is not None:
            commit_kwargs["snapshot_properties"] = snapshot_properties

        # Never abort files in response to a commit exception. Preserving
        # possible orphan files is safer than deleting files which another
        # attempt may still commit or which a snapshot may already reference.
        if self.overwrite_partition is not None:
            # Always call overwrite() even with empty messages, so that
            # FileStoreCommit.overwrite can handle the empty case properly
            # (e.g. static overwrite with empty data should delete the partition).
            logger.info(
                "Committing overwrite to table %s, %d non-empty messages",
                self.table.identifier, len(non_empty_messages)
            )
            if snapshot_properties is None:
                prepared = self._prepare_native_commit(non_empty_messages)
                if prepared is not None:
                    native, messages = prepared
                    # Keep publication failures outside the preparation fallback.
                    native.commit(messages)
                    return
            self.file_store_commit.overwrite(
                overwrite_partition=self.overwrite_partition,
                **commit_kwargs)
        else:
            if (not non_empty_messages
                    and self.table.options.snapshot_ignore_empty_commit()):
                return
            logger.info(
                "Committing table %s, %d non-empty messages",
                self.table.identifier, len(non_empty_messages)
            )
            if snapshot_properties is None:
                prepared = self._prepare_native_commit(non_empty_messages)
                if prepared is not None:
                    native, messages = prepared
                    # Mutation is deliberately outside the fallback boundary:
                    # an exception can mean the snapshot was already published.
                    native.commit(commit_identifier, messages)
                    return
            self.file_store_commit.commit(**commit_kwargs)

    def _prepare_native_commit(self, messages):
        if (not self.table.options.native_commit_enabled()
                or self._commit_callbacks):
            return None
        # data-file.path-directory keeps the whole pipeline on the Python
        # path (which resolves the relocated directory); see the matching
        # write / read / plan fallbacks.
        if self.table.options.data_file_path_directory() is not None:
            return None
        try:
            from pypaimon.write.native_commit import (
                create_native_commit, native_messages_supported,
                to_native_commit_messages)
            if not native_messages_supported(self.table, messages):
                return None
            if self._native_commit is None:
                self._native_commit = create_native_commit(
                    self.table, self.commit_user, self.overwrite_partition)
            if self._native_commit is None:
                return None
            return self._native_commit, to_native_commit_messages(self.table, messages)
        except Exception as error:
            # No native mutation has started. Preserve the normal Python path
            # when the optional runtime, FileIO or wire bridge is unavailable.
            logger.debug("Native commit preparation failed; using Python: %s", error)
            return None

    def abort(self, commit_messages: List[CommitMessage]):
        prepared = self._prepare_native_commit(commit_messages)
        if prepared is not None:
            native, messages = prepared
            native.abort(messages)
            return
        self.file_store_commit.abort(commit_messages)

    def close(self):
        try:
            if self._native_commit is not None:
                self._native_commit.close()
        finally:
            self.file_store_commit.close()


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
