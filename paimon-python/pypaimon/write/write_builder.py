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
import uuid
from abc import ABC
from typing import Optional

from pypaimon.write.table_commit import (BatchTableCommit, StreamTableCommit,
                                         TableCommit)
from pypaimon.write.table_update import (BatchTableUpdate, StreamTableUpdate,
                                         TableUpdate)
from pypaimon.write.table_write import (BatchTableWrite, StreamTableWrite,
                                        TableWrite)

logger = logging.getLogger(__name__)


class WriteBuilder(ABC):
    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = self._create_commit_user()
        self.restore_snapshot_id = None

    def new_write(self) -> TableWrite:
        """Returns a table write."""

    def new_update(self) -> TableUpdate:
        """Returns a table update."""

    def new_commit(self) -> TableCommit:
        """Returns a table commit."""

    def with_restore_snapshot(self, snapshot_id: int):
        """Restore dynamic-bucket data and HASH state from a snapshot; 0 is empty."""
        from pypaimon.table.bucket_mode import BucketMode
        if self.table.bucket_mode() != BucketMode.HASH_DYNAMIC:
            raise ValueError('Restore snapshots are only valid for HASH_DYNAMIC tables')
        if isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0:
            raise ValueError('Restore snapshot id must be a nonnegative integer')
        self.restore_snapshot_id = snapshot_id
        return self

    def _with_commit_user(self, commit_user: str):
        """Reuse the identity of an existing write or commit operation."""
        self.commit_user = commit_user
        return self

    def _create_commit_user(self):
        commit_user_prefix = self.table.options.commit_user_prefix()
        if commit_user_prefix is not None:
            return f"{commit_user_prefix}_{uuid.uuid4()}"
        else:
            return str(uuid.uuid4())

    def _native_write(self, static_partition=None, stream=False, **kwargs):
        from pypaimon.read.merge_engine_support import check_sequence_field_supported

        # Keep invalid configurations outside the native fallback handler and
        # enforce the same contract regardless of which writer is selected.
        check_sequence_field_supported(self.table)
        if not self.table.options.native_write_enabled():
            return None
        try:
            from pypaimon.write.native_write import create_native_write
            return create_native_write(self.table, self.commit_user,
                                       static_partition, stream,
                                       restore_snapshot_id=self.restore_snapshot_id, **kwargs)
        except Exception as error:
            # Construction has not written any data; the normal writer is safe.
            logger.debug('Native writer preparation failed; using Python: %s', error)
            return None


class BatchWriteBuilder(WriteBuilder):

    def __init__(self, table):
        super().__init__(table)
        self.static_partition = None

    def overwrite(self, static_partition: Optional[dict] = None):
        self.static_partition = static_partition if static_partition is not None else {}
        return self

    def new_write(self) -> BatchTableWrite:
        return (self._native_write(self.static_partition)
                or BatchTableWrite(self.table, self.commit_user, self.static_partition,
                                   restore_snapshot_id=self.restore_snapshot_id))

    def new_update(self) -> BatchTableUpdate:
        return BatchTableUpdate(self.table, self.commit_user)

    def new_commit(self) -> BatchTableCommit:
        commit = BatchTableCommit(self.table, self.commit_user, self.static_partition)
        return commit


class StreamWriteBuilder(WriteBuilder):

    def new_write(self) -> StreamTableWrite:
        return (self._native_write(stream=True)
                or StreamTableWrite(self.table, self.commit_user,
                                    restore_snapshot_id=self.restore_snapshot_id))

    def new_update(self) -> StreamTableUpdate:
        return StreamTableUpdate(self.table, self.commit_user)

    def new_commit(self) -> StreamTableCommit:
        commit = StreamTableCommit(self.table, self.commit_user)
        return commit


def _new_update_by_row_id(
        table, commit_user: str, commit_identifier: int,
        _precomputed_files_info=None):
    """Build an internal updater with an operation's existing commit identity."""
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER

    if commit_identifier == BATCH_COMMIT_IDENTIFIER:
        return (table.new_batch_write_builder()
                ._with_commit_user(commit_user)
                .new_update()
                .new_update_by_row_id(_precomputed_files_info))
    return (table.new_stream_write_builder()
            ._with_commit_user(commit_user)
            .new_update()
            .new_update_by_row_id(
                commit_identifier, _precomputed_files_info))
