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

import uuid
from abc import ABC
from typing import Optional

from pypaimon.write.table_commit import (BatchTableCommit, StreamTableCommit,
                                         TableCommit)
from pypaimon.write.table_update import (BatchTableUpdate, StreamTableUpdate,
                                         TableUpdate)
from pypaimon.write.table_write import (BatchTableWrite, StreamTableWrite,
                                        TableWrite)


class WriteBuilder(ABC):
    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = self._create_commit_user()
        self.static_partition = None

    def overwrite(self, static_partition: Optional[dict] = None):
        self.static_partition = static_partition if static_partition is not None else {}
        return self

    def new_write(self) -> TableWrite:
        """Returns a table write."""

    def new_update(self) -> TableUpdate:
        """Returns a table update."""

    def new_commit(self) -> TableCommit:
        """Returns a table commit."""

    def _create_commit_user(self):
        commit_user_prefix = self.table.options.commit_user_prefix()
        if commit_user_prefix is not None:
            return f"{commit_user_prefix}_{uuid.uuid4()}"
        else:
            return str(uuid.uuid4())


class BatchWriteBuilder(WriteBuilder):

    def new_write(self) -> BatchTableWrite:
        return BatchTableWrite(self.table, self.commit_user, self.static_partition)

    def new_update(self) -> BatchTableUpdate:
        return BatchTableUpdate(self.table, self.commit_user)

    def new_commit(self) -> BatchTableCommit:
        commit = BatchTableCommit(self.table, self.commit_user, self.static_partition)
        return commit


class StreamWriteBuilder(WriteBuilder):
    """Streaming write/commit factory for coordinated multi-commit workflows.

    Writers and commits created from the same builder are linked automatically
    once :meth:`~pypaimon.write.table_write.TableWrite.with_dynamic_bucket_index`
    is enabled, so HASH-index snapshot refresh callbacks stay registered
    regardless of whether ``new_write()`` or ``new_commit()`` is called first.  Creating
    writers or commits from separate builders (or constructing
    ``StreamTableCommit`` directly) skips this wiring and can break dynamic
    bucket index maintenance across successive commits.
    """

    def __init__(self, table):
        super().__init__(table)
        self._stream_writers = []
        self._stream_commits = []

    def new_write(self) -> StreamTableWrite:
        writer = StreamTableWrite(self.table, self.commit_user, self.static_partition)
        writer._stream_write_builder = self
        self._stream_writers.append(writer)
        for commit in self._stream_commits:
            writer.register_hash_index_commit_callbacks(commit)
        return writer

    def new_update(self) -> StreamTableUpdate:
        return StreamTableUpdate(self.table, self.commit_user)

    def new_commit(self) -> StreamTableCommit:
        commit = StreamTableCommit(self.table, self.commit_user, self.static_partition)
        commit._stream_write_builder = self
        self._stream_commits.append(commit)
        for writer in self._stream_writers:
            writer.register_hash_index_commit_callbacks(commit)
        return commit

    def _detach_writer(self, writer: StreamTableWrite) -> None:
        try:
            self._stream_writers.remove(writer)
        except ValueError:
            pass

    def _detach_commit(self, commit: StreamTableCommit) -> None:
        try:
            self._stream_commits.remove(commit)
        except ValueError:
            pass
        for writer in self._stream_writers:
            writer._unregister_hash_index_commit_callback(commit)
