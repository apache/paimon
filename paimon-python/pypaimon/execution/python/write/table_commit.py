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

from pypaimon.execution.python.write.file_store_commit import FileStoreCommit


class PythonTableCommitMixin:
    """Adapt the Python committer to the common table commit lifecycle."""

    def _init_python_commit(self):
        snapshot_commit = self.table.new_snapshot_commit()
        if snapshot_commit is None:
            raise RuntimeError("Table does not provide a SnapshotCommit instance")
        self.file_store_commit = FileStoreCommit(
            snapshot_commit, self.table, self.commit_user,
            commit_callbacks=self._commit_callbacks)

    def _commit_with_python(self, **kwargs):
        return self.file_store_commit.commit(**kwargs)

    def _overwrite_with_python(self, **kwargs):
        return self.file_store_commit.overwrite(**kwargs)

    def _abort_with_python(self, messages):
        return self.file_store_commit.abort(messages)

    def _close_python_commit(self):
        return self.file_store_commit.close()

    def _truncate_table_with_python(self, commit_identifier):
        return self.file_store_commit.truncate_table(commit_identifier)

    def _truncate_partitions_with_python(self, partitions, commit_identifier):
        return self.file_store_commit.drop_partitions(partitions, commit_identifier)
