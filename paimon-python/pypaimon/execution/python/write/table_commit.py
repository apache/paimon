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


class PythonTableCommit:
    """Adapt Python publication to the common table-commit execution protocol."""

    def __init__(self, file_store_commit, overwrite_partition=None):
        self.file_store_commit = file_store_commit
        self.overwrite_partition = overwrite_partition

    def commit(self, messages, commit_identifier, snapshot_properties=None):
        kwargs = dict(commit_messages=messages, commit_identifier=commit_identifier)
        if snapshot_properties is not None:
            kwargs['snapshot_properties'] = snapshot_properties
        if self.overwrite_partition is not None:
            return self.file_store_commit.overwrite(
                overwrite_partition=self.overwrite_partition, **kwargs)
        return self.file_store_commit.commit(**kwargs)

    def abort(self, messages):
        return self.file_store_commit.abort(messages)

    def truncate_table(self, commit_identifier):
        return self.file_store_commit.truncate_table(commit_identifier)

    def truncate_partitions(self, partitions, commit_identifier):
        return self.file_store_commit.drop_partitions(partitions, commit_identifier)

    def close(self):
        return self.file_store_commit.close()


def create_python_commit(owner):
    """Allocate snapshot publication and conflict checks for Python execution."""
    from pypaimon.execution.python.write.file_store_commit import FileStoreCommit

    snapshot_commit = owner.table.new_snapshot_commit()
    if snapshot_commit is None:
        raise RuntimeError("Table does not provide a SnapshotCommit instance")
    try:
        commit = FileStoreCommit(
            snapshot_commit, owner.table, owner.commit_user,
            commit_callbacks=owner._commit_callbacks, close_commit_callbacks=False,
            dedup=owner._dedup)
        return PythonTableCommit(commit, owner.overwrite_partition)
    except Exception:
        close = getattr(snapshot_commit, 'close', None)
        if close is not None:
            try:
                close()
            except Exception:
                pass
        raise
