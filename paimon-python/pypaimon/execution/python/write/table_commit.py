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


def create_python_commit(owner):
    """Allocate snapshot publication and conflict checks for Python execution."""
    from pypaimon.execution.python.write.file_store_commit import FileStoreCommit

    snapshot_commit = owner.table.new_snapshot_commit()
    if snapshot_commit is None:
        raise RuntimeError("Table does not provide a SnapshotCommit instance")
    try:
        return FileStoreCommit(
            snapshot_commit, owner.table, owner.commit_user,
            commit_callbacks=owner._commit_callbacks, close_commit_callbacks=False)
    except Exception:
        close = getattr(snapshot_commit, 'close', None)
        if close is not None:
            try:
                close()
            except Exception:
                pass
        raise
