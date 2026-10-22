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

class NativeTableCommit:
    """Adapt prepared Rust messages to the common commit execution protocol."""

    def __init__(self, native_commit, is_batch=False):
        self._native_commit = native_commit
        self._is_batch = is_batch
        self._callbacks_registered = False

    def commit(self, messages, commit_identifier, snapshot_properties=None):
        kwargs = ({} if snapshot_properties is None else
                  {'snapshot_properties': snapshot_properties})
        try:
            if self._is_batch:
                return self._native_commit.commit(messages, **kwargs)
            return self._native_commit.commit(commit_identifier, messages, **kwargs)
        except Exception as error:
            self._raise_commit_error(error)

    def check_row_id_conflicts(self, messages, latest_snapshot_id, check_from_snapshot):
        try:
            return self._native_commit.check_row_id_conflicts(messages, latest_snapshot_id, check_from_snapshot)
        except Exception as error:
            self._raise_commit_error(error)

    @staticmethod
    def _raise_commit_error(error):
        from pypaimon.execution.native.write.native_commit import native_commit_error

        converted = native_commit_error(error)
        if converted is error:
            raise error
        raise converted from error

    def abort(self, messages):
        return self._native_commit.abort(messages)

    def truncate_table(self, commit_identifier):
        # The public batch API supplies the batch identifier. The Rust batch
        # binding owns that same identifier for both truncate operations.
        return self._native_commit.truncate_table()

    def truncate_partitions(self, partitions, commit_identifier):
        return self._native_commit.truncate_partitions(partitions)

    def close(self):
        return self._native_commit.close()

    def _register_callbacks(self, table, callbacks):
        if callbacks and not self._callbacks_registered:
            from pypaimon.execution.native.write.native_commit import native_commit_callback

            self._native_commit.add_commit_callback(native_commit_callback(table, callbacks))
            self._callbacks_registered = True


def prepare_native_commit(owner, messages, *, batch=False):
    """Prepare the wire bridge before any snapshot mutation starts."""
    from pypaimon.execution.native.write.native_commit import (
        create_native_commit, native_messages_supported,
        to_native_commit_messages,
    )
    if not native_messages_supported(owner.table, messages):
        return None
    if owner._native_commit is None or (batch and not owner._native_commit._is_batch):
        native = create_native_commit(
            owner.table, owner.commit_user, owner.overwrite_partition,
            **({'batch': True} if batch else {}))
        if native is None:
            return None
        if owner._native_commit is not None:
            owner._native_commit.close()
        owner._native_commit = NativeTableCommit(
            native, is_batch=batch or owner.overwrite_partition is not None)
    if owner._native_commit is None:
        return None
    native_messages = to_native_commit_messages(owner.table, messages)
    # Callbacks may be registered after the native committer is created.
    # The public commit lifecycle owns callback closure in either execution path.
    owner._native_commit._register_callbacks(owner.table, owner._commit_callbacks)
    return owner._native_commit, native_messages
