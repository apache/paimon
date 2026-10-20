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

def prepare_native_commit(owner, messages, *, batch=False):
    """Prepare the wire bridge before any snapshot mutation starts."""
    from pypaimon.execution.native.write.native_commit import (
        create_native_commit, native_commit_callback, native_messages_supported,
        to_native_commit_messages,
    )
    if not native_messages_supported(owner.table, messages):
        return None
    if owner._native_commit is None or (batch and not owner._native_commit_is_batch):
        native = create_native_commit(
            owner.table, owner.commit_user, owner.overwrite_partition,
            **({'batch': True} if batch else {}))
        if native is None:
            return None
        if owner._native_commit is not None:
            owner._native_commit.close()
        owner._native_commit = native
        owner._native_commit_is_batch = batch or owner.overwrite_partition is not None
        owner._native_callbacks_registered = False
    if owner._native_commit is None:
        return None
    native_messages = to_native_commit_messages(owner.table, messages)
    # Callbacks may be registered after the native committer is created.
    # FileStoreCommit owns their close lifecycle in either execution path.
    if owner._commit_callbacks and not owner._native_callbacks_registered:
        owner._native_commit.add_commit_callback(native_commit_callback(owner.table, owner._commit_callbacks))
        owner._native_callbacks_registered = True
    return owner._native_commit, native_messages
