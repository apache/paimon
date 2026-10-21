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

"""Select execution implementations before data is read or published.

Only preparation belongs in the fallback boundary. An execution failure must
reach the caller: a writer or committer may already have produced side effects.
The independent plan/read/write/commit options keep their existing meaning.
"""

import logging

logger = logging.getLogger(__name__)


def propagate_fork_safety_error(error):
    from pypaimon.execution.native.runtime import _raise_if_native_fork_safety_error
    _raise_if_native_fork_safety_error(error)


def try_prepare_native(factory, *args, **kwargs):
    """Return a prepared executor, or None when preparation permits fallback."""
    try:
        return factory(*args, **kwargs)
    except Exception as error:
        propagate_fork_safety_error(error)
        logger.debug('Native execution preparation failed; using Python: %s', error)
        return None


def create_python_write(table, commit_user, static_partition=None, stream=False,
                        restore_snapshot_id=None, fixed_bucket=False, bucket_plan=None):
    if fixed_bucket:
        from pypaimon.write.postpone_batch_table_write import PostponeFixedBucketBatchTableWrite
        return PostponeFixedBucketBatchTableWrite(table, commit_user, static_partition, bucket_plan)
    from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
    if stream:
        return StreamTableWrite(table, commit_user, restore_snapshot_id=restore_snapshot_id)
    return BatchTableWrite(table, commit_user, static_partition, restore_snapshot_id=restore_snapshot_id)


def create_write(table, commit_user, static_partition=None, stream=False,
                 restore_snapshot_id=None, fixed_bucket=False, bucket_plan=None):
    from pypaimon.read.merge_engine_support import check_sequence_field_supported
    check_sequence_field_supported(table)
    if table.options.native_write_enabled():
        def prepare():
            from pypaimon.execution.native.write.native_write import create_native_write
            return create_native_write(
                table, commit_user, static_partition, stream,
                restore_snapshot_id=restore_snapshot_id, fixed_bucket=fixed_bucket, bucket_plan=bucket_plan)
        native = try_prepare_native(prepare)
        if native is not None:
            return native
    return create_python_write(
        table, commit_user, static_partition, stream, restore_snapshot_id, fixed_bucket, bucket_plan)


def switch_writer_to_python(owner):
    """Switch only before the native writer has accepted its first mutation."""
    if owner._python_writer is not None:
        return owner._python_writer
    if owner._written:
        raise RuntimeError('Cannot switch to the Python writer after native data was written')
    owner._native_writer.close()
    owner._native_writer = None
    owner._python_writer = owner._new_python_writer()
    return owner._python_writer


def create_postpone_bucket_planner(table, known_num_buckets=None):
    """Resolve metadata before Ray materializes or writes any input."""
    if table.options.native_write_enabled():
        def prepare():
            from pypaimon.execution.native.write.postpone_bucket import create_native_postpone_bucket_planner
            return create_native_postpone_bucket_planner(table, known_num_buckets)
        native = try_prepare_native(prepare)
        if native is not None:
            return native
    from pypaimon.write.postpone_bucket import PostponeBucketPlanner
    return PostponeBucketPlanner(table, known_num_buckets=known_num_buckets)


def dynamic_bucket_data_buckets(table, partition, snapshot):
    """Resolve complete bucket metadata before assigning any input keys."""
    if table.options.native_write_enabled():
        def prepare():
            from pypaimon.execution.native.write.bucket_metadata import dynamic_bucket_data_buckets as native_buckets
            return native_buckets(table, partition, snapshot)
        native = try_prepare_native(prepare)
        if native is not None:
            return native
    from pypaimon.execution.python.write.bucket_metadata import dynamic_bucket_data_buckets as python_buckets
    return python_buckets(table, partition, snapshot)


def prepare_native_commit(owner, messages, *, batch=False):
    if not owner.table.options.native_commit_enabled():
        return None

    def prepare():
        from pypaimon.execution.native.write.commit import prepare_native_commit as prepare_commit
        return prepare_commit(owner, messages, batch=batch)
    return try_prepare_native(prepare)


def prepare_native_write_operation(operation, table, commit_user, *args, **kwargs):
    names = {
        'update': 'create_native_update',
        'update_by_row_id': 'create_native_update_by_row_id',
        'shard_update': 'create_native_shard_updator',
        'upsert': 'create_native_upsert',
        'predicate_update': 'create_native_predicate_update',
        'predicate_delete': 'create_native_predicate_delete',
        'delete': 'create_native_delete',
    }
    name = names[operation]

    def prepare():
        from pypaimon.execution.native.write import native_update
        native = getattr(native_update, name)(table, commit_user, *args, **kwargs)
        if native is not None and operation == 'update_by_row_id':
            from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
            # Preserve the public return type without inheriting Python's file
            # rewriting implementation in the Native adapter.
            TableUpdateByRowId.register(native_update.NativeTableUpdateByRowId)
        return native
    return try_prepare_native(prepare)


def try_native_predicate_row_ids(scan_table, predicate, splits):
    def prepare():
        from pypaimon.execution.native.write.native_update import native_predicate_row_ids
        return native_predicate_row_ids(scan_table, predicate, splits)
    return try_prepare_native(prepare)


def prepare_native_merge_into(*args, **kwargs):
    # MERGE preparation can consume user input. Preserve its existing boundary:
    # only an explicit unsupported result selects Python; failures propagate.
    from pypaimon.execution.native.write.native_merge_into import create_native_merge_into
    return create_native_merge_into(*args, **kwargs)


def python_commit(owner):
    """Create Python commit resources only after that backend is selected."""
    if owner._python_commit is None:
        from pypaimon.execution.python.write.table_commit import create_python_commit
        owner._python_commit = create_python_commit(owner)
    return owner._python_commit


def commit_table_messages(
        owner,
        commit_messages,
        commit_identifier,
        snapshot_properties=None):
    """Publish prepared messages while preserving files on any failure."""
    non_empty_messages = [msg for msg in commit_messages if not msg.is_empty()]
    commit_kwargs = {
        "commit_messages": non_empty_messages,
        "commit_identifier": commit_identifier,
    }
    if snapshot_properties is not None:
        commit_kwargs["snapshot_properties"] = snapshot_properties
    native_kwargs = ({} if snapshot_properties is None else
                     {"snapshot_properties": snapshot_properties})

    # Never abort files in response to a commit exception. Preserving
    # possible orphan files is safer than deleting files which another
    # attempt may still commit or which a snapshot may already reference.
    if owner.overwrite_partition is not None:
        # Always call overwrite() even with empty messages, so that
        # FileStoreCommit.overwrite can handle the empty case properly
        # (e.g. static overwrite with empty data should delete the partition).
        logger.info(
            "Committing overwrite to table %s, %d non-empty messages",
            owner.table.identifier, len(non_empty_messages)
        )
        prepared = owner._prepare_native_commit(non_empty_messages)
        if prepared is not None:
            native, messages = prepared
            # Keep publication failures outside the preparation fallback.
            native.commit(messages, **native_kwargs)
            return
        python_commit(owner).overwrite(
            overwrite_partition=owner.overwrite_partition,
            **commit_kwargs)
    else:
        if (not non_empty_messages
                and owner.table.options.snapshot_ignore_empty_commit()):
            return
        logger.info(
            "Committing table %s, %d non-empty messages",
            owner.table.identifier, len(non_empty_messages)
        )
        prepared = owner._prepare_native_commit(non_empty_messages)
        if prepared is not None:
            native, messages = prepared
            # Mutation is deliberately outside the fallback boundary:
            # an exception can mean the snapshot was already published.
            if owner._native_commit_is_batch:
                native.commit(messages, **native_kwargs)
            else:
                native.commit(commit_identifier, messages, **native_kwargs)
            return
        python_commit(owner).commit(**commit_kwargs)


def abort_table_commit(owner, messages):
    prepared = owner._prepare_native_commit(messages)
    if prepared is not None:
        native, wire_messages = prepared
        native.abort(wire_messages)
        return
    python_commit(owner).abort(messages)


def truncate_table_commit(owner, commit_identifier):
    prepared = owner._prepare_native_commit([], batch=True)
    if prepared is not None:
        prepared[0].truncate_table()
        return
    python_commit(owner).truncate_table(commit_identifier)


def truncate_partitions_commit(owner, partitions, commit_identifier):
    prepared = owner._prepare_native_commit([], batch=True)
    if prepared is not None:
        prepared[0].truncate_partitions(partitions)
        return
    python_commit(owner).drop_partitions(partitions, commit_identifier)


def close_table_commit(owner):
    """Close only initialized backends and each registered callback once."""
    if owner._closed:
        return
    owner._closed = True
    try:
        if owner._native_commit is not None:
            owner._native_commit.close()
    finally:
        try:
            if owner._python_commit is not None:
                owner._python_commit.close()
        finally:
            for callback in owner._commit_callbacks:
                try:
                    callback.close()
                except Exception:
                    pass
