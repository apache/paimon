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
