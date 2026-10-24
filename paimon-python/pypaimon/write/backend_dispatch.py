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
import sys

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.write.commit_identifier import validate_commit_identifier

logger = logging.getLogger(__name__)


def propagate_fork_safety_error(error):
    """Recognize an already-loaded SDK error without loading another backend."""
    runtime = sys.modules.get('pypaimon_rust')
    error_type = getattr(runtime, 'ForkSafetyError', None)
    if error_type is None:
        return
    current, seen = error, set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, error_type):
            raise current
        current = current.__cause__ or current.__context__


def try_prepare_native(factory, *args, **kwargs):
    """Return a prepared executor, or None when preparation permits fallback."""
    try:
        return factory(*args, **kwargs)
    except Exception as error:
        propagate_fork_safety_error(error)
        if isinstance(error, TableNoPermissionException):
            raise
        logger.debug('Native execution preparation failed; using Python: %s', error)
        return None


def create_python_write(table, commit_user, static_partition=None, stream=False,
                        restore_snapshot_id=None, fixed_bucket=False, bucket_plan=None,
                        public_writer=None):
    if fixed_bucket:
        from pypaimon.write.postpone_batch_table_write import PostponeFixedBucketBatchTableWrite
        writer_type = PostponeFixedBucketBatchTableWrite
    else:
        from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
        writer_type = StreamTableWrite if stream else BatchTableWrite
    writer = object.__new__(writer_type)
    _initialize_python_write(writer, table, commit_user, static_partition, stream,
                             restore_snapshot_id, fixed_bucket, bucket_plan, public_writer)
    return writer


def _initialize_python_write(owner, table, commit_user, static_partition, stream,
                             restore_snapshot_id, fixed_bucket, bucket_plan, public_writer=None):
    if fixed_bucket:
        from pypaimon.execution.python.write.postpone_batch_table_write import PythonPostponeFixedBucketWrite
        execution = PythonPostponeFixedBucketWrite(public_writer if public_writer is not None else owner)
        execution.initialize(table, commit_user, static_partition, bucket_plan)
    else:
        from pypaimon.execution.python.write.table_write import PythonTableWrite
        execution = PythonTableWrite(public_writer if public_writer is not None else owner)
        execution.initialize(table, commit_user, static_partition,
                             restore_snapshot_id=restore_snapshot_id)
    owner.table = table
    owner.commit_user = commit_user
    owner.static_partition = static_partition
    owner._execution = execution
    from pypaimon.write.table_write import BatchTableWrite
    if isinstance(owner, BatchTableWrite):
        owner._batch_committed = False


def _prepare_write(table, commit_user, static_partition, stream,
                   restore_snapshot_id, fixed_bucket, bucket_plan):
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
            from pypaimon.write.write_dispatch import NativeTableWrite
            return NativeTableWrite.from_native(native)


def initialize_write(owner, table, commit_user, static_partition=None, stream=False,
                     restore_snapshot_id=None, fixed_bucket=False, bucket_plan=None):
    """Select once while retaining the caller's public class and batch hooks."""
    execution = _prepare_write(table, commit_user, static_partition, stream,
                               restore_snapshot_id, fixed_bucket, bucket_plan)
    if execution is None:
        _initialize_python_write(owner, table, commit_user, static_partition, stream,
                                 restore_snapshot_id, fixed_bucket, bucket_plan)
    else:
        owner.table = table
        owner.commit_user = commit_user
        owner.static_partition = static_partition
        execution._public_writer = owner
        owner._execution = execution


def create_write(table, commit_user, static_partition=None, stream=False,
                 restore_snapshot_id=None, fixed_bucket=False, bucket_plan=None):
    execution = _prepare_write(table, commit_user, static_partition, stream,
                               restore_snapshot_id, fixed_bucket, bucket_plan)
    if execution is not None:
        return execution
    return create_python_write(
        table, commit_user, static_partition, stream, restore_snapshot_id, fixed_bucket, bucket_plan)


def prepare_write_commit(owner, commit_identifier):
    from pypaimon.write.write_dispatch import NativeTableWrite
    execution = owner._execution
    if isinstance(execution, NativeTableWrite):
        return (execution.prepare_commit(commit_identifier) if execution.stream else
                execution.prepare_commit())
    return execution.prepare_commit(commit_identifier)


def prepare_batch_write(owner):
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
    from pypaimon.write.write_dispatch import NativeTableWrite
    execution = owner._execution
    if isinstance(execution, NativeTableWrite):
        return execution.prepare_commit()
    if owner._batch_committed:
        raise RuntimeError('BatchTableWrite only supports one-time committing.')
    owner._batch_committed = True
    return owner._prepare_commit(BATCH_COMMIT_IDENTIFIER)


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
    from pypaimon.execution.python.write.postpone_bucket import PythonPostponeBucketPlanner
    return PythonPostponeBucketPlanner(table, known_num_buckets=known_num_buckets)


def create_input_router(table, bucket_plan=None):
    """Select bucket routing before a distributed task receives input rows."""
    if table.options.native_write_enabled():
        def prepare():
            from pypaimon.execution.native.write.input_routing import create_native_input_router
            return create_native_input_router(table, bucket_plan)
        native = try_prepare_native(prepare)
        if native is not None:
            return native
    if bucket_plan is not None:
        from pypaimon.execution.python.write.input_routing import PostponeFixedBucketInputRouter
        return PostponeFixedBucketInputRouter(table, bucket_plan)
    from pypaimon.execution.python.write.input_routing import FixedBucketInputRouter
    return FixedBucketInputRouter(table.table_schema)


def create_dynamic_bucket_assigner_router(table, num_channels, num_assigners):
    """Select routing to dynamic bucket assigners before a task receives rows."""
    if table.options.native_write_enabled():
        from pypaimon.execution.native.write.input_routing import create_native_dynamic_bucket_assigner_router
        try:
            native = create_native_dynamic_bucket_assigner_router(table, num_channels, num_assigners)
        except NotImplementedError:
            native = None
        if native is not None:
            return native
    from pypaimon.execution.python.write.input_routing import PythonDynamicBucketAssignerRouter
    return PythonDynamicBucketAssignerRouter(table, num_channels, num_assigners)


def create_dynamic_bucket_assigner(table, num_channels, num_assigners, assign_id,
                                   ignore_existing=False, base_snapshot_id=None):
    """Select restoration before assignment; authorization failures are terminal."""
    if table.options.native_write_enabled():
        from pypaimon.execution.native.write.input_routing import create_native_dynamic_bucket_assigner
        # Resolving the writer snapshot can authorize and read HASH metadata.
        # Only an explicitly unsupported environment returns None; never turn
        # an authorization/restoration exception into another backend attempt.
        native = create_native_dynamic_bucket_assigner(
            table, num_channels, num_assigners, assign_id, ignore_existing, base_snapshot_id)
        if native is not None:
            return native
    from pypaimon.execution.python.write.input_routing import PythonDynamicBucketAssigner
    return PythonDynamicBucketAssigner(
        table, num_channels, num_assigners, assign_id, ignore_existing, base_snapshot_id)


def load_postpone_bucket_metadata(table):
    """Select metadata planning before public bucket arithmetic consumes input."""
    if table.options.native_write_enabled():
        def prepare():
            from pypaimon.execution.native.write.postpone_bucket import create_native_postpone_bucket_planner
            planner = create_native_postpone_bucket_planner(table)
            return None if planner is None else planner.current_plan().as_dict()
        metadata = try_prepare_native(prepare)
        if metadata is not None:
            return metadata
    from pypaimon.execution.python.write.postpone_bucket import PythonPostponeBucketPlanner
    return PythonPostponeBucketPlanner._load_bucket_metadata(table)


def prepare_native_commit(owner, messages, *, batch=False):
    if owner._python_commit is not None or not owner.table.options.native_commit_enabled():
        return None

    def prepare():
        from pypaimon.execution.native.write.commit import prepare_native_commit as prepare_commit
        return prepare_commit(owner, messages, batch=batch)
    return try_prepare_native(prepare)


def prepare_native_write_operation(operation, table, commit_user, *args, **kwargs):
    if not table.options.native_write_enabled():
        return None
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


def prepare_native_merge_into(*args, **kwargs):
    # MERGE preparation can consume user input. Preserve its existing boundary:
    # only an explicit unsupported result selects Python; failures propagate.
    from pypaimon.execution.native.write.native_merge_into import create_native_merge_into
    return create_native_merge_into(*args, **kwargs)


def create_merge_batch_transform(operation, *args, native_enabled=False, **kwargs):
    """Select the joined-batch executor before a Ray task consumes input."""
    if native_enabled:
        from pypaimon.execution.native.runtime import native_runtime_available
        native_enabled = native_runtime_available()
    if native_enabled:
        from pypaimon.execution.native.write import merge_transform
    else:
        from pypaimon.execution.python.write import merge_transform
    builders = {
        'update': merge_transform._build_matched_transform,
        'delete': merge_transform._build_matched_delete_transform,
        'insert': merge_transform._build_insert_transform,
    }
    return builders[operation](*args, **kwargs)


def create_python_row_id_update(table, commit_user, commit_identifier, precomputed=None):
    """Create an updater after Python execution has already been selected."""
    from pypaimon.execution.python.write.table_update_by_row_id import PythonTableUpdateByRowId
    return PythonTableUpdateByRowId().initialize(table, commit_user, commit_identifier, precomputed)


def create_python_row_id_delete(table, precomputed=None):
    """Create a deleter without preparing another execution backend."""
    from pypaimon.execution.python.write.table_delete import PythonTableDeleteByRowId
    return PythonTableDeleteByRowId().initialize(table, precomputed)


def create_python_shard_updator(owner, table, projection, columns, commit_user, shard, shard_count):
    """Initialize the Python shard resources only after backend selection."""
    from pypaimon.execution.python.write.table_update import ShardTableUpdator
    execution = ShardTableUpdator(owner)
    execution.initialize(table, projection, columns, commit_user, shard, shard_count)
    return execution


def create_python_update_operation(context, commit_identifier, predicate=None, *, has_callable=False, has_array=False):
    from pypaimon.execution.python.write.table_update import PythonTableUpdate
    return PythonTableUpdate(context, commit_identifier, predicate, has_callable=has_callable, has_array=has_array)


def create_python_upsert_operation(context, data, upsert_keys, update_columns, *, rows=False):
    from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsert
    return PythonTableUpsert(context, data, upsert_keys, update_columns, rows=rows)


def create_python_merge_into_operation(
        table, source, *, on, when_matched, when_not_matched, commit_user, commit_identifier):
    from pypaimon.execution.python.write.merge_into import PythonTableMergeInto
    return PythonTableMergeInto(
        table, source, on=on, when_matched=when_matched, when_not_matched=when_not_matched,
        commit_user=commit_user, commit_identifier=commit_identifier)


def create_shard_update_execution(owner, table, projection, columns, commit_user, shard, shard_count):
    """Select the direct shard constructor's backend before capturing files."""
    from pypaimon.write.update_validation import _authorize_update
    _authorize_update(table)
    native = prepare_native_write_operation(
        'shard_update', table, commit_user, projection, columns, shard, shard_count)
    if native is not None:
        return native
    return create_python_shard_updator(owner, table, projection, columns, commit_user, shard, shard_count)


def create_row_id_plan_execution(table, plan):
    """Select pure routing once; execution errors never replay another backend."""
    from pypaimon.execution.native.write.row_id_plan import create_native_row_id_plan, NativeRowIdPlan
    native = create_native_row_id_plan(table, plan)
    if native is not None:
        return NativeRowIdPlan(table, plan, native=native)
    from pypaimon.execution.python.write.row_id_plan import PythonRowIdPlan
    return PythonRowIdPlan(plan)


def create_row_id_update_execution(table, commit_user, commit_identifier, precomputed=None):
    """Select one incremental updater before capturing target files."""
    from pypaimon.write.update_validation import _authorize_update
    _authorize_update(table)
    from pypaimon.write.row_id_plan import CapturedRowIdPlan
    if isinstance(precomputed, CapturedRowIdPlan):
        _validate_captured_row_id_plan(table, precomputed)
        if table.options.native_write_enabled():
            from pypaimon.execution.native.write.native_update import create_native_update_by_row_id
            native = create_native_update_by_row_id(table, commit_user, commit_identifier, precomputed)
            if native is not None:
                return native
    if precomputed is None:
        native = prepare_native_write_operation('update_by_row_id', table, commit_user, commit_identifier)
        if native is not None:
            return native
    return create_python_row_id_update(table, commit_user, commit_identifier, precomputed)


def create_row_id_delete_execution(table, precomputed=None):
    """Select one deleter, reusing captured metadata when supplied."""
    from pypaimon.write.update_validation import _authorize_update
    _authorize_update(table)
    from pypaimon.write.row_id_plan import CapturedRowIdPlan
    if isinstance(precomputed, CapturedRowIdPlan):
        _validate_captured_row_id_plan(table, precomputed)
        if table.options.native_write_enabled():
            from pypaimon.execution.native.write.native_update import create_native_row_id_delete
            native = create_native_row_id_delete(table, precomputed)
            if native is not None:
                return native
    if precomputed is None and table.options.native_write_enabled():
        def prepare():
            from pypaimon.execution.native.write.native_update import create_native_row_id_delete
            return create_native_row_id_delete(table)
        native = try_prepare_native(prepare)
        if native is not None:
            return native
    return create_python_row_id_delete(table, precomputed)


def _validate_captured_row_id_plan(table, captured):
    if (table.table_path != captured.table.table_path or table.current_branch() != captured.table.current_branch()
            or table.table_schema.id != captured.table.table_schema.id
            or table.fields != captured.table.fields):
        raise ValueError('Captured row-ID plan belongs to a different table, branch or schema')


def native_row_id_update_selected(worker):
    from pypaimon.execution.native.write.native_update import NativeTableUpdateByRowId
    execution = vars(worker).get('_execution', worker)
    return isinstance(execution, NativeTableUpdateByRowId)


def validate_row_id_update_rows(worker, captured_plan, first_row_id, row_ids):
    """Validate liveness in the selected executor before positional rewriting."""
    if native_row_id_update_selected(worker):
        # Core validates the captured anchor's DV before staging any output.
        return
    from pypaimon.execution.python.write.row_id_plan import validate_live_row_ids
    validate_live_row_ids(worker.table, captured_plan, first_row_id, row_ids)


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
    commit_identifier = validate_commit_identifier(commit_identifier)
    non_empty_messages = [msg for msg in commit_messages if not msg.is_empty()]
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
    else:
        if (not non_empty_messages
                and owner.table.options.snapshot_ignore_empty_commit()):
            return
        logger.info(
            "Committing table %s, %d non-empty messages",
            owner.table.identifier, len(non_empty_messages)
        )
    prepared = owner._prepare_native_commit(non_empty_messages)
    if prepared is None:
        execution, messages = python_commit(owner), non_empty_messages
    else:
        execution, messages = prepared
    # Mutation is deliberately outside the preparation fallback boundary:
    # an exception can mean the snapshot was already published.
    return execution.commit(messages, commit_identifier, snapshot_properties=snapshot_properties)


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
        prepared[0].truncate_table(commit_identifier)
        return
    python_commit(owner).truncate_table(commit_identifier)


def truncate_partitions_commit(owner, partitions, commit_identifier):
    prepared = owner._prepare_native_commit([], batch=True)
    if prepared is not None:
        prepared[0].truncate_partitions(partitions, commit_identifier)
        return
    python_commit(owner).truncate_partitions(partitions, commit_identifier)


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


def execute_row_id_conflict_check(table, messages, latest_snapshot_id, check_from_snapshot):
    if table.options.native_commit_enabled():
        from pypaimon.execution.native.write.native_commit import prepare_native_row_id_conflict_check
        # Unsupported configurations return None explicitly. Permission or
        # transport failures must never restart the check in another backend.
        native = prepare_native_row_id_conflict_check(table, messages)
        if native is not None:
            commit, native_messages = native
            failure = None
            try:
                commit.check_row_id_conflicts(native_messages, latest_snapshot_id, check_from_snapshot)
            except BaseException as error:
                failure = error
                raise
            finally:
                try:
                    commit.close()
                except BaseException:
                    if failure is None:
                        raise
            return
    from pypaimon.execution.python.write.commit.row_id_conflict import check_row_id_conflicts as check
    check(table, messages, latest_snapshot_id, check_from_snapshot)
