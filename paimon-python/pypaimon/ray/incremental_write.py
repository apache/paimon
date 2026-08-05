# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Resumable incremental Ray writes."""

import hashlib
import json
import logging
import time

import pyarrow as pa

from pypaimon.write.commit.conflict_detection import CommitConflictError

logger = logging.getLogger(__name__)

_CHECKPOINT_PROPERTY = "ray.write-paimon.checkpoint"
_CHECKPOINT_VERSION = 1
_SPLITS_PER_WINDOW = 64
_TAG_PREFIX = "_pypaimon_write_"


def incremental_write_paimon(
        source,
        target,
        catalog_options,
        *,
        operation_id,
        commit_interval_seconds,
        update_cols,
        concurrency=None,
        ray_remote_args=None):
    """Write a Ray source with periodic commits and optional source checkpoints."""
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.ray.offset_source import PaimonOffsetSource

    resumable = isinstance(source, PaimonOffsetSource)
    if resumable:
        _validate_operation_id(operation_id)
    elif operation_id is not None:
        raise ValueError(
            "operation_id requires a PaimonOffsetSource; a plain Ray Dataset "
            "does not expose resumable source offsets.")
    if (isinstance(commit_interval_seconds, bool)
            or not isinstance(commit_interval_seconds, (int, float))
            or commit_interval_seconds <= 0):
        raise ValueError("commit_interval_seconds must be positive.")

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    update_cols, to_write_batch = _prepare_incremental_target(
        table, target, update_cols)

    if not resumable:
        return _write_dataset_periodically(
            source.map_batches(to_write_batch, batch_format="pyarrow"),
            table,
            commit_interval_seconds,
            concurrency,
            ray_remote_args,
        )

    return _write_offset_source(
        source,
        target,
        catalog,
        table,
        operation_id,
        commit_interval_seconds,
        update_cols,
        to_write_batch,
        concurrency,
        ray_remote_args,
    )


def _prepare_incremental_target(table, target, update_cols):
    from pypaimon.common.options.core_options import MergeEngine
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.bucket_mode import BucketMode

    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    update_cols = list(dict.fromkeys(update_cols))

    if not table.is_primary_key_table:
        raise ValueError(
            "incremental write_paimon requires a primary-key target.")
    if table.bucket_mode() != BucketMode.HASH_FIXED:
        raise ValueError(
            "incremental write_paimon requires a fixed-bucket target.")
    if table.cross_partition_update:
        raise ValueError(
            "incremental write_paimon does not support cross-partition "
            "updates.")
    if table.options.merge_engine() != MergeEngine.PARTIAL_UPDATE:
        raise ValueError(
            "incremental write_paimon requires "
            "'merge-engine'='partial-update'.")
    if table.options.sequence_field():
        raise ValueError(
            "incremental write_paimon does not support sequence fields.")

    primary_keys = list(table.primary_keys)
    invalid = [name for name in update_cols if name not in table.field_names]
    if invalid:
        raise ValueError(
            "update column {!r} is not in target {!r}.".format(
                invalid[0], target))
    key_updates = [name for name in update_cols if name in primary_keys]
    if key_updates:
        raise ValueError(
            "primary-key column {!r} cannot be updated.".format(
                key_updates[0]))
    omitted_non_null = [
        field.name for field in table.table_schema.fields
        if (field.name not in primary_keys
            and field.name not in update_cols
            and not field.type.nullable)
    ]
    if omitted_non_null:
        raise ValueError(
            "unprovided partial-update column {!r} must be nullable.".format(
                omitted_non_null[0]))

    target_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields)
    required = primary_keys + update_cols

    def to_write_batch(batch):
        missing = [name for name in required if name not in batch.column_names]
        if missing:
            raise ValueError("source is missing columns {}.".format(missing))
        arrays = [
            batch.column(field.name).cast(field.type)
            if field.name in required
            else pa.nulls(batch.num_rows, type=field.type)
            for field in target_schema
        ]
        return pa.Table.from_arrays(arrays, schema=target_schema)
    return update_cols, to_write_batch


def _write_offset_source(
        source, target, catalog, table, operation_id,
        commit_interval_seconds, update_cols, to_write_batch,
        concurrency, ray_remote_args):
    from pypaimon.write.ray_datasink import _write_primary_key_groups

    commit_user = _commit_user(operation_id)
    checkpoint_tags = _checkpoint_tags(operation_id)
    source_tag = _source_tag(operation_id, target)
    retained = _get_tag(catalog, source.table_identifier, source_tag)
    loaded = _load_checkpoint(
        catalog, target, table, operation_id, update_cols,
        commit_user, checkpoint_tags,
        recover_without_tag=retained is not None)
    checkpoint_snapshot, state = loaded if loaded is not None else (None, None)
    latest_snapshot = table.snapshot_manager().get_latest_snapshot()
    planned_schema_id = (
        state["schema_id"] if state is not None else table.table_schema.id)
    base_snapshot = (
        checkpoint_snapshot if checkpoint_snapshot is not None
        else latest_snapshot)

    splits_per_window = (
        state["source"].get("splits_per_window", 1)
        if state is not None else _SPLITS_PER_WINDOW)
    bound = source._bind(
        catalog,
        checkpoint_plan=state["source"] if state is not None else None,
        source_tag=source_tag if retained is not None else None,
        retained_snapshot_id=(
            retained.snapshot.id if retained is not None else None),
        splits_per_window=splits_per_window,
    )
    _ensure_tag(
        catalog, source.table_identifier, source_tag,
        bound.plan["snapshot_id"])

    next_offset = state["next_offset"] if state is not None else 0
    checkpoint_id = (
        checkpoint_snapshot.commit_identifier
        if checkpoint_snapshot is not None else 0)
    if next_offset > bound.num_units:
        raise RuntimeError("Checkpoint is beyond the source unit count.")
    if state is not None and next_offset == bound.num_units:
        return
    if state is None:
        checkpoint_id = 1
        state = _checkpoint_state(
            operation_id, planned_schema_id, update_cols,
            bound.plan, 0)
        base_snapshot = _commit_checkpoint(
            catalog, target, table, base_snapshot,
            planned_schema_id, commit_user, checkpoint_tags,
            checkpoint_id, [], state)
        if bound.num_units == 0:
            return

    pending_messages = []
    pending_offset = next_offset
    last_commit_time = time.monotonic()

    try:
        while pending_offset < bound.num_units:
            end = min(
                pending_offset + bound.plan["splits_per_window"],
                bound.num_units)
            dataset = bound.read_window(pending_offset, end).map_batches(
                to_write_batch, batch_format="pyarrow")
            messages = _write_primary_key_groups(
                dataset,
                table,
                overwrite=False,
                static_partition=None,
                concurrency=concurrency,
                ray_remote_args=ray_remote_args,
                prepare_only=True,
            )
            pending_messages.extend(messages)
            pending_offset = end
            complete = end == bound.num_units
            if (complete
                    or time.monotonic() - last_commit_time
                    >= commit_interval_seconds):
                checkpoint_id += 1
                state = _checkpoint_state(
                    operation_id, planned_schema_id, update_cols,
                    bound.plan, pending_offset)
                try:
                    base_snapshot = _commit_checkpoint(
                        catalog, target, table, base_snapshot,
                        planned_schema_id, commit_user, checkpoint_tags,
                        checkpoint_id, pending_messages, state)
                except Exception:
                    # Avoid aborting files after an uncertain commit outcome.
                    pending_messages = []
                    raise
                pending_messages = []
                logger.info(
                    "Committed incremental write %s at offset %d/%d.",
                    operation_id, pending_offset, bound.num_units)
                if not complete:
                    last_commit_time = time.monotonic()
    except Exception:
        if pending_messages:
            _abort_messages(table, pending_messages)
        raise


def _write_dataset_periodically(
        dataset, table, commit_interval_seconds, concurrency,
        ray_remote_args):
    from pypaimon.write.ray_datasink import _write_primary_key_groups

    committer = _PeriodicDatasetCommitter(
        table,
        table.snapshot_manager().get_latest_snapshot(),
        table.table_schema.id)
    windows = _dataset_windows(dataset, commit_interval_seconds)
    try:
        try:
            for window in windows:
                _write_primary_key_groups(
                    window,
                    table,
                    overwrite=False,
                    static_partition=None,
                    concurrency=concurrency,
                    ray_remote_args=ray_remote_args,
                    on_group_result=committer.add_group,
                )
                committer.commit()
        except Exception:
            # Commit completed groups before reporting the failure.
            committer.commit()
            raise
    except Exception:
        committer.abort_pending()
        raise
    finally:
        windows.close()
        committer.close()


def _dataset_windows(dataset, interval):
    """Yield time windows without copying Ray blocks through the driver."""
    import ray.data

    bundles = []
    source = dataset.iter_internal_ref_bundles()
    last_window = time.monotonic()
    try:
        for bundle in source:
            bundles.append(bundle)
            if time.monotonic() - last_window < interval:
                continue
            current, bundles = bundles, []
            try:
                yield ray.data.from_arrow_refs([
                    ref for item in current for ref in item.block_refs])
            finally:
                for item in current:
                    item.destroy_if_owned()
            last_window = time.monotonic()
        if bundles:
            current, bundles = bundles, []
            try:
                yield ray.data.from_arrow_refs([
                    ref for item in current for ref in item.block_refs])
            finally:
                for item in current:
                    item.destroy_if_owned()
    finally:
        for item in bundles:
            item.destroy_if_owned()
        close = getattr(source, "close", None)
        if close is not None:
            close()


class _PeriodicDatasetCommitter:

    def __init__(self, table, base_snapshot, schema_id):
        self._table = table
        self._base_snapshot = base_snapshot
        self._schema_id = schema_id
        builder = table.new_stream_write_builder()
        self._commit_user = builder.commit_user
        self._commit = builder.new_commit()
        self._pending = []
        self._next_commit_id = 1

    def add_group(self, messages):
        self._pending.extend(message for message in messages
                             if not message.is_empty())

    def commit(self):
        if not self._pending:
            return
        messages = self._pending
        self._pending = []
        commit_id = self._next_commit_id
        base_id = self._base_snapshot.id if self._base_snapshot is not None else 0
        self._commit.protect_from_external_commits(
            self._base_snapshot, self._schema_id, allow_maintenance=True)
        self._commit.commit(messages, commit_id)
        committed = _find_committed_snapshot(
            self._table, self._commit_user, commit_id, base_id)
        if committed is None:
            raise RuntimeError("Committed periodic write snapshot is missing.")
        self._base_snapshot = committed
        _validate_schema(self._table, self._schema_id)
        self._next_commit_id += 1

    def abort_pending(self):
        if not self._pending:
            return
        messages = self._pending
        self._pending = []
        _abort_messages(self._table, messages)

    def close(self):
        try:
            self._commit.close()
        except Exception:
            logger.warning("Failed to close periodic write commit.", exc_info=True)


def delete_write_paimon_checkpoint(
        target, catalog_options, operation_id):
    """Delete a completed incremental write checkpoint."""
    from pypaimon.catalog.catalog_factory import CatalogFactory

    _validate_operation_id(operation_id)
    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
    loaded = _load_checkpoint(
        catalog, target, table, operation_id, None,
        _commit_user(operation_id), _checkpoint_tags(operation_id),
        restore_tag=False, recover_without_tag=True,
        validate_schema=False)
    if loaded is None:
        return False
    _, state = loaded
    if state["next_offset"] != state["source"]["num_units"]:
        raise RuntimeError("Cannot delete an incomplete checkpoint.")

    deleted = False
    for tag in _checkpoint_tags(operation_id):
        deleted = _delete_tag(catalog, target, tag, True) or deleted
    source_table = state["source"]["table"]
    deleted = _delete_tag(
        catalog, source_table, _source_tag(operation_id, target), True
    ) or deleted
    return deleted


def _commit_checkpoint(
        catalog, target, table, base_snapshot, schema_id, commit_user,
        checkpoint_tags, checkpoint_id, messages, state):
    from pypaimon.write.table_commit import StreamTableCommit

    properties = {
        _CHECKPOINT_PROPERTY: json.dumps(
            state, sort_keys=True, separators=(",", ":"))
    }
    commit = StreamTableCommit(table, commit_user, None)
    commit.with_snapshot_properties(properties)
    commit.protect_from_external_commits(
        base_snapshot, schema_id, allow_maintenance=True)
    base_id = base_snapshot.id if base_snapshot is not None else 0
    try:
        try:
            if messages:
                commit.commit(messages, checkpoint_id)
            else:
                commit.commit_metadata(checkpoint_id)
        except CommitConflictError:
            raise
        except Exception:
            committed = _find_checkpoint_snapshot(
                table, commit_user, checkpoint_id, base_id)
            if committed is None:
                raise
            base_snapshot = committed
        else:
            base_snapshot = _find_checkpoint_snapshot(
                table, commit_user, checkpoint_id, base_id)
            if base_snapshot is None:
                raise RuntimeError("Committed checkpoint snapshot is missing.")
    finally:
        try:
            commit.close()
        except Exception:
            logger.warning("Failed to close checkpoint commit.", exc_info=True)
    _store_checkpoint_tag(
        catalog, target, checkpoint_tags, base_snapshot, checkpoint_id)
    if _read_checkpoint(base_snapshot) != state:
        raise RuntimeError(
            "Checkpoint was committed by another writer with different state.")
    _validate_schema(table, schema_id)
    return base_snapshot


def _checkpoint_state(
        operation_id, schema_id, update_cols, source_plan, next_offset):
    return {
        "version": _CHECKPOINT_VERSION,
        "operation_id": operation_id,
        "schema_id": schema_id,
        "update_cols": list(update_cols),
        "source": dict(source_plan),
        "next_offset": next_offset,
    }


def _load_checkpoint(
        catalog, target, table, operation_id, update_cols,
        commit_user, checkpoint_tags, restore_tag=True,
        recover_without_tag=False, validate_schema=True):
    snapshots = [
        tagged.snapshot for tagged in (
            _get_tag(catalog, target, tag) for tag in checkpoint_tags)
        if tagged is not None
    ]
    checkpoint = max(snapshots, key=lambda item: item.id) if snapshots else None
    latest = table.snapshot_manager().get_latest_snapshot()
    if latest is not None and (checkpoint is not None or recover_without_tag):
        lower = checkpoint.id + 1 if checkpoint is not None else 1
        for snapshot_id in range(latest.id, lower - 1, -1):
            snapshot = table.snapshot_manager().get_snapshot_by_id(snapshot_id)
            if (snapshot is not None
                    and snapshot.commit_user == commit_user
                    and _read_checkpoint(snapshot, strict=False) is not None):
                checkpoint = snapshot
                break
    if checkpoint is None:
        return None
    state = _read_checkpoint(checkpoint)
    if checkpoint.commit_user != commit_user:
        raise RuntimeError("Checkpoint tag belongs to another writer.")
    if state["operation_id"] != operation_id:
        raise RuntimeError("operation_id hash collision.")
    if validate_schema:
        _validate_schema(table, state["schema_id"])
    if update_cols is not None and state["update_cols"] != list(update_cols):
        raise ValueError(
            "operation_id {!r} was already used with update_cols {}.".format(
                operation_id, state["update_cols"]))
    if restore_tag:
        _store_checkpoint_tag(
            catalog, target, checkpoint_tags, checkpoint,
            checkpoint.commit_identifier)
    return checkpoint, state


def _read_checkpoint(snapshot, strict=True):
    encoded = (snapshot.properties or {}).get(_CHECKPOINT_PROPERTY)
    if encoded is None:
        return None
    try:
        state = json.loads(encoded)
        next_offset = state["next_offset"]
        num_units = state["source"]["num_units"]
        splits_per_window = state["source"].get("splits_per_window", 1)
        if (state["version"] != _CHECKPOINT_VERSION
                or isinstance(next_offset, bool)
                or not isinstance(next_offset, int)
                or next_offset < 0
                or not isinstance(num_units, int)
                or next_offset > num_units
                or isinstance(splits_per_window, bool)
                or not isinstance(splits_per_window, int)
                or splits_per_window < 1):
            raise ValueError
        return state
    except Exception as error:
        if not strict:
            return None
        raise RuntimeError("Invalid incremental write checkpoint.") from error


def _find_checkpoint_snapshot(table, commit_user, checkpoint_id, after_id):
    snapshot = _find_committed_snapshot(
        table, commit_user, checkpoint_id, after_id)
    if snapshot is None or _read_checkpoint(snapshot, strict=False) is None:
        return None
    return snapshot


def _find_committed_snapshot(table, commit_user, commit_id, after_id):
    manager = table.snapshot_manager()
    latest = manager.get_latest_snapshot()
    if latest is None:
        return None
    for snapshot_id in range(latest.id, after_id, -1):
        snapshot = manager.get_snapshot_by_id(snapshot_id)
        if (snapshot is not None
                and snapshot.commit_user == commit_user
                and snapshot.commit_identifier == commit_id):
            return snapshot
    return None


def _store_checkpoint_tag(
        catalog, target, checkpoint_tags, snapshot, checkpoint_id):
    slot = checkpoint_id % 2
    tag = checkpoint_tags[slot]
    previous = _get_tag(catalog, target, tag)
    if previous is None or previous.snapshot.id != snapshot.id:
        if previous is not None:
            _delete_tag(catalog, target, tag)
        catalog.create_tag(target, tag, snapshot_id=snapshot.id)
    _delete_tag(catalog, target, checkpoint_tags[1 - slot], True)


def _ensure_tag(catalog, table, tag, snapshot_id):
    previous = _get_tag(catalog, table, tag)
    if previous is not None:
        if previous.snapshot.id != snapshot_id:
            raise ValueError(
                "operation_id is bound to another source snapshot.")
        return
    catalog.create_tag(table, tag, snapshot_id=snapshot_id)


def _get_tag(catalog, table, tag):
    from pypaimon.catalog.catalog_exception import TagNotExistException

    try:
        return catalog.get_tag(table, tag)
    except TagNotExistException:
        return None


def _delete_tag(catalog, table, tag, ignore_missing=False):
    from pypaimon.catalog.catalog_exception import TagNotExistException

    try:
        catalog.delete_tag(table, tag)
        return True
    except TagNotExistException:
        if not ignore_missing:
            raise
        return False


def _abort_messages(table, messages):
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()


def _validate_schema(table, schema_id):
    latest = table.schema_manager.latest()
    if latest is None or latest.id != schema_id:
        raise RuntimeError("Target schema changed during incremental write.")


def _validate_operation_id(operation_id):
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id must be a non-empty string.")


def _digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _commit_user(operation_id):
    return "pypaimon-ray-write-" + _digest(operation_id)


def _checkpoint_tags(operation_id):
    base = _TAG_PREFIX + _digest(operation_id)
    return base + "_0", base + "_1"


def _source_tag(operation_id, target):
    return _TAG_PREFIX + _digest(target + "\0" + operation_id) + "_source"
