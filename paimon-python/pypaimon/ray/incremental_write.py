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
_CHECKPOINT_MODE = "primary-key-source-offset"
_CHECKPOINT_VERSION = 1
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
    """Write a replayable Paimon source with periodic checkpoints."""
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.options.core_options import MergeEngine
    from pypaimon.ray.offset_source import PaimonOffsetSource
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.bucket_mode import BucketMode
    from pypaimon.write.ray_datasink import _prepare_primary_key_groups

    if not isinstance(source, PaimonOffsetSource):
        raise ValueError(
            "incremental write_paimon requires a PaimonOffsetSource.")
    _validate_operation_id(operation_id)
    if (isinstance(commit_interval_seconds, bool)
            or not isinstance(commit_interval_seconds, (int, float))
            or commit_interval_seconds <= 0):
        raise ValueError("commit_interval_seconds must be positive.")
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    update_cols = list(dict.fromkeys(update_cols))

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(target)
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
    if (state is not None
            and not state["complete"]
            and latest_snapshot is not None
            and latest_snapshot.id != checkpoint_snapshot.id):
        raise RuntimeError("Concurrent target commit detected.")
    planned_schema_id = (
        state["schema_id"] if state is not None else table.table_schema.id)
    base_snapshot = (
        checkpoint_snapshot if checkpoint_snapshot is not None
        else latest_snapshot)

    bound = source._bind(
        catalog,
        checkpoint_plan=state["source"] if state is not None else None,
        source_tag=source_tag if retained is not None else None,
        retained_snapshot_id=(
            retained.snapshot.id if retained is not None else None),
    )
    _ensure_tag(
        catalog, source.table_identifier, source_tag,
        bound.plan["snapshot_id"])

    next_offset = state["next_offset"] if state is not None else 0
    num_written = state["num_written"] if state is not None else 0
    checkpoint_id = state["checkpoint_id"] if state is not None else 0
    if next_offset > bound.num_units:
        raise RuntimeError("Checkpoint is beyond the source unit count.")
    if state is not None and state["complete"]:
        return {"num_written": num_written}
    if state is None:
        checkpoint_id = 1
        state = _checkpoint_state(
            operation_id, planned_schema_id, update_cols,
            bound.plan, 0, 0, checkpoint_id, bound.num_units == 0)
        base_snapshot = _commit_checkpoint(
            catalog, target, table, base_snapshot,
            planned_schema_id, commit_user, checkpoint_tags,
            checkpoint_id, [], state)
        if state["complete"]:
            return {"num_written": 0}

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

    pending_messages = []
    pending_rows = 0
    pending_offset = next_offset
    last_commit_time = time.monotonic()

    try:
        for _, end in bound.windows(next_offset):
            dataset = bound.read_window(pending_offset, end).map_batches(
                to_write_batch, batch_format="pyarrow")
            messages, rows = _prepare_primary_key_groups(
                dataset,
                table,
                concurrency=concurrency,
                ray_remote_args=ray_remote_args,
            )
            pending_messages.extend(messages)
            pending_rows += rows
            pending_offset = end
            complete = end == bound.num_units
            if (complete
                    or time.monotonic() - last_commit_time
                    >= commit_interval_seconds):
                checkpoint_id += 1
                state = _checkpoint_state(
                    operation_id, planned_schema_id, update_cols,
                    bound.plan, pending_offset,
                    num_written + pending_rows, checkpoint_id, complete)
                try:
                    base_snapshot = _commit_checkpoint(
                        catalog, target, table, base_snapshot,
                        planned_schema_id, commit_user, checkpoint_tags,
                        checkpoint_id, pending_messages, state)
                except Exception:
                    # Commit failures have a known or uncertain outcome; the
                    # commit layer owns cleanup in the known case.
                    pending_messages = []
                    raise
                num_written += pending_rows
                pending_messages = []
                pending_rows = 0
                logger.info(
                    "Committed incremental write %s at offset %d/%d.",
                    operation_id, pending_offset, bound.num_units)
                if not complete:
                    last_commit_time = time.monotonic()

        return {"num_written": num_written}
    except Exception:
        if pending_messages:
            _abort_messages(table, pending_messages)
        raise


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
        restore_tag=False, recover_without_tag=True)
    if loaded is None:
        return False
    _, state = loaded
    if not state["complete"]:
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
    commit.protect_from_external_commits(base_snapshot, schema_id)
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
    return base_snapshot


def _checkpoint_state(
        operation_id, schema_id, update_cols, source_plan, next_offset,
        num_written, checkpoint_id, complete):
    return {
        "version": _CHECKPOINT_VERSION,
        "mode": _CHECKPOINT_MODE,
        "operation_id": operation_id,
        "schema_id": schema_id,
        "update_cols": list(update_cols),
        "source": dict(source_plan),
        "next_offset": next_offset,
        "num_written": num_written,
        "checkpoint_id": checkpoint_id,
        "complete": complete,
    }


def _load_checkpoint(
        catalog, target, table, operation_id, update_cols,
        commit_user, checkpoint_tags, restore_tag=True,
        recover_without_tag=False):
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
    latest_schema = table.schema_manager.latest()
    if latest_schema is None or state["schema_id"] != latest_schema.id:
        raise RuntimeError("Target schema changed during incremental write.")
    if update_cols is not None and state["update_cols"] != list(update_cols):
        raise ValueError(
            "operation_id {!r} was already used with update_cols {}.".format(
                operation_id, state["update_cols"]))
    if restore_tag:
        _store_checkpoint_tag(
            catalog, target, checkpoint_tags, checkpoint,
            state["checkpoint_id"])
    return checkpoint, state


def _read_checkpoint(snapshot, strict=True):
    encoded = (snapshot.properties or {}).get(_CHECKPOINT_PROPERTY)
    if encoded is None:
        return None
    try:
        state = json.loads(encoded)
        required = {
            "operation_id", "schema_id", "update_cols", "source",
            "next_offset", "num_written", "checkpoint_id", "complete",
        }
        if (state.get("version") != _CHECKPOINT_VERSION
                or state.get("mode") != _CHECKPOINT_MODE
                or not required.issubset(state)
                or not isinstance(state["source"], dict)
                or not isinstance(state["complete"], bool)):
            raise ValueError
        for name in ("next_offset", "num_written", "checkpoint_id"):
            if (isinstance(state[name], bool)
                    or not isinstance(state[name], int)
                    or state[name] < 0):
                raise ValueError
        num_units = state["source"].get("num_units")
        if (not isinstance(num_units, int)
                or state["next_offset"] > num_units
                or (state["complete"]
                    and state["next_offset"] != num_units)):
            raise ValueError
        return state
    except Exception as error:
        if not strict:
            return None
        raise RuntimeError("Invalid incremental write checkpoint.") from error


def _find_checkpoint_snapshot(table, commit_user, checkpoint_id, after_id):
    manager = table.snapshot_manager()
    latest = manager.get_latest_snapshot()
    if latest is None:
        return None
    for snapshot_id in range(latest.id, after_id, -1):
        snapshot = manager.get_snapshot_by_id(snapshot_id)
        if (snapshot is not None
                and snapshot.commit_user == commit_user
                and snapshot.commit_identifier == checkpoint_id
                and _read_checkpoint(snapshot, strict=False) is not None):
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
    if not messages:
        return
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()


def _validate_operation_id(operation_id):
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id must be a non-empty string.")
    if len(operation_id) > 256:
        raise ValueError("operation_id must contain at most 256 characters.")


def _digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _commit_user(operation_id):
    return "pypaimon-ray-write-" + _digest(operation_id)


def _checkpoint_tags(operation_id):
    base = _TAG_PREFIX + _digest(operation_id)
    return base + "_0", base + "_1"


def _source_tag(operation_id, target):
    return _TAG_PREFIX + _digest(target + "\0" + operation_id) + "_source"
