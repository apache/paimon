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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Merge source coordinates at the write boundary of a new table group."""

import copy
import hashlib
import json
import secrets
import time
import uuid
from decimal import Decimal
from contextlib import ExitStack, contextmanager

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon import Schema as PaimonSchema
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.lerobot.loader import (
    _commit_messages, _prepare_dataset, _video_fps,
)
from pypaimon.multimodal.lerobot.metadata import (
    _EPISODE_CONTROL_COLUMNS, _append_arrow,
    _commit_metadata, _companion_identifier, _companion_table_identifiers,
    _dataset_fps, _json_value, _load_dataset_metadata, _tag_snapshot_id,
    _episode_task_indices, _metadata_table, _prepare_metadata_tables,
    _source_episode_tables, _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.schema import (
    _V3_REQUIRED_FEATURE_DTYPES, _merge_schemas, _plan_schema_evolution,
    _require_v3, _schema_from_info, _validate_lerobot_schema, _validate_v3_required_features,
    _video_feature_names,
)
from pypaimon.multimodal.lerobot.source import (
    _close_quietly, _import_lerobot_dataset, _load_hub_info,
    _open_resolved_dataset, _pandas_index_column, _resolved_source,
    _validate_info_paths,
)
from pypaimon.multimodal.source_utils import _validate_source_kerberos
from pypaimon.multimodal.table import _target_schema
from pypaimon.multimodal.lerobot.text_fields import _read_text_fields, _text_fields_schema


_CONTROLS = ("index", "episode_index", "task_index", "subtask_index")
_STATS = ("min", "max", "mean", "std", "count")
_ENCODER_KEYS = {
    "video.codec", "video.pix_fmt", "video.gop_size", "video.crf",
    "video.encoder", "video.extra_options", "video.video_backend",
    "video.has_audio", "has_audio", "video.bit_rate",
}
# These properties describe source storage or are rebuilt for the merged dataset.
_REBUILT_INFO = {
    "total_frames", "total_episodes", "total_tasks", "total_subtasks", "total_videos",
    "total_chunks", "splits", "data_path", "video_path", "chunks_size",
    "data_files_size_in_mb", "video_files_size_in_mb", "features",
}
_DATASET_ID_COLUMN = "dataset_id"
_SOURCE_HASH_COLUMN = "__paimon_source_hash"
_COORDINATED_COLUMN = "__paimon_coordinated"
_CONTROL_COUNTS_COLUMN = "__paimon_control_counts"
_EPISODE_VARIANT_GROUPS = ("meta", "data", "videos", "stats")
_EPISODE_EXTRA_VARIANT_GROUP_DESCRIPTION = "pypaimon.lerobot.episode-variant-group"
_VARIANT_TYPE = pa.struct([
    pa.field("value", pa.binary(), nullable=False),
    pa.field("metadata", pa.binary(), nullable=False),
])
_SOURCE_TASKS_TYPE = pa.list_(pa.struct([
    pa.field("task_index", pa.int64(), nullable=False),
    pa.field("task", pa.string(), nullable=False),
]))
_SOURCE_SUBTASKS_TYPE = pa.list_(pa.struct([
    pa.field("subtask_index", pa.int64(), nullable=False),
    pa.field("subtask", pa.string(), nullable=False),
]))
_DATASETS_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field(_SOURCE_HASH_COLUMN, pa.string()),
    pa.field(_COORDINATED_COLUMN, pa.bool_()),
    pa.field(_CONTROL_COUNTS_COLUMN, pa.string()),
    pa.field("name", pa.string()),
    pa.field("info", pa.string(), nullable=False),
    pa.field("tasks", _SOURCE_TASKS_TYPE, nullable=False),
    pa.field("subtasks", _SOURCE_SUBTASKS_TYPE),
    pa.field("stats", pa.string()),
])


def _stable_dataset_uuid(dataset_id):
    """Use a caller UUID directly, or derive one from a stable caller key."""
    try:
        return str(uuid.UUID(dataset_id))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, "paimon:lerobot:" + dataset_id))


def _source_hash(sources, names, extra_fields):
    request = {"sources": [str(source) for source in sources],
               "names": names, "extra_dataset_fields": extra_fields}
    return hashlib.sha256(json.dumps(
        request, sort_keys=True, ensure_ascii=False,
        separators=(",", ":")).encode("utf-8")).hexdigest()


def _dataset_absent(table, physical_id, source_hash, count, dataset_table=False):
    """Check the same pinned snapshot the optimistic commit will use."""
    key = "id" if dataset_table else _DATASET_ID_COLUMN
    hash_key = _SOURCE_HASH_COLUMN

    def condition(snapshot):
        if snapshot is None:
            return True
        pinned = table._copy_with_snapshot(snapshot)
        builder = pinned.new_read_builder().with_projection([key, hash_key])
        from pypaimon.common.predicate_builder import PredicateBuilder
        builder.with_filter(PredicateBuilder(pinned.fields).equal(key, physical_id))
        row_count = 0
        read = builder.new_read()
        for split in builder.new_scan().plan().splits():
            with read.to_arrow_batch_reader([split]) as batches:
                for batch in batches:
                    row_count += batch.num_rows
                    ids = batch.column(batch.schema.get_field_index(key))
                    if pc.any(pc.fill_null(pc.not_equal(ids, physical_id), True)).as_py():
                        raise ValueError("LeRobot Dataset %r has conflicting %s rows."
                                         % (physical_id, table.identifier))
                    hashes = batch.column(batch.schema.get_field_index(hash_key))
                    if pc.any(pc.fill_null(pc.not_equal(hashes, source_hash), True)).as_py():
                        raise ValueError("LeRobot Dataset %r was used for different sources."
                                         % physical_id)
        if not row_count:
            return True
        if row_count != count:
            raise ValueError("LeRobot Dataset %r has conflicting %s rows."
                             % (physical_id, table.identifier))
        return False

    return condition


def _published_dataset(connection, table_name, physical_id, source_hash):
    """Short-circuit complete retries before opening an expensive source."""
    frames_name = _companion_identifier(connection._identifier(table_name), "_frames")
    frames = connection.get_table(frames_name).raw_table
    identifiers = _companion_table_identifiers(frames)
    datasets = connection.catalog.get_table(identifiers["datasets"])
    if _SOURCE_HASH_COLUMN not in _target_schema(datasets).names:
        return False
    builder = datasets.new_read_builder().with_projection(["id", _SOURCE_HASH_COLUMN])
    from pypaimon.common.predicate_builder import PredicateBuilder
    builder.with_filter(PredicateBuilder(datasets.fields).equal("id", physical_id))
    rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
    if not rows:
        return False
    if len(rows) != 1 or rows[0][_SOURCE_HASH_COLUMN] != source_hash:
        raise ValueError("LeRobot Dataset %r was used for different sources."
                         % physical_id)
    return True


def _check_source_compatibility(source_rows, incoming_info, incoming_stats):
    new_features = _feature_contract(incoming_info["features"])
    for source in source_rows:
        old_info = json.loads(source["info"])
        for key in ("codebase_version", "robot_type"):
            if old_info.get(key) != incoming_info.get(key):
                raise ValueError("LeRobot source has incompatible info.%s." % key)
        old_features = _feature_contract(old_info["features"])
        for name in old_features.keys() & new_features.keys():
            if old_features[name] != new_features[name]:
                raise ValueError("LeRobot source has incompatible features.%s." % name)
        if (source["stats"] is None) != (incoming_stats is None):
            raise ValueError("LeRobot append has incompatible statistics.")


def _dataset_publish_condition(table, physical_id, source_hash,
                               incoming_info, incoming_stats):
    absent = _dataset_absent(table, physical_id, source_hash, 1, dataset_table=True)

    def condition(snapshot):
        if not absent(snapshot):
            return False
        if snapshot is not None:
            from pypaimon.multimodal.lerobot.dataset import _read_arrow
            rows = _read_arrow(table._copy_with_snapshot(snapshot),
                               ["info", "stats"]).to_pylist()
            _check_source_compatibility(rows, incoming_info, incoming_stats)
        return True

    return condition


def _evolve_component(connection, component, schema, schema_evolution, required,
                      video_fields=()):
    """Replan add-column after another writer changes the schema."""
    for attempt in range(8):
        latest = connection.catalog.get_table(component.identifier)
        latest_schema = _target_schema(latest)
        changes = _plan_schema_evolution(
            latest_schema, schema, schema_evolution, required, video_fields)
        if not changes:
            return
        try:
            connection.catalog.alter_table(component.identifier, changes)
            return
        except Exception:
            refreshed = connection.catalog.get_table(component.identifier)
            if _target_schema(refreshed).equals(latest_schema) or attempt == 7:
                raise


def _refresh_global_index(connection, identifier, column):
    """Retry an index build if another appender indexed the same row range."""
    for attempt in range(8):
        table = connection.catalog.get_table(identifier)
        try:
            table.create_global_index(column, index_type="btree")
            return
        except RuntimeError as error:
            if "overlapping row range" not in str(error) or attempt == 7:
                raise


def _next_dataset_uuid(previous=None):
    """Generate a UUIDv7 after the last source, preserving serial append order."""
    timestamp_ms = time.time_ns() // 1_000_000
    if previous is not None:
        timestamp_ms = max(timestamp_ms, (uuid.UUID(previous).int >> 80) + 1)
    value = ((timestamp_ms << 80) | (7 << 76) |
             (secrets.randbits(12) << 64) | (2 << 62) | secrets.randbits(62))
    return str(uuid.UUID(int=value))


def _json_table(table):
    return {row["key"]: json.loads(row["value"]) for row in table.to_pylist()}


def _recover_incomplete_append(connection, table_name):
    """Restore component snapshots to the Dataset table's committed counts."""
    from pypaimon.multimodal.lerobot.dataset import (
        _read_arrow, _source_dataset_ranges,
    )

    frames_name = _companion_identifier(
        connection._identifier(table_name), "_frames")
    frames = connection.get_table(frames_name).raw_table
    identifiers = _companion_table_identifiers(frames)
    datasets = connection.catalog.get_table(identifiers["datasets"])
    episodes = connection.catalog.get_table(identifiers["episodes"])
    source_columns = ["id", "info"]
    if _COORDINATED_COLUMN in _target_schema(datasets).names:
        source_columns.append(_COORDINATED_COLUMN)
    source_rows = _read_arrow(datasets, source_columns).sort_by("id").to_pylist()
    if not source_rows:
        raise ValueError("LeRobot append recovery requires Dataset metadata.")
    if any(row.get(_COORDINATED_COLUMN) for row in source_rows):
        raise ValueError("Count-based LeRobot recovery cannot run on a group with "
                         "coordinated Datasets; retry the same dataset_id instead.")
    ranges = _source_dataset_ranges(source_rows)
    expected = {"frames": ranges[-1][4], "episodes": ranges[-1][2]}
    restored = []
    for name, table in (("frames", frames), ("episodes", episodes)):
        manager = table.snapshot_manager()
        latest = manager.get_latest_snapshot()
        if latest is not None and latest.next_row_id == expected[name]:
            continue
        if latest is None or latest.next_row_id < expected[name]:
            raise RuntimeError(
                "LeRobot %s has fewer rows than Dataset metadata; rollback "
                "cannot restore the group." % name)
        previous = next((snapshot for snapshot in reversed(manager.list_snapshots())
                         if snapshot.next_row_id == expected[name]), None)
        if previous is None:
            raise RuntimeError(
                "LeRobot %s has no retained snapshot with %d rows."
                % (name, expected[name]))
        table.rollback_to(previous.id)
        restored.append(name)
    return restored


@contextmanager
def _recover_failed_append(connection, table_name):
    try:
        yield
    except BaseException as error:
        try:
            _recover_incomplete_append(connection, table_name)
        except Exception as recovery_error:
            raise RuntimeError(
                "LeRobot append failed and automatic recovery failed: %s. "
                "Call recover_lerobot_append after resolving the cause."
                % recovery_error) from error
        raise


def _json_native_arrow_type(data_type):
    if pa.types.is_null(data_type) or pa.types.is_boolean(data_type) \
            or pa.types.is_integer(data_type) or pa.types.is_floating(data_type) \
            or pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return True
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type) \
            or pa.types.is_fixed_size_list(data_type):
        return _json_native_arrow_type(data_type.value_type)
    if pa.types.is_struct(data_type):
        return all(_json_native_arrow_type(field.type) for field in data_type)
    if pa.types.is_dictionary(data_type):
        return _json_native_arrow_type(data_type.value_type)
    return False


def _variant_value(value):
    def preserve_large_integers(item):
        if isinstance(item, int) and not isinstance(item, bool) \
                and not -(1 << 63) <= item < (1 << 63):
            return Decimal(item)
        if isinstance(item, dict):
            return {key: preserve_large_integers(child) for key, child in item.items()}
        if isinstance(item, list):
            return [preserve_large_integers(child) for child in item]
        return item

    variant = GenericVariant.from_python(preserve_large_integers(_json_value(value)))
    return {"value": variant.value(), "metadata": variant.metadata()}


def _episode_variant_group(original, group):
    prefix = group + "/"
    return _variant_value({key[len(prefix):]: value
                           for key, value in original.items()
                           if key.startswith(prefix)})


def _episode_variant_groups(schema):
    groups = dict.fromkeys(_EPISODE_VARIANT_GROUPS)
    for name in schema.names:
        if "/" in name:
            group, key = name.split("/", 1)
            if not group or not key:
                raise ValueError("Invalid LeRobot Episode field name %r." % name)
            groups[group] = None
    return tuple(groups)


def _validate_episode_variant(table, source_index):
    fields = [field for field in table.schema if "/" in field.name]
    groups = _episode_variant_groups(table.schema)
    for field in fields:
        if not _json_native_arrow_type(field.type):
            raise ValueError(
                "LeRobot source %d Episode field %r must use a JSON-native "
                "Arrow type; found %s."
                % (source_index, field.name, field.type))
    names = [field.name for field in fields]
    for row_index, original in enumerate(table.select(names).to_pylist()):
        for group in groups:
            try:
                _episode_variant_group(original, group)
            except (TypeError, ValueError, OverflowError) as error:
                raise ValueError(
                    "LeRobot source %d Episode row %d has invalid JSON-native "
                    "%s metadata: %s."
                    % (source_index, row_index, group, error)) from error


def _vocabulary_rows(table, label):
    if table is None:
        return None
    index = label + "_index"
    source_label = _pandas_index_column(table.schema, label)
    return [{index: row[index], label: row[source_label]}
            for row in table.select([index, source_label]).to_pylist()]


def _stats_json(table):
    return None if table is None else json.dumps(
        _json_table(table), ensure_ascii=False, separators=(",", ":"))


def _merge_stats(items):
    """Combine population moments; quantile summaries cannot be merged exactly."""
    if any(set(item) - set(_STATS) - {"q01", "q10", "q50", "q90", "q99"}
           for item in items):
        raise ValueError("Unsupported LeRobot statistics fields.")
    if any(not set(_STATS).issubset(item) for item in items):
        raise ValueError("LeRobot statistics require min/max/mean/std/count.")
    arrays = [{key: np.asarray(item[key], dtype=np.float64 if key in ("mean", "std") else None)
               for key in _STATS}
              for item in items]
    first = arrays[0]
    for item in arrays:
        if any(item[key].shape != first[key].shape for key in _STATS):
            raise ValueError("LeRobot statistics shapes must match.")
        count = item["count"]
        if count.shape != (1,) or not np.all(np.isfinite(count)) \
                or np.any(count <= 0) or np.any(count != np.floor(count)):
            raise ValueError("LeRobot statistics count must be one positive integer.")
        if any(item[key].shape != item["mean"].shape for key in ("min", "max", "std")):
            raise ValueError("LeRobot statistic shapes must match their mean.")
    count = sum(int(item["count"][0]) for item in arrays)
    mean = sum(item["mean"] * item["count"] for item in arrays) / count
    variance = sum(item["count"] * (
        item["std"] ** 2 + (item["mean"] - mean) ** 2) for item in arrays) / count

    def extrema(key, reduction):
        dtype = object if all(np.issubdtype(item[key].dtype, np.integer)
                              for item in arrays) else np.float64
        return reduction([item[key].astype(dtype) for item in arrays]).tolist()

    return {
        "min": extrema("min", np.minimum.reduce),
        "max": extrema("max", np.maximum.reduce),
        "mean": mean.tolist(), "std": np.sqrt(variance).tolist(),
        "count": [count],
    }


def _batch_stats(values):
    values = np.asarray(values)
    return {"min": [values.min().item()], "max": [values.max().item()],
            "mean": [values.mean()], "std": [values.std()],
            "count": [len(values)]}


def _vocabulary(plans, kind, existing_sources=None):
    key = kind + "_table"
    tables = [plan["metadata"][key] for plan in plans]
    if all(table is None for table in tables):
        return None
    singular = kind[:-1]
    index_name = singular + "_index"
    first_table = next(table for table in tables if table is not None)
    schema = first_table.schema
    label = _pandas_index_column(schema, singular)
    schema = first_table.select([index_name, label]).schema
    rows, by_text = [], {}
    for source in existing_sources or ():
        for entry in sorted(source.get(kind) or (), key=lambda row: row[index_name]):
            text = entry[singular]
            if text not in by_text:
                by_text[text] = len(rows)
                rows.append({index_name: len(rows), label: text})
    for ordinal, (plan, table) in enumerate(zip(plans, tables)):
        if table is None:
            plan[index_name] = {}
            continue
        source_label = _pandas_index_column(table.schema, singular)
        if (not table.schema.field(index_name).equals(
                schema.field(index_name), check_metadata=False)
                or not (pa.types.is_string(table.schema.field(source_label).type)
                        or pa.types.is_large_string(table.schema.field(source_label).type))):
            raise ValueError("LeRobot source %d has incompatible %s schema." % (ordinal, kind))
        mapping = {}
        source_texts = set()
        for row in sorted(table.select([index_name, source_label]).to_pylist(),
                          key=lambda row: row[index_name]):
            text = row[source_label]
            if text in source_texts:
                raise ValueError("LeRobot source %d has duplicate %s text %r; "
                                 "original frame IDs would be ambiguous."
                                 % (ordinal, singular, text))
            source_texts.add(text)
            if text not in by_text:
                by_text[text] = len(rows)
                rows.append({index_name: len(rows), label: text})
            mapping[row[index_name]] = by_text[text]
        plan[index_name] = mapping
    return pa.Table.from_pylist(rows, schema=schema)


def _feature_contract(features):
    result = copy.deepcopy(features)
    for feature in result.values():
        feature.pop("nullable", None)
        if feature.get("dtype") == "video":
            feature.pop("fps", None)
            for key in ("info", "video_info"):
                details = feature.get(key)
                details = details if isinstance(details, dict) else {}
                feature[key] = {k: v for k, v in details.items()
                                if k not in _ENCODER_KEYS
                                and k != "video.fps"}
    return result


def _plan_merge(plans, extra_dataset_fields=None, schema_evolution=True,
                existing_sources=None):
    first = plans[0]
    mixed_fps = len({plan["metadata"]["fps"] for plan in plans}) > 1
    reserved_frame_fields = {_DATASET_ID_COLUMN, _SOURCE_HASH_COLUMN}
    for plan in plans:
        if reserved_frame_fields.intersection(plan["schema"].names):
            raise ValueError("LeRobot source frame features use reserved dataset_id column.")
    frame_schema = _merge_schemas([plan["schema"] for plan in plans], schema_evolution,
                                  _V3_REQUIRED_FEATURE_DTYPES)
    features, contracts = {}, {}
    for ordinal, plan in enumerate(plans):
        for name, feature in plan["info"]["features"].items():
            contract = _feature_contract({name: feature})[name]
            if name in contracts and contracts[name] != contract:
                raise ValueError("LeRobot source %d has incompatible features.%s." % (ordinal, name))
            contracts[name] = contract
            features.setdefault(name, copy.deepcopy(feature))
    for field in frame_schema:
        if field.nullable:
            features[field.name]["nullable"] = True
    info = copy.deepcopy(first["info"])
    info["features"] = features
    metadata = dict(first["metadata"])
    metadata["frame_schema"] = frame_schema
    # Keep ungrouped fields typed and turn each slash prefix into one VARIANT column.
    groups = tuple(dict.fromkeys(group for plan in plans
                                 for group in _episode_variant_groups(
                                     plan["metadata"]["episodes_schema"])))
    if _DATASET_ID_COLUMN in groups:
        raise ValueError("LeRobot Episode VARIANT group uses reserved column dataset_id.")
    reserved_episode_fields = {_DATASET_ID_COLUMN, _SOURCE_HASH_COLUMN, *groups}
    for ordinal, plan in enumerate(plans):
        conflicts = reserved_episode_fields.intersection(
            plan["metadata"]["episodes_schema"].names)
        if conflicts:
            raise ValueError("LeRobot source %d Episode uses reserved columns: %s."
                             % (ordinal, sorted(conflicts)))
    schemas = []
    for plan in plans:
        source_schema = plan["metadata"]["episodes_schema"]
        source_groups = _episode_variant_groups(source_schema)
        schemas.append(pa.schema(
            [field for field in source_schema if "/" not in field.name]
            + [pa.field(group, _VARIANT_TYPE, nullable=False,
                        metadata={b"description":
                                  _EPISODE_EXTRA_VARIANT_GROUP_DESCRIPTION.encode("utf-8")})
               for group in source_groups if group not in _EPISODE_VARIANT_GROUPS]))
    metadata["episodes_schema"] = _merge_schemas(schemas, schema_evolution, _EPISODE_CONTROL_COLUMNS)
    frame_offset = episode_offset = 0
    for ordinal, plan in enumerate(plans):
        other = plan["info"]
        compared_keys = ("codebase_version", "robot_type")
        for key in compared_keys:
            if info.get(key) != other.get(key):
                raise ValueError("LeRobot source %d has incompatible info.%s." % (ordinal, key))
        # Validate every Episode before creating any target table.
        try:
            for name in plan["videos"]:
                _video_fps(other, other["features"][name], name)
            for table in _validated_episode_tables(
                    plan["metadata"], plan["videos"], check_episode_fps=mixed_fps):
                _validate_episode_variant(table, ordinal)
        except ValueError as error:
            raise ValueError("LeRobot source %d: %s" % (ordinal, error)) from error
        plan["frame_offset"] = frame_offset
        plan["episode_offset"] = episode_offset
        plan["control_stats"] = {}
        plan["control_counts"] = {}
        frame_offset += other["total_frames"]
        episode_offset += other["total_episodes"]
    for key in list(info):
        if key not in _REBUILT_INFO and key not in ("codebase_version", "robot_type", "fps") \
                and any(plan["info"].get(key) != info[key] for plan in plans):
            del info[key]
    metadata["tasks_table"] = _vocabulary(plans, "tasks", existing_sources)
    metadata["subtasks_table"] = _vocabulary(plans, "subtasks", existing_sources)
    for field in (
            pa.field(_DATASET_ID_COLUMN, pa.string(), nullable=False),
            pa.field(_SOURCE_HASH_COLUMN, pa.string()),
            pa.field("meta", _VARIANT_TYPE, nullable=False),
            pa.field("data", _VARIANT_TYPE, nullable=False),
            pa.field("videos", _VARIANT_TYPE, nullable=False),
            pa.field("stats", _VARIANT_TYPE, nullable=False)):
        metadata["episodes_schema"] = metadata["episodes_schema"].append(field)
    episode_names = metadata["episodes_schema"].names
    if len(episode_names) != len(set(episode_names)):
        raise ValueError("Merged LeRobot Episode schema has duplicate columns: %s."
                         % sorted(name for name in set(episode_names)
                                  if episode_names.count(name) > 1))
    # Use the same Arrow-to-Paimon conversion as table creation before creating frames.
    PaimonSchema.from_pyarrow_schema(metadata["episodes_schema"])
    metadata["datasets_table"] = pa.Table.from_pylist([
        {
            "id": plan[_DATASET_ID_COLUMN],
            _SOURCE_HASH_COLUMN: plan.get("source_hash"),
            _COORDINATED_COLUMN: plan.get("coordinated"),
            _CONTROL_COUNTS_COLUMN: None,
            "name": plan["dataset_name"],
            "info": json.dumps(_json_value(plan["info"]),
                               ensure_ascii=False, separators=(",", ":")),
            "tasks": _vocabulary_rows(plan["metadata"]["source_tasks_table"], "task"),
            "subtasks": _vocabulary_rows(plan["metadata"]["source_subtasks_table"], "subtask"),
            "stats": _stats_json(plan["metadata"]["stats_table"]),
            **plan.get("extra_dataset_fields", {}),
        }
        for ordinal, plan in enumerate(plans)
    ], schema=pa.schema(list(_DATASETS_SCHEMA)
                        + _text_fields_schema(extra_dataset_fields or {})))
    info.update(total_frames=frame_offset, total_episodes=episode_offset,
                total_tasks=metadata["tasks_table"].num_rows,
                splits={"train": "0:%d" % episode_offset})
    info.pop("fps_mode", None)
    if mixed_fps:
        info["fps"] = None
        for feature in info["features"].values():
            if feature.get("dtype") != "video":
                continue
            feature.pop("fps", None)
            for key in ("info", "video_info"):
                if isinstance(feature.get(key), dict):
                    feature[key].pop("video.fps", None)
    if metadata["subtasks_table"] is not None:
        info["total_subtasks"] = metadata["subtasks_table"].num_rows
    for key in ("total_chunks", "total_videos", "data_path", "video_path",
                "chunks_size", "data_files_size_in_mb", "video_files_size_in_mb"):
        info.pop(key, None)
    # Encoder descriptions are not training features. Retain only shared values.
    for name in _video_feature_names(info):
        for key in ("info", "video_info"):
            details = [p["info"]["features"][name].get(key) for p in plans
                       if name in p["info"]["features"]]
            details = [d if isinstance(d, dict) else {} for d in details]
            info["features"][name][key] = {
                k: v for k, v in details[0].items()
                if all(d.get(k) == v for d in details[1:])}
    stats = [p["metadata"]["stats_table"] for p in plans]
    if any(s is None for s in stats) and not all(s is None for s in stats):
        raise ValueError("LeRobot stats must be present in all sources or absent in all.")
    merged_stats = None
    if stats[0] is not None:
        decoded = [_json_table(s) for s in stats]
        names = dict.fromkeys(name for item in decoded for name in item)
        merged_stats = {
            name: (decoded[0][name] if len(decoded) == 1 else
                   _merge_stats([item[name] for item in decoded if name in item]))
            for name in names if name not in _CONTROLS}
    metadata["info_table"] = _metadata_table(info)
    metadata["stats_table"] = (None if merged_stats is None
                               else _metadata_table(merged_stats))
    return metadata, merged_stats


def _transform_frames(plan, batch, episode):
    batch = batch.append_column(
        pa.field(_DATASET_ID_COLUMN, pa.string(), nullable=False),
        pa.array([plan[_DATASET_ID_COLUMN]] * batch.num_rows, type=pa.string()))
    batch = batch.append_column(
        pa.field(_SOURCE_HASH_COLUMN, pa.string()),
        pa.array([plan.get("source_hash")] * batch.num_rows, type=pa.string()))
    for name in _CONTROLS:
        if name not in batch.column_names:
            continue
        column = batch[name]
        if name in ("task_index", "subtask_index"):
            counts = plan.setdefault("control_counts", {}).setdefault(name, {})
            for value in column.to_pylist():
                if value is not None:
                    key = str(int(value))
                    counts[key] = counts.get(key, 0) + 1
        if name == "index":
            offset = plan["frame_offset"]
            values = column
            stat_column = pc.add_checked(
                column, pa.scalar(offset, type=column.type))
        elif name == "episode_index":
            values = column
        else:
            mapping = plan[name]
            mapped = pa.array([
                None if name == "subtask_index" and value is None
                else mapping[value]
                for value in column.to_pylist()
            ], type=column.type)
            values = column
        batch = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name), values)
        if name != "index":
            stat_column = mapped if name in ("task_index", "subtask_index") else values
        stat_values = ([value for value in stat_column.to_pylist()
                        if value is not None]
                       if stat_column.null_count else stat_column.to_numpy())
        if len(stat_values) == 0:
            continue
        stats = _batch_stats(stat_values)
        episode_stats = plan["control_stats"].setdefault(episode, {})
        episode_stats[name] = (_merge_stats([episode_stats[name], stats])
                               if name in episode_stats else stats)
    return batch


def _control_stats_from_frames(table, plan, batch_size):
    """Rebuild Episode stats from committed Frames during a partial retry."""
    controls = [name for name in _CONTROLS if name in _target_schema(table).names]
    builder = table.new_read_builder().with_projection(controls)
    from pypaimon.common.predicate_builder import PredicateBuilder
    builder.with_filter(PredicateBuilder(table.fields).equal(
        _DATASET_ID_COLUMN, plan[_DATASET_ID_COLUMN]))
    reader = builder.new_read()
    for split in builder.new_scan().plan().splits():
        for batch in reader.to_arrow([split]).to_batches(max_chunksize=batch_size):
            for episode in set(batch["episode_index"].to_pylist()):
                selected = pa.Table.from_batches([batch]).filter(
                    pc.equal(batch["episode_index"], episode))
                _transform_frames(plan, selected, episode)


def _merged_episodes(plans, schema):
    extra_groups = tuple(dict.fromkeys(
        group for plan in plans
        for group in _episode_variant_groups(plan["metadata"]["episodes_schema"])
        if group not in _EPISODE_VARIANT_GROUPS))
    for plan in plans:
        validated = plan["metadata"]["episodes"]
        task_indices = plan["metadata"]["task_indices"]
        episode_offset = 0
        for table in _source_episode_tables(plan["metadata"]):
            source_groups = set(_episode_variant_groups(table.schema))
            records = []
            for original in table.to_pylist():
                try:
                    expected = validated[episode_offset]
                    source_tasks = _episode_task_indices(
                        original["tasks"], task_indices, episode_offset)
                except (IndexError, KeyError, TypeError, ValueError):
                    raise ValueError(
                        "LeRobot Episode metadata changed after validation.") from None
                if (original["episode_index"] != episode_offset
                        or original["dataset_from_index"] != expected["dataset_from_index"]
                        or original["dataset_to_index"] != expected["dataset_to_index"]
                        or original["length"] != expected["length"]
                        or source_tasks != list(expected["task_indices"])):
                    raise ValueError("LeRobot Episode metadata changed after validation.")
                row = {name: original[name] for name in schema.names
                       if name in original}
                for group in ("meta", "data", "videos"):
                    row[group] = _episode_variant_group(original, group)
                for group in extra_groups:
                    row[group] = (_episode_variant_group(original, group)
                                  if group in source_groups
                                  else None)
                stats = {}
                for key, value in original.items():
                    if key.startswith("stats/"):
                        feature, statistic = key[6:].rsplit("/", 1)
                        # Rebased control values have new statistics. Their
                        # original quantiles cannot describe the merged IDs.
                        if feature not in _CONTROLS or not statistic.startswith("q"):
                            stats.setdefault(feature, {})[statistic] = value
                row[_DATASET_ID_COLUMN] = plan[_DATASET_ID_COLUMN]
                row[_SOURCE_HASH_COLUMN] = plan.get("source_hash")
                episode = row["episode_index"]
                if "subtask_index" not in plan["control_stats"][episode]:
                    if "subtask_index" in stats:
                        stats["subtask_index"] = {
                            metric: None for metric in stats["subtask_index"]}
                for name, values in plan["control_stats"][episode].items():
                    stats[name] = values
                row["stats"] = _variant_value(stats)
                records.append(row)
                episode_offset += 1
            yield pa.Table.from_pylist(records, schema=schema)
        if episode_offset != len(validated):
            raise ValueError("LeRobot Episode metadata changed after validation.")


def _import_sources(connection, table_name, sources, batch_size, options,
                    source_options, tag_name, parallelism=1,
                    dataset_name=None, extra_dataset_fields=None,
                    schema_evolution=True, append=False, dataset_id=None,
                    coordinated=False):
    from functools import partial
    from pypaimon.multimodal.lerobot.api import _create_target_table, _validated_counts

    physical_id = _stable_dataset_uuid(dataset_id) if dataset_id is not None else None
    source_hash = _source_hash(sources, dataset_name, extra_dataset_fields) \
        if dataset_id is not None else None
    if coordinated and tag_name is not None:
        raise ValueError("Coordinated LeRobot append cannot create a tag; "
                         "call create_lerobot_tag after publication.")
    if append and physical_id is not None and _published_dataset(
            connection, table_name, physical_id, source_hash):
        return

    if parallelism > 1:
        from pypaimon.multimodal.lerobot.ray_loader import _require_ray, _ray_source_uri
        _require_ray()
        sources = [_ray_source_uri(source) for source in sources]
    _validate_source_kerberos(sources, source_options, "LeRobot")
    with ExitStack() as stack:
        plans = []
        for ordinal, source in enumerate(sources):
            resolved, info = stack.enter_context(_resolved_source(source, source_options))
            if info is None:
                info = _load_hub_info(resolved)
            _require_v3(info, "source %d" % ordinal)
            _validate_info_paths(info)
            _dataset_fps(info.get("fps"))
            _validated_counts(info, "source %d" % ordinal)
            _validate_v3_required_features(info)
            schema = _schema_from_info(info)
            videos = _video_feature_names(info)
            dataset = _open_resolved_dataset(
                None if resolved.file_io is not None else _import_lerobot_dataset(),
                resolved, info, download_videos=bool(videos))
            dataset_resources = stack.enter_context(ExitStack())
            if callable(getattr(dataset, "close", None)):
                dataset_resources.callback(_close_quietly, dataset, "dataset")
            plans.append({"dataset": dataset, "dataset_resources": dataset_resources,
                          "source": resolved, "info": info,
                          _DATASET_ID_COLUMN: None,
                          "dataset_name": (dataset_name[ordinal] if dataset_name is not None
                                           else None),
                          "schema": schema, "videos": videos,
                          "extra_dataset_fields": _read_text_fields(resolved, extra_dataset_fields),
                          "source_hash": source_hash,
                          "coordinated": coordinated,
                          "metadata": _load_dataset_metadata(dataset, info, resolved)})
        existing = None
        if append:
            if options:
                raise ValueError("LeRobot append cannot change table options.")
            from pypaimon.multimodal.lerobot.dataset import (
                _read_arrow, _source_dataset_ranges,
            )
            frames_name = _companion_identifier(
                connection._identifier(table_name), "_frames")
            table = connection.get_table(frames_name)
            identifiers = _companion_table_identifiers(table.raw_table)
            tables = {name: connection.catalog.get_table(identifier)
                      for name, identifier in identifiers.items()}
            if tag_name is not None:
                for component in (table.raw_table, *tables.values()):
                    if _tag_snapshot_id(connection.catalog, component.identifier,
                                        tag_name) is not None:
                        raise ValueError("LeRobot tag %s already exists on %s."
                                         % (tag_name, component.identifier))
            if _DATASET_ID_COLUMN not in _target_schema(tables["episodes"]).names:
                raise ValueError("LeRobot append requires a source-local table group.")
            source_columns = [name for name in (
                "id", "info", "tasks", "subtasks", "stats", _SOURCE_HASH_COLUMN,
                _COORDINATED_COLUMN, _CONTROL_COUNTS_COLUMN)
                if name in _target_schema(tables["datasets"]).names]
            source_rows = _read_arrow(tables["datasets"], source_columns).sort_by("id").to_pylist()
            if not source_rows:
                raise ValueError("LeRobot append requires an initialized table group.")
            ranges = _source_dataset_ranges(source_rows)
            expected_counts = {"frames": ranges[-1][4],
                               "episodes": ranges[-1][2],
                               "datasets": len(source_rows)}
            if not coordinated:
                for name, component in (("frames", table.raw_table),
                                        ("episodes", tables["episodes"]),
                                        ("datasets", tables["datasets"])):
                    snapshot = component.snapshot_manager().get_latest_snapshot()
                    if snapshot is None or snapshot.next_row_id != expected_counts[name]:
                        raise ValueError(
                            "LeRobot group has incomplete %s data; call "
                            "recover_lerobot_append before appending." % name)
                if any(row.get(_COORDINATED_COLUMN) for row in source_rows):
                    raise ValueError("LeRobot group has coordinated Datasets; "
                                     "append with coordinated=True.")
            existing = (table, tables, source_rows, ranges)
        previous = source_rows[-1]["id"] if existing is not None else None
        for plan in plans:
            previous = physical_id or _next_dataset_uuid(previous)
            plan[_DATASET_ID_COLUMN] = previous
        metadata, stats = _plan_merge(
            plans, extra_dataset_fields, schema_evolution if existing is None else True,
            source_rows if existing is not None else None)
        target_schema = metadata["frame_schema"]
        target_schema = target_schema.append(
            pa.field(_DATASET_ID_COLUMN, pa.string(), nullable=False))
        target_schema = target_schema.append(pa.field(_SOURCE_HASH_COLUMN, pa.string()))
        if existing is None:
            table = _create_target_table(connection, table_name, target_schema,
                                         options, metadata,
                                         _video_feature_names(_json_table(metadata["info_table"])))
            tables = _prepare_metadata_tables(connection, table.raw_table, metadata)
        else:
            table, tables, source_rows, ranges = existing
            existing_episode_schema = _target_schema(tables["episodes"])
            existing_groups = set(_EPISODE_VARIANT_GROUPS)
            existing_groups.update(
                field.name for field in existing_episode_schema
                if (field.metadata or {}).get(b"description") ==
                _EPISODE_EXTRA_VARIANT_GROUP_DESCRIPTION.encode("utf-8"))
            incoming_groups = set(group for plan in plans
                                  for group in _episode_variant_groups(
                                      plan["metadata"]["episodes_schema"]))
            conflicts = sorted(
                name for name in set(existing_episode_schema.names)
                & set(metadata["episodes_schema"].names)
                if (name in existing_groups) != (name in incoming_groups))
            if conflicts:
                raise ValueError("LeRobot append Episode VARIANT groups conflict with "
                                 "plain columns: %s." % conflicts)
            first_info = ranges[0][0]
            old_features = {}
            for source in source_rows:
                old_features.update(_feature_contract(json.loads(source["info"])["features"]))
            for ordinal, plan in enumerate(plans):
                for key in ("codebase_version", "robot_type"):
                    if first_info.get(key) != plan["info"].get(key):
                        raise ValueError(
                            "LeRobot source %d has incompatible info.%s."
                            % (ordinal, key))
                new_features = _feature_contract(plan["info"]["features"])
                for name in old_features.keys() & new_features.keys():
                    if old_features[name] != new_features[name]:
                        raise ValueError("LeRobot source %d has incompatible features.%s."
                                         % (ordinal, name))
            old_stats = source_rows[0]["stats"]
            new_stats = metadata["datasets_table"]["stats"][0].as_py()
            if (old_stats is None) != (new_stats is None):
                raise ValueError("LeRobot append has incompatible statistics.")
            _validate_lerobot_schema(target_schema, _target_schema(table.raw_table), "append")
            if not coordinated:
                # Internal coordination columns are optional for ordinary appends to
                # groups created before those columns existed. The business schema
                # evolution switch must not require adding them.
                target_schema = pa.schema([
                    field for field in target_schema
                    if field.name != _SOURCE_HASH_COLUMN or
                    field.name in _target_schema(table.raw_table).names],
                    metadata=target_schema.metadata)
                metadata["episodes_schema"] = pa.schema([
                    field for field in metadata["episodes_schema"]
                    if field.name != _SOURCE_HASH_COLUMN or
                    field.name in existing_episode_schema.names],
                    metadata=metadata["episodes_schema"].metadata)
                dataset_schema = _target_schema(tables["datasets"])
                missing_internal = {_SOURCE_HASH_COLUMN, _COORDINATED_COLUMN,
                                    _CONTROL_COUNTS_COLUMN} - \
                    set(dataset_schema.names)
                if missing_internal:
                    metadata["datasets_table"] = metadata["datasets_table"].drop(
                        sorted(missing_internal))
            # Validate every component before making any catalog changes.
            requests = [
                (table.raw_table, target_schema, (*_V3_REQUIRED_FEATURE_DTYPES, _DATASET_ID_COLUMN)),
                (tables["episodes"], metadata["episodes_schema"],
                 (*_EPISODE_CONTROL_COLUMNS, _DATASET_ID_COLUMN)),
                (tables["datasets"], metadata["datasets_table"].schema,
                 tuple(field.name for field in _DATASETS_SCHEMA if not field.nullable)),
            ]
            video_fields = _video_feature_names(_json_table(metadata["info_table"]))
            changes = [_plan_schema_evolution(
                _target_schema(component), schema, schema_evolution, required,
                video_fields if ordinal == 0 else ())
                for ordinal, (component, schema, required) in enumerate(requests)]
            for (component, schema, required), component_changes in zip(requests, changes):
                if component_changes:
                    _evolve_component(connection, component, schema, schema_evolution,
                                      required, video_fields if component is table.raw_table else ())
            # Writers and Ray workers must use the reloaded schema, including new field IDs.
            table = connection.get_table(table.identifier)
            tables = {name: connection.catalog.get_table(component.identifier)
                      for name, component in tables.items()}
            if not coordinated:
                frame_offset = ranges[-1][4]
                for plan in plans:
                    plan["frame_offset"] += frame_offset
            metadata["datasets_table"] = pa.Table.from_pylist(
                metadata["datasets_table"].to_pylist(),
                schema=_target_schema(tables["datasets"]))
        if existing is not None and not coordinated:
            stack.enter_context(_recover_failed_append(connection, table_name))
        episode_schema = _target_schema(tables["episodes"])
        frames_needed = episodes_needed = True
        if coordinated:
            frames_condition = _dataset_absent(
                table.raw_table, physical_id, source_hash, plans[0]["info"]["total_frames"])
            episodes_condition = _dataset_absent(
                tables["episodes"], physical_id, source_hash,
                plans[0]["info"]["total_episodes"])
            frames_needed = frames_condition(
                table.raw_table.snapshot_manager().get_latest_snapshot())
            episodes_needed = episodes_condition(
                tables["episodes"].snapshot_manager().get_latest_snapshot())
        commit = None
        if frames_needed:
            commit = table.raw_table.new_batch_write_builder().new_commit()
            if coordinated:
                commit.with_commit_condition(frames_condition)
            stack.callback(commit.close)
        episode_commit = episode_write = None
        if episodes_needed:
            episode_builder = tables["episodes"].new_batch_write_builder()
            episode_commit = episode_builder.new_commit()
            if coordinated:
                episode_commit.with_commit_condition(episodes_condition)
            stack.callback(episode_commit.close)
            episode_write = episode_builder.new_write()
            stack.callback(episode_write.close)
        episode_commit_started = False
        episode_messages = None
        try:
            messages = []
            try:
                if frames_needed and parallelism > 1:
                    from pypaimon.multimodal.lerobot.ray_loader import _prepare_sources_ray
                    messages = _prepare_sources_ray(
                        table.raw_table, plans, source_options, batch_size, parallelism)
                for plan in plans:
                    if frames_needed and parallelism == 1:
                        messages.extend(_prepare_dataset(
                            table.raw_table, plan["dataset"], plan["info"], plan["source"],
                            plan["schema"], batch_size, plan["metadata"], plan["videos"],
                            transform=partial(_transform_frames, plan)))
                    elif not frames_needed:
                        _control_stats_from_frames(table.raw_table, plan, batch_size)
                    plan["dataset_resources"].close()
                    plan["dataset"] = None
                if episodes_needed:
                    for episodes in _merged_episodes(plans, episode_schema):
                        strict_arrow_table(episodes, episode_schema,
                                           "merged Episodes", 0, "LeRobot")
                        episode_write.write_arrow(episodes)
                    episode_messages = episode_write.prepare_commit()
            except BaseException:
                if commit is not None:
                    commit.abort(messages)
                raise
            frames_snapshot = (_commit_messages(commit, messages)
                               if commit is not None else None)
            if episode_commit is not None:
                episode_commit_started = True
                episodes_snapshot = _commit_messages(episode_commit, episode_messages)
            else:
                episodes_snapshot = None
        finally:
            if episode_write is not None and not episode_commit_started:
                if episode_messages is None:
                    episode_write.abort()
                else:
                    episode_commit.abort(episode_messages)
        rows = metadata["datasets_table"].to_pylist()
        for row, plan in zip(rows, plans):
            row[_CONTROL_COUNTS_COLUMN] = json.dumps(
                plan["control_counts"], sort_keys=True, separators=(",", ":"))
        metadata["datasets_table"] = pa.Table.from_pylist(
            rows, schema=metadata["datasets_table"].schema)
        if existing is None and stats is not None:
            for name in _CONTROLS:
                parts = [s[name] for p in plans for s in p["control_stats"].values() if name in s]
                if parts:
                    stats[name] = _merge_stats(parts)
                else:
                    stats.pop(name, None)
            metadata["stats_table"] = _metadata_table(stats)
        if existing is None:
            _commit_metadata(connection, tables, tag_name, metadata, table.identifier,
                             frames_snapshot, episodes_snapshot)
        else:
            for column in ("dataset_id", "index"):
                if coordinated:
                    _refresh_global_index(connection, table.identifier, column)
                elif table.raw_table.create_global_index(column, index_type="btree") <= 0:
                    raise RuntimeError("LeRobot append did not refresh %s index."
                                       % column)
            condition = (_dataset_publish_condition(
                tables["datasets"], physical_id, source_hash,
                plans[0]["info"], rows[0]["stats"])
                if coordinated else None)
            if _append_arrow(tables["datasets"], metadata["datasets_table"],
                             commit_condition=condition) is None:
                raise RuntimeError("LeRobot append did not commit Dataset metadata.")
            if tag_name is not None:
                from pypaimon.multimodal.lerobot.metadata import create_lerobot_tag
                create_lerobot_tag(connection, table_name, tag_name)
