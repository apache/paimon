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
import json
from contextlib import ExitStack

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.lerobot.loader import (
    _commit_messages, _prepare_dataset, _video_fps,
)
from pypaimon.multimodal.lerobot.metadata import (
    _EPISODE_FPS_COLUMN, _append_arrow_tables, _commit_metadata, _json_value,
    _load_dataset_metadata,
    _metadata_table, _prepare_metadata_tables,
    _source_episode_tables, _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.schema import (
    _require_v3, _schema_from_info, _validate_v3_required_features,
    _video_feature_names,
)
from pypaimon.multimodal.lerobot.source import (
    _close_quietly, _import_lerobot_dataset, _load_hub_info,
    _open_resolved_dataset, _pandas_index_column, _resolved_source,
    _validate_info_paths,
)
from pypaimon.multimodal.source_utils import _validate_source_kerberos
from pypaimon.multimodal.table import _target_schema


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
_DATASET_INDEX_COLUMN = "dataset_index"
_SOURCE_FRAME_INDEX_COLUMN = "source_index"
_DATASETS_SCHEMA = pa.schema([
    pa.field(_DATASET_INDEX_COLUMN, pa.int64(), nullable=False),
    pa.field("dataset_name", pa.string()),
    pa.field("fps", pa.int64(), nullable=False),
    pa.field("episode_from_index", pa.int64(), nullable=False),
    pa.field("episode_to_index", pa.int64(), nullable=False),
    pa.field("frame_from_index", pa.int64(), nullable=False),
    pa.field("frame_to_index", pa.int64(), nullable=False),
    pa.field("info_json", pa.string(), nullable=False),
    pa.field("tasks_ipc", pa.binary(), nullable=False),
    pa.field("subtasks_ipc", pa.binary()),
    pa.field("stats_ipc", pa.binary()),
    pa.field("episodes_schema_ipc", pa.binary(), nullable=False),
])


def _arrow_ipc(table):
    if table is None:
        return None
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _json_table(table):
    return {row["key"]: json.loads(row["value"]) for row in table.to_pylist()}


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


def _vocabulary(plans, kind):
    key = kind + "_table"
    tables = [plan["metadata"][key] for plan in plans]
    if all(table is None for table in tables):
        return None
    if any(table is None for table in tables):
        raise ValueError("LeRobot sources disagree on %s metadata." % kind)
    singular = kind[:-1]
    index_name = singular + "_index"
    schema = tables[0].schema
    label = _pandas_index_column(schema, singular)
    schema = tables[0].select([index_name, label]).schema
    rows, by_text = [], {}
    for ordinal, (plan, table) in enumerate(zip(plans, tables)):
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
        if feature.get("dtype") == "video":
            feature.pop("fps", None)
            for key in ("info", "video_info"):
                feature[key] = {k: v for k, v in feature.get(key, {}).items()
                                if k not in _ENCODER_KEYS
                                and k != "video.fps"}
    return result


def _same_fields(left, right):
    return (set(left.names) == set(right.names)
            and all(left.field(name).equals(right.field(name), check_metadata=False)
                    for name in right.names))


def _plan_merge(plans, fps_mode="dataset"):
    first = plans[0]
    reserved_frame_fields = {
        _DATASET_INDEX_COLUMN, _SOURCE_FRAME_INDEX_COLUMN,
    }
    if reserved_frame_fields.intersection(first["schema"].names):
        raise ValueError("LeRobot source frame features use reserved source index columns.")
    info = copy.deepcopy(first["info"])
    metadata = dict(first["metadata"])
    # Quantiles are omitted rather than presenting source summaries as merged values.
    metadata["episodes_schema"] = pa.schema([
        field.with_nullable(True) if field.name.startswith("stats/subtask_index/") else field
        for field in metadata["episodes_schema"]
        if not any(field.name.startswith("stats/%s/q" % name) for name in _CONTROLS)
        and not (fps_mode == "source" and field.name == _EPISODE_FPS_COLUMN)
    ], metadata=metadata["episodes_schema"].metadata)
    frame_offset = episode_offset = 0
    for ordinal, plan in enumerate(plans):
        other = plan["info"]
        compared_keys = ("codebase_version", "robot_type")
        if fps_mode == "dataset":
            compared_keys += ("fps",)
        for key in compared_keys:
            if info.get(key) != other.get(key):
                raise ValueError("LeRobot source %d has incompatible info.%s." % (ordinal, key))
        expected_features = _feature_contract(info["features"])
        actual_features = _feature_contract(other["features"])
        for name in set(expected_features) | set(actual_features):
            if expected_features.get(name) != actual_features.get(name):
                raise ValueError("LeRobot source %d has incompatible features.%s." % (ordinal, name))
        if not _same_fields(plan["schema"], first["schema"]):
            raise ValueError("LeRobot source %d has incompatible frame schema." % ordinal)
        episode_schema = pa.schema([
            field.with_nullable(True) if field.name.startswith("stats/subtask_index/") else field
            for field in plan["metadata"]["episodes_schema"]
            if not any(field.name.startswith("stats/%s/q" % name) for name in _CONTROLS)
            and not (fps_mode == "source" and field.name == _EPISODE_FPS_COLUMN)
        ])
        if not _same_fields(episode_schema, metadata["episodes_schema"]):
            raise ValueError("LeRobot source %d has incompatible Episode schema." % ordinal)
        # Validate every Episode before creating any target table.
        plan["metadata"]["fps_mode"] = fps_mode
        try:
            for name in plan["videos"]:
                _video_fps(other, other["features"][name], name)
            for unused in _validated_episode_tables(plan["metadata"], first["videos"]):
                pass
        except ValueError as error:
            raise ValueError("LeRobot source %d: %s" % (ordinal, error)) from error
        plan["frame_offset"] = frame_offset
        plan["episode_offset"] = episode_offset
        plan["control_stats"] = {}
        frame_offset += other["total_frames"]
        episode_offset += other["total_episodes"]
    for key in list(info):
        if key not in _REBUILT_INFO and key not in ("codebase_version", "robot_type", "fps") \
                and any(plan["info"].get(key) != info[key] for plan in plans):
            del info[key]
    metadata["tasks_table"] = _vocabulary(plans, "tasks")
    metadata["subtasks_table"] = _vocabulary(plans, "subtasks")
    reserved_episode_fields = {
        _DATASET_INDEX_COLUMN,
        "source_frame_from_index", "source_frame_to_index",
        "source_episode_ipc",
    }
    if reserved_episode_fields.intersection(metadata["episodes_schema"].names):
        raise ValueError("Source Episode metadata uses reserved source index columns.")
    for field in (
            pa.field(_DATASET_INDEX_COLUMN, pa.int64(), nullable=False),
            pa.field("source_frame_from_index", pa.int64(), nullable=False),
            pa.field("source_frame_to_index", pa.int64(), nullable=False),
            pa.field("source_episode_ipc", pa.binary(), nullable=False)):
        metadata["episodes_schema"] = metadata["episodes_schema"].append(field)
    metadata["datasets_table"] = pa.Table.from_pylist([
        {
            _DATASET_INDEX_COLUMN: ordinal,
            "dataset_name": plan["dataset_name"],
            "fps": plan["metadata"]["fps"],
            "episode_from_index": plan["episode_offset"],
            "episode_to_index": (plan["episode_offset"]
                                 + plan["info"]["total_episodes"]),
            "frame_from_index": plan["frame_offset"],
            "frame_to_index": plan["frame_offset"] + plan["info"]["total_frames"],
            "info_json": json.dumps(_json_value(plan["info"]),
                                    ensure_ascii=False, separators=(",", ":")),
            "tasks_ipc": _arrow_ipc(plan["metadata"]["source_tasks_table"]),
            "subtasks_ipc": _arrow_ipc(plan["metadata"]["source_subtasks_table"]),
            "stats_ipc": _arrow_ipc(plan["metadata"]["stats_table"]),
            "episodes_schema_ipc": plan["metadata"]["episodes_schema"].serialize().to_pybytes(),
        }
        for ordinal, plan in enumerate(plans)
    ], schema=_DATASETS_SCHEMA)
    info.update(total_frames=frame_offset, total_episodes=episode_offset,
                total_tasks=metadata["tasks_table"].num_rows,
                splits={"train": "0:%d" % episode_offset})
    mixed_fps = len({plan["metadata"]["fps"] for plan in plans}) > 1
    if fps_mode == "source":
        info["fps_mode"] = "source"
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
    for name in first["videos"]:
        for key in ("info", "video_info"):
            details = [p["info"]["features"][name].get(key, {}) for p in plans]
            info["features"][name][key] = {
                k: v for k, v in details[0].items()
                if all(d.get(k) == v for d in details[1:])}
    stats = [p["metadata"]["stats_table"] for p in plans]
    if any(s is None for s in stats) and not all(s is None for s in stats):
        raise ValueError("LeRobot stats must be present in all sources or absent in all.")
    merged_stats = None
    if stats[0] is not None:
        decoded = [_json_table(s) for s in stats]
        if any(set(s) != set(decoded[0]) for s in decoded[1:]):
            raise ValueError("LeRobot statistics features must match.")
        merged_stats = {name: _merge_stats([s[name] for s in decoded])
                        for name in decoded[0] if name not in _CONTROLS}
    metadata["info_table"] = _metadata_table(info)
    metadata["stats_table"] = (None if merged_stats is None
                               else _metadata_table(merged_stats))
    return metadata, merged_stats


def _transform_frames(plan, batch, episode):
    batch = batch.append_column(
        pa.field(_DATASET_INDEX_COLUMN, pa.int64(), nullable=False),
        pa.array([plan[_DATASET_INDEX_COLUMN]] * batch.num_rows, type=pa.int64()))
    batch = batch.append_column(pa.field(_SOURCE_FRAME_INDEX_COLUMN, pa.int64(),
                                         nullable=False), batch["index"])
    for name in _CONTROLS:
        if name not in batch.column_names:
            continue
        column = batch[name]
        if name == "index":
            offset = plan["frame_offset"]
            values = pc.add_checked(column, pa.scalar(offset, type=column.type))
        elif name == "episode_index":
            values = column
        else:
            mapping = plan[name]
            values = pa.array([
                None if name == "subtask_index" and value is None
                else mapping[value]
                for value in column.to_pylist()
            ], type=column.type)
        batch = batch.set_column(batch.schema.get_field_index(name), batch.schema.field(name), values)
        stat_values = ([value for value in values.to_pylist()
                        if value is not None]
                       if values.null_count else values.to_numpy())
        if len(stat_values) == 0:
            continue
        stats = _batch_stats(stat_values)
        episode_stats = plan["control_stats"].setdefault(episode, {})
        episode_stats[name] = (_merge_stats([episode_stats[name], stats])
                               if name in episode_stats else stats)
    return batch


def _stat_value(data_type, value):
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return [_stat_value(data_type.value_type, value)]
    if pa.types.is_fixed_size_list(data_type):
        return [_stat_value(data_type.value_type, value)] * data_type.list_size
    return value


def _merged_episodes(plans, schema):
    for dataset_index, plan in enumerate(plans):
        for table in _source_episode_tables(plan["metadata"]):
            records = []
            for offset, original in enumerate(table.to_pylist()):
                row = {name: original[name] for name in schema.names
                       if name in original}
                row[_DATASET_INDEX_COLUMN] = dataset_index
                row["source_frame_from_index"] = original["dataset_from_index"]
                row["source_frame_to_index"] = original["dataset_to_index"]
                row["source_episode_ipc"] = _arrow_ipc(table.slice(offset, 1))
                episode = row["episode_index"]
                for key in ("dataset_from_index", "dataset_to_index"):
                    row[key] += plan["frame_offset"]
                if "subtask_index" not in plan["control_stats"][episode]:
                    for key in row:
                        if key.startswith("stats/subtask_index/"):
                            row[key] = None
                for name, stats in plan["control_stats"][episode].items():
                    for stat, value in stats.items():
                        key = "stats/%s/%s" % (name, stat)
                        if key in row:
                            row[key] = _stat_value(schema.field(key).type, value[0])
                records.append(row)
            yield pa.Table.from_pylist(records, schema=schema)


def _import_sources(connection, table_name, sources, batch_size, options,
                    source_options, tag_name, parallelism=1,
                    fps_mode="dataset", dataset_names=None):
    from functools import partial
    from pypaimon.multimodal.lerobot.api import _create_target_table, _validated_counts

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
                          _DATASET_INDEX_COLUMN: ordinal,
                          "dataset_name": (dataset_names[ordinal] if dataset_names is not None
                                           else None),
                          "schema": schema, "videos": videos,
                          "metadata": _load_dataset_metadata(dataset, info, resolved)})
        metadata, stats = _plan_merge(plans, fps_mode)
        target_schema = plans[0]["schema"]
        for field in (
                pa.field(_DATASET_INDEX_COLUMN, pa.int64(), nullable=False),
                pa.field(_SOURCE_FRAME_INDEX_COLUMN, pa.int64(), nullable=False)):
            target_schema = target_schema.append(field)
        table = _create_target_table(connection, table_name, target_schema,
                                     options, metadata, plans[0]["videos"])
        tables = _prepare_metadata_tables(connection, table.raw_table, metadata)
        episode_schema = _target_schema(tables["episodes"])
        commit = table.raw_table.new_batch_write_builder().new_commit()
        try:
            messages = []
            try:
                if parallelism > 1:
                    from pypaimon.multimodal.lerobot.ray_loader import _prepare_sources_ray
                    messages = _prepare_sources_ray(
                        table.raw_table, plans, source_options, batch_size, parallelism)
                for plan in plans:
                    if parallelism == 1:
                        messages.extend(_prepare_dataset(
                            table.raw_table, plan["dataset"], plan["info"], plan["source"],
                            plan["schema"], batch_size, plan["metadata"], plan["videos"],
                            transform=partial(_transform_frames, plan)))
                    plan["dataset_resources"].close()
                    plan["dataset"] = None
                for episodes in _merged_episodes(plans, episode_schema):
                    strict_arrow_table(episodes, episode_schema,
                                       "merged Episodes", 0, "LeRobot")
            except BaseException:
                commit.abort(messages)
                raise
            frames_snapshot = _commit_messages(commit, messages)
        finally:
            commit.close()
        episodes_snapshot = _append_arrow_tables(
            tables["episodes"], _merged_episodes(plans, episode_schema))
        if stats is not None:
            for name in _CONTROLS:
                parts = [s[name] for p in plans for s in p["control_stats"].values() if name in s]
                if parts:
                    stats[name] = _merge_stats(parts)
                else:
                    stats.pop(name, None)
            metadata["stats_table"] = _metadata_table(stats)
        _commit_metadata(connection, tables, tag_name, metadata, table.identifier,
                         frames_snapshot, episodes_snapshot)
