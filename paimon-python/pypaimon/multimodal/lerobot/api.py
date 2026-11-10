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

"""Public LeRobot import API."""

import json
import numbers
import sys
from functools import partial
from typing import Mapping, Optional

import pyarrow as pa

from pypaimon.catalog.catalog_exception import TableAlreadyExistException
from pypaimon.multimodal.lerobot.metadata import (
    _COMPANION_OPTION_KEYS,
    _json_value,
    _companion_identifier,
    _append_arrow_tables,
    _load_dataset_metadata,
    _managed_table_options,
    _prepare_metadata_tables,
    _dataset_fps,
    _commit_metadata,
    _validate_tag_name,
    _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.loader import _write_dataset
from pypaimon.multimodal.lerobot.schema import (
    _require_v3,
    _schema_from_info,
    _validate_v3_required_features,
    _video_feature_names,
)
from pypaimon.multimodal.lerobot.source import (
    _close_quietly,
    _import_lerobot_dataset,
    _info_dict,
    _load_hub_info,
    _open_resolved_dataset,
    _resolved_source,
    _validate_info_paths,
)
from pypaimon.multimodal.source_utils import (
    _validated_source_options,
    _validate_source_kerberos,
)


_VIDEO_LAYOUT_ERROR = (
    "LeRobot video import requires a bucket-unaware target table so each "
    "Episode is written by one writer."
)


def load_from_lerobot(
        connection,
        table_name: str,
        source,
        *,
        batch_size: int = 1024,
        options: Optional[Mapping[str, object]] = None,
        source_options: Optional[Mapping[str, object]] = None,
        tag_name: Optional[str] = None,
        parallelism: int = 1,
        fps_mode: str = "dataset",
        dataset_names=None,
) -> None:
    """Import one or more LeRobot v3 sources into a new Paimon table group.

    ``table_name`` is the group prefix; frames are stored in ``<table_name>_frames``.
    Sources must share training features. ``fps_mode='source'`` allows mixed
    FPS. ``_datasets`` retains source metadata; optional ``dataset_names``
    are caller-provided labels, never inferred from paths. ``tag_name`` pins
    component snapshots.
    FileIO credentials come from ``source_options``; ``parallelism > 1`` uses Ray.
    """
    if sys.version_info < (3, 10):
        raise RuntimeError(
            "load_from_lerobot requires Python 3.10 or newer; install and "
            "run 'pypaimon[lerobot]' on a supported Python version.")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) \
            or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")

    if isinstance(parallelism, bool) or not isinstance(parallelism, int) \
            or parallelism <= 0:
        raise ValueError("parallelism must be a positive integer.")
    if fps_mode not in ("dataset", "source"):
        raise ValueError("fps_mode must be 'dataset' or 'source'.")
    if dataset_names is not None:
        if not isinstance(dataset_names, (list, tuple)) or len(dataset_names) != (
                len(source) if isinstance(source, (list, tuple)) else 1) \
                or any(not isinstance(name, str) or not name.strip()
                       for name in dataset_names):
            raise ValueError("dataset_names must contain one non-empty name per source.")
        dataset_names = [name.strip() for name in dataset_names]
    if isinstance(source, (list, tuple)) or fps_mode == "source" \
            or dataset_names is not None:
        if not isinstance(source, (list, tuple)):
            source = [source]
        if not source:
            raise ValueError("LeRobot sources must not be empty.")
        if tag_name is not None:
            _validate_tag_name(tag_name)
        from pypaimon.multimodal.lerobot.aggregate import _import_sources
        return _import_sources(
            connection, table_name, source, batch_size, options,
            _validated_source_options(source_options), tag_name, parallelism,
            fps_mode, dataset_names)
    if parallelism > 1:
        from pypaimon.multimodal.lerobot.ray_loader import _require_ray, _ray_source_uri
        _require_ray()
        source = _ray_source_uri(source)

    if tag_name is not None:
        _validate_tag_name(tag_name)

    validated_source_options = _validated_source_options(source_options)
    _validate_source_kerberos(
        [source], validated_source_options, "LeRobot")
    with _resolved_source(source, validated_source_options) as (
            resolved_source, local_info):
        if local_info is None:
            local_info = _load_hub_info(resolved_source)
        _require_v3(local_info, resolved_source.path)
        _validate_info_paths(local_info)
        _schema_from_info(local_info)
        _dataset_fps(local_info.get("fps"))
        _validated_counts(local_info, resolved_source.path)
        _validate_v3_required_features(local_info)
        video_fields = _video_feature_names(local_info)
        LeRobotDataset = (_import_lerobot_dataset()
                          if parallelism == 1 and resolved_source.file_io is None
                          else None)
        dataset = _open_resolved_dataset(
            LeRobotDataset,
            resolved_source,
            local_info,
            download_videos=bool(video_fields),
        )
        try:
            info = _info_dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)
            _validated_counts(info, resolved_source.path)
            _validate_v3_required_features(info)
            video_fields = _video_feature_names(info)

            lerobot_schema = _schema_from_info(info)
            # Preserve source extensions omitted by LeRobot DatasetInfo.to_dict().
            metadata = _load_dataset_metadata(
                dataset, local_info, resolved_source)
            from pypaimon.multimodal.lerobot.aggregate import (
                _DATASETS_SCHEMA, _stats_json, _vocabulary_rows,
            )
            metadata["datasets_table"] = pa.Table.from_pylist([{
                "index": 0,
                "name": None,
                "info": json.dumps(_json_value(local_info), ensure_ascii=False),
                "tasks": _vocabulary_rows(metadata["tasks_table"], "task"),
                "subtasks": _vocabulary_rows(metadata["subtasks_table"], "subtask"),
                "stats": _stats_json(metadata["stats_table"]),
            }], schema=_DATASETS_SCHEMA)
            metadata["source_local_metadata"] = True
            metadata["fps_mode"] = "dataset"
            _import_dataset(
                connection,
                table_name,
                dataset,
                info,
                resolved_source,
                lerobot_schema,
                batch_size,
                options,
                metadata,
                tag_name,
                video_fields,
                parallelism,
                validated_source_options,
            )
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                _close_quietly(dataset, "dataset")


def _import_dataset(
        connection,
        table_name,
        dataset,
        info,
        source,
        source_schema,
        batch_size,
        options,
        metadata,
        tag_name,
        video_fields,
        parallelism=1,
        source_options=None):
    table = _create_target_table(
        connection,
        table_name,
        source_schema,
        options,
        metadata,
        video_fields,
    )
    tables = _prepare_metadata_tables(
        connection, table.raw_table, metadata)
    episodes_snapshot_id = _append_arrow_tables(
        tables["episodes"],
        _validated_episode_tables(metadata, video_fields),
    )
    frames_snapshot_id = None
    if int(info["total_frames"]) > 0:
        if parallelism > 1:
            from pypaimon.multimodal.lerobot.ray_loader import _write_dataset_ray
            write_dataset = partial(
                _write_dataset_ray, parallelism=parallelism,
                source_options=source_options)
        else:
            write_dataset = _write_dataset
        frames_snapshot_id = write_dataset(
            table,
            dataset,
            info,
            source,
            source_schema,
            batch_size,
            metadata,
            video_fields,
        )
    _commit_metadata(
        connection,
        tables,
        tag_name,
        metadata,
        table.identifier,
        frames_snapshot_id,
        episodes_snapshot_id,
    )


def _validated_counts(info, source):
    total_frames = _required_count(info, "total_frames", source)
    total_episodes = _required_count(info, "total_episodes", source)
    total_tasks = _required_count(info, "total_tasks", source)
    if (total_frames == 0) != (total_episodes == 0):
        raise ValueError(
            "LeRobot metadata %s has inconsistent counts: total_frames=%d "
            "and total_episodes=%d must both be zero or both be positive."
            % (source, total_frames, total_episodes))
    if total_frames == 0:
        raise ValueError(
            "load_from_lerobot requires a non-empty LeRobot Dataset v3.")
    return total_frames, total_episodes, total_tasks


def _required_count(info, name, source):
    if name not in info:
        raise ValueError(
            "LeRobot metadata %s is missing required field %s."
            % (source, name))
    value = info[name]
    if isinstance(value, bool) or not isinstance(value, numbers.Integral) \
            or value < 0:
        raise ValueError(
            "LeRobot metadata %s field %s must be a non-negative integer; "
            "found %r." % (source, name, value))
    return int(value)


def _create_target_table(
        connection, table_name, source_schema, options, metadata,
        video_fields=()):
    create_options = dict(options or {})
    configured = create_options.get("video-frame-field")
    if configured is not None:
        requested = {
            name.strip() for name in str(configured).split(",")
            if name.strip()
        }
        if requested != set(video_fields):
            raise ValueError(
                "LeRobot video features %s do not match "
                "'video-frame-field'=%r."
                % (list(video_fields), configured)
            )
    if video_fields:
        if str(create_options.get("bucket", "-1")).strip() != "-1":
            raise ValueError(_VIDEO_LAYOUT_ERROR)
        create_options["video-frame-field"] = ",".join(video_fields)
    managed_options = _managed_table_options(
        connection._identifier(table_name), metadata)
    reserved_options = set(_COMPANION_OPTION_KEYS.values()).intersection(
        create_options)
    if "pypaimon.lerobot.fps-mode" in create_options:
        reserved_options.add("pypaimon.lerobot.fps-mode")
    if reserved_options:
        raise ValueError(
            "%s are managed by load_from_lerobot."
            % sorted(reserved_options))
    create_options.update(managed_options)
    if metadata.get("source_local_metadata"):
        create_options["pypaimon.lerobot.fps-mode"] = metadata["fps_mode"]
    frames_name = _companion_identifier(connection._identifier(table_name), "_frames")
    try:
        table = connection.create_table(
            frames_name,
            schema=source_schema,
            options=create_options,
        )
    except TableAlreadyExistException as error:
        raise ValueError(
            "LeRobot target %s already exists; use a new target table."
            % frames_name
        ) from error
    return table
