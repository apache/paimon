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

import numbers
import sys
from typing import Mapping, Optional

from pypaimon.catalog.catalog_exception import TableAlreadyExistException
from pypaimon.multimodal.lerobot.metadata import (
    _COMPANION_OPTION_KEYS,
    _companion_identifier,
    _managed_table_options,
    _validate_tag_name,
)
from pypaimon.multimodal.source_utils import (
    _validated_source_options,
)
from pypaimon.multimodal.lerobot.text_fields import _validated_text_fields


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
        dataset_name=None,
        extra_dataset_fields: Optional[Mapping[str, str]] = None,
        schema_evolution: bool = True,
        append: bool = False,
        dataset_id: Optional[str] = None,
        coordinated: bool = False,
) -> None:
    """Import one or more LeRobot v3 sources into a new Paimon table group.

    ``table_name`` is the group prefix; frames are stored in ``<table_name>_frames``.
    Schema evolution adds new columns by default; missing values become null.
    Common fields must keep their types and feature definitions. Differing
    source FPS is handled automatically. ``dataset_name`` is a label or an ordered
    list of labels. ``extra_dataset_fields`` maps columns to relative UTF-8
    text file paths or globs in each source. Globs produce maps keyed by paths
    relative to their static directory prefix, without the final suffix.
    ``_datasets`` retains source metadata. ``tag_name`` pins component snapshots.
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
    if not isinstance(schema_evolution, bool):
        raise ValueError("schema_evolution must be a boolean.")
    if not isinstance(coordinated, bool):
        raise ValueError("coordinated must be a boolean.")
    if coordinated and dataset_id is None:
        raise ValueError("coordinated import requires a dataset_id.")
    if dataset_id is not None and not coordinated:
        raise ValueError("dataset_id requires coordinated=True.")
    if dataset_id is not None and (not isinstance(dataset_id, str) or not dataset_id.strip()):
        raise ValueError("dataset_id must be a non-empty string.")
    if dataset_name is not None:
        if isinstance(dataset_name, str):
            dataset_name = [dataset_name]
        if not isinstance(dataset_name, (list, tuple)) or len(dataset_name) != (
                len(source) if isinstance(source, (list, tuple)) else 1) \
                or any(not isinstance(name, str) or not name.strip()
                       for name in dataset_name):
            raise ValueError("dataset_name must contain one non-empty name per source.")
        dataset_name = [name.strip() for name in dataset_name]
    if extra_dataset_fields is not None:
        from pypaimon.multimodal.lerobot.aggregate import _DATASETS_SCHEMA
        extra_dataset_fields = _validated_text_fields(extra_dataset_fields, _DATASETS_SCHEMA.names)
    if not isinstance(source, (list, tuple)):
        source = [source]
    if not source:
        raise ValueError("LeRobot sources must not be empty.")
    if dataset_id is not None and len(source) != 1:
        raise ValueError("dataset_id requires exactly one source.")
    if tag_name is not None:
        _validate_tag_name(tag_name)
    from pypaimon.multimodal.lerobot.aggregate import _import_sources
    return _import_sources(
        connection, table_name, source, batch_size, options,
        _validated_source_options(source_options), tag_name, parallelism,
        dataset_name, extra_dataset_fields, schema_evolution, append,
        dataset_id, coordinated)


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
    managed_options = _managed_table_options(connection._identifier(table_name))
    reserved_options = set(_COMPANION_OPTION_KEYS.values()).intersection(
        create_options)
    if reserved_options:
        raise ValueError(
            "%s are managed by load_from_lerobot."
            % sorted(reserved_options))
    create_options.update(managed_options)
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
