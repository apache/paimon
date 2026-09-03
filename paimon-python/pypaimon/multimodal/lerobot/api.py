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
    _append_arrow_tables,
    _load_dataset_metadata,
    _managed_table_options,
    _prepare_metadata_tables,
    _positive_integer,
    _publish_dataset,
    _reserve_dataset_version,
    _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.loader import _write_dataset
from pypaimon.multimodal.lerobot.schema import (
    _require_v3,
    _schema_from_info,
    _validate_v3_required_features,
)
from pypaimon.multimodal.lerobot.source import (
    _close_quietly,
    _import_lerobot_dataset,
    _load_hub_info,
    _open_resolved_dataset,
    _resolved_source,
    _validate_info_paths,
)
from pypaimon.multimodal.source_utils import (
    _validated_source_options,
    _validate_source_kerberos,
)


def load_from_lerobot(
        connection,
        table_name: str,
        source,
        *,
        batch_size: int = 1024,
        options: Optional[Mapping[str, object]] = None,
        source_options: Optional[Mapping[str, object]] = None,
) -> int:
    """Import LeRobot Dataset v3 and return its version ID.

    A new target table is created from LeRobot metadata. Episode, task, and
    version metadata are stored in companion Paimon tables.
    FileIO URI credentials come only from ``source_options`` and are not
    inherited from the target Catalog.
    """
    if sys.version_info < (3, 10):
        raise RuntimeError(
            "load_from_lerobot requires Python 3.10 or newer; install and "
            "run 'pypaimon[lerobot]' on a supported Python version.")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) \
            or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")

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
        _positive_integer(local_info.get("fps"), "fps")
        _validated_counts(local_info, resolved_source.path)
        _validate_v3_required_features(local_info)
        LeRobotDataset = _import_lerobot_dataset()
        dataset = _open_resolved_dataset(
            LeRobotDataset, resolved_source, local_info)
        try:
            info = dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)
            _validated_counts(info, resolved_source.path)
            _validate_v3_required_features(info)

            lerobot_schema = _schema_from_info(info)
            metadata = _load_dataset_metadata(
                dataset, info, resolved_source)
            return _import_dataset(
                connection,
                table_name,
                dataset,
                info,
                resolved_source,
                lerobot_schema,
                batch_size,
                options,
                metadata,
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
        metadata):
    table = _create_target_table(
        connection, table_name, source_schema, options)
    tables = _prepare_metadata_tables(
        connection, table.raw_table, metadata)
    version_id = 1
    _reserve_dataset_version(
        tables["versions"],
        version_id,
        metadata,
    )
    episodes_snapshot_id = _append_arrow_tables(
        tables["episodes"],
        _validated_episode_tables(metadata),
    )
    frames_snapshot_id = None
    if int(info["total_frames"]) > 0:
        frames_snapshot_id = _write_dataset(
            table,
            dataset,
            info,
            source,
            source_schema,
            batch_size,
            metadata,
        )
    _publish_dataset(
        connection,
        tables,
        version_id,
        metadata,
        table.identifier,
        frames_snapshot_id,
        episodes_snapshot_id,
    )
    return version_id


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
        connection, table_name, source_schema, options):
    create_options = dict(options or {})
    managed_options = _managed_table_options(
        connection._identifier(table_name))
    reserved_options = set(managed_options).intersection(create_options)
    if reserved_options:
        raise ValueError(
            "%s are managed by load_from_lerobot."
            % sorted(reserved_options))
    create_options.update(managed_options)
    try:
        table = connection.create_table(
            table_name,
            schema=source_schema,
            options=create_options,
        )
    except TableAlreadyExistException as error:
        raise ValueError(
            "LeRobot target %s already exists; use a new target table."
            % connection._identifier(table_name)
        ) from error
    return table
