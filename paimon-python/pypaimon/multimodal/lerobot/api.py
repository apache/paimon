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
from dataclasses import dataclass
from typing import Mapping, Optional

from pypaimon.catalog.catalog_exception import TableAlreadyExistException
from pypaimon.multimodal.lerobot.metadata import (
    _DEFAULT_DATASET_ID_OPTION,
    _drop_import_tables,
    _frame_schema,
    _load_dataset_metadata,
    _managed_table_options,
    _new_id,
    _prepare_metadata_tables,
    _publish_dataset,
    _reject_subtasks,
    _reserve_dataset_version,
)
from pypaimon.multimodal.lerobot.loader import _write_dataset
from pypaimon.multimodal.lerobot.schema import (
    _require_v3,
    _schema_from_info,
)
from pypaimon.multimodal.lerobot.source import (
    _close_quietly,
    _has_tasks,
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


@dataclass(frozen=True)
class LeRobotLoadResult:
    """Published Paimon state for one imported LeRobot Dataset."""

    dataset_id: str
    version_id: str
    frames_snapshot_id: Optional[int]
    episodes_snapshot_id: Optional[int]
    tasks_snapshot_id: Optional[int]


def load_from_lerobot(
        connection,
        table_name: str,
        source,
        *,
        batch_size: int = 1024,
        dataset_id: Optional[str] = None,
        options: Optional[Mapping[str, object]] = None,
        source_options: Optional[Mapping[str, object]] = None,
) -> LeRobotLoadResult:
    """Import LeRobot Dataset v3 and return its published Paimon state.

    A new target table is created from LeRobot metadata. Episode, task, and
    dataset metadata are stored in companion Paimon tables. ``dataset_id``
    defaults to the target identifier. FileIO URI
    credentials come only from ``source_options`` and are not inherited from
    the target Catalog.
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
        _schema_from_info(local_info, include_task=False)
        total_frames, _, total_tasks = \
            _validated_counts(local_info, resolved_source.path)
        if total_frames == 0 and (
                resolved_source.root is not None
                or resolved_source.file_io is not None):
            source_schema = _schema_from_info(
                local_info,
                include_task=total_tasks > 0,
            )
            _reject_subtasks(None, resolved_source)
            metadata = _load_dataset_metadata(
                None, local_info, resolved_source)
            return _import_dataset(
                connection,
                table_name,
                None,
                local_info,
                resolved_source,
                source_schema,
                batch_size,
                dataset_id,
                options,
                metadata,
            )
        LeRobotDataset = _import_lerobot_dataset()
        dataset = _open_resolved_dataset(
            LeRobotDataset, resolved_source, local_info)
        try:
            info = dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)
            _validated_counts(info, resolved_source.path)

            lerobot_schema = _schema_from_info(
                info, include_task=_has_tasks(dataset, info))
            _reject_subtasks(dataset, resolved_source)
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
                dataset_id,
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
        dataset_id,
        options,
        metadata):
    table, owner_id = _create_target_table(
        connection, table_name, source_schema, options)
    try:
        resolved_dataset_id = _resolved_dataset_id(dataset_id, table)
        tables = _prepare_metadata_tables(
            connection, table.raw_table, owner_id)
        version_id = _new_id()
        _reserve_dataset_version(
            tables["datasets"],
            resolved_dataset_id,
            version_id,
            info,
            source,
            metadata,
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
                resolved_dataset_id,
                metadata,
            )
        episodes_snapshot_id, tasks_snapshot_id = _publish_dataset(
            connection,
            tables,
            resolved_dataset_id,
            version_id,
            info,
            source,
            metadata,
            table.identifier,
            frames_snapshot_id,
        )
        return LeRobotLoadResult(
            dataset_id=resolved_dataset_id,
            version_id=version_id,
            frames_snapshot_id=frames_snapshot_id,
            episodes_snapshot_id=episodes_snapshot_id,
            tasks_snapshot_id=tasks_snapshot_id,
        )
    except BaseException as error:
        try:
            _drop_import_tables(
                connection.catalog, table.raw_table, owner_id)
        except BaseException as cleanup_error:
            raise RuntimeError(
                "LeRobot import failed and cleanup also failed: %s"
                % cleanup_error
            ) from error
        raise


def _validated_counts(info, source):
    total_frames = _required_count(info, "total_frames", source)
    total_episodes = _required_count(info, "total_episodes", source)
    total_tasks = _required_count(info, "total_tasks", source)
    if (total_frames == 0) != (total_episodes == 0):
        raise ValueError(
            "LeRobot metadata %s has inconsistent counts: total_frames=%d "
            "and total_episodes=%d must both be zero or both be positive."
            % (source, total_frames, total_episodes))
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


def _resolved_dataset_id(value, table):
    if value is not None and (
            not isinstance(value, str) or not value.strip()):
        raise ValueError("dataset_id must be a non-empty string.")
    if value is None:
        default_id = table.raw_table.table_schema.options.get(
            _DEFAULT_DATASET_ID_OPTION)
        if not default_id:
            raise ValueError(
                "LeRobot table %s has no default dataset_id."
                % table.identifier)
        return default_id
    return value.strip()


def _create_target_table(
        connection, table_name, source_schema, options):
    owner_id = _new_id()
    create_options = dict(options or {})
    managed_options = _managed_table_options(
        connection._identifier(table_name), owner_id)
    reserved_options = set(managed_options).intersection(create_options)
    if reserved_options:
        raise ValueError(
            "%s are managed by load_from_lerobot."
            % sorted(reserved_options))
    create_options.update(managed_options)
    try:
        table = connection.create_table(
            table_name,
            schema=_frame_schema(source_schema),
            options=create_options,
        )
    except TableAlreadyExistException as error:
        raise ValueError(
            "LeRobot target %s already exists; use a new target table."
            % connection._identifier(table_name)
        ) from error
    return table, owner_id
