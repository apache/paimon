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

import pyarrow as pa

from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)
from pypaimon.multimodal.lerobot.metadata import (
    _DEFAULT_DATASET_ID_OPTION,
    _OWNER_ID_OPTION,
    _frame_schema,
    _load_dataset_metadata,
    _managed_table_options,
    _new_id,
    _prepare_metadata_tables,
    _publish_dataset,
    _reject_subtasks,
)
from pypaimon.multimodal.lerobot.loader import (
    _strict_lerobot_table,
    _write_dataset,
)
from pypaimon.multimodal.lerobot.schema import (
    _require_v3,
    _schema_from_info,
    _validate_lerobot_schema,
)
from pypaimon.multimodal.lerobot.source import (
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
from pypaimon.multimodal.table import _target_schema


def load_from_lerobot(
        connection,
        table_name: str,
        source,
        *,
        batch_size: int = 1024,
        dataset_id: Optional[str] = None,
        options: Optional[Mapping[str, object]] = None,
        source_options: Optional[Mapping[str, object]] = None):
    """Import LeRobot Dataset v3 and return the committed snapshot ID.

    A missing target table is created from LeRobot metadata. Episode, task, and
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
            table, owner_id = _validated_table(
                connection,
                table_name,
                source_schema,
                options,
                resolved_source,
            )
            resolved_dataset_id = _resolved_dataset_id(
                dataset_id, table)
            metadata = _load_dataset_metadata(
                None, local_info, resolved_source)
            tables = _prepare_metadata_tables(
                connection, table.raw_table, owner_id)
            metadata_version = _new_id()
            _publish_dataset(
                connection,
                tables,
                resolved_dataset_id,
                metadata_version,
                local_info,
                resolved_source,
                metadata,
                table.identifier,
                None,
            )
            return None
        LeRobotDataset = _import_lerobot_dataset()
        dataset = _open_resolved_dataset(
            LeRobotDataset, resolved_source, local_info)
        try:
            info = dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)
            row_count, _, _ = \
                _validated_counts(info, resolved_source.path)

            lerobot_schema = _schema_from_info(
                info, include_task=_has_tasks(dataset, info))
            _reject_subtasks(dataset, resolved_source)
            table, owner_id = _validated_table(
                connection,
                table_name,
                lerobot_schema,
                options,
                resolved_source,
            )
            resolved_dataset_id = _resolved_dataset_id(
                dataset_id, table)
            metadata = _load_dataset_metadata(
                dataset, info, resolved_source)
            tables = _prepare_metadata_tables(
                connection, table.raw_table, owner_id)
            metadata_version = _new_id()

            if row_count == 0:
                _publish_dataset(
                    connection,
                    tables,
                    resolved_dataset_id,
                    metadata_version,
                    info,
                    resolved_source,
                    metadata,
                    table.identifier,
                    None,
                )
                return None
            snapshot_id = _write_dataset(
                table,
                dataset,
                info,
                resolved_source,
                lerobot_schema,
                batch_size,
                resolved_dataset_id,
                metadata_version,
                metadata,
            )
            _publish_dataset(
                connection,
                tables,
                resolved_dataset_id,
                metadata_version,
                info,
                resolved_source,
                metadata,
                table.identifier,
                snapshot_id,
            )
            return snapshot_id
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                close()


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
                "Self-contained LeRobot table %s has no default dataset_id."
                % table.identifier)
        return default_id
    return value.strip()


def _validated_table(
        connection, table_name, source_schema, options, source):
    try:
        table = connection.get_table(table_name)
    except (DatabaseNotExistException, TableNotExistException):
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
        table = connection.create_table(
            table_name,
            schema=_frame_schema(source_schema),
            options=create_options,
        )
        _validate_target_schema(table, _frame_schema(source_schema), source)
        return table, owner_id

    owner_id = table.raw_table.table_schema.options.get(_OWNER_ID_OPTION)
    if owner_id is None:
        raise ValueError(
            "Existing LeRobot target %s is not managed by "
            "load_from_lerobot; use a new target table." % table.identifier)
    if table.raw_table.identifier.get_branch_name() is not None:
        raise ValueError(
            "Self-contained LeRobot import does not support table branches.")
    _validate_target_schema(table, _frame_schema(source_schema), source)
    return table, owner_id


def _validate_target_schema(table, source_schema, source):
    target_schema = _target_schema(table.raw_table)
    _validate_lerobot_schema(
        source_schema, target_schema, source.path)
    _strict_lerobot_table(
        pa.Table.from_batches([], schema=source_schema),
        target_schema,
        source,
        0,
    )
