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

import sys
from typing import Mapping, Optional

import pyarrow as pa

from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
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
        options: Optional[Mapping[str, object]] = None,
        source_options: Optional[Mapping[str, object]] = None):
    """Import LeRobot Dataset v3 and return the committed snapshot ID.

    A missing target table is created from LeRobot metadata. An existing table
    receives the same strict schema validation and append semantics as
    :meth:`MultimodalConnection.load_from_hdf5`. FileIO URI credentials come
    only from ``source_options`` and are not inherited from the target Catalog.
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
        if int(local_info.get("total_frames", 0)) == 0:
            source_schema = _schema_from_info(
                local_info,
                include_task=int(local_info.get("total_tasks", 0)) > 0,
            )
            _validated_table(
                connection,
                table_name,
                source_schema,
                options,
                resolved_source,
            )
            return None
        LeRobotDataset = _import_lerobot_dataset()
        dataset = _open_resolved_dataset(
            LeRobotDataset, resolved_source, local_info)
        try:
            info = dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)

            source_schema = _schema_from_info(
                info, include_task=_has_tasks(dataset, info))
            table = _validated_table(
                connection,
                table_name,
                source_schema,
                options,
                resolved_source,
            )

            row_count = int(info.get("total_frames", len(dataset)))
            if row_count == 0:
                return None
            return _write_dataset(
                table,
                dataset,
                info,
                resolved_source,
                source_schema,
                batch_size,
            )
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                close()


def _validated_table(
        connection, table_name, source_schema, options, source):
    table = _get_or_create_table(
        connection, table_name, source_schema, options)
    target_schema = _target_schema(table.raw_table)
    _validate_lerobot_schema(
        source_schema, target_schema, source.path)
    _strict_lerobot_table(
        pa.Table.from_batches([], schema=source_schema),
        target_schema,
        source,
        0,
    )
    return table


def _get_or_create_table(connection, table_name, schema, options):
    try:
        return connection.get_table(table_name)
    except (DatabaseNotExistException, TableNotExistException):
        return connection.create_table(
            table_name,
            schema=schema,
            options=options,
        )
