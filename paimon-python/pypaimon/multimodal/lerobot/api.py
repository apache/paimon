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
from pypaimon.multimodal.lerobot.loader import (
    _strict_lerobot_table,
    _write_dataset,
)
from pypaimon.multimodal.lerobot.schema import (
    _require_v3,
    _schema_from_info,
    _validate_lerobot_schema,
    _video_feature_names,
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
from pypaimon.table.bucket_mode import BucketMode


_VIDEO_LAYOUT_ERROR = (
    "LeRobot video import requires an unpartitioned, bucket-unaware "
    "target table so each Episode is written by one writer."
)


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
        video_fields = _video_feature_names(local_info)
        total_frames, _, total_tasks = \
            _validated_counts(local_info, resolved_source.path)
        if total_frames == 0:
            source_schema = _schema_from_info(
                local_info,
                include_task=total_tasks > 0,
            )
            _validated_table(
                connection,
                table_name,
                source_schema,
                options,
                resolved_source,
                video_fields,
            )
            return None
        LeRobotDataset = _import_lerobot_dataset()
        dataset = _open_resolved_dataset(
            LeRobotDataset,
            resolved_source,
            local_info,
            download_videos=bool(video_fields),
        )
        try:
            info = dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)
            row_count, _, _ = \
                _validated_counts(info, resolved_source.path)

            source_schema = _schema_from_info(
                info, include_task=_has_tasks(dataset, info))
            video_fields = _video_feature_names(info)
            table = _validated_table(
                connection,
                table_name,
                source_schema,
                options,
                resolved_source,
                video_fields,
            )

            if row_count == 0:
                return None
            return _write_dataset(
                table,
                dataset,
                info,
                resolved_source,
                source_schema,
                batch_size,
                video_fields,
            )
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


def _validated_table(
        connection, table_name, source_schema, options, source,
        video_fields=()):
    table = _get_or_create_table(
        connection, table_name, source_schema, options, video_fields)
    target_schema = _target_schema(table.raw_table)
    _validate_lerobot_schema(
        source_schema, target_schema, source.path)
    _strict_lerobot_table(
        pa.Table.from_batches([], schema=source_schema),
        target_schema,
        source,
        0,
    )
    configured = table.raw_table.options.video_frame_fields()
    if configured != set(video_fields):
        raise ValueError(
            "LeRobot video features %s require table option "
            "'video-frame-field'=%r; found %s."
            % (list(video_fields), ",".join(video_fields), sorted(configured))
        )
    if video_fields and (
            table.raw_table.partition_keys
            or table.raw_table.bucket_mode() != BucketMode.BUCKET_UNAWARE):
        raise ValueError(_VIDEO_LAYOUT_ERROR)
    return table


def _get_or_create_table(
        connection, table_name, schema, options, video_fields=()):
    try:
        return connection.get_table(table_name)
    except (DatabaseNotExistException, TableNotExistException):
        options = dict(options or {})
        if video_fields and str(options.get("bucket", "-1")).strip() != "-1":
            raise ValueError(_VIDEO_LAYOUT_ERROR)
        configured = options.get("video-frame-field")
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
            options["video-frame-field"] = ",".join(video_fields)
        return connection.create_table(
            table_name,
            schema=schema,
            options=options,
        )
