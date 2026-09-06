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

"""Public ROSBag ingestion API."""

import os
import sys
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

from pypaimon.common.options import Options
from pypaimon.multimodal.rosbag.loader import _load_rosbag_manifests
from pypaimon.multimodal.rosbag.source import _discover_rosbag_sources
from pypaimon.multimodal.source_utils import (
    _SourceFileIO,
    _validated_source_options,
    _validate_source_kerberos,
)


@dataclass(frozen=True)
class RosbagStagingConfig:
    """Local temporary-space controls for ROSBag ingestion.

    In serial mode, ``max_bytes`` covers remote source copies and validated
    Arrow IPC output. Ray applies it to each worker's remote source copy;
    Ray object-store capacity is configured separately.
    """

    directory: Optional[str] = None
    max_bytes: Optional[int] = None
    min_free_bytes: int = 1 << 30
    copy_buffer_bytes: int = 8 << 20


@dataclass(frozen=True)
class RosbagLoadResult:
    """Counts and committed snapshot for one ROSBag load.

    ``batch_count`` is the number of transform outputs in serial mode and
    ``None`` in Ray mode, where Ray may reorganize output blocks.
    """

    source_count: int
    batch_count: Optional[int]
    row_count: int
    snapshot_id: Optional[int]


def load_from_rosbag(
        table,
        paths,
        *,
        transform: Callable,
        default_typestore=None,
        typestore_factory=None,
        source_options: Optional[Mapping[str, object]] = None,
        staging: Optional[RosbagStagingConfig] = None,
        allow_storage_fragment: bool = False):
    """Validate and append ROS1/ROS2 transforms in one commit.

    ``transform(reader, source)`` returns Arrow tables or record batches.
    Every source and transform result is validated before a Paimon writer is
    created. The operation is append-only and a commit exception can have an
    unknown result, so callers must inspect table state before retrying.
    """
    if sys.version_info < (3, 10):
        raise RuntimeError(
            "load_from_rosbag requires Python 3.10 or newer; the rosbag "
            "extra is not available on older Python versions.")
    if not callable(transform):
        raise ValueError("transform must be callable.")
    if default_typestore is not None and typestore_factory is not None:
        raise ValueError(
            "default_typestore and typestore_factory are mutually exclusive.")
    if typestore_factory is not None and not callable(typestore_factory):
        raise ValueError("typestore_factory must be callable.")
    if staging is None:
        staging = RosbagStagingConfig()
    if not isinstance(staging, RosbagStagingConfig):
        raise ValueError("staging must be a RosbagStagingConfig.")
    _validate_staging_config(staging)

    path_values = _path_values(paths)
    options = _validated_source_options(source_options)
    _validate_source_kerberos(path_values, options, source_name="ROSBag")
    source_file_io = _SourceFileIO(Options(options))
    try:
        manifests = _discover_rosbag_sources(
            path_values,
            source_file_io,
            allow_storage_fragment=allow_storage_fragment,
        )
        if not manifests:
            return RosbagLoadResult(0, 0, 0, None)
        try:
            from rosbags.highlevel import AnyReader
        except ImportError as error:
            raise ImportError(
                "load_from_rosbag requires rosbags; install "
                "'pypaimon[rosbag]'.") from error
        return _load_rosbag_manifests(
            table,
            manifests,
            transform,
            source_file_io,
            AnyReader,
            default_typestore=default_typestore,
            typestore_factory=typestore_factory,
            staging=staging,
        )
    finally:
        source_file_io.close()


def _path_values(paths):
    if isinstance(paths, (str, os.PathLike)):
        return [paths]
    if isinstance(paths, bytes):
        raise ValueError("paths must be a path or an iterable of paths.")
    try:
        return list(paths)
    except TypeError as error:
        raise ValueError(
            "paths must be a path or an iterable of paths.") from error


def _validate_staging_config(staging):
    if (
            staging.max_bytes is not None
            and (
                isinstance(staging.max_bytes, bool)
                or not isinstance(staging.max_bytes, int)
                or staging.max_bytes < 0)):
        raise ValueError("staging.max_bytes must be a non-negative integer.")
    if (
            isinstance(staging.min_free_bytes, bool)
            or not isinstance(staging.min_free_bytes, int)
            or staging.min_free_bytes < 0):
        raise ValueError(
            "staging.min_free_bytes must be a non-negative integer.")
    if (
            isinstance(staging.copy_buffer_bytes, bool)
            or not isinstance(staging.copy_buffer_bytes, int)
            or staging.copy_buffer_bytes <= 0):
        raise ValueError(
            "staging.copy_buffer_bytes must be a positive integer.")
