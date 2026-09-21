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

"""Distributed ROSBag validation and ingestion with a materialize barrier."""

import pickle
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from typing import Any, Dict, Mapping, Optional


def load_from_rosbag(
        table_identifier: str,
        paths,
        catalog_options: Dict[str, str],
        *,
        transform,
        default_typestore=None,
        typestore_factory=None,
        source_options: Optional[Mapping[str, object]] = None,
        staging=None,
        allow_storage_fragment: bool = False,
        concurrency: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None):
    """Validate ROSBag sources on Ray before starting the Paimon sink.

    ``transform(reader, source)`` returns Arrow tables or record batches.
    Ray may retry it, so it must be deterministic and free of non-idempotent
    external side effects. Validated output is materialized before the
    append-only Paimon write starts; a commit exception may have an unknown
    result.
    """
    if sys.version_info < (3, 10):
        raise RuntimeError(
            "Ray ROSBag loading requires Python 3.10 or newer; the rosbag "
            "extra is not available on older Python versions.")
    if not callable(transform):
        raise ValueError("transform must be callable.")
    if default_typestore is not None and typestore_factory is not None:
        raise ValueError(
            "default_typestore and typestore_factory are mutually exclusive.")
    if typestore_factory is not None and not callable(typestore_factory):
        raise ValueError("typestore_factory must be callable.")

    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.options import Options
    from pypaimon.multimodal.rosbag.api import (
        RosbagLoadResult,
        RosbagStagingConfig,
        _path_values,
        _validate_staging_config,
    )
    from pypaimon.multimodal.rosbag.source import _discover_rosbag_sources
    from pypaimon.multimodal.source_utils import (
        _SourceFileIO,
        _validated_source_options,
        _validate_source_kerberos,
    )
    from pypaimon.multimodal.table import _target_schema
    from pypaimon.ray.ray_paimon import _require_ray_data, write_paimon

    if staging is None:
        staging = RosbagStagingConfig()
    if not isinstance(staging, RosbagStagingConfig):
        raise ValueError("staging must be a RosbagStagingConfig.")
    _validate_staging_config(staging)
    options = _validated_source_options(source_options)
    path_values = _path_values(paths)
    _validate_source_kerberos(path_values, options, source_name="ROSBag")
    source_file_io = _SourceFileIO(Options(options))
    try:
        manifests = _discover_rosbag_sources(
            path_values,
            source_file_io,
            allow_storage_fragment=allow_storage_fragment,
        )
    finally:
        source_file_io.close()
    if not manifests:
        return RosbagLoadResult(0, None, 0, None)

    ray_data = _require_ray_data()
    _require_ray_serializable("transform", transform)
    if default_typestore is not None:
        _require_ray_serializable("default_typestore", default_typestore)
    if typestore_factory is not None:
        _require_ray_serializable("typestore_factory", typestore_factory)
    table = CatalogFactory.create(catalog_options).get_table(table_identifier)
    target_schema = _target_schema(table)
    inputs = ray_data.from_items(
        [{"manifest": pickle.dumps(manifest)} for manifest in manifests],
        override_num_blocks=len(manifests),
    )
    transformed = inputs.map_batches(
        _TransformRosbagSource,
        fn_constructor_kwargs={
            "transform": transform,
            "default_typestore": default_typestore,
            "typestore_factory": typestore_factory,
            "source_options": options,
            "staging": staging,
            "target_schema": target_schema,
        },
        batch_format="pyarrow",
        batch_size=1,
        concurrency=concurrency,
        **dict(ray_remote_args or {}),
    )
    validated = transformed.materialize()
    from pypaimon.multimodal.rosbag.staging import _verify_manifest_members
    source_file_io = _SourceFileIO(Options(options))
    try:
        for manifest in manifests:
            _verify_manifest_members(manifest, source_file_io)
    finally:
        source_file_io.close()
    write_result = write_paimon(
        validated,
        table_identifier,
        catalog_options,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )
    return RosbagLoadResult(
        source_count=len(manifests),
        batch_count=None,
        row_count=0 if write_result is None else write_result.row_count,
        snapshot_id=(
            None if write_result is None else write_result.snapshot_id),
    )


def _require_ray_serializable(name, value):
    import ray.cloudpickle as cloudpickle

    try:
        cloudpickle.dumps(value)
    except Exception as error:
        raise ValueError(
            "%s must be Ray-serializable: %s" % (name, error)) from error


class _TransformRosbagSource:
    """Ray callable that will validate one complete source per input batch."""

    def __init__(
            self,
            *,
            transform,
            default_typestore,
            typestore_factory,
            source_options,
            staging,
            target_schema):
        self.transform = transform
        self.default_typestore = default_typestore
        self.typestore_factory = typestore_factory
        self.source_options = source_options
        self.staging = staging
        self.target_schema = target_schema

    def __call__(self, batch):
        if batch.num_rows != 1:
            raise ValueError(
                "Ray ROSBag transform requires one source per batch.")

        from pypaimon.common.options import Options
        from pypaimon.multimodal.rosbag.loader import (
            _transform_rosbag_manifest,
        )
        from pypaimon.multimodal.source_utils import _SourceFileIO
        try:
            from rosbags.highlevel import AnyReader
        except ImportError as error:
            raise ImportError(
                "Ray ROSBag loading requires rosbags on every worker; "
                "install 'pypaimon[ray,rosbag]'.") from error

        manifest = pickle.loads(batch["manifest"][0].as_py())
        source_file_io = _SourceFileIO(Options(self.source_options))
        try:
            with TemporaryDirectory(
                    prefix="pypaimon_ray_rosbag_",
                    dir=self.staging.directory) as temp_dir:
                for table in _transform_rosbag_manifest(
                        manifest,
                        self.transform,
                        source_file_io,
                        AnyReader,
                        self.target_schema,
                        default_typestore=self.default_typestore,
                        typestore_factory=self.typestore_factory,
                        staging=self.staging,
                        staging_root=Path(temp_dir) / "source"):
                    if table.num_rows:
                        yield table
        finally:
            source_file_io.close()
