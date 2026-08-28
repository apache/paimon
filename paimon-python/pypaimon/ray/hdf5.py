# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Distributed HDF5 ingestion using the multimodal transform contract."""

from typing import Any, Dict, Mapping, Optional


def load_from_hdf5(
        table_identifier: str,
        paths,
        catalog_options: Dict[str, str],
        *,
        transform,
        source_options: Optional[Mapping[str, object]] = None,
        concurrency: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None) -> None:
    """Transform complete HDF5 files on Ray and append them in one commit.

    The transform has the same ``(h5py.File, Hdf5File)`` contract as
    :meth:`MultimodalConnection.load_from_hdf5`. Discovery runs on the driver;
    workers open and transform complete files, and the Paimon Ray sink commits
    all worker messages once.
    """
    if not callable(transform):
        raise ValueError("transform must be callable.")

    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.options import Options
    from pypaimon.multimodal.hdf5 import (
        _Hdf5SourceFileIO,
        _discover_hdf5_files,
        _path_values,
        _validate_source_kerberos,
        _validated_source_options,
    )
    from pypaimon.multimodal.table import _target_schema
    from pypaimon.ray.ray_paimon import _require_ray_data, write_paimon

    ray_data = _require_ray_data()
    validated_options = _validated_source_options(source_options)
    path_values = _path_values(paths)
    _validate_source_kerberos(path_values, validated_options)
    source_file_io = _Hdf5SourceFileIO(Options(validated_options))
    try:
        files = _discover_hdf5_files(path_values, source_file_io)
    finally:
        source_file_io.close()
    if not files:
        return

    table = CatalogFactory.create(catalog_options).get_table(table_identifier)
    target_schema = _target_schema(table)
    inputs = ray_data.from_items(
        [{"path": source.path} for source in files],
        override_num_blocks=len(files),
    )
    transformed = inputs.map_batches(
        _TransformHdf5File,
        fn_constructor_kwargs={
            "transform": transform,
            "source_options": validated_options,
            "target_schema": target_schema,
        },
        batch_format="pyarrow",
        batch_size=1,
        concurrency=concurrency,
        **dict(ray_remote_args or {}),
    )
    write_paimon(
        transformed,
        table_identifier,
        catalog_options,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


class _TransformHdf5File:

    def __init__(self, *, transform, source_options, target_schema):
        self.transform = transform
        self.source_options = source_options
        self.target_schema = target_schema

    def __call__(self, batch):
        if batch.num_rows != 1:
            raise ValueError("Ray HDF5 transform requires one source per batch.")

        from pypaimon.common.options import Options
        from pypaimon.multimodal.hdf5 import (
            Hdf5File,
            _Hdf5SourceFileIO,
            _transform_hdf5_file,
        )

        try:
            import h5py
        except ImportError as error:
            raise ImportError(
                "load_from_hdf5 requires h5py; install 'pypaimon[ray,hdf5]'."
            ) from error

        source = Hdf5File(path=batch["path"][0].as_py())
        source_file_io = _Hdf5SourceFileIO(Options(self.source_options))
        try:
            for table in _transform_hdf5_file(
                    source,
                    self.transform,
                    source_file_io,
                    h5py,
                    self.target_schema):
                if table.num_rows:
                    yield table
        finally:
            source_file_io.close()
