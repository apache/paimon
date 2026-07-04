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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Top-level API for reading and writing Paimon tables with Ray Datasets.

Usage::

    from pypaimon.ray import read_paimon, write_paimon

    ds = read_paimon("db.table", catalog_options={"warehouse": "/path"})
    write_paimon(ds, "db.table", catalog_options={"warehouse": "/path"})
"""

import importlib
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from pypaimon.common.predicate import Predicate

if TYPE_CHECKING:
    import ray.data


def _require_ray_data():
    try:
        return importlib.import_module("ray.data")
    except ModuleNotFoundError as e:
        if e.name not in ("ray", "ray.data"):
            raise
        raise ImportError(
            "PyPaimon Ray APIs require the 'ray' package. "
            "Install it with: pip install pypaimon[ray]"
        ) from e


def read_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    dynamic_options: Optional[Dict[str, str]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **read_args,
) -> "ray.data.Dataset":
    """Read a Paimon table into a Ray Dataset.

    Args:
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``,
            e.g. ``{"warehouse": "/path/to/warehouse"}``.
        filter: Optional predicate to push down into the scan.
        projection: Optional list of column names to read.
        limit: Optional row limit for the scan.
        snapshot_id: Optional snapshot id to time-travel to. Mutually
            exclusive with ``tag_name``.
        tag_name: Optional tag name to time-travel to. Mutually
            exclusive with ``snapshot_id``.
        dynamic_options: Optional dynamic options to override at read time.
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in read tasks.
        concurrency: Optional max number of Ray read tasks to run concurrently.
        override_num_blocks: Optional override for the number of output blocks.
        **read_args: Additional kwargs forwarded to ``ray.data.read_datasource``.

    Returns:
        A ``ray.data.Dataset`` containing the table data.
    """
    ray_data = _require_ray_data()

    from pypaimon.read.datasource.ray_datasource import RayDatasource
    from pypaimon.read.datasource.split_provider import CatalogSplitProvider
    from pypaimon.schema.data_types import PyarrowFieldParser

    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    if override_num_blocks is not None and override_num_blocks < 1:
        raise ValueError(
            "override_num_blocks must be at least 1, got {}".format(override_num_blocks)
        )

    split_provider = CatalogSplitProvider(
        table_identifier=table_identifier,
        catalog_options=catalog_options,
        predicate=filter,
        projection=projection,
        limit=limit,
        snapshot_id=snapshot_id,
        tag_name=tag_name,
        dynamic_options=dynamic_options,
    )

    if not split_provider.splits():
        schema = PyarrowFieldParser.from_paimon_schema(
            split_provider.read_type()
        )
        import pyarrow
        empty_table = pyarrow.Table.from_arrays(
            [pyarrow.array([], type=field.type) for field in schema],
            schema=schema,
        )
        return ray_data.from_arrow(empty_table)

    datasource = RayDatasource(split_provider)
    ds = ray_data.read_datasource(
        datasource,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        **read_args,
    )
    # Per-task limit short-circuits each worker's reader, but N workers
    # could collectively overshoot the user-visible limit. Cap on top.
    if limit is not None:
        ds = ds.limit(limit)
    return ds


def map_blobs(
    dataset: "ray.data.Dataset",
    columns,
    fn: Callable,
    *,
    file_io=None,
    parallelism: int = 64,
    batch_size: Optional[int] = 1024,
    fn_kwargs: Optional[Dict[str, Any]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    **map_args,
) -> "ray.data.Dataset":
    """Fetch BLOB payloads in Ray batches and call ``fn``.

    ``fn(scalar_batch, blobs, **fn_kwargs)`` receives a ``pyarrow.Table`` of
    non-BLOB columns and a row-aligned ``dict`` of BLOB bytes. Return a small
    Ray-compatible batch; for side-effect-only work, return an empty
    ``pyarrow.Table`` instead of ``None``. Tune ``batch_size`` for BLOB size and
    worker memory.
    """
    _require_ray_data()

    if not callable(fn):
        raise ValueError("fn must be callable")
    if isinstance(columns, str):
        blob_cols = [columns]
    else:
        blob_cols = list(dict.fromkeys(columns))
    if not blob_cols:
        raise ValueError("columns must contain at least one BLOB column")
    if parallelism < 1:
        raise ValueError("parallelism must be at least 1, got {}".format(parallelism))
    if batch_size is not None and batch_size < 1:
        raise ValueError("batch_size must be at least 1, got {}".format(batch_size))

    resolved_file_io = file_io
    if resolved_file_io is None:
        resolved_file_io = getattr(dataset, "_paimon_blob_file_io", None)
    if resolved_file_io is None:
        raise ValueError(
            "map_blobs requires a FileIO. Use table.scan().to_ray() or "
            "pass file_io= explicitly.")

    batch_format = map_args.pop("batch_format", "pyarrow")
    if batch_format != "pyarrow":
        raise ValueError("map_blobs requires batch_format='pyarrow'")

    kwargs = dict(map_args)
    kwargs["batch_format"] = "pyarrow"
    if batch_size is not None:
        kwargs.setdefault("batch_size", batch_size)
    if ray_remote_args is not None:
        _set_map_batches_remote_args(dataset, kwargs, ray_remote_args)

    all_blob_cols = getattr(dataset, "_paimon_blob_columns", None)
    if all_blob_cols is None:
        all_blob_cols = blob_cols

    return dataset.map_batches(
        _map_blob_batch,
        fn_kwargs={
            "file_io": resolved_file_io,
            "blob_cols": blob_cols,
            "all_blob_cols": list(all_blob_cols),
            "parallelism": parallelism,
            "fn": fn,
            "fn_kwargs": dict(fn_kwargs or {}),
        },
        **kwargs)


def _set_map_batches_remote_args(dataset, kwargs, ray_remote_args):
    import inspect

    param = inspect.signature(dataset.map_batches).parameters.get("ray_remote_args")
    if param is not None and param.kind != inspect.Parameter.VAR_KEYWORD:
        kwargs["ray_remote_args"] = ray_remote_args
    else:
        kwargs.update(ray_remote_args)


def _map_blob_batch(
        batch, file_io, blob_cols, all_blob_cols, parallelism, fn, fn_kwargs):
    from pypaimon.multimodal.blob_read import fetch_blob_bodies

    missing = [name for name in blob_cols if name not in batch.schema.names]
    if missing:
        raise ValueError("BLOB column(s) not found in Ray Dataset: {}".format(
            ", ".join(missing)))

    bodies = fetch_blob_bodies(
        file_io, batch.select(blob_cols).to_pydict(), blob_cols, parallelism)
    all_blob = set(all_blob_cols)
    scalar_cols = [name for name in batch.schema.names if name not in all_blob]
    result = fn(batch.select(scalar_cols), bodies, **fn_kwargs)
    if result is None:
        raise ValueError(
            "map_blobs UDF must return a Ray-compatible batch, such as a "
            "pyarrow.Table. For side-effect-only processing, return an empty "
            "pyarrow.Table instead of None.")
    return result


def write_paimon(
    dataset: "ray.data.Dataset",
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    overwrite: bool = False,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    hash_fixed_precluster: str = "auto",
) -> None:
    """Write a Ray Dataset to a Paimon table.

    HASH_FIXED rows are assigned to the correct bucket by the Paimon
    writer. Optional pre-clustering is only a file-count optimization.
    The legacy ``map_groups`` pre-clustering mode materializes each
    ``(partition_keys..., bucket)`` group on one Ray node and should
    only be used when every group fits in memory. HASH_DYNAMIC and
    CROSS_PARTITION primary-key Ray writes are rejected because Ray
    write tasks create independent Paimon writers.

    Args:
        dataset: The Ray Dataset to write.
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        overwrite: If ``True``, overwrite existing data in the table.
        concurrency: Optional max number of Ray write tasks to run concurrently.
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in write tasks.
        hash_fixed_precluster: HASH_FIXED pre-clustering mode. ``"auto"``
            and ``"off"`` write append-only HASH_FIXED tables directly
            and reject HASH_FIXED primary-key tables. ``"map_groups"``
            preserves the legacy small-file optimization and its single
            group memory bound for HASH_FIXED primary-key tables.
    """
    _require_ray_data()

    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.ray.shuffle import maybe_apply_repartition
    from pypaimon.write.ray_datasink import PaimonDatasink

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    dataset = maybe_apply_repartition(dataset, table, hash_fixed_precluster)

    datasink = PaimonDatasink(table, overwrite=overwrite)

    write_kwargs = {}
    if ray_remote_args is not None:
        write_kwargs["ray_remote_args"] = ray_remote_args
    if concurrency is not None:
        write_kwargs["concurrency"] = concurrency

    dataset.write_datasink(datasink, **write_kwargs)
