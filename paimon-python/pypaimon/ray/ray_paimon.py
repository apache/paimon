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

import hashlib
import importlib
import uuid
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


def map_with_blobs(
    dataset: "ray.data.Dataset",
    columns,
    fn: Callable,
    *,
    file_io=None,
    all_blob_columns=None,
    parallelism: int = 64,
    batch_size: Optional[int] = 1024,
    blob_uri_affinity: bool = False,
    prefetch_bytes: int = 64 * 1024 * 1024,
    fn_kwargs: Optional[Dict[str, Any]] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    **map_args,
) -> "ray.data.Dataset":
    """Fetch BLOB payloads in Ray batches and call ``fn``.

    ``fn(scalar_batch, blobs, **fn_kwargs)`` receives a ``pyarrow.Table`` of
    non-BLOB columns and a row-aligned ``dict`` of BLOB bytes. Return a small
    Ray-compatible batch; for side-effect-only work, return an empty
    ``pyarrow.Table`` instead of ``None``. Call this directly on
    ``scan().to_ray()`` output, or pass ``file_io`` and ``all_blob_columns``.
    Tune ``batch_size`` for BLOB size and worker memory. Set
    ``blob_uri_affinity=True`` to shuffle descriptors by URI and offset before
    reading. This lets each worker coalesce adjacent ranges across multiple
    ``fn`` batches, bounded by ``prefetch_bytes``.
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
    if not isinstance(blob_uri_affinity, bool):
        raise ValueError("blob_uri_affinity must be a boolean")
    if blob_uri_affinity and batch_size is None:
        raise ValueError("blob_uri_affinity requires batch_size")
    if (isinstance(prefetch_bytes, bool)
            or not isinstance(prefetch_bytes, int)
            or prefetch_bytes < 1):
        raise ValueError("prefetch_bytes must be a positive integer")

    resolved_file_io = file_io
    if resolved_file_io is None:
        resolved_file_io = getattr(dataset, "_paimon_blob_file_io", None)
    if resolved_file_io is None:
        raise ValueError(
            "map_with_blobs requires a FileIO. Use table.scan().to_ray() or "
            "pass file_io= explicitly.")

    batch_format = map_args.pop("batch_format", "pyarrow")
    if batch_format != "pyarrow":
        raise ValueError("map_with_blobs requires batch_format='pyarrow'")

    kwargs = dict(map_args)
    kwargs["batch_format"] = "pyarrow"
    if batch_size is not None and not blob_uri_affinity:
        kwargs.setdefault("batch_size", batch_size)
    if ray_remote_args is not None:
        _set_map_batches_remote_args(dataset, kwargs, ray_remote_args)

    all_blob_cols = all_blob_columns
    if all_blob_cols is None:
        all_blob_cols = getattr(dataset, "_paimon_blob_columns", None)
    if all_blob_cols is None:
        raise ValueError(
            "map_with_blobs requires all_blob_columns when Dataset lacks "
            "BLOB metadata.")

    all_blob = set(all_blob_cols)
    invalid = [name for name in blob_cols if name not in all_blob]
    if invalid:
        raise ValueError("Column {!r} is not a BLOB column.".format(invalid[0]))

    mapper = _map_blob_batch
    affinity_cols = []
    if blob_uri_affinity:
        dataset, affinity_cols = _cluster_by_blob_uri(dataset, blob_cols)
        mapper = _map_blob_affinity_block
        kwargs["batch_size"] = None

    mapper_kwargs = {
        "file_io": resolved_file_io,
        "blob_cols": blob_cols,
        "all_blob_cols": list(all_blob_cols),
        "parallelism": parallelism,
        "fn": fn,
        "fn_kwargs": dict(fn_kwargs or {}),
    }
    if blob_uri_affinity:
        mapper_kwargs.update({
            "fn_batch_size": batch_size,
            "prefetch_bytes": prefetch_bytes,
            "affinity_cols": affinity_cols,
        })
    return dataset.map_batches(
        mapper, fn_kwargs=mapper_kwargs, **kwargs)


def _cluster_by_blob_uri(dataset, blob_cols):
    token = uuid.uuid4().hex
    key_col = "__paimon_blob_key_{}".format(token)
    offset_col = "__paimon_blob_offset_{}".format(token)
    with_keys = dataset.map_batches(
        _append_blob_affinity_keys,
        fn_kwargs={
            "blob_cols": blob_cols,
            "key_col": key_col,
            "offset_col": offset_col,
        },
        batch_format="pyarrow",
        zero_copy_batch=True,
    )
    return with_keys.sort([key_col, offset_col]), [key_col, offset_col]


def _append_blob_affinity_keys(batch, blob_cols, key_col, offset_col):
    import pyarrow as pa
    from pypaimon.table.row.blob import BlobDescriptor

    empty_key = b"\0" * 16
    uri_keys = {}
    keys = []
    offsets = []
    columns = [batch.column(name) for name in blob_cols]
    for row in range(batch.num_rows):
        descriptor = None
        for column in columns:
            value = column[row]
            raw = value.as_py() if value.is_valid else None
            if raw is not None and BlobDescriptor.is_blob_descriptor(raw):
                descriptor = BlobDescriptor.deserialize(raw)
                break
        if descriptor is not None:
            key = uri_keys.get(descriptor.uri)
            if key is None:
                key = hashlib.blake2b(
                    descriptor.uri.encode("utf-8"), digest_size=16).digest()
                uri_keys[descriptor.uri] = key
            keys.append(key)
            offsets.append(descriptor.offset)
        else:
            keys.append(empty_key)
            offsets.append(-1)
    return batch.append_column(
        key_col, pa.array(keys, type=pa.binary(16))
    ).append_column(
        offset_col, pa.array(offsets, type=pa.int64())
    )


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

    scalar_cols = _blob_scalar_columns(batch, blob_cols, all_blob_cols)
    bodies = fetch_blob_bodies(
        file_io, batch.select(blob_cols).to_pydict(), blob_cols, parallelism)
    return _call_blob_fn(fn, batch.select(scalar_cols), bodies, fn_kwargs)


def _map_blob_affinity_block(
        batch, file_io, blob_cols, all_blob_cols, parallelism, fn, fn_kwargs,
        fn_batch_size, prefetch_bytes, affinity_cols):
    from pypaimon.multimodal.blob_read import fetch_blob_bodies

    if batch.num_rows == 0:
        return

    scalar_cols = _blob_scalar_columns(
        batch, blob_cols, all_blob_cols, affinity_cols)

    for start, end in _blob_prefetch_windows(
            batch, blob_cols, fn_batch_size, prefetch_bytes):
        window = batch.slice(start, end - start)
        bodies = fetch_blob_bodies(
            file_io,
            window.select(blob_cols).to_pydict(),
            blob_cols,
            parallelism,
        )
        scalar = window.select(scalar_cols)
        for batch_start in range(0, window.num_rows, fn_batch_size):
            size = min(fn_batch_size, window.num_rows - batch_start)
            fn_bodies = {
                name: values[batch_start:batch_start + size]
                for name, values in bodies.items()
            }
            yield _call_blob_fn(
                fn, scalar.slice(batch_start, size), fn_bodies, fn_kwargs)


def _blob_scalar_columns(batch, blob_cols, all_blob_cols, internal_cols=()):
    missing = [name for name in blob_cols if name not in batch.schema.names]
    if missing:
        raise ValueError("BLOB column(s) not found in Ray Dataset: {}".format(
            ", ".join(missing)))

    all_blob = set(all_blob_cols)
    excluded = all_blob | set(internal_cols)
    scalar_cols = [name for name in batch.schema.names if name not in excluded]
    unknown = _unknown_blob_descriptor_columns(batch, scalar_cols)
    if unknown:
        raise ValueError(
            "Column {!r} holds BLOB descriptors this table does not own "
            "(likely from a joined BLOB table). Fetch it with its own "
            "table.map_with_blobs() in a separate pass, or drop it before "
            "mapping.".format(unknown[0]))
    return scalar_cols


def _call_blob_fn(fn, scalar, bodies, fn_kwargs):
    result = fn(scalar, bodies, **fn_kwargs)
    if result is None:
        raise ValueError(
            "map_with_blobs UDF must return a Ray-compatible batch, such as a "
            "pyarrow.Table. For side-effect-only processing, return an empty "
            "pyarrow.Table instead of None.")
    return result


def _blob_prefetch_windows(batch, blob_cols, fn_batch_size, max_bytes):
    start = 0
    end = 0
    size = 0
    while end < batch.num_rows:
        next_end = min(end + fn_batch_size, batch.num_rows)
        next_size = _blob_payload_size(
            batch.slice(end, next_end - end), blob_cols, max_bytes)
        if end > start and size + next_size > max_bytes:
            yield start, end
            start = end
            size = 0
        size += next_size
        end = next_end
    if end > start:
        yield start, end


def _blob_payload_size(batch, blob_cols, unknown_size):
    from pypaimon.table.row.blob import BlobDescriptor

    total = 0
    for name in blob_cols:
        for value in batch.column(name):
            if not value.is_valid:
                continue
            raw = value.as_py()
            if BlobDescriptor.is_blob_descriptor(raw):
                length = BlobDescriptor.deserialize(raw).length
                total += length if length >= 0 else unknown_size
            else:
                total += len(raw)
    return total


def _unknown_blob_descriptor_columns(batch, scalar_cols):
    return [
        name for name in scalar_cols
        if _looks_like_blob_descriptor(batch.column(name))]


def _looks_like_blob_descriptor(column):
    import pyarrow as pa
    from pypaimon.table.row.blob import BlobDescriptor

    if not (pa.types.is_binary(column.type) or pa.types.is_large_binary(column.type)):
        return False
    chunks = getattr(column, "chunks", None) or [column]
    for chunk in chunks:
        for value in chunk:
            if value.is_valid:
                return BlobDescriptor.is_blob_descriptor(value.as_py())
    return False


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
    writer. For primary-key tables, ``map_groups`` writes each complete
    ``(partition_keys..., bucket)`` group in one Ray task. Postpone-bucket
    writes follow ``postpone.batch-write-fixed-bucket`` by default. Their
    bucket plan is resolved once on the driver and workers write sorted
    blocks to real buckets.
    HASH_DYNAMIC and CROSS_PARTITION primary-key Ray writes are rejected
    because Ray write tasks create independent Paimon writers.

    Args:
        dataset: The Ray Dataset to write.
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        overwrite: If ``True``, overwrite existing data in the table.
        concurrency: Optional max number of Ray write tasks to run concurrently.
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in write tasks.
        hash_fixed_precluster: Pre-clustering mode. ``"auto"`` follows
            table options, ``"off"`` disables it, and ``"map_groups"``
            explicitly enables HASH_FIXED grouping.
    """
    _require_ray_data()

    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.write.ray_datasink import write_paimon_dataset

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    write_paimon_dataset(
        dataset,
        table,
        overwrite=overwrite,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
        hash_fixed_precluster=hash_fixed_precluster,
    )
