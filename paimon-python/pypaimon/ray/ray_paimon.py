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

from typing import Any, Dict, List, Optional

import ray.data

from pypaimon.common.predicate import Predicate


def read_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **read_args,
) -> ray.data.Dataset:
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
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in read tasks.
        concurrency: Optional max number of Ray read tasks to run concurrently.
        override_num_blocks: Optional override for the number of output blocks.
        **read_args: Additional kwargs forwarded to ``ray.data.read_datasource``.

    Returns:
        A ``ray.data.Dataset`` containing the table data.
    """
    from pypaimon.read.datasource.ray_datasource import RayDatasource
    from pypaimon.read.datasource.split_provider import CatalogSplitProvider

    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    if override_num_blocks is not None and override_num_blocks < 1:
        raise ValueError(
            "override_num_blocks must be at least 1, got {}".format(override_num_blocks)
        )

    datasource = RayDatasource(
        CatalogSplitProvider(
            table_identifier=table_identifier,
            catalog_options=catalog_options,
            predicate=filter,
            projection=projection,
            limit=limit,
            snapshot_id=snapshot_id,
            tag_name=tag_name,
        )
    )
    ds = ray.data.read_datasource(
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


def write_paimon(
    dataset: ray.data.Dataset,
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    overwrite: bool = False,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a Ray Dataset to a Paimon table.

    For HASH_FIXED tables, rows are automatically clustered by
    ``(partition_keys..., bucket)`` before writing so that each
    (partition, bucket) lands in a single Ray task. This avoids the
    small-file storm that Ray's default round-robin distribution would
    otherwise produce. No user configuration is required.

    Args:
        dataset: The Ray Dataset to write.
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        overwrite: If ``True``, overwrite existing data in the table.
        concurrency: Optional max number of Ray write tasks to run concurrently.
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in write tasks.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.ray.shuffle import maybe_apply_repartition
    from pypaimon.write.ray_datasink import PaimonDatasink

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    dataset = maybe_apply_repartition(dataset, table)

    datasink = PaimonDatasink(table, overwrite=overwrite)

    write_kwargs = {}
    if ray_remote_args is not None:
        write_kwargs["ray_remote_args"] = ray_remote_args
    if concurrency is not None:
        write_kwargs["concurrency"] = concurrency

    dataset.write_datasink(datasink, **write_kwargs)
