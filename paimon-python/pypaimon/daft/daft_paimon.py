################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""
Top-level API for reading and writing Paimon tables with Daft DataFrames.

Usage::

    from pypaimon.daft import explain_paimon_scan, read_paimon, write_paimon

    df = read_paimon("db.table", catalog_options={"warehouse": "/path"})
    explain = explain_paimon_scan("db.table", catalog_options={"warehouse": "/path"})
    write_paimon(df, "db.table", catalog_options={"warehouse": "/path"})
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import urlparse

if TYPE_CHECKING:
    import daft

    from pypaimon.daft.daft_explain import PaimonScanExplain
    from pypaimon.table.file_store_table import FileStoreTable


def _enrich_options_with_rest_token(
    catalog_options: Dict[str, str], table: "FileStoreTable"
) -> Dict[str, str]:
    # REST catalogs (DLF) keep OSS STS tokens on table.file_io and the bucket on table.table_path,
    # not in catalog_options; fold both in so Daft's IOConfig routes to OSS with valid credentials.
    if catalog_options.get("metastore") != "rest":
        return catalog_options
    file_io = getattr(table, "file_io", None)
    if file_io is None or not hasattr(file_io, "try_to_refresh_token"):
        return catalog_options
    file_io.try_to_refresh_token()
    if file_io.token is None:
        return catalog_options
    enriched = {**catalog_options, **file_io.token.token}
    parsed = urlparse(getattr(table, "table_path", "") or "")
    if parsed.scheme and parsed.netloc:
        enriched["warehouse"] = f"{parsed.scheme}://{parsed.netloc}"
    return enriched


def _time_travel_table(
    table: FileStoreTable,
    snapshot_id: int | None = None,
    tag_name: str | None = None,
) -> FileStoreTable:
    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    travel_options: dict[str, str] = {}
    if snapshot_id is not None:
        travel_options["scan.snapshot-id"] = str(snapshot_id)
    if tag_name is not None:
        travel_options["scan.tag-name"] = tag_name
    if travel_options:
        return table.copy(travel_options)
    return table


def _source_for_table(
    table: FileStoreTable,
    catalog_options: Dict[str, str] | None = None,
    io_config=None,
):
    from daft import context, runners
    from daft.daft import StorageConfig

    from pypaimon.daft.daft_datasource import PaimonDataSource
    from pypaimon.daft.daft_io_config import (
        _convert_paimon_catalog_options_to_io_config,
    )

    if catalog_options is None:
        catalog_options = {}

    io_config = io_config or _convert_paimon_catalog_options_to_io_config(
        _enrich_options_with_rest_token(catalog_options, table)
    )
    io_config = io_config or context.get_context().daft_planning_config.default_io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    return PaimonDataSource(
        table, storage_config=storage_config, catalog_options=catalog_options
    )


def _read_table(
    table: FileStoreTable,
    catalog_options: Dict[str, str] | None = None,
    io_config=None,
    snapshot_id: int | None = None,
    tag_name: str | None = None,
) -> "daft.DataFrame":
    """Read a Paimon table object into a lazy Daft DataFrame."""
    table = _time_travel_table(table, snapshot_id=snapshot_id, tag_name=tag_name)
    return _source_for_table(table, catalog_options=catalog_options, io_config=io_config).read()


def _normalize_explain_filters(filters: Any) -> tuple[Any, list[Any]]:
    if filters is None:
        return None, []

    if isinstance(filters, (list, tuple)):
        if not filters:
            return None, []
        filter_exprs = list(filters)
        combined = filter_exprs[0]
        for filter_expr in filter_exprs[1:]:
            combined = combined & filter_expr
    else:
        filter_exprs = [filters]
        combined = filters

    return combined, [getattr(filter_expr, "_expr", filter_expr) for filter_expr in filter_exprs]


def _explain_table(
    table: FileStoreTable,
    catalog_options: Dict[str, str] | None = None,
    io_config=None,
    snapshot_id: int | None = None,
    tag_name: str | None = None,
    filters: Any = None,
    partition_filters: Any = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    verbose: bool = False,
) -> "PaimonScanExplain":
    """Explain a Paimon table object using Daft's datasource pushdown model."""
    from daft.io.pushdowns import Pushdowns

    table = _time_travel_table(table, snapshot_id=snapshot_id, tag_name=tag_name)
    source = _source_for_table(table, catalog_options=catalog_options, io_config=io_config)
    filter_expr, filter_pyexprs = _normalize_explain_filters(filters)
    partition_filter_expr, _ = _normalize_explain_filters(partition_filters)
    if filter_pyexprs:
        source.push_filters(filter_pyexprs)
    return source.explain_scan(
        Pushdowns(
            filters=filter_expr,
            partition_filters=partition_filter_expr,
            columns=columns,
            limit=limit,
        ),
        verbose=verbose,
    )


def _write_table(
    df: "daft.DataFrame",
    table: FileStoreTable,
    mode: str = "append",
) -> "daft.DataFrame":
    """Write a Daft DataFrame to a Paimon table object."""
    from pypaimon.daft.daft_datasink import PaimonDataSink

    return df.write_sink(PaimonDataSink(table, mode))


def read_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    io_config=None,
) -> "daft.DataFrame":
    """Read a Paimon table into a lazy Daft DataFrame.

    Returns a lazy DataFrame backed by Daft's optimizer. Use standard
    DataFrame operations (.select, .where, .limit) for projection,
    filtering, and limit — they are automatically pushed down into the
    Paimon scan via Daft's DataSource protocol.

    Args:
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``,
            e.g. ``{"warehouse": "/path/to/warehouse"}``.
        snapshot_id: Optional snapshot id to time-travel to. Mutually
            exclusive with ``tag_name``.
        tag_name: Optional tag name to time-travel to. Mutually
            exclusive with ``snapshot_id``.
        io_config: Optional Daft IOConfig for accessing object storage.
            If None, will be inferred from the catalog options.

    Returns:
        A lazy ``daft.DataFrame`` backed by this Paimon table.
    """
    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    from pypaimon.catalog.catalog_factory import CatalogFactory

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    return _read_table(
        table, catalog_options=catalog_options,
        io_config=io_config, snapshot_id=snapshot_id, tag_name=tag_name,
    )


def explain_paimon_scan(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    filters: Any = None,
    partition_filters: Any = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    io_config=None,
    verbose: bool = False,
) -> "PaimonScanExplain":
    """Explain a Paimon scan through Daft's reader-routing layer.

    The optional ``filters`` argument accepts a Daft expression or a list of
    Daft expressions. Lists are treated as conjunctions, matching how multiple
    pushed filters reach Daft datasources.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    return _explain_table(
        table,
        catalog_options=catalog_options,
        io_config=io_config,
        snapshot_id=snapshot_id,
        tag_name=tag_name,
        filters=filters,
        partition_filters=partition_filters,
        columns=columns,
        limit=limit,
        verbose=verbose,
    )


def write_paimon(
    df: "daft.DataFrame",
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    mode: str = "append",
) -> "daft.DataFrame":
    """Write a Daft DataFrame to a Paimon table.

    Args:
        df: The Daft DataFrame to write.
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        mode: Write mode — ``"append"`` or ``"overwrite"``.

    Returns:
        A summary ``daft.DataFrame`` with columns
        (operation, rows, file_size, file_name).
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    return _write_table(df, table, mode=mode)
