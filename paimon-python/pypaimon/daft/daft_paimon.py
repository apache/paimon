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

import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional, Union
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


# Time-travel targets are mutually exclusive: at most one may be set.
TimeTravelTimestamp = Union[int, str, "datetime.datetime"]


def _validate_single_time_travel(
    snapshot_id: int | None,
    tag_name: str | None,
    timestamp: TimeTravelTimestamp | None,
) -> None:
    specified = [
        name
        for name, value in (
            ("snapshot_id", snapshot_id),
            ("tag_name", tag_name),
            ("timestamp", timestamp),
        )
        if value is not None
    ]
    if len(specified) > 1:
        raise ValueError(
            "Only one of snapshot_id, tag_name, timestamp can be set, "
            f"got: {specified}"
        )


def _timestamp_scan_option(timestamp: TimeTravelTimestamp) -> dict[str, str]:
    """Map a timestamp to the matching Paimon scan option.

    ``datetime`` / ``int`` (epoch millis) -> ``scan.timestamp-millis``;
    ``str`` (e.g. ``'2026-07-09 10:00:00'``) -> ``scan.timestamp``.
    A naive ``datetime`` is interpreted in the local timezone.
    """
    # bool is a subclass of int; reject it so True/False can't become a timestamp.
    if isinstance(timestamp, bool):
        raise TypeError("timestamp must be int millis, datetime, or str, not bool")
    if isinstance(timestamp, datetime.datetime):
        return {"scan.timestamp-millis": str(int(timestamp.timestamp() * 1000))}
    if isinstance(timestamp, int):
        return {"scan.timestamp-millis": str(timestamp)}
    if isinstance(timestamp, str):
        return {"scan.timestamp": timestamp}
    raise TypeError(
        "timestamp must be int (epoch millis), datetime, or str, "
        f"got: {type(timestamp).__name__}"
    )


def _time_travel_table(
    table: FileStoreTable,
    snapshot_id: int | None = None,
    tag_name: str | None = None,
    timestamp: TimeTravelTimestamp | None = None,
) -> FileStoreTable:
    _validate_single_time_travel(snapshot_id, tag_name, timestamp)

    travel_options: dict[str, str] = {}
    if snapshot_id is not None:
        travel_options["scan.snapshot-id"] = str(snapshot_id)
    if tag_name is not None:
        travel_options["scan.tag-name"] = tag_name
    if timestamp is not None:
        travel_options.update(_timestamp_scan_option(timestamp))
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
        serialize_io_config,
    )

    if catalog_options is None:
        catalog_options = {}

    # Keep the caller's io_config for source restoration and blob File fallback.
    explicit_io_config_bytes = serialize_io_config(io_config) if io_config is not None else None

    io_config = io_config or _convert_paimon_catalog_options_to_io_config(
        _enrich_options_with_rest_token(catalog_options, table)
    )
    io_config = io_config or context.get_context().daft_planning_config.default_io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    return PaimonDataSource(
        table,
        storage_config=storage_config,
        catalog_options=catalog_options,
        explicit_io_config_bytes=explicit_io_config_bytes,
    )


def _read_table(
    table: FileStoreTable,
    catalog_options: Dict[str, str] | None = None,
    io_config=None,
    snapshot_id: int | None = None,
    tag_name: str | None = None,
    timestamp: TimeTravelTimestamp | None = None,
) -> "daft.DataFrame":
    """Read a Paimon table object into a lazy Daft DataFrame."""
    table = _time_travel_table(
        table, snapshot_id=snapshot_id, tag_name=tag_name, timestamp=timestamp
    )
    return _source_for_table(table, catalog_options=catalog_options, io_config=io_config).read()


def _normalize_explain_filters(filters: Any) -> Any:
    if filters is None:
        return None

    if isinstance(filters, (list, tuple)):
        if not filters:
            return None
        combined = filters[0]
        for filter_expr in filters[1:]:
            combined = combined & filter_expr
        return combined

    return filters


def _explain_table(
    table: FileStoreTable,
    catalog_options: Dict[str, str] | None = None,
    io_config=None,
    snapshot_id: int | None = None,
    tag_name: str | None = None,
    timestamp: TimeTravelTimestamp | None = None,
    filters: Any = None,
    partition_filters: Any = None,
    columns: list[str] | None = None,
    limit: int | None = None,
    verbose: bool = False,
) -> "PaimonScanExplain":
    """Explain a Paimon table object using Daft's datasource pushdown model."""
    from daft.io.pushdowns import Pushdowns

    table = _time_travel_table(
        table, snapshot_id=snapshot_id, tag_name=tag_name, timestamp=timestamp
    )
    source = _source_for_table(table, catalog_options=catalog_options, io_config=io_config)
    return source.explain_scan(
        Pushdowns(
            filters=_normalize_explain_filters(filters),
            partition_filters=_normalize_explain_filters(partition_filters),
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
    """Write a Daft DataFrame to a Paimon table object.

    Primary-key HASH modes materialize each ``(partition, bucket)`` group in
    one Daft worker to guarantee a single Paimon writer owner. A large fixed
    bucket can therefore become a single-worker memory/CPU hot spot. Dynamic
    buckets are normally bounded by ``dynamic-bucket.target-row-num``. Each
    group also returns its commit-message list in one pickled binary cell, so
    groups producing many files can create large task results.
    """
    from pypaimon.daft.daft_datasink import (
        COMMIT_MESSAGES_COLUMN,
        PaimonCommitDataSink,
        PaimonDataSink,
        make_dynamic_bucket_assignment_udf,
        make_dynamic_routing_udf,
        make_fixed_bucket_udf,
        make_group_write_udf,
    )
    from pypaimon.table.bucket_mode import BucketMode
    from pypaimon.index.dynamic_bucket import SHORT_MAX_VALUE

    if not table.is_primary_key_table:
        return df.write_sink(PaimonDataSink(table, mode))

    _validate_write_column_names(df, table)
    bucket_mode = table.bucket_mode()
    if bucket_mode == BucketMode.POSTPONE_MODE:
        return df.write_sink(PaimonDataSink(table, mode))
    if bucket_mode == BucketMode.CROSS_PARTITION:
        raise ValueError(
            "CROSS_PARTITION primary-key Daft writes require a persistent "
            "global primary-key index, which PyPaimon does not yet support"
        )

    import daft

    data_columns = [daft.col(name) for name in table.field_names]
    commit_column = _pick_internal_column(
        df.column_names, COMMIT_MESSAGES_COLUMN
    )
    commit_sink = PaimonCommitDataSink(table, mode, commit_column)

    if bucket_mode == BucketMode.HASH_FIXED:
        bucket_column = _pick_internal_column(
            df.column_names + [commit_column], "__paimon_bucket__"
        )
        bucket_udf = make_fixed_bucket_udf(table)
        with_bucket = df.with_column(
            bucket_column,
            bucket_udf(*data_columns),
        )
        group_keys = list(table.partition_keys) + [bucket_column]
        group_write = make_group_write_udf(
            table,
            mode,
            commit_sink.commit_user,
            precomputed_bucket=True,
        )
        prepared = with_bucket.groupby(*group_keys).map_groups(
            group_write(
                *data_columns,
                daft.col(bucket_column),
            ).alias(commit_column)
        )
        return prepared.write_sink(commit_sink)

    if bucket_mode == BucketMode.HASH_DYNAMIC:
        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        base_snapshot_id = (
            latest_snapshot.id if latest_snapshot is not None else 0
        )
        input_partitions = df.num_partitions() or 1
        max_buckets = table.options.dynamic_bucket_max_buckets()
        # Bucket ids are stored as Java shorts and Paimon allocates ids in
        # [0, Short.MAX_VALUE), so every assigner must own at least one id.
        num_assigners = min(max(1, input_partitions), SHORT_MAX_VALUE)
        if max_buckets > 0:
            num_assigners = min(num_assigners, max_buckets)
        num_channels = num_assigners

        assigner_column = _pick_internal_column(
            df.column_names + [commit_column], "__paimon_assigner__"
        )
        bucket_column = _pick_internal_column(
            df.column_names + [commit_column, assigner_column],
            "__paimon_bucket__",
        )
        key_hash_column = _pick_internal_column(
            df.column_names + [
                commit_column,
                assigner_column,
                bucket_column,
            ],
            "__paimon_key_hash__",
        )
        new_mapping_column = _pick_internal_column(
            df.column_names + [
                commit_column,
                assigner_column,
                bucket_column,
                key_hash_column,
            ],
            "__paimon_new_mapping__",
        )
        base_snapshot_column = _pick_internal_column(
            df.column_names + [
                commit_column,
                assigner_column,
                bucket_column,
                key_hash_column,
                new_mapping_column,
            ],
            "__paimon_base_snapshot__",
        )
        routing_udf = make_dynamic_routing_udf(
            table,
            num_channels,
            num_assigners,
            assigner_column,
            key_hash_column,
        )
        with_routing = df.select(
            *data_columns,
            routing_udf(*data_columns),
        )
        bucket_assignment_udf = make_dynamic_bucket_assignment_udf(
            table,
            num_channels,
            num_assigners,
            bucket_column,
            key_hash_column,
            new_mapping_column,
            base_snapshot_column,
            ignore_existing=mode == "overwrite",
            base_snapshot_id=base_snapshot_id,
        )
        assigner_group_keys = list(table.partition_keys) + [assigner_column]
        with_bucket = with_routing.groupby(*assigner_group_keys).map_groups(
            bucket_assignment_udf(
                *data_columns,
                daft.col(assigner_column),
                daft.col(key_hash_column),
            )
        )
        # After a parallelism change, keys handled by different current
        # assigners can restore to the same historical bucket. Regroup by the
        # final bucket so exactly one file writer owns it in this commit.
        group_write = make_group_write_udf(
            table,
            mode,
            commit_sink.commit_user,
            precomputed_bucket=True,
            base_snapshot_id=base_snapshot_id,
        )
        group_keys = list(table.partition_keys) + [bucket_column]
        prepared = with_bucket.groupby(*group_keys).map_groups(
            group_write(
                *data_columns,
                daft.col(bucket_column),
                daft.col(key_hash_column),
                daft.col(new_mapping_column),
                daft.col(base_snapshot_column),
            ).alias(commit_column)
        )
        return prepared.write_sink(commit_sink)

    raise ValueError(
        f"Unsupported primary-key bucket mode for Daft writes: {bucket_mode.name}"
    )


def _validate_write_column_names(df: "daft.DataFrame", table: FileStoreTable) -> None:
    input_names = list(df.column_names)
    target_names = list(table.field_names)
    if len(set(input_names)) != len(input_names):
        raise ValueError(
            f"Cannot write to Paimon with duplicate input field names: {input_names}"
        )
    missing = [name for name in target_names if name not in input_names]
    extra = [name for name in input_names if name not in target_names]
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing fields: {missing}")
        if extra:
            details.append(f"extra fields: {extra}")
        raise ValueError(f"Paimon write schema mismatch: {'; '.join(details)}")


def _pick_internal_column(existing_names, preferred: str) -> str:
    existing = set(existing_names)
    if preferred not in existing:
        return preferred
    suffix = 1
    while f"{preferred}_{suffix}" in existing:
        suffix += 1
    return f"{preferred}_{suffix}"


def read_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    timestamp: Optional[TimeTravelTimestamp] = None,
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
            exclusive with ``tag_name`` and ``timestamp``.
        tag_name: Optional tag name to time-travel to. Mutually
            exclusive with ``snapshot_id`` and ``timestamp``.
        timestamp: Optional timestamp to time-travel to the latest snapshot
            at or before it. Accepts an ``int`` (epoch milliseconds) or
            ``datetime`` (mapped to ``scan.timestamp-millis``), or a ``str``
            such as ``'2026-07-09 10:00:00'`` (mapped to ``scan.timestamp``).
            A naive ``datetime`` is interpreted in the local timezone.
            Mutually exclusive with ``snapshot_id`` and ``tag_name``.
        io_config: Optional Daft IOConfig for accessing object storage.
            If None, will be inferred from the catalog options.

    Returns:
        A lazy ``daft.DataFrame`` backed by this Paimon table.
    """
    _validate_single_time_travel(snapshot_id, tag_name, timestamp)

    from pypaimon.catalog.catalog_factory import CatalogFactory

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    return _read_table(
        table, catalog_options=catalog_options,
        io_config=io_config, snapshot_id=snapshot_id, tag_name=tag_name,
        timestamp=timestamp,
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
    timestamp: Optional[TimeTravelTimestamp] = None,
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
        timestamp=timestamp,
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
