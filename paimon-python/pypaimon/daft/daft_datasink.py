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

from __future__ import annotations

import logging
import pickle
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from daft.datatype import DataType
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.table.file_store_table import FileStoreTable


logger = logging.getLogger(__name__)

_PaimonIdentifier = tuple[str, str, str | None]
COMMIT_MESSAGES_COLUMN = "__paimon_commit_messages__"
_WORKER_TABLE_CACHE: dict[tuple[Any, ...], FileStoreTable] = {}
_WORKER_TABLE_CACHE_SIZE = 32


def _options_to_dict(options: Any) -> dict[str, Any]:
    if options is None:
        return {}
    if isinstance(options, dict):
        return dict(options)

    to_map = getattr(options, "to_map", None)
    if callable(to_map):
        return dict(to_map())

    data = getattr(options, "data", None)
    if isinstance(data, dict):
        return dict(data)

    return {}


def _extract_catalog_options(table: FileStoreTable) -> dict[str, Any]:
    file_io = getattr(table, "file_io", None)
    properties = getattr(file_io, "properties", None)
    if properties is None:
        properties = getattr(file_io, "catalog_options", None)
    return _options_to_dict(properties)


def _extract_identifier(table: FileStoreTable) -> _PaimonIdentifier | None:
    identifier = getattr(table, "identifier", None)
    if identifier is None:
        return None

    get_database_name = getattr(identifier, "get_database_name", None)
    get_table_name = getattr(identifier, "get_table_name", None)
    get_branch_name = getattr(identifier, "get_branch_name", None)

    database_name = (
        get_database_name()
        if callable(get_database_name)
        else getattr(identifier, "database", None)
    )
    table_name = (
        get_table_name()
        if callable(get_table_name)
        else getattr(identifier, "object", None)
    )
    branch_name = (
        get_branch_name()
        if callable(get_branch_name)
        else getattr(identifier, "branch", None)
    )
    if database_name is None or table_name is None:
        return None
    return database_name, table_name, branch_name


def _to_paimon_identifier(identifier: _PaimonIdentifier) -> Any:
    database_name, table_name, branch_name = identifier
    if branch_name:
        from pypaimon.common.identifier import Identifier

        return Identifier(database_name, table_name, branch_name)
    return f"{database_name}.{table_name}"


def _load_table(
    catalog_options: dict[str, Any],
    table_identifier: _PaimonIdentifier | None,
    table_path: str | None,
) -> FileStoreTable:
    if catalog_options and table_identifier is not None:
        from pypaimon.catalog.catalog_factory import CatalogFactory

        catalog = CatalogFactory.create(catalog_options)
        return catalog.get_table(_to_paimon_identifier(table_identifier))

    if table_path:
        from pypaimon.table.file_store_table import FileStoreTable

        return FileStoreTable.from_path(table_path)

    raise RuntimeError(
        "Unable to reconstruct Paimon table while deserializing PaimonDataSink."
    )


def _load_worker_table(
    catalog_options: dict[str, Any],
    table_identifier: _PaimonIdentifier | None,
    table_path: str | None,
    schema_token: tuple[int, int],
) -> FileStoreTable:
    """Reuse immutable table metadata within a Daft worker process."""
    cache_key = (
        tuple(sorted((str(key), repr(value))
                     for key, value in catalog_options.items())),
        table_identifier,
        table_path,
        schema_token,
    )
    cached = _WORKER_TABLE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    loaded = _load_table(catalog_options, table_identifier, table_path)
    if len(_WORKER_TABLE_CACHE) >= _WORKER_TABLE_CACHE_SIZE:
        _WORKER_TABLE_CACHE.pop(next(iter(_WORKER_TABLE_CACHE)))
    _WORKER_TABLE_CACHE[cache_key] = loaded
    return loaded


class PaimonDataSink(DataSink[list[Any]]):
    """DataSink for writing data to an Apache Paimon table."""

    def __init__(
        self,
        table: FileStoreTable,
        mode: str = "append",
    ) -> None:
        if mode not in ("append", "overwrite"):
            raise ValueError(f"Only 'append' or 'overwrite' mode is supported, got: {mode!r}")
        self._mode = mode
        self._catalog_options = _extract_catalog_options(table)
        self._table_identifier = _extract_identifier(table)
        table_path = getattr(table, "table_path", None)
        self._table_path = str(table_path) if table_path is not None else None
        self._commit_user: str | None = None
        self._init_table(table)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "_mode": self._mode,
            "_catalog_options": self._catalog_options,
            "_table_identifier": self._table_identifier,
            "_table_path": self._table_path,
            "_commit_user": self._commit_user,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._mode = state["_mode"]
        self._catalog_options = state["_catalog_options"]
        self._table_identifier = state["_table_identifier"]
        self._table_path = state["_table_path"]
        self._commit_user = state["_commit_user"]
        table = _load_table(
            self._catalog_options,
            self._table_identifier,
            self._table_path,
        )
        self._init_table(table)

    def _init_table(self, table: FileStoreTable) -> None:
        self._table = table

        from pypaimon.schema.data_types import PyarrowFieldParser

        self._target_schema: pa.Schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        self._write_builder = table.new_batch_write_builder()
        if (
            self._commit_user is not None
            and hasattr(self._write_builder, "commit_user")
        ):
            self._write_builder.commit_user = self._commit_user
        else:
            self._commit_user = getattr(self._write_builder, "commit_user", None)
        if self._mode == "overwrite":
            self._write_builder.overwrite({})

    def name(self) -> str:
        return "Paimon Write"

    def schema(self) -> Schema:
        return Schema._from_field_name_and_types(
            [
                ("operation", DataType.string()),
                ("rows", DataType.int64()),
                ("file_size", DataType.int64()),
                ("file_name", DataType.string()),
            ]
        )

    def _validate_input_schema(self, input_schema: pa.Schema) -> None:
        target_names = self._target_schema.names
        input_names = input_schema.names

        if len(set(input_names)) != len(input_names):
            raise ValueError(
                f"Cannot write to Paimon with duplicate input field names: "
                f"{input_names}"
            )
        if len(set(target_names)) != len(target_names):
            raise ValueError(
                f"Cannot write to Paimon with duplicate target field names: "
                f"{target_names}"
            )

        missing = [name for name in target_names if name not in input_names]
        extra = [name for name in input_names if name not in target_names]
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing fields: {missing}")
            if extra:
                details.append(f"extra fields: {extra}")
            detail = "; ".join(details)
            raise ValueError(f"Paimon write schema mismatch: {detail}")

    def _align_batch_to_target_schema(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if batch.schema.names != self._target_schema.names:
            batch = batch.select(self._target_schema.names)
        if batch.schema != self._target_schema:
            batch = batch.cast(self._target_schema)
        return batch

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[list[Any]]]:
        table_write = self._write_builder.new_write()

        total_rows = 0
        total_bytes = 0
        last_input_schema: pa.Schema | None = None
        try:
            for mp in micropartitions:
                for rb in mp.get_record_batches():
                    batch = rb.to_arrow_record_batch()
                    if batch.schema != last_input_schema:
                        self._validate_input_schema(batch.schema)
                        last_input_schema = batch.schema
                    batch = self._align_batch_to_target_schema(batch)
                    table_write.write_arrow_batch(batch)
                    total_rows += batch.num_rows
                    total_bytes += batch.nbytes
            commit_messages = table_write.prepare_commit()
            table_write.close()
        except Exception:
            try:
                table_write.abort()
            except Exception:
                logger.warning("Failed to abort Daft Paimon table write.", exc_info=True)
            raise

        yield WriteResult(
            result=list(commit_messages),
            bytes_written=total_bytes,
            rows_written=total_rows,
        )

    def finalize(self, write_results: list[WriteResult[list[Any]]]) -> MicroPartition:
        all_commit_messages = [msg for wr in write_results for msg in wr.result]

        table_commit = self._write_builder.new_commit()
        non_empty_workers = sum(
            any(not message.is_empty() for message in result.result)
            for result in write_results
        )
        primary_key_error = self._primary_key_write_error(non_empty_workers)
        if primary_key_error is not None:
            try:
                table_commit.abort(all_commit_messages)
            except Exception:
                logger.warning(
                    "Failed to abort uncommitted direct Daft PK files.",
                    exc_info=True,
                )
            finally:
                table_commit.close()
            raise ValueError(primary_key_error)
        try:
            table_commit.commit(all_commit_messages)
        finally:
            table_commit.close()

        operation_label = "OVERWRITE" if self._mode == "overwrite" else "ADD"
        all_files = [f for msg in all_commit_messages for f in msg.new_files]

        return MicroPartition.from_pydict(
            {
                "operation": pa.array([operation_label] * len(all_files), type=pa.string()),
                "rows": pa.array([f.row_count for f in all_files], type=pa.int64()),
                "file_size": pa.array([f.file_size for f in all_files], type=pa.int64()),
                "file_name": pa.array([f.file_name for f in all_files], type=pa.string()),
            }
        )

    def _primary_key_write_error(
        self, non_empty_workers: int
    ) -> str | None:
        if not self._table.is_primary_key_table:
            return None

        from pypaimon.table.bucket_mode import BucketMode

        bucket_mode = self._table.bucket_mode()
        if bucket_mode == BucketMode.POSTPONE_MODE:
            return None
        if bucket_mode in (
            BucketMode.HASH_FIXED,
            BucketMode.HASH_DYNAMIC,
        ) and non_empty_workers <= 1:
            return None
        if bucket_mode in (BucketMode.HASH_FIXED, BucketMode.HASH_DYNAMIC):
            reason = "require a single non-empty Daft write task"
        elif bucket_mode == BucketMode.CROSS_PARTITION:
            reason = (
                "require a persistent global primary-key index, which "
                "PyPaimon does not yet support"
            )
        else:
            reason = f"are unsafe for {bucket_mode.name}"
        return (
            f"Direct PaimonDataSink primary-key writes {reason}. Use "
            "pypaimon.daft.write_paimon or PaimonTable.append/overwrite "
            "for coordinated input."
        )


class PaimonCommitDataSink(PaimonDataSink):
    """Commit files already produced by partition/bucket group UDFs."""

    def __init__(
        self,
        table: FileStoreTable,
        mode: str = "append",
        commit_messages_column: str = COMMIT_MESSAGES_COLUMN,
    ) -> None:
        self._commit_messages_column = commit_messages_column
        super().__init__(table, mode)

    def __getstate__(self) -> dict[str, Any]:
        state = super().__getstate__()
        state["_commit_messages_column"] = self._commit_messages_column
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._commit_messages_column = state.pop(
            "_commit_messages_column", COMMIT_MESSAGES_COLUMN
        )
        super().__setstate__(state)

    @property
    def commit_user(self) -> str | None:
        return self._commit_user

    def name(self) -> str:
        return "Paimon Commit"

    def _primary_key_write_error(
        self, non_empty_workers: int
    ) -> str | None:
        return None

    def write(
        self, micropartitions: Iterator[MicroPartition]
    ) -> Iterator[WriteResult[list[Any]]]:
        commit_messages = []
        total_bytes = 0
        for mp in micropartitions:
            for rb in mp.get_record_batches():
                batch = rb.to_arrow_record_batch()
                column_index = batch.schema.get_field_index(
                    self._commit_messages_column
                )
                if column_index < 0:
                    raise ValueError(
                        "Missing internal column "
                        f"{self._commit_messages_column!r}"
                    )
                for payload in batch.column(column_index).to_pylist():
                    if payload is None:
                        continue
                    total_bytes += len(payload)
                    decoded = pickle.loads(payload)
                    if not isinstance(decoded, list):
                        raise TypeError(
                            "Invalid Paimon group-write result: expected a list "
                            f"of commit messages, got {type(decoded).__name__}"
                        )
                    commit_messages.extend(decoded)

        yield WriteResult(
            result=commit_messages,
            bytes_written=total_bytes,
            rows_written=sum(
                file.row_count
                for message in commit_messages
                for file in message.new_files
            ),
        )


def _series_to_arrow_table(columns, schema: pa.Schema) -> pa.Table:
    arrays = [column.to_arrow() for column in columns]
    table = pa.Table.from_arrays(arrays, names=schema.names)
    if table.schema != schema:
        table = table.cast(schema)
    return table


def make_fixed_bucket_udf(table: FileStoreTable):
    """Create a Daft batch UDF which computes Paimon's fixed bucket."""
    import daft
    from daft import DataType, Series

    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.write.row_key_extractor import FixedBucketRowKeyExtractor

    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    extractor = FixedBucketRowKeyExtractor(table.table_schema)

    @daft.func.batch(return_dtype=DataType.int32())
    def _fixed_bucket(*columns) -> Series:
        arrow_table = _series_to_arrow_table(columns, schema)
        buckets = []
        for batch in arrow_table.to_batches():
            _, batch_buckets = extractor.extract_partition_bucket_batch(batch)
            buckets.extend(batch_buckets)
        return Series.from_arrow(pa.array(buckets, type=pa.int32()))

    return _fixed_bucket


def make_dynamic_routing_udf(
    table: FileStoreTable,
    num_channels: int,
    num_assigners: int,
    assigner_column: str,
    key_hash_column: str,
):
    """Calculate each dynamic key hash once and route it to an assigner."""
    import daft
    from daft import DataType, Series

    from pypaimon.index.dynamic_bucket import compute_assigner, to_signed_int32
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor

    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    extractor = DynamicBucketRowKeyExtractor(table.table_schema)
    return_fields = {
        assigner_column: DataType.int32(),
        key_hash_column: DataType.int32(),
    }

    @daft.func.batch(
        return_dtype=DataType.struct(return_fields),
        unnest=True,
    )
    def _dynamic_routing(*columns) -> Series:
        arrow_table = _series_to_arrow_table(columns, schema)
        assigners = []
        key_hashes = []
        for batch in arrow_table.to_batches():
            _, partition_hashes, batch_key_hashes = (
                extractor.extract_hashes_batch(batch)
            )
            assigners.extend([
                compute_assigner(
                    partition_hash, key_hash, num_channels, num_assigners
                )
                for partition_hash, key_hash in zip(
                    partition_hashes, batch_key_hashes
                )
            ])
            key_hashes.extend(
                to_signed_int32(value) for value in batch_key_hashes
            )
        return Series.from_arrow(
            pa.StructArray.from_arrays(
                [
                    pa.array(assigners, type=pa.int32()),
                    pa.array(key_hashes, type=pa.int32()),
                ],
                names=[assigner_column, key_hash_column],
            )
        )

    return _dynamic_routing


def make_dynamic_bucket_assignment_udf(
    table: FileStoreTable,
    num_channels: int,
    num_assigners: int,
    bucket_column: str,
    key_hash_column: str,
    new_mapping_column: str,
    base_snapshot_column: str,
    ignore_existing: bool = False,
    base_snapshot_id: int | None = None,
):
    """Assign persistent buckets inside one partition/assigner group."""
    import daft
    from daft import DataType, Series

    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor

    catalog_options = _extract_catalog_options(table)
    table_identifier = _extract_identifier(table)
    table_path_value = getattr(table, "table_path", None)
    table_path = str(table_path_value) if table_path_value is not None else None
    schema_token = (
        table.table_schema.id,
        table.table_schema.time_millis,
    )
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    data_column_count = len(schema)
    partition_names = set(table.partition_keys)
    output_fields = [
        field for field in schema if field.name not in partition_names
    ]
    return_fields = {
        field.name: DataType.from_arrow_type(field.type)
        for field in output_fields
    }
    return_fields[key_hash_column] = DataType.int32()
    return_fields[bucket_column] = DataType.int32()
    return_fields[new_mapping_column] = DataType.bool()
    return_fields[base_snapshot_column] = DataType.int64()

    @daft.func.batch(
        return_dtype=DataType.struct(return_fields),
        unnest=True,
    )
    def _assign_dynamic_bucket(*columns) -> Series:
        assigner_values = columns[data_column_count].to_pylist()
        key_hash_values = columns[data_column_count + 1].to_pylist()
        if not assigner_values:
            return Series.from_arrow(
                pa.array([], type=pa.struct([
                    pa.field(field.name, field.type) for field in output_fields
                ] + [
                    pa.field(key_hash_column, pa.int32()),
                    pa.field(bucket_column, pa.int32()),
                    pa.field(new_mapping_column, pa.bool_()),
                    pa.field(base_snapshot_column, pa.int64()),
                ]))
            )
        assign_id = assigner_values[0]
        if any(value != assign_id for value in assigner_values):
            raise RuntimeError(
                "A dynamic-bucket group contained multiple assigners"
            )

        arrow_table = _series_to_arrow_table(
            columns[:data_column_count], schema
        ).combine_chunks()
        worker_table = _load_worker_table(
            catalog_options,
            table_identifier,
            table_path,
            schema_token,
        )
        extractor = DynamicBucketRowKeyExtractor(
            worker_table.table_schema,
            table=worker_table,
            num_channels=num_channels,
            num_assigners=num_assigners,
            assign_id=assign_id,
            ignore_existing=ignore_existing,
            maintain_index=False,
            base_snapshot_id=base_snapshot_id,
        )
        buckets = []
        new_mappings = []
        offset = 0
        for batch in arrow_table.to_batches():
            batch_hashes = key_hash_values[offset:offset + batch.num_rows]
            _, batch_buckets, batch_new_mappings = (
                extractor.extract_partition_bucket_status_from_hashes_batch(
                    batch, batch_hashes
                )
            )
            buckets.extend(batch_buckets)
            new_mappings.extend(batch_new_mappings)
            offset += batch.num_rows
        arrays = [
            arrow_table.column(field.name).combine_chunks()
            for field in output_fields
        ]
        arrays.append(pa.array(key_hash_values, type=pa.int32()))
        arrays.append(pa.array(buckets, type=pa.int32()))
        arrays.append(pa.array(new_mappings, type=pa.bool_()))
        arrays.append(pa.array(
            [extractor.base_snapshot_id] * arrow_table.num_rows,
            type=pa.int64(),
        ))
        return Series.from_arrow(
            pa.StructArray.from_arrays(
                arrays,
                names=[field.name for field in output_fields] + [
                    key_hash_column,
                    bucket_column,
                    new_mapping_column,
                    base_snapshot_column,
                ],
            )
        )

    return _assign_dynamic_bucket


def make_group_write_udf(
    table: FileStoreTable,
    mode: str,
    commit_user: str | None,
    *,
    precomputed_bucket: bool = False,
    base_snapshot_id: int | None = None,
):
    """Create a UDF which owns one complete Paimon write group."""
    import daft
    from daft import DataType, Series

    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.bucket_mode import BucketMode

    catalog_options = _extract_catalog_options(table)
    table_identifier = _extract_identifier(table)
    table_path_value = getattr(table, "table_path", None)
    table_path = str(table_path_value) if table_path_value is not None else None
    schema_token = (
        table.table_schema.id,
        table.table_schema.time_millis,
    )
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    data_column_count = len(schema)
    dynamic_bucket = table.bucket_mode() == BucketMode.HASH_DYNAMIC

    @daft.func.batch(return_dtype=DataType.binary())
    def _write_group(*columns) -> Series:
        data_columns = columns[:data_column_count]
        bucket = None
        key_hash_values = None
        new_mapping_values = None
        group_base_snapshot_id = base_snapshot_id
        if precomputed_bucket:
            bucket_values = columns[data_column_count].to_pylist()
            if not bucket_values:
                return Series.from_arrow(pa.array([], type=pa.binary()))
            bucket = bucket_values[0]
            if any(value != bucket for value in bucket_values):
                raise RuntimeError(
                    "A Paimon write group contained multiple buckets"
                )
            if dynamic_bucket:
                key_hash_values = columns[data_column_count + 1].to_pylist()
                new_mapping_values = columns[data_column_count + 2].to_pylist()
                base_snapshot_values = columns[
                    data_column_count + 3
                ].to_pylist()
                observed_base_snapshot_id = base_snapshot_values[0]
                if any(
                    value != observed_base_snapshot_id
                    for value in base_snapshot_values
                ):
                    raise RuntimeError(
                        "A dynamic-bucket group contained multiple base "
                        "snapshots"
                    )
                if group_base_snapshot_id is None:
                    group_base_snapshot_id = observed_base_snapshot_id
                elif observed_base_snapshot_id != group_base_snapshot_id:
                    raise RuntimeError(
                        "A dynamic-bucket group did not match the driver "
                        "base snapshot"
                    )

        arrow_table = _series_to_arrow_table(data_columns, schema)
        worker_table = _load_worker_table(
            catalog_options,
            table_identifier,
            table_path,
            schema_token,
        )
        write_builder = worker_table.new_batch_write_builder()
        if commit_user is not None:
            write_builder.commit_user = commit_user
        if mode == "overwrite":
            write_builder.overwrite({})
        table_write = write_builder.new_write()
        if precomputed_bucket and dynamic_bucket:
            table_write.with_dynamic_bucket_index(
                ignore_existing=mode == "overwrite",
                base_snapshot_id=group_base_snapshot_id,
            )
        try:
            if precomputed_bucket:
                offset = 0
                for batch in arrow_table.to_batches():
                    batch_hashes = None
                    batch_new_mappings = None
                    if key_hash_values is not None:
                        batch_hashes = key_hash_values[
                            offset:offset + batch.num_rows
                        ]
                        batch_new_mappings = new_mapping_values[
                            offset:offset + batch.num_rows
                        ]
                    table_write.write_arrow_batch_to_bucket(
                        batch,
                        bucket,
                        batch_hashes,
                        batch_new_mappings,
                    )
                    offset += batch.num_rows
            else:
                table_write.write_arrow(arrow_table)
            commit_messages = table_write.prepare_commit()
            table_write.close()
        except Exception:
            try:
                table_write.abort()
            except Exception:
                logger.warning(
                    "Failed to abort Daft Paimon group write.",
                    exc_info=True,
                )
            raise

        return Series.from_arrow(
            pa.array([pickle.dumps(commit_messages)], type=pa.binary())
        )

    return _write_group
