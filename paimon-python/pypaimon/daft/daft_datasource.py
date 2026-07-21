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

from dataclasses import dataclass, replace
import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import daft
from daft.dependencies import pa
from daft.expressions import ExpressionsProjection
from daft.io.partitioning import PartitionField
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from pypaimon.daft.daft_compat import require_file_range_reads
from pypaimon.daft.daft_explain import (
    PaimonReaderSplitExplain,
    PaimonScanExplain,
    READER_MODE_NATIVE_PARQUET,
    READER_MODE_PYPAIMON_FALLBACK,
)
from pypaimon.daft.daft_predicate_visitor import convert_filters_to_paimon
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    DataField,
    DataType,
    MapType,
    RowType,
    VectorType,
    is_array_blob_type,
    is_blob_type,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pypaimon.common.predicate import Predicate
    from pypaimon.manifest.schema.data_file_meta import DataFileMeta
    from pypaimon.read.explain import ExplainSplitInfo
    from pypaimon.read.split import Split
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import PyExpr, StorageConfig
    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)

PAIMON_FILE_FORMAT_PARQUET = "parquet"
PAIMON_FILE_FORMAT_ORC = "orc"
PAIMON_FILE_FORMAT_AVRO = "avro"

_PaimonIdentifier = tuple[str, str, str | None]

# Daft's Parquet reader applies casts independently of PyPaimon. Keep this
# list limited to promotions whose results are covered by end-to-end parity
# tests; logical schema-change support alone is not sufficient.
_NATIVE_READ_ATOMIC_PROMOTIONS = frozenset({("INT", "BIGINT")})


def _promote_time32_type_for_daft(data_type: pa.DataType) -> pa.DataType:
    """Use Daft's supported time representation without changing values."""
    if pa.types.is_time32(data_type):
        return pa.time64("us")
    if pa.types.is_struct(data_type):
        fields = [_promote_time32_field_for_daft(field) for field in data_type]
        return data_type if fields == list(data_type) else pa.struct(fields)
    if pa.types.is_list(data_type):
        value_field = _promote_time32_field_for_daft(data_type.value_field)
        return data_type if value_field == data_type.value_field else pa.list_(value_field)
    if pa.types.is_map(data_type):
        key_type = _promote_time32_type_for_daft(data_type.key_type)
        item_type = _promote_time32_type_for_daft(data_type.item_type)
        return (
            data_type
            if key_type == data_type.key_type and item_type == data_type.item_type
            else pa.map_(key_type, item_type, keys_sorted=data_type.keys_sorted)
        )
    return data_type


def _promote_time32_field_for_daft(field: pa.Field) -> pa.Field:
    data_type = _promote_time32_type_for_daft(field.type)
    if data_type == field.type:
        return field
    return pa.field(
        field.name,
        data_type,
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _promote_time32_schema_for_daft(schema: pa.Schema) -> pa.Schema:
    fields = [_promote_time32_field_for_daft(field) for field in schema]
    if fields == list(schema):
        return schema
    return pa.schema(fields, metadata=schema.metadata)


def _promote_time32_batch_for_daft(batch: pa.RecordBatch) -> pa.RecordBatch:
    schema = _promote_time32_schema_for_daft(batch.schema)
    return batch if schema == batch.schema else batch.cast(schema, safe=False)


def _native_read_fields_compatible(
    file_fields: list[DataField],
    current_fields: list[DataField],
) -> bool:
    """Whether Daft can align the selected current fields by physical name."""
    file_fields_by_id = {field.id: field for field in file_fields}
    file_fields_by_name = {field.name: field for field in file_fields}

    for current_field in current_fields:
        file_field = file_fields_by_id.get(current_field.id)
        if file_field is None:
            # A missing nullable field is a later addition and Daft fills it
            # with NULL. The same physical name under another id is instead a
            # drop-then-readd and must be resolved by PyPaimon.
            if (
                current_field.name in file_fields_by_name
                or not current_field.type.nullable
            ):
                return False
            continue
        if file_field.name != current_field.name:
            return False
        if not _native_read_types_compatible(file_field.type, current_field.type):
            return False
    return True


def _native_read_types_compatible(
    file_type: DataType,
    current_type: DataType,
) -> bool:
    if file_type.nullable and not current_type.nullable:
        return False
    if type(file_type) is not type(current_type):
        return False
    if isinstance(file_type, RowType) and isinstance(current_type, RowType):
        return _native_read_fields_compatible(file_type.fields, current_type.fields)
    if isinstance(file_type, ArrayType) and isinstance(current_type, ArrayType):
        return _native_read_types_compatible(file_type.element, current_type.element)
    if isinstance(file_type, VectorType) and isinstance(current_type, VectorType):
        return (
            file_type.length == current_type.length
            and _native_read_types_compatible(file_type.element, current_type.element)
        )
    if isinstance(file_type, MapType) and isinstance(current_type, MapType):
        return (
            _native_read_types_compatible(file_type.key, current_type.key)
            and _native_read_types_compatible(file_type.value, current_type.value)
        )
    if not isinstance(file_type, AtomicType):
        return False
    file_type_name = file_type.type.upper()
    current_type_name = current_type.type.upper()
    if file_type_name == "TIME" or file_type_name.startswith("TIME("):
        return False
    return (
        file_type_name == current_type_name
        or (file_type_name, current_type_name) in _NATIVE_READ_ATOMIC_PROMOTIONS
    )


@dataclass(frozen=True, slots=True)
class _ReadPushdownState:
    reader_predicate: Predicate | None
    planning_predicate: Predicate | None
    requested_columns: list[str] | None
    task_columns: list[str] | None
    read_columns: list[str] | None
    source_limit: int | None


@dataclass(frozen=True, slots=True)
class _ReaderRouting:
    reader_mode: str
    fallback_reason: str | None

    @property
    def use_native_reader(self) -> bool:
        return self.reader_mode == READER_MODE_NATIVE_PARQUET


def _options_to_dict(options: Any) -> dict[str, Any]:
    if options is None:
        return {}
    if isinstance(options, dict):
        return dict(options)
    return dict(options.to_map())


def _extract_catalog_options(table: FileStoreTable) -> dict[str, Any]:
    # Every FileIO exposes catalog properties via ``properties`` (CachingFileIO
    # delegates to its wrapped FileIO), so no per-implementation handling needed.
    return _options_to_dict(table.file_io.properties)


def _extract_identifier(table: FileStoreTable) -> _PaimonIdentifier | None:
    identifier = table.identifier
    if identifier is None:
        return None

    database_name = identifier.get_database_name()
    table_name = identifier.get_table_name()
    if database_name is None or table_name is None:
        return None
    return database_name, table_name, identifier.get_branch_name()


def _extract_table_options(table: FileStoreTable) -> dict[str, Any]:
    return _options_to_dict(table.schema().options)


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
    table_options: dict[str, Any],
) -> FileStoreTable:
    if catalog_options and table_identifier is not None:
        from pypaimon.catalog.catalog_factory import CatalogFactory

        catalog = CatalogFactory.create(catalog_options)
        table = catalog.get_table(_to_paimon_identifier(table_identifier))
    elif table_path:
        from pypaimon.table.file_store_table import FileStoreTable

        table = FileStoreTable.from_path(table_path)
    else:
        raise RuntimeError(
            "Unable to reconstruct Paimon table while deserializing PaimonDataSource."
        )

    if table_options:
        table = table.copy(table_options)
    return table


def _build_storage_config(
    table: FileStoreTable,
    catalog_options: dict[str, Any],
    multithreaded_io: bool,
    explicit_io_config_bytes: bytes | None,
) -> StorageConfig:
    from daft import context
    from daft.daft import StorageConfig

    from pypaimon.daft.daft_io_config import _convert_paimon_catalog_options_to_io_config

    if explicit_io_config_bytes is not None:
        from daft.io import IOConfig

        io_config = IOConfig._from_serialized(explicit_io_config_bytes)
    else:
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token

        io_config = _convert_paimon_catalog_options_to_io_config(
            _enrich_options_with_rest_token(catalog_options, table)
        )
    io_config = io_config or context.get_context().daft_planning_config.default_io_config
    return StorageConfig(multithreaded_io, io_config)


class _PaimonPKSplitTask(DataSourceTask):
    """DataSourceTask for PK-table splits that require LSM-tree merge.

    Used when split.raw_convertible is False (overlapping levels exist) or
    when the file format is not Parquet (ORC, Avro). Delegates to pypaimon's
    native reader which handles LSM merging internally.
    """

    def __init__(
        self,
        table_catalog_options: dict[str, Any],
        table_identifier: _PaimonIdentifier | None,
        table_path: str | None,
        table_options: dict[str, Any],
        split: Split,
        schema: Schema,
        read_columns: list[str] | None = None,
        limit: int | None = None,
        predicate: Predicate | None = None,
        output_columns: list[str] | None = None,
        blob_column_names: set[str] | None = None,
        explicit_io_config_bytes: bytes | None = None,
        array_blob_column_names: set[str] | None = None,
    ) -> None:
        self._table_catalog_options = table_catalog_options
        self._table_identifier = table_identifier
        self._table_path = table_path
        self._table_options = table_options
        self._split = split
        self._schema = schema
        self._read_columns = read_columns
        self._limit = limit
        self._predicate = predicate
        self._output_columns = output_columns
        self._blob_column_names = blob_column_names or set()
        self._array_blob_column_names = array_blob_column_names or set()
        self._explicit_io_config_bytes = explicit_io_config_bytes

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        table = _load_table(
            self._table_catalog_options,
            self._table_identifier,
            self._table_path,
            self._table_options,
        )
        read_builder = table.new_read_builder()
        if self._read_columns is not None:
            read_builder = read_builder.with_projection(self._read_columns)
        if self._limit is not None:
            read_builder = read_builder.with_limit(self._limit)
        if self._predicate is not None:
            read_builder = read_builder.with_filter(self._predicate)

        has_blob_columns = bool(
            self._blob_column_names or self._array_blob_column_names
        )
        blob_io_config_bytes = (
            self._blob_io_config_bytes(table) if has_blob_columns else None
        )
        reader = read_builder.new_read().to_arrow_batch_reader([self._split])
        for batch in iter(reader.read_next_batch, None):
            if self._output_columns is not None:
                batch = batch.select(self._output_columns)
            if has_blob_columns:
                batch = _convert_blob_columns(
                    batch,
                    self._blob_column_names,
                    blob_io_config_bytes,
                    self._array_blob_column_names,
                )
            batch = _promote_time32_batch_for_daft(batch)
            rb = RecordBatch.from_arrow_record_batches([batch], batch.schema)
            if has_blob_columns:
                rb = _cast_blob_columns_to_file(
                    rb,
                    self._blob_column_names,
                    self._array_blob_column_names,
                )
            yield rb

    def _blob_io_config_bytes(self, table: FileStoreTable) -> bytes | None:
        """Serialized IOConfig embedded into blob File columns, in priority order: refreshed
        REST-DLF token / catalog creds (refreshed at read time, so long reads don't freeze a
        short STS token), then the explicit read_paimon io_config, then the OSS env alias."""
        from pypaimon.daft.daft_io_config import (
            _convert_paimon_catalog_options_to_file_io_config,
            _with_oss_alias,
            serialize_io_config,
        )
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token

        enriched = _enrich_options_with_rest_token(self._table_catalog_options, table)
        io_config = _convert_paimon_catalog_options_to_file_io_config(enriched)
        if io_config is not None:
            return serialize_io_config(io_config)
        if self._explicit_io_config_bytes is not None:
            # oss:// blobs need the s3 alias even from an explicit io_config (opendal File.open is broken).
            if urlparse(str(getattr(table, "table_path", "") or "")).scheme == "oss":
                from daft.io import IOConfig
                return serialize_io_config(_with_oss_alias(IOConfig._from_serialized(self._explicit_io_config_bytes)))
            return self._explicit_io_config_bytes
        io_config = _convert_paimon_catalog_options_to_file_io_config(enriched, require_credentials=False)
        return serialize_io_config(io_config) if io_config is not None else None


def _convert_blob_columns(
    batch: pa.RecordBatch,
    blob_column_names: set[str],
    io_config_bytes: bytes | None = None,
    array_blob_column_names: set[str] | None = None,
) -> pa.RecordBatch:
    """Replace serialized BlobDescriptor columns with the File physical struct layout."""
    from pypaimon.daft.daft_blob import (
        FILE_PHYSICAL_TYPE,
        blob_array_column_to_file_array,
        blob_column_to_file_array,
    )

    array_blob_column_names = array_blob_column_names or set()

    arrays = []
    fields = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        if field.name in blob_column_names and (pa.types.is_large_binary(field.type) or pa.types.is_binary(field.type)):
            arrays.append(blob_column_to_file_array(col, io_config_bytes))
            fields.append(pa.field(field.name, FILE_PHYSICAL_TYPE, nullable=field.nullable))
        elif field.name in array_blob_column_names and (
            pa.types.is_list(field.type) or pa.types.is_large_list(field.type)
        ):
            converted = blob_array_column_to_file_array(col, io_config_bytes)
            arrays.append(converted)
            fields.append(pa.field(field.name, converted.type, nullable=field.nullable))
        else:
            arrays.append(col)
            fields.append(field)
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _cast_blob_columns_to_file(
    rb: RecordBatch,
    blob_column_names: set[str],
    array_blob_column_names: set[str] | None = None,
) -> RecordBatch:
    """Cast struct-typed blob columns in a RecordBatch to DataType.file()."""
    from daft.datatype import DataType

    file_dtype = DataType.file()
    array_blob_column_names = array_blob_column_names or set()
    columns = {}
    for i, field in enumerate(rb.schema()):
        col = rb.get_column(i)
        if field.name in blob_column_names:
            col = col.cast(file_dtype)
        elif field.name in array_blob_column_names:
            col = col.cast(DataType.list(file_dtype))
        columns[field.name] = col
    return RecordBatch.from_pydict(columns)


def _blob_native_covering_files(
    files: list[DataFileMeta],
    task_columns: list[str],
    blob_column_names: set[str],
    partition_keys: list[str],
) -> list[DataFileMeta] | None:
    """Return the parquet files that can serve a blob-table split via Daft's
    native reader, or ``None`` if the split must use the pypaimon fallback.

    A blob table stores each column bunch in its own file: scalar columns in
    parquet, BLOB / ARRAY<BLOB> columns in ``.blob`` files, vector columns in
    ``.vector`` files, aligned by row id. Reading the base parquet files
    natively is only correct when every projected data column lives in parquet
    files that each fully cover the projection over disjoint row-id ranges --
    i.e. no blob / vector file carries a projected column, no cross-file field
    merge is required, and no two covering files overlap. Partition columns are
    path-derived, so they are excluded from file coverage. File-name
    conventions mirror ``DataFileMeta.is_blob_file`` / ``is_vector_file``.
    """
    partitions = set(partition_keys)
    projected = {c for c in task_columns if c not in partitions}
    if projected & set(blob_column_names):
        return None

    covering: list[DataFileMeta] = []
    for f in files:
        name = f.file_name
        write_cols = set(f.write_cols or [])
        carried = write_cols & projected
        if name.endswith(".blob") or ".vector." in name:
            if carried:
                return None  # a projected column lives in a blob/vector bunch
            continue
        if not name.endswith(".parquet"):
            return None  # unknown bunch format; stay on the safe fallback path
        if projected <= write_cols:
            covering.append(f)
        elif carried:
            return None  # partial coverage -> cross-file field merge required
        # else: parquet file irrelevant to the projection -> skip it

    if not covering:
        return None

    # The covering parquet files must not overlap (else rows are duplicated)
    # and together must span every row-id range present in the split. Otherwise
    # a row-id range whose projected column is absent from any covering file --
    # e.g. an older data-evolution range written before the column existed --
    # would be silently dropped here, whereas the pypaimon fallback returns
    # those rows with the column read as null via schema evolution.
    covering_ranges = []
    for f in covering:
        if f.first_row_id is None:
            return None
        covering_ranges.append((f.first_row_id, f.first_row_id + f.row_count))
    covering_ranges.sort()
    merged: list[tuple[int, int]] = []
    for start, end in covering_ranges:
        if merged and start < merged[-1][1]:
            return None
        if merged and start == merged[-1][1]:
            merged[-1] = (merged[-1][0], end)  # adjacent -> extend
        else:
            merged.append((start, end))

    for f in files:
        start, end = f.first_row_id, f.first_row_id + f.row_count
        if not any(ms <= start and end <= me for ms, me in merged):
            return None  # a present row-id range is not covered -> would drop rows
    return covering


class PaimonDataSource(DataSource):
    """DataSource for Apache Paimon tables.

    Uses pypaimon for catalog metadata and scan planning (file listing,
    partition pruning, statistics-based file skipping), then yields
    DataSourceTask objects executed by Daft's native Parquet reader.

    For primary-key tables whose splits cannot be read directly without an
    LSM-tree merge, a _PaimonPKSplitTask is yielded which delegates back
    to pypaimon's native reader.
    """

    def __init__(
        self,
        table: FileStoreTable,
        storage_config: StorageConfig,
        catalog_options: dict[str, str],
        explicit_io_config_bytes: bytes | None = None,
    ) -> None:
        self._storage_config = storage_config
        self._explicit_io_config_bytes = explicit_io_config_bytes
        self._catalog_options = dict(catalog_options or {})
        self._table_catalog_options = {
            **_extract_catalog_options(table),
            **self._catalog_options,
        }
        self._table_identifier = _extract_identifier(table)
        table_path = getattr(table, "table_path", None)
        self._table_path = str(table_path) if table_path is not None else None
        self._table_options = _extract_table_options(table)
        self._init_table(table)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "_multithreaded_io": self._storage_config.multithreaded_io,
            "_explicit_io_config_bytes": self._explicit_io_config_bytes,
            "_catalog_options": self._catalog_options,
            "_table_catalog_options": self._table_catalog_options,
            "_table_identifier": self._table_identifier,
            "_table_path": self._table_path,
            "_table_options": self._table_options,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._explicit_io_config_bytes = state.get("_explicit_io_config_bytes")
        self._catalog_options = state["_catalog_options"]
        self._table_catalog_options = state["_table_catalog_options"]
        self._table_identifier = state["_table_identifier"]
        self._table_path = state["_table_path"]
        self._table_options = state["_table_options"]

        table = _load_table(
            self._table_catalog_options,
            self._table_identifier,
            self._table_path,
            self._table_options,
        )
        self._storage_config = _build_storage_config(
            table,
            self._table_catalog_options,
            state["_multithreaded_io"],
            self._explicit_io_config_bytes,
        )
        self._init_table(table)

    def _init_table(self, table: FileStoreTable) -> None:
        self._table = table

        from pypaimon.schema.data_types import PyarrowFieldParser

        pa_schema = _promote_time32_schema_for_daft(
            PyarrowFieldParser.from_paimon_schema(table.fields)
        )

        self._scalar_blob_column_names = {
            field.name for field in table.fields if is_blob_type(field.type)
        }
        self._array_blob_column_names = {
            field.name for field in table.fields if is_array_blob_type(field.type)
        }
        self._has_blob_columns = bool(
            self._scalar_blob_column_names or self._array_blob_column_names
        )

        if self._has_blob_columns:
            require_file_range_reads()
            from daft.datatype import DataType

            base_schema = Schema.from_pyarrow_schema(pa_schema)
            fields = []
            for f in base_schema:
                if f.name in self._scalar_blob_column_names:
                    fields.append((f.name, DataType.file()))
                elif f.name in self._array_blob_column_names:
                    fields.append((f.name, DataType.list(DataType.file())))
                else:
                    fields.append((f.name, f.dtype))
            self._schema = Schema.from_field_name_and_types(fields)
        else:
            self._schema = Schema.from_pyarrow_schema(pa_schema)

        warehouse = (
            self._catalog_options.get("warehouse")
            or self._table_catalog_options.get("warehouse")
            or ""
        )
        self._warehouse_scheme = urlparse(warehouse).scheme

        self._file_format = table.options.file_format().lower()
        self._is_parquet = self._file_format == PAIMON_FILE_FORMAT_PARQUET

        self._partition_field_arrow_types: dict[str, pa.DataType] = (
            {
                f.name: _promote_time32_type_for_daft(
                    PyarrowFieldParser.from_paimon_type(f.type)
                )
                for f in table.partition_keys_fields
            }
            if table.partition_keys
            else {}
        )

    @property
    def name(self) -> str:
        table_path = getattr(self._table, "table_path", None)
        return f"PaimonDataSource({table_path})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        partition_key_names = set(self._table.partition_keys)
        return [PartitionField.create(f) for f in self._schema if f.name in partition_key_names]

    def _read_table_for_scan(self) -> FileStoreTable:
        if self._has_blob_columns:
            return self._table.copy({"blob-as-descriptor": "true"})
        return self._table

    def _scan_read_builder(
        self,
        table: FileStoreTable,
        read_pushdowns: _ReadPushdownState,
    ) -> Any:
        read_builder = table.new_read_builder()

        if read_pushdowns.requested_columns is not None:
            read_builder = read_builder.with_projection(read_pushdowns.requested_columns)

        if read_pushdowns.source_limit is not None:
            read_builder = read_builder.with_limit(read_pushdowns.source_limit)

        if read_pushdowns.planning_predicate is not None:
            read_builder = read_builder.with_filter(read_pushdowns.planning_predicate)
            logger.debug(
                "Applied Paimon filter pushdown predicate: %s",
                read_pushdowns.planning_predicate,
            )

        return read_builder

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        read_table = self._read_table_for_scan()
        read_pushdowns = self._read_pushdown_state(read_table, pushdowns)
        read_builder = self._scan_read_builder(read_table, read_pushdowns)

        if self._table.partition_keys and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partition keys %s but no partition filter was specified. "
                "This will result in a full table scan.",
                self.name,
                list(self._table.partition_keys),
            )

        plan = read_builder.new_scan().plan()

        pv_cache: dict[tuple[tuple[str, Any], ...], RecordBatch | None] = {}
        schema_incompatibility_cache: dict[int, bool] = {}

        for split in plan.splits():
            if self._partition_filter_skips_split(split, pushdowns, pv_cache):
                continue

            has_deletion_vectors = self._split_has_deletion_vectors(split)
            has_auth = self._split_has_auth(split)
            routing = self._reader_routing(
                raw_convertible=split.raw_convertible,
                has_deletion_vectors=has_deletion_vectors,
                has_auth=has_auth,
            )

            native_files = (
                split.files
                if routing.use_native_reader
                else self._blob_table_native_files(
                    split.files, read_pushdowns.task_columns, has_deletion_vectors
                )
            )
            if native_files is not None and self._has_incompatible_file_schema(
                read_table,
                [data_file.schema_id for data_file in native_files],
                read_pushdowns.task_columns,
                schema_incompatibility_cache,
            ):
                native_files = None
                routing = self._reader_routing(
                    raw_convertible=split.raw_convertible,
                    has_deletion_vectors=has_deletion_vectors,
                    has_auth=has_auth,
                    has_incompatible_schema=True,
                )

            if native_files is not None:
                task_schema = (
                    self._schema
                    if routing.use_native_reader
                    else self._project_schema(read_pushdowns.task_columns)
                )
                pv = None
                if self._table.partition_keys:
                    pv = self._partition_values(split, pv_cache)

                for data_file in native_files:
                    file_uri = self._build_file_uri(self._data_file_path(data_file))
                    yield DataSourceTask.parquet(
                        path=file_uri,
                        schema=task_schema,
                        pushdowns=pushdowns,
                        num_rows=data_file.row_count,
                        size_bytes=data_file.file_size,
                        partition_values=pv,
                        storage_config=self._storage_config,
                    )
            else:
                logger.debug(
                    "Split with %d files using pypaimon fallback (%s).",
                    len(split.files),
                    routing.fallback_reason,
                )
                yield _PaimonPKSplitTask(
                    self._table_catalog_options,
                    self._table_identifier,
                    self._table_path,
                    _extract_table_options(read_table),
                    split,
                    self._project_schema(read_pushdowns.task_columns),
                    read_pushdowns.read_columns,
                    read_pushdowns.source_limit,
                    read_pushdowns.reader_predicate,
                    read_pushdowns.task_columns,
                    self._scalar_blob_column_names,
                    self._explicit_io_config_bytes,
                    self._array_blob_column_names,
                )

    def explain_scan(self, pushdowns: Pushdowns, verbose: bool = False) -> PaimonScanExplain:
        read_table = self._read_table_for_scan()
        read_pushdowns = self._read_pushdown_state(read_table, pushdowns)
        read_builder = self._scan_read_builder(read_table, read_pushdowns)

        paimon_scan = read_builder.explain(verbose=True)
        split_details = paimon_scan.splits or []

        native_split_count = 0
        native_file_count = 0
        fallback_split_count = 0
        fallback_file_count = 0
        fallback_reasons: dict[str, int] = {}
        explained_splits: list[PaimonReaderSplitExplain] | None = [] if verbose else None
        pv_cache: dict[tuple[tuple[str, Any], ...], RecordBatch | None] = {}
        schema_incompatibility_cache: dict[int, bool] = {}

        for split in split_details:
            if self._partition_filter_skips_explain_split(split, pushdowns, pv_cache):
                continue

            routing = self._reader_routing(
                raw_convertible=split.raw_convertible,
                has_deletion_vectors=split.has_deletion_vectors,
                has_auth=paimon_scan.has_auth,
            )
            blob_native_files = (
                None
                if routing.use_native_reader
                else self._blob_table_native_files(
                    getattr(split, "data_files", None) or [],
                    read_pushdowns.task_columns,
                    split.has_deletion_vectors,
                )
            )
            candidate_files = (
                getattr(split, "data_files", None)
                if routing.use_native_reader
                else blob_native_files
            )
            candidate_schema_ids = [
                data_file.schema_id for data_file in candidate_files or []
            ]
            if candidate_schema_ids and self._has_incompatible_file_schema(
                read_table,
                candidate_schema_ids,
                read_pushdowns.task_columns,
                schema_incompatibility_cache,
            ):
                blob_native_files = None
                routing = self._reader_routing(
                    raw_convertible=split.raw_convertible,
                    has_deletion_vectors=split.has_deletion_vectors,
                    has_auth=paimon_scan.has_auth,
                    has_incompatible_schema=True,
                )

            # For a blob-native split only the covering parquet files are read
            # natively; report their counts so the verbose per-split detail
            # matches the native_parquet_file_count aggregate (the skipped
            # .blob / .vector files must not appear as natively read). The
            # Paimon split row_count sums every bunch file, so it double-counts
            # the same rows across the parquet and .blob bunches; the parquet
            # reader only returns the covering files' rows.
            split_file_count = split.file_count
            split_file_size = split.file_size
            split_file_paths = split.file_paths
            split_row_count = split.row_count

            if routing.use_native_reader or blob_native_files is not None:
                native_split_count += 1
                if blob_native_files is not None:
                    split_file_count = len(blob_native_files)
                    split_file_size = sum(f.file_size for f in blob_native_files)
                    split_file_paths = [
                        f.file_path for f in blob_native_files if f.file_path is not None
                    ]
                    split_row_count = sum(f.row_count for f in blob_native_files)
                native_file_count += split_file_count
                reader_mode = READER_MODE_NATIVE_PARQUET
                fallback_reason = None
            else:
                fallback_split_count += 1
                fallback_file_count += split_file_count
                reader_mode = routing.reader_mode
                fallback_reason = routing.fallback_reason
                reason = routing.fallback_reason or "unknown"
                fallback_reasons[reason] = fallback_reasons.get(reason, 0) + 1

            if explained_splits is not None:
                explained_splits.append(
                    PaimonReaderSplitExplain(
                        partition=split.partition,
                        bucket=split.bucket,
                        file_count=split_file_count,
                        row_count=split_row_count,
                        file_size=split_file_size,
                        reader_mode=reader_mode,
                        fallback_reason=fallback_reason,
                        file_paths=split_file_paths,
                    )
                )

        if not verbose:
            paimon_scan = replace(paimon_scan, splits=None)

        pushed_filters, remaining_filters = self._filter_pushdown_explain(pushdowns)
        return PaimonScanExplain(
            paimon_scan=paimon_scan,
            native_parquet_split_count=native_split_count,
            native_parquet_file_count=native_file_count,
            pypaimon_fallback_split_count=fallback_split_count,
            pypaimon_fallback_file_count=fallback_file_count,
            fallback_reasons=fallback_reasons,
            pushed_filters=pushed_filters,
            remaining_filters=remaining_filters,
            partition_filters=self._format_partition_filters(pushdowns),
            requested_columns=read_pushdowns.requested_columns,
            task_columns=read_pushdowns.task_columns,
            fallback_read_columns=read_pushdowns.read_columns,
            requested_limit=pushdowns.limit,
            source_limit=read_pushdowns.source_limit,
            limit_pushed=pushdowns.limit is not None and read_pushdowns.source_limit == pushdowns.limit,
            splits=explained_splits,
        )

    def _reader_routing(
        self,
        raw_convertible: bool,
        has_deletion_vectors: bool,
        has_auth: bool = False,
        has_incompatible_schema: bool = False,
    ) -> _ReaderRouting:
        can_use_native_reader = (
            self._is_parquet
            and not self._has_blob_columns
            and raw_convertible
            and not has_deletion_vectors
            and not has_auth
            and not has_incompatible_schema
        )
        if can_use_native_reader:
            return _ReaderRouting(READER_MODE_NATIVE_PARQUET, None)

        if not self._is_parquet:
            reason = "non-parquet format"
        elif has_incompatible_schema:
            reason = "schema evolution requires PyPaimon normalization"
        elif self._has_blob_columns:
            reason = "blob columns present"
        elif has_auth:
            reason = "query auth active"
        elif has_deletion_vectors:
            reason = "deletion vectors present"
        elif not raw_convertible:
            reason = (
                "LSM merge required"
                if self._table.is_primary_key_table
                else "data-evolution merge required"
            )
        else:
            reason = "data-evolution merge required"
        return _ReaderRouting(READER_MODE_PYPAIMON_FALLBACK, reason)

    def _blob_table_native_files(
        self,
        files: list[DataFileMeta],
        task_columns: list[str] | None,
        has_deletion_vectors: bool,
    ) -> list[DataFileMeta] | None:
        """Files of a blob-table split that can be read via the native parquet
        reader because no BLOB column is projected, or ``None`` to keep the
        pypaimon fallback. Only applies to non-PK parquet blob tables without
        deletion vectors and with an explicit projection."""
        if (
            not self._has_blob_columns
            or not self._is_parquet
            or has_deletion_vectors
            or self._table.is_primary_key_table
            or task_columns is None
        ):
            return None
        blob_column_names = self._scalar_blob_column_names | self._array_blob_column_names
        return _blob_native_covering_files(
            files, task_columns, blob_column_names, self._table.partition_keys
        )

    @staticmethod
    def _has_incompatible_file_schema(
        table: FileStoreTable,
        schema_ids: list[int],
        task_columns: list[str] | None,
        cache: dict[int, bool],
    ) -> bool:
        current_schema = table.table_schema
        if task_columns is None:
            current_fields = current_schema.fields
        else:
            task_column_set = set(task_columns)
            current_fields = [
                field
                for field in current_schema.fields
                if field.name in task_column_set
            ]
        for schema_id in schema_ids:
            if schema_id not in cache:
                file_schema = (
                    current_schema
                    if schema_id == current_schema.id
                    else table.schema_manager.get_schema(schema_id)
                )
                cache[schema_id] = not _native_read_fields_compatible(
                    file_schema.fields,
                    current_fields,
                )
            if cache[schema_id]:
                return True
        return False

    @staticmethod
    def _split_has_deletion_vectors(split: Split) -> bool:
        deletion_files = getattr(split, "data_deletion_files", None)
        return deletion_files is not None and any(df is not None for df in deletion_files)

    @staticmethod
    def _split_has_auth(split) -> bool:
        return isinstance(split, QueryAuthSplit)

    def _partition_filter_skips_split(
        self,
        split: Split,
        pushdowns: Pushdowns,
        pv_cache: dict[tuple[tuple[str, Any], ...], RecordBatch | None],
    ) -> bool:
        if not self._table.partition_keys or pushdowns.partition_filters is None:
            return False
        pv = self._partition_values(split, pv_cache)
        return self._partition_filter_skips_values(pv, pushdowns)

    def _partition_filter_skips_explain_split(
        self,
        split: ExplainSplitInfo,
        pushdowns: Pushdowns,
        pv_cache: dict[tuple[tuple[str, Any], ...], RecordBatch | None],
    ) -> bool:
        if not self._table.partition_keys or pushdowns.partition_filters is None:
            return False
        pv = self._partition_values_from_dict(split.partition, pv_cache)
        return self._partition_filter_skips_values(pv, pushdowns)

    @staticmethod
    def _partition_filter_skips_values(
        partition_values: RecordBatch | None,
        pushdowns: Pushdowns,
    ) -> bool:
        return (
            partition_values is not None
            and len(partition_values.filter(ExpressionsProjection([pushdowns.partition_filters]))) == 0
        )

    def _format_partition_filters(self, pushdowns: Pushdowns) -> list[str]:
        if pushdowns.partition_filters is None:
            return []
        return self._format_pyexprs([getattr(pushdowns.partition_filters, "_expr", pushdowns.partition_filters)])

    def _filter_pushdown_explain(self, pushdowns: Pushdowns) -> tuple[list[str], list[str]]:
        if pushdowns.filters is None:
            return [], []

        py_expr = getattr(pushdowns.filters, "_expr", pushdowns.filters)
        pushed_filters, remaining_filters, _ = convert_filters_to_paimon(self._table, [py_expr])
        return self._format_pyexprs(pushed_filters), self._format_pyexprs(remaining_filters)

    @staticmethod
    def _format_pyexprs(py_exprs: list[PyExpr]) -> list[str]:
        from daft.expressions import Expression

        result = []
        for py_expr in py_exprs:
            try:
                result.append(str(Expression._from_pyexpr(py_expr)))
            except Exception:
                result.append(str(py_expr))
        return result

    def _build_file_uri(self, file_path: str) -> str:
        """Reconstruct a full URI from a (potentially scheme-stripped) file_path."""
        if urlparse(file_path).scheme:
            return file_path
        if self._warehouse_scheme:
            return f"{self._warehouse_scheme}://{file_path}"
        return f"file://{file_path}"

    @staticmethod
    def _data_file_path(data_file: DataFileMeta) -> str:
        return data_file.external_path if data_file.external_path else data_file.file_path

    def _build_partition_values(self, split: Split) -> daft.recordbatch.RecordBatch | None:
        """Build a single-row RecordBatch encoding the partition values for a split."""
        return self._build_partition_values_from_dict(split.partition.to_dict())

    def _partition_values(
        self,
        split: Split,
        pv_cache: dict[tuple[tuple[str, Any], ...], RecordBatch | None],
    ) -> RecordBatch | None:
        return self._partition_values_from_dict(split.partition.to_dict(), pv_cache)

    def _partition_values_from_dict(
        self,
        partition_dict: dict[str, Any],
        pv_cache: dict[tuple[tuple[str, Any], ...], RecordBatch | None],
    ) -> RecordBatch | None:
        pv_key = tuple(sorted(partition_dict.items()))
        if pv_key not in pv_cache:
            pv_cache[pv_key] = self._build_partition_values_from_dict(partition_dict)
        return pv_cache[pv_key]

    def _build_partition_values_from_dict(self, partition_dict: dict[str, Any]) -> daft.recordbatch.RecordBatch | None:
        if not self._table.partition_keys:
            return None

        arrays: dict[str, daft.Series] = {}
        for pfield in self._table.partition_keys_fields:
            value = partition_dict.get(pfield.name)
            arrow_type = self._partition_field_arrow_types[pfield.name]
            arrays[pfield.name] = daft.Series.from_arrow(pa.array([value], type=arrow_type), name=pfield.name)

        if not arrays:
            return None
        return daft.recordbatch.RecordBatch.from_pydict(arrays)

    def _valid_output_columns(self, columns: list[str] | None) -> list[str] | None:
        if columns is None:
            return None
        schema_names = {field.name for field in self._schema}
        return [name for name in columns if name in schema_names]

    def _task_columns(
        self,
        table: FileStoreTable,
        output_columns: list[str] | None,
        pushdowns: Pushdowns,
    ) -> list[str] | None:
        if output_columns is None:
            return None

        task_columns = list(output_columns)
        filter_required_column_names = getattr(pushdowns, "filter_required_column_names", None)
        required_fields = filter_required_column_names() if filter_required_column_names else set()
        return self._append_existing_columns(table, task_columns, required_fields)

    def _fallback_read_columns(
        self,
        table: FileStoreTable,
        task_columns: list[str] | None,
        paimon_predicate: Predicate | None,
    ) -> list[str] | None:
        if task_columns is None:
            return None

        read_columns = list(task_columns)
        if paimon_predicate is not None:
            from pypaimon.read.push_down_utils import _get_all_fields

            return self._append_existing_columns(table, read_columns, _get_all_fields(paimon_predicate))

        return read_columns

    @staticmethod
    def _append_existing_columns(
        table: FileStoreTable,
        columns: list[str],
        required_fields: set[str],
    ) -> list[str]:
        if not required_fields:
            return columns
        existing = set(columns)
        columns.extend(
            field.name
            for field in table.fields
            if field.name in required_fields and field.name not in existing
        )
        return columns

    def _project_schema(self, columns: list[str] | None) -> Schema:
        if columns is None:
            return self._schema
        field_map = {field.name: field for field in self._schema}
        return Schema.from_field_name_and_types(
            [(name, field_map[name].dtype) for name in columns if name in field_map]
        )

    def _read_pushdown_state(
        self,
        table: FileStoreTable,
        pushdowns: Pushdowns,
    ) -> _ReadPushdownState:
        reader_predicate, filters_consumed = self._pushdown_filter_state(pushdowns)
        planning_predicate = self._planning_predicate(reader_predicate)
        # Partition filters arrive on a separate Daft channel (pushdowns.
        # partition_filters), not in pushdowns.filters. Convert and AND them in
        # so plan() prunes partitions at the manifest level; otherwise plan()
        # enumerates every split and we skip in Python -- a full-table plan.
        planning_predicate = self._and_predicates(
            planning_predicate, self._partition_planning_predicate(pushdowns))
        requested_columns = self._valid_output_columns(pushdowns.columns)
        task_columns = self._task_columns(table, requested_columns, pushdowns)
        read_columns = self._fallback_read_columns(table, task_columns, reader_predicate)
        source_limit = self._source_limit(
            pushdowns,
            reader_predicate,
            planning_predicate,
            filters_consumed,
        )
        return _ReadPushdownState(
            reader_predicate=reader_predicate,
            planning_predicate=planning_predicate,
            requested_columns=requested_columns,
            task_columns=task_columns,
            read_columns=read_columns,
            source_limit=source_limit,
        )

    def _pushdown_filter_state(self, pushdowns: Pushdowns) -> tuple[Predicate | None, bool]:
        if pushdowns.filters is None:
            return None, True

        py_expr = getattr(pushdowns.filters, "_expr", pushdowns.filters)
        _, remaining_filters, paimon_predicate = convert_filters_to_paimon(self._table, [py_expr])
        return paimon_predicate, not remaining_filters

    def _planning_predicate(self, pushdown_predicate: Predicate | None) -> Predicate | None:
        if pushdown_predicate is None:
            return None
        if not self._can_plan_predicate(pushdown_predicate):
            return None
        return pushdown_predicate

    def _partition_planning_predicate(self, pushdowns: Pushdowns) -> Predicate | None:
        """partition_filters -> Paimon predicate for plan-time pruning. Excludes
        isNull (_predicate_contains_is_null): pruning drops the whole null
        partition, which the post-filter can't restore -- so exclude it even for
        PK tables (stricter than the row path's _can_plan_predicate)."""
        partition_filters = getattr(pushdowns, "partition_filters", None)
        if partition_filters is None:
            return None
        py_expr = getattr(partition_filters, "_expr", partition_filters)
        _, remaining, paimon_predicate = convert_filters_to_paimon(self._table, [py_expr])
        if remaining:
            # Unconverted parts still apply via _partition_filter_skips_split.
            logger.debug("Partition filter not pushed to plan: %s", remaining)
        if paimon_predicate is None or self._predicate_contains_is_null(paimon_predicate):
            return None  # isNull -> post-filter only (see docstring)
        return paimon_predicate

    @staticmethod
    def _and_predicates(left: Predicate | None, right: Predicate | None) -> Predicate | None:
        if left is None:
            return right
        if right is None:
            return left
        from pypaimon.common.predicate_builder import PredicateBuilder
        return PredicateBuilder.and_predicates([left, right])

    @staticmethod
    def _source_limit(
        pushdowns: Pushdowns,
        reader_predicate: Predicate | None,
        planning_predicate: Predicate | None,
        filters_consumed: bool,
    ) -> int | None:
        if pushdowns.limit is None:
            return None
        if pushdowns.partition_filters is not None:
            return None
        if not filters_consumed:
            return None
        if reader_predicate is not None and planning_predicate is None:
            return None
        return pushdowns.limit

    def _can_plan_predicate(self, predicate: Predicate) -> bool:
        # Missing value null-count stats make isNull unsafe for scan planning.
        if not self._predicate_contains_is_null(predicate):
            return True
        return self._table.is_primary_key_table and not self._table.options.deletion_vectors_enabled()

    def _predicate_contains_is_null(self, predicate: Predicate) -> bool:
        if predicate.method == "isNull":
            return True
        if predicate.method in ("and", "or"):
            return any(self._predicate_contains_is_null(child) for child in predicate.literals or [])
        return False
