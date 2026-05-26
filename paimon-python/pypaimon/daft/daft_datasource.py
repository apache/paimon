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

from dataclasses import dataclass
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
from pypaimon.daft.daft_predicate_visitor import convert_filters_to_paimon

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pypaimon.common.predicate import Predicate
    from pypaimon.read.table_read import TableRead
    from pypaimon.read.split import Split
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import PyExpr, StorageConfig
    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)

PAIMON_FILE_FORMAT_PARQUET = "parquet"
PAIMON_FILE_FORMAT_ORC = "orc"
PAIMON_FILE_FORMAT_AVRO = "avro"


@dataclass(frozen=True, slots=True)
class _ReadPushdownState:
    reader_predicate: Predicate | None
    planning_predicate: Predicate | None
    requested_columns: list[str] | None
    task_columns: list[str] | None
    read_columns: list[str] | None
    source_limit: int | None


class _PaimonPKSplitTask(DataSourceTask):
    """DataSourceTask for PK-table splits that require LSM-tree merge.

    Used when split.raw_convertible is False (overlapping levels exist) or
    when the file format is not Parquet (ORC, Avro). Delegates to pypaimon's
    native reader which handles LSM merging internally.
    """

    def __init__(
        self,
        table_read: TableRead,
        split: Split,
        schema: Schema,
        output_columns: list[str] | None = None,
        blob_column_names: set[str] | None = None,
    ) -> None:
        self._table_read = table_read
        self._split = split
        self._schema = schema
        self._output_columns = output_columns
        self._blob_column_names = blob_column_names or set()

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        reader = self._table_read.to_arrow_batch_reader([self._split])
        for batch in iter(reader.read_next_batch, None):
            if self._output_columns is not None:
                batch = batch.select(self._output_columns)
            if self._blob_column_names:
                batch = _convert_blob_columns(batch, self._blob_column_names)
            rb = RecordBatch.from_arrow_record_batches([batch], batch.schema)
            if self._blob_column_names:
                rb = _cast_blob_columns_to_file(rb, self._blob_column_names)
            yield rb


def _convert_blob_columns(batch: pa.RecordBatch, blob_column_names: set[str]) -> pa.RecordBatch:
    """Replace serialized BlobDescriptor columns with the File physical struct layout."""
    from pypaimon.daft.daft_blob import FILE_PHYSICAL_TYPE, blob_column_to_file_array

    arrays = []
    fields = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        if field.name in blob_column_names and (pa.types.is_large_binary(field.type) or pa.types.is_binary(field.type)):
            arrays.append(blob_column_to_file_array(col))
            fields.append(pa.field(field.name, FILE_PHYSICAL_TYPE, nullable=field.nullable))
        else:
            arrays.append(col)
            fields.append(field)
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _cast_blob_columns_to_file(rb: RecordBatch, blob_column_names: set[str]) -> RecordBatch:
    """Cast struct-typed blob columns in a RecordBatch to DataType.file()."""
    from daft.datatype import DataType

    file_dtype = DataType.file()
    columns = {}
    for i, field in enumerate(rb.schema()):
        col = rb.get_column(i)
        if field.name in blob_column_names:
            col = col.cast(file_dtype)
        columns[field.name] = col
    return RecordBatch.from_pydict(columns)


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
    ) -> None:
        self._table = table
        self._storage_config = storage_config
        self._catalog_options = catalog_options

        from pypaimon.schema.data_types import PyarrowFieldParser

        pa_schema = PyarrowFieldParser.from_paimon_schema(table.fields)

        self._blob_column_names: set[str] = {field.name for field in pa_schema if pa.types.is_large_binary(field.type)}
        self._has_blob_columns = bool(self._blob_column_names)

        if self._has_blob_columns:
            require_file_range_reads()
            from daft.datatype import DataType

            base_schema = Schema.from_pyarrow_schema(pa_schema)
            fields = []
            for f in base_schema:
                if f.name in self._blob_column_names:
                    fields.append((f.name, DataType.file()))
                else:
                    fields.append((f.name, f.dtype))
            self._schema = Schema.from_field_name_and_types(fields)
        else:
            self._schema = Schema.from_pyarrow_schema(pa_schema)

        warehouse = catalog_options.get("warehouse", "")
        self._warehouse_scheme = urlparse(warehouse).scheme

        self._file_format = table.options.file_format().lower()
        self._is_parquet = self._file_format == PAIMON_FILE_FORMAT_PARQUET

        self._partition_field_arrow_types: dict[str, pa.DataType] = (
            {f.name: PyarrowFieldParser.from_paimon_type(f.type) for f in table.partition_keys_fields}
            if table.partition_keys
            else {}
        )

        self._paimon_predicate: Predicate | None = None
        self._remaining_filters: list[PyExpr] | None = None

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

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        """Push filters down to Paimon scan.

        Converts Daft expressions to Paimon predicates where possible.
        Returns (pushed_filters, remaining_filters).
        """
        pushed_filters, remaining_filters, paimon_predicate = convert_filters_to_paimon(self._table, filters)

        self._paimon_predicate = paimon_predicate
        self._remaining_filters = remaining_filters

        if pushed_filters:
            logger.debug(
                "Paimon filter pushdown: %d filters pushed, %d remaining",
                len(pushed_filters),
                len(remaining_filters),
            )

        return pushed_filters, remaining_filters

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        read_table = self._table
        if self._has_blob_columns:
            read_table = self._table.copy({"blob-as-descriptor": "true"})

        read_builder = read_table.new_read_builder()
        read_pushdowns = self._read_pushdown_state(read_table, pushdowns)

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

        if self._table.partition_keys and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partition keys %s but no partition filter was specified. "
                "This will result in a full table scan.",
                self.name,
                list(self._table.partition_keys),
            )

        plan = read_builder.new_scan().plan()

        pv_cache: dict[tuple[Any, ...], RecordBatch | None] = {}

        for split in plan.splits():
            if self._table.partition_keys and pushdowns.partition_filters is not None:
                pv_key = tuple(sorted(split.partition.to_dict().items()))
                if pv_key not in pv_cache:
                    pv_cache[pv_key] = self._build_partition_values(split)
                pv = pv_cache[pv_key]
                if pv is not None and len(pv.filter(ExpressionsProjection([pushdowns.partition_filters]))) == 0:
                    continue

            _deletion_files = getattr(split, "data_deletion_files", None)
            has_deletion_vectors = _deletion_files is not None and any(df is not None for df in _deletion_files)

            can_use_native_reader = (
                self._is_parquet
                and not self._has_blob_columns
                and (not self._table.is_primary_key_table or split.raw_convertible)
                and not has_deletion_vectors
            )

            if can_use_native_reader:
                pv = None
                if self._table.partition_keys:
                    pv_key = tuple(sorted(split.partition.to_dict().items()))
                    if pv_key not in pv_cache:
                        pv_cache[pv_key] = self._build_partition_values(split)
                    pv = pv_cache[pv_key]

                for data_file in split.files:
                    file_uri = self._build_file_uri(data_file.file_path)
                    yield DataSourceTask.parquet(
                        path=file_uri,
                        schema=self._schema,
                        pushdowns=pushdowns,
                        num_rows=data_file.row_count,
                        size_bytes=data_file.file_size,
                        partition_values=pv,
                        storage_config=self._storage_config,
                    )
            else:
                if not self._is_parquet:
                    reason = "non-parquet format"
                elif self._has_blob_columns:
                    reason = "blob columns present"
                elif has_deletion_vectors:
                    reason = "deletion vectors present"
                else:
                    reason = "LSM merge required"
                logger.debug(
                    "Split with %d files using pypaimon fallback (%s).",
                    len(split.files),
                    reason,
                )
                yield _PaimonPKSplitTask(
                    self._fallback_read_builder(
                        read_table,
                        read_pushdowns.read_columns,
                        read_pushdowns.source_limit,
                        read_pushdowns.reader_predicate,
                    ).new_read(),
                    split,
                    self._project_schema(read_pushdowns.task_columns),
                    read_pushdowns.task_columns,
                    self._blob_column_names,
                )

    def _build_file_uri(self, file_path: str) -> str:
        """Reconstruct a full URI from a (potentially scheme-stripped) file_path."""
        if self._warehouse_scheme:
            return f"{self._warehouse_scheme}://{file_path}"
        return f"file://{file_path}"

    def _build_partition_values(self, split: Split) -> daft.recordbatch.RecordBatch | None:
        """Build a single-row RecordBatch encoding the partition values for a split."""
        if not self._table.partition_keys:
            return None

        partition_dict = split.partition.to_dict()
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

    def _fallback_read_builder(
        self,
        table: FileStoreTable,
        read_columns: list[str] | None,
        limit: int | None,
        predicate: Predicate | None,
    ) -> Any:
        read_builder = table.new_read_builder()
        if read_columns is not None:
            read_builder = read_builder.with_projection(read_columns)
        if limit is not None:
            read_builder = read_builder.with_limit(limit)
        if predicate is not None:
            read_builder = read_builder.with_filter(predicate)
        return read_builder

    def _read_pushdown_state(
        self,
        table: FileStoreTable,
        pushdowns: Pushdowns,
    ) -> _ReadPushdownState:
        reader_predicate, filters_consumed = self._pushdown_filter_state(pushdowns)
        planning_predicate = self._planning_predicate(reader_predicate)
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
        if self._remaining_filters is not None:
            return self._paimon_predicate, not self._remaining_filters
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
        if self._paimon_predicate is not None or self._requires_fallback_reader():
            return pushdown_predicate
        return None

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

    def _requires_fallback_reader(self) -> bool:
        return not self._is_parquet or self._has_blob_columns or self._table.is_primary_key_table

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
