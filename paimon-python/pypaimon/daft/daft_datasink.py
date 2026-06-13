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

from typing import TYPE_CHECKING, Any

import pyarrow as pa
from daft.datatype import DataType
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.table.file_store_table import FileStoreTable


_PaimonIdentifier = tuple[str, str, str | None]


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


class PaimonDataSink(DataSink[list[Any]]):
    """DataSink for writing data to an Apache Paimon table.

    Delegates all file I/O and commit logic to pypaimon's BatchTableWrite /
    BatchTableCommit APIs. Each `write()` call (one per parallel worker) opens
    an independent BatchTableWrite, accumulates micro-partitions, then calls
    `prepare_commit()` to obtain CommitMessage objects. `finalize()` gathers
    all CommitMessages from all workers and performs a single atomic commit.
    """

    def __init__(self, table: FileStoreTable, mode: str = "append") -> None:
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
        finally:
            table_write.close()

        yield WriteResult(
            result=list(commit_messages),
            bytes_written=total_bytes,
            rows_written=total_rows,
        )

    def finalize(self, write_results: list[WriteResult[list[Any]]]) -> MicroPartition:
        all_commit_messages = [msg for wr in write_results for msg in wr.result]

        table_commit = self._write_builder.new_commit()
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
