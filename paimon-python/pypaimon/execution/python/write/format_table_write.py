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

import io
import uuid
from collections import defaultdict
from typing import Dict, List, Optional

import pyarrow

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.execution.python.write.partition_spec import (
    _UNREPRESENTABLE_PARTITION_VALUE, _partition_spec_value, _partition_value_to_string)
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.format.format_commit_message import (
    FormatTableCommitMessage,
)
from pypaimon.table.format.format_table import (
    Format,
    FormatTable,
)
from pypaimon.execution.python.write.writer.parquet_writer_options import create_parquet_writer_options
from pypaimon.utils.file_store_path_factory import _escape_partition_component, _is_null_or_whitespace_only


def _partition_component(value: Optional[str], default_part_value: str) -> Optional[str]:
    """The unescaped name of a partition directory."""
    return default_part_value if _is_null_or_whitespace_only(value) else value


def _partition_path(
    partition_spec: dict, partition_keys: List[str], only_value: bool, default_part_value: str
) -> str:
    parts = []
    for k in partition_keys:
        v = _partition_component(partition_spec.get(k), default_part_value)
        if not v or (only_value and v in ('.', '..')):
            raise ValueError(f"Partition value '{v}' cannot be used as a partition path component.")
        if only_value and v != default_part_value and v.startswith(('_', '.')):
            raise ValueError(
                f"Partition value '{v}' cannot be written in a value-only partition "
                "layout: scans treat a directory whose name starts with '_' or "
                "'.' as hidden and would silently drop this partition and all "
                "of its data. Use a partition value that does not start with "
                "'_' or '.', or switch to the key-value partition layout."
            )
        value = _escape_partition_component(v)
        parts.append(value if only_value else f"{_escape_partition_component(k)}={value}")
    return "/".join(parts)


def _validate_partition_columns(
    partition_keys: List[str],
    data: pyarrow.RecordBatch,
) -> None:
    """Raise if partition key missing from data (wrong column indexing)."""
    names = set(data.schema.names) if data.schema else set()
    missing = [k for k in partition_keys if k not in names]
    if missing:
        raise ValueError(
            f"Partition column(s) missing from input data: {missing}. "
            f"Data columns: {list(names)}. "
            "Ensure partition keys exist in the Arrow schema."
        )


def _partition_from_row(
    row: pyarrow.RecordBatch,
    partition_keys: List[str],
    row_index: int,
) -> tuple:
    out = []
    for k in partition_keys:
        col = row.column(row.schema.get_field_index(k))
        val = col[row_index]
        is_none = val is None or (
            hasattr(val, "as_py") and val.as_py() is None
        )
        if is_none:
            out.append(None)
        else:
            out.append(val.as_py() if hasattr(val, "as_py") else val)
    return tuple(out)


def _write_arrow(writer, data: pyarrow.Table) -> None:
    """Fan out through the public batch hook, including caller overrides."""
    for batch in data.to_batches():
        writer.write_arrow_batch(batch)


def _write_pandas(writer, df) -> None:
    pa_schema = PyarrowFieldParser.from_paimon_schema(writer.table.fields)
    batch = pyarrow.RecordBatch.from_pandas(df, schema=pa_schema)
    writer.write_arrow_batch(batch)


class PythonFormatTableWrite:
    """Batch write for format table: Arrow/Pandas to partition dirs."""

    def __init__(
        self,
        table: FormatTable,
        overwrite: bool = False,
        static_partitions: Optional[Dict[str, str]] = None,
    ):
        self.table = table
        self._overwrite = overwrite
        self._static_partitions = (
            static_partitions if static_partitions is not None else {}
        )
        self._written_paths: List[str] = []
        self._overwritten_dirs: set = set()
        opt = table.options().get(
            "format-table.partition-path-only-value", "false"
        )
        self._partition_only_value = opt.lower() == "true"
        self._file_format = table.format()
        options = CoreOptions.from_dict(table.options())
        self._default_part_value = options.options.get(CoreOptions.PARTITION_DEFAULT_NAME)
        self._legacy_partition_name = options.options.get(CoreOptions.PARTITION_GENERATE_LEGACY_NAME)
        self._partition_types = {f.name: f.type for f in table.fields if f.name in table.partition_keys}
        for data_type in self._partition_types.values():
            _partition_value_to_string(None, data_type, self._legacy_partition_name)
        # A static partition names directories, so it is compared by the directory names: 'true' names the
        # directory of True, NaN names the directory of NaN, and 0.0 and -0.0 name different directories.
        self._static_partition_components = {
            key: self._static_partition_component(key, value) for key, value in self._static_partitions.items()
        }
        # Files leave the partition columns out, as Java's do. A table whose columns are all partition columns
        # would then write files without columns, which lose their rows: CSV and JSON files keep its partition
        # columns instead, and the other formats refuse it.
        only_partitions = bool(table.fields) and all(f.name in table.partition_keys for f in table.fields)
        if only_partitions and self._file_format not in (Format.CSV, Format.JSON):
            raise ValueError(
                f"Format table writes of {self._file_format.value} require at least one non-partition column")
        self._files_keep_partition_columns = only_partitions
        self._parquet_writer_options = (
            create_parquet_writer_options(options)
            if self._file_format == Format.PARQUET else {}
        )
        self._target_file_row_num = options.target_file_row_num()
        max_target_file_row_num = (
            CoreOptions.TARGET_FILE_ROW_NUM.default_value()
        )
        if self._target_file_row_num < 1:
            raise ValueError(
                f"{CoreOptions.TARGET_FILE_ROW_NUM.key()} "
                "should be at least 1"
            )
        if self._target_file_row_num > max_target_file_row_num:
            raise ValueError(
                f"{CoreOptions.TARGET_FILE_ROW_NUM.key()} "
                f"should be at most {max_target_file_row_num}"
            )
        self._data_file_prefix = "data-"
        self._suffix = {
            "parquet": ".parquet",
            "csv": ".csv",
            "json": ".json",
            "orc": ".orc",
            "text": ".txt",
        }.get(self._file_format.value, ".parquet")

    def _static_partition_component(self, key, value):
        data_type = self._partition_types.get(key)
        if data_type is not None and value is not None and value != self._default_part_value:
            parsed = _partition_spec_value(value, data_type, legacy_ltz=self._legacy_partition_name)
            # A value that does not convert, such as a DECIMAL overflow, names no partition. It must not
            # select the null partition, whose files the overwrite would delete.
            if parsed is None or parsed is _UNREPRESENTABLE_PARTITION_VALUE:
                raise ValueError(f"Static partition value '{value}' cannot be represented as {data_type}")
            value = _partition_value_to_string(parsed, data_type, self._legacy_partition_name)
        return _partition_component(value, self._default_part_value)

    def write_arrow(self, data: pyarrow.Table) -> None:
        return _write_arrow(self, data)

    def write_arrow_batch(self, data: pyarrow.RecordBatch) -> None:
        partition_keys = self.table.partition_keys
        if not partition_keys:
            part_spec = {}
            self._write_single_batch(data, part_spec)
            return
        _validate_partition_columns(partition_keys, data)
        # Group rows by partition
        parts_to_indices = defaultdict(list)
        for i in range(data.num_rows):
            part = tuple(_partition_value_to_string(value, self._partition_types[key], self._legacy_partition_name)
                         for key, value in zip(partition_keys, _partition_from_row(data, partition_keys, i)))
            parts_to_indices[part].append(i)
        for indices in parts_to_indices.values():
            part_spec = dict(zip(partition_keys, _partition_from_row(data, partition_keys, indices[0])))
            sub = data.take(pyarrow.array(indices))
            self._write_single_batch(sub, part_spec)

    def write_pandas(self, df) -> None:
        return _write_pandas(self, df)

    def _write_single_batch(
        self,
        data: pyarrow.RecordBatch,
        partition_spec: dict,
    ) -> None:
        if data.num_rows == 0:
            return
        location = self.table.location()
        partition_only_value = self._partition_only_value
        components = {
            key: _partition_component(
                _partition_value_to_string(value, self._partition_types[key], self._legacy_partition_name),
                self._default_part_value)
            for key, value in partition_spec.items()
        }
        part_path = _partition_path(
            components,
            self.table.partition_keys,
            partition_only_value,
            self._default_part_value,
        )
        if part_path:
            if location.startswith('file:'):
                location = self.table.file_io.to_filesystem_path(location)
            dir_path = f"{location}/{part_path}"
        else:
            dir_path = location
        # When overwrite: clear partition dir only once per write session
        overwrite_this = (
            self._overwrite
            and dir_path not in self._overwritten_dirs
        )
        if overwrite_this:
            should_delete = (
                not self._static_partitions
                or all(
                    components.get(k) == v
                    for k, v in self._static_partition_components.items()
                )
            )
            if should_delete:
                if self.table.file_io.exists(dir_path):
                    from pypaimon.execution.python.write.format_table_commit import (
                        _delete_data_files_in_path,
                    )
                    _delete_data_files_in_path(self.table.file_io, dir_path)
                self._overwritten_dirs.add(dir_path)
        self.table.file_io.check_or_mkdirs(dir_path)

        if data.num_rows <= self._target_file_row_num:
            self._write_file(data, dir_path)
            return

        for offset in range(
                0, data.num_rows, self._target_file_row_num):
            self._write_file(
                data.slice(offset, self._target_file_row_num),
                dir_path,
            )

    def _write_file(
        self,
        data: pyarrow.RecordBatch,
        dir_path: str,
    ) -> None:
        file_name = f"{self._data_file_prefix}{uuid.uuid4().hex}{self._suffix}"
        path = f"{dir_path}/{file_name}"

        fmt = self._file_format
        tbl = pyarrow.Table.from_batches([data])
        if not self._files_keep_partition_columns:
            tbl = tbl.select([name for name in tbl.column_names if name not in self.table.partition_keys])
        if fmt == Format.PARQUET:
            import pyarrow.parquet as pq
            buf = io.BytesIO()
            pq.write_table(tbl, buf, compression="zstd", **self._parquet_writer_options)
            raw = buf.getvalue()
        elif fmt == Format.CSV:
            import pyarrow.csv as csv
            buf = io.BytesIO()
            csv.write_csv(tbl, buf)
            raw = buf.getvalue()
        elif fmt == Format.JSON:
            import json
            lines = []
            for i in range(tbl.num_rows):
                row = {
                    tbl.column_names[j]: tbl.column(j)[i].as_py()
                    for j in range(tbl.num_columns)
                }
                lines.append(json.dumps(row) + "\n")
            raw = "".join(lines).encode("utf-8")
        elif fmt == Format.ORC:
            import pyarrow.orc as orc
            buf = io.BytesIO()
            orc.write_table(tbl, buf)
            raw = buf.getvalue()
        elif fmt == Format.TEXT:
            pa_f0 = tbl.schema.field(0).type
            if tbl.num_columns != 1 or not pyarrow.types.is_string(pa_f0):
                raise ValueError(
                    "TEXT format only supports a single string column, "
                    f"got {tbl.num_columns} columns"
                )
            line_delimiter = self.table.options().get(
                "text.line-delimiter", "\n"
            )
            lines = []
            col = tbl.column(0)
            for i in range(tbl.num_rows):
                val = col[i]
                py_val = val.as_py() if hasattr(val, "as_py") else val
                line = "" if py_val is None else str(py_val)
                lines.append(line + line_delimiter)
            raw = "".join(lines).encode("utf-8")
        else:
            raise ValueError(f"Format table write not implemented for {fmt}")

        with self.table.file_io.new_output_stream(path) as out:
            out.write(raw)

        self._written_paths.append(path)

    def prepare_commit(self) -> List[FormatTableCommitMessage]:
        return [
            FormatTableCommitMessage(
                written_paths=list(self._written_paths)
            )
        ]

    def close(self) -> None:
        pass
