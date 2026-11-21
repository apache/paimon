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
import math
import uuid
from collections import defaultdict
from typing import Dict, List, Optional

import pyarrow
import pyarrow.fs as pafs

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.execution.python.read.format_table_scan import _is_data_file_name
from pypaimon.execution.python.write.partition_spec import (
    _UNREPRESENTABLE_PARTITION_VALUE, _partition_spec_value, _partition_value_to_string)
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.format.format_data_split import FormatDataSplit
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


_UNKNOWN_PARTITION = object()


def _earlier_partition_path(partition_spec: dict, partition_keys: List[str], only_value: bool) -> Optional[str]:
    """The directory where earlier versions wrote a partition's files: str() of the values, without escaping,
    up to the first null value."""
    parts = []
    for key in partition_keys:
        value = partition_spec.get(key)
        if value is None:
            break
        text = str(value)
        if only_value and text in ('', '.', '..'):
            return None
        parts.append(text if only_value else f"{key}={text}")
    return "/".join(parts)


def _same_stored_partition_value(stored, value) -> bool:
    if stored is None or value is None:
        return stored is None and value is None
    if isinstance(stored, float) and isinstance(value, float) and not (math.isnan(stored) or math.isnan(value)):
        # 0.0 and -0.0 are equal, but name different partitions.
        return stored == value and math.copysign(1.0, stored) == math.copysign(1.0, value)
    return _same_partition_value(stored, value)


def _same_partition_value(value, parsed) -> bool:
    if isinstance(value, float) and isinstance(parsed, float) and math.isnan(value) and math.isnan(parsed):
        return True
    return type(value) is type(parsed) and value == parsed


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


def _write_arrow(writer, data: pyarrow.Table, execution: 'PythonFormatTableWrite') -> None:
    """Fan out through the public batch hook, including caller overrides."""
    execution._check_overwrite_partitions(data)
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
            if not isinstance(value, str):
                # A typed value takes the form the partition column gives its rows, such as a DECIMAL's
                # scale: Decimal('1.0') names the directory 1.00 of a DECIMAL(4, 2) row.
                try:
                    parsed = pyarrow.scalar(parsed, type=PyarrowFieldParser.from_paimon_type(data_type)).as_py()
                except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError, OverflowError, TypeError) as e:
                    raise ValueError(
                        f"Static partition value {value!r} cannot be represented as {data_type}") from e
            value = _partition_value_to_string(parsed, data_type, self._legacy_partition_name)
            # A value whose directory name reads back as another value, such as non-UTF-8 BINARY, would
            # select that value's directory, and the overwrite would delete its files.
            self._check_partition_value_round_trips(key, parsed, value, {})
        return _partition_component(value, self._default_part_value)

    def write_arrow(self, data: pyarrow.Table) -> None:
        return _write_arrow(self, data, self)

    def _check_overwrite_partitions(self, data: pyarrow.Table) -> None:
        # An overwrite deletes a partition's files when it first writes to it. Check every partition of the
        # input first, so that a partition rejected in a later batch fails the write before it deletes any file.
        if self._overwrite and self.table.partition_keys:
            for batch in data.to_batches():
                self._partition_groups(batch)

    def write_arrow_batch(self, data: pyarrow.RecordBatch) -> None:
        partition_keys = self.table.partition_keys
        if not partition_keys:
            part_spec = {}
            self._write_single_batch(data, part_spec)
            return
        for part_spec, indices in self._partition_groups(data):
            sub = data.take(pyarrow.array(indices))
            self._write_single_batch(sub, part_spec)

    def _partition_groups(self, data: pyarrow.RecordBatch) -> List[tuple]:
        """Groups the rows by partition, checking every partition before any of them is written."""
        partition_keys = self.table.partition_keys
        _validate_partition_columns(partition_keys, data)
        parts_to_indices = defaultdict(list)
        parsed = {}
        for i in range(data.num_rows):
            part = []
            for key, value in zip(partition_keys, _partition_from_row(data, partition_keys, i)):
                text = _partition_value_to_string(value, self._partition_types[key], self._legacy_partition_name)
                if value is not None:
                    self._check_partition_value_round_trips(key, value, text, parsed)
                part.append(text)
            parts_to_indices[tuple(part)].append(i)
        groups = []
        for indices in parts_to_indices.values():
            part_spec = dict(zip(partition_keys, _partition_from_row(data, partition_keys, indices[0])))
            self._partition_dir(part_spec)
            groups.append((part_spec, indices))
        return groups

    def _check_partition_value_round_trips(self, key, value, text, parsed: dict) -> None:
        # Reads, partition filters and overwrites take a partition value from its directory name. A value that
        # the name does not keep, such as non-UTF-8 BINARY or a local time in a DST overlap, would read back as
        # another value and share its directory, even where files keep the partition columns. Null and blank
        # values name the default partition, as in Java.
        if _is_null_or_whitespace_only(text):
            return
        if text == self._default_part_value:
            raise ValueError(
                f"Partition value {value!r} of '{key}' cannot be written: it equals the default partition "
                f"name, which names the partition of null values, so it would read back as null. Use another "
                f"value or set {CoreOptions.PARTITION_DEFAULT_NAME.key()} to a name that no value uses.")
        if (key, text) not in parsed:
            try:
                parsed[key, text] = _partition_spec_value(
                    text, self._partition_types[key], legacy_ltz=self._legacy_partition_name)
            except ValueError:
                parsed[key, text] = _UNREPRESENTABLE_PARTITION_VALUE
        result = parsed[key, text]
        if not _same_partition_value(value, result):
            raise ValueError(
                f"Partition value {value!r} of '{key}' cannot be written: its partition directory name "
                f"'{text}' would read back as "
                f"{'another value' if result is _UNREPRESENTABLE_PARTITION_VALUE else repr(result)}.")

    def write_pandas(self, df) -> None:
        return _write_pandas(self, df)

    def _write_single_batch(
        self,
        data: pyarrow.RecordBatch,
        partition_spec: dict,
    ) -> None:
        if data.num_rows == 0:
            return
        components, dir_path = self._partition_dir(partition_spec)
        if self._files_keep_partition_columns:
            data = self._null_default_partition_columns(data, components)
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
                self._clear_partition(partition_spec, dir_path)
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

    def _null_default_partition_columns(self, data: pyarrow.RecordBatch, components: dict) -> pyarrow.RecordBatch:
        """Store null for the values that name the default partition, such as blank strings.

        Their directory reads, and partition filters select, them as null, as in Java. A file that kept the
        blank value would read back a value that no partition filter finds.
        """
        keys = [key for key in self.table.partition_keys if components[key] == self._default_part_value]
        if not keys:
            return data
        columns = [pyarrow.nulls(data.num_rows, type=field.type) if field.name in keys else data.column(i)
                   for i, field in enumerate(data.schema)]
        return pyarrow.RecordBatch.from_arrays(columns, schema=data.schema)

    def _clear_partition(self, partition_spec: dict, dir_path: str) -> None:
        """Delete the files of a partition that an overwrite replaces, and only those.

        Earlier versions named directories with str() of the values, without escaping, stopped the path at the
        first null value, and kept the partition columns in the files. A directory can then hold another
        partition's files, as pt=a%3Ab holds the earlier files of 'a%3Ab' and the files of 'a:b', and a
        partition's earlier files can be elsewhere, as those of True are in pt=True. A file is deleted when it is
        the partition's: a file without partition columns in the partition's own directory, or a file whose
        partition columns hold only the partition's values.
        """
        from pypaimon.execution.python.write.format_table_commit import _delete_data_files_in_path
        file_io = self.table.file_io
        if not self.table.partition_keys:
            if file_io.exists(dir_path):
                _delete_data_files_in_path(file_io, dir_path)
            return
        location = self._location()
        own = dir_path[len(location) + 1:]
        # Only an escaped name, which has '%', can be an earlier version's name of another value.
        self._delete_partition_files(dir_path, partition_spec, verify='%' in own, own_directory=True)
        earlier = _earlier_partition_path(partition_spec, self.table.partition_keys, self._partition_only_value)
        if earlier is not None and earlier != own:
            self._delete_partition_files(
                f"{location}/{earlier}" if earlier else location, partition_spec, verify=True, own_directory=False)

    def _delete_partition_files(self, path: str, partition_spec: dict, verify: bool, own_directory: bool) -> None:
        """Delete the data files directly in a directory that belong to a partition."""
        file_io = self.table.file_io
        try:
            infos = file_io.list_status(path)
        except Exception:
            # As before, an overwrite skips a directory it cannot list, such as an earlier version's directory
            # that does not exist or whose str() name the file system refuses.
            return
        for info in infos:
            name = info.path.split("/")[-1] if "/" in info.path else info.path
            if info.type != pafs.FileType.File or not _is_data_file_name(name):
                continue
            if verify:
                full_path = info.path if info.path.startswith(("/", "file:")) else f"{path.rstrip('/')}/{name}"
                rows = self._file_partition_values(full_path, getattr(info, "size", None) or 0)
                if rows is _UNKNOWN_PARTITION:
                    continue
                if rows is None:
                    if not own_directory:
                        continue
                elif not all(self._is_partition(row, partition_spec) for row in rows):
                    continue
            try:
                file_io.delete(info.path, False)
            except Exception:
                pass

    def _file_partition_values(self, path: str, size: int):
        """The partition values of a file's rows, None for a file without partition columns."""
        from pypaimon.execution.python.read.format_table_read import _read_file_to_arrow
        keys = self.table.partition_keys
        try:
            tbl = _read_file_to_arrow(
                self.table.file_io, FormatDataSplit(file_path=path, file_size=size), self._file_format, None,
                list(keys), self._partition_types, legacy_partition_name=self._legacy_partition_name,
                csv_skip_empty_lines=self.table.options().get('csv.skip-empty-lines', 'false').lower() == 'true')
        except Exception:
            return _UNKNOWN_PARTITION
        present = [key for key in keys if key in tbl.column_names]
        if not present:
            return None
        if len(present) != len(keys):
            return _UNKNOWN_PARTITION
        return list(zip(*[tbl[key].to_pylist() for key in keys]))

    def _is_partition(self, row: tuple, partition_spec: dict) -> bool:
        return all(_same_stored_partition_value(value, partition_spec.get(key))
                   for key, value in zip(self.table.partition_keys, row))

    def _location(self) -> str:
        location = self.table.location()
        if location.startswith('file:'):
            location = self.table.file_io.to_filesystem_path(location)
        return location

    def _partition_dir(self, partition_spec: dict) -> tuple:
        """The partition directory names and the directory of a partition, raising if it cannot be written."""
        location = self.table.location()
        components = {
            key: _partition_component(
                _partition_value_to_string(value, self._partition_types[key], self._legacy_partition_name),
                self._default_part_value)
            for key, value in partition_spec.items()
        }
        part_path = _partition_path(
            components,
            self.table.partition_keys,
            self._partition_only_value,
            self._default_part_value,
        )
        if not part_path:
            return components, location
        return components, f"{self._location()}/{part_path}"

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
