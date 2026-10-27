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

import ast
import io
import math
import re
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


def _same_partition_value(value, other) -> bool:
    """Whether two values name the same partition: NaNs name one, and 0.0 and -0.0, which are equal, two."""
    if isinstance(value, float) and isinstance(other, float):
        if math.isnan(value) or math.isnan(other):
            return math.isnan(value) and math.isnan(other)
        return value == other and math.copysign(1.0, value) == math.copysign(1.0, other)
    return type(value) is type(other) and value == other


def _distinct_values(column: pyarrow.Array) -> tuple:
    """The rows' codes and the values they stand for. Floats are told apart by bits, as 0.0 and -0.0 are equal."""
    keyed = column
    if pyarrow.types.is_float32(column.type) or pyarrow.types.is_float64(column.type):
        keyed = column.view(pyarrow.int32() if pyarrow.types.is_float32(column.type) else pyarrow.int64())
    encoded = keyed.dictionary_encode()
    values = encoded.dictionary
    if keyed is not column:
        values = values.view(column.type)
    null_code = len(values)
    codes = [null_code if code is None else code for code in encoded.indices.to_pylist()]
    return codes, values.to_pylist() + [None]


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
        self._overwritten_partitions: set = set()
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
            # A static value is a value, as a partition filter's is: an LTZ text is a local time, also where
            # legacy names write the UTC fields to the directory.
            parsed = _partition_spec_value(value, data_type)
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
        parsed = {}
        # Each distinct value is formatted and checked once. Null and blank values share a directory, but an
        # overwrite clears the earlier directory of each, so values are told apart by repr.
        columns, distinct = [], []
        for key in partition_keys:
            codes, values = _distinct_values(self._partition_column(data, key))
            for value in values:
                if value is not None:
                    text = _partition_value_to_string(value, self._partition_types[key], self._legacy_partition_name)
                    self._check_partition_value_round_trips(key, value, text, parsed)
            columns.append(codes)
            distinct.append([(value, (type(value), repr(value))) for value in values])
        by_codes = defaultdict(list)
        for i, codes in enumerate(zip(*columns)):
            by_codes[codes].append(i)
        groups = {}
        for codes, indices in by_codes.items():
            entries = [values[code] for values, code in zip(distinct, codes)]
            identity = tuple(identity for _, identity in entries)
            if identity in groups:
                groups[identity][1].extend(indices)
            else:
                part_spec = dict(zip(partition_keys, [value for value, _ in entries]))
                self._partition_dir(part_spec)
                groups[identity] = (part_spec, indices)
        return [(part_spec, sorted(indices)) for part_spec, indices in groups.values()]

    def _partition_column(self, data: pyarrow.RecordBatch, key: str) -> pyarrow.Array:
        """A partition column in its declared type, such as timestamp[us] rows of a TIMESTAMP(9) partition."""
        column = data.column(data.schema.get_field_index(key))
        declared = PyarrowFieldParser.from_paimon_type(self._partition_types[key])
        if column.type == declared:
            return column
        if pyarrow.types.is_timestamp(column.type) and pyarrow.types.is_timestamp(declared) \
                and (column.type.tz is None) != (declared.tz is None):
            # A local time is no instant: leave the value as it is, so that the check below refuses it.
            return column
        try:
            return column.cast(declared)
        except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError) as e:
            raise ValueError(f"Partition column '{key}' of type {column.type} cannot be written as {declared}") from e

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
        if self._overwrite and all(
                components.get(k) == v for k, v in self._static_partition_components.items()):
            self._clear_partition(partition_spec, dir_path)
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

        A write clears a directory once, before it writes there, and the other directories of each partition once:
        null and blank values share a directory, but earlier versions wrote '' to pt= and ' ' to 'pt= '.
        """
        from pypaimon.execution.python.write.format_table_commit import _delete_data_files_in_path
        file_io = self.table.file_io
        keys = self.table.partition_keys
        # The directories and partitions are recorded once cleared, so that a write retried after a failure clears
        # them again.
        first_in_dir = dir_path not in self._overwritten_dirs
        if not keys:
            if first_in_dir and file_io.exists(dir_path):
                _delete_data_files_in_path(file_io, dir_path)
            self._overwritten_dirs.add(dir_path)
            return
        location = self._location()
        own = dir_path[len(location) + 1:]
        if first_in_dir:
            # An escaped name, which has '%', and the default partition name can be an earlier version's name of
            # another value, such as the string '__DEFAULT_PARTITION__'.
            default_name = _escape_partition_component(self._default_part_value)
            verify = '%' in own or any(component.split('=', 1)[-1] == default_name for component in own.split('/'))
            self._delete_partition_files(dir_path, partition_spec, verify=verify, own_directory=True)
            self._overwritten_dirs.add(dir_path)
        # Null and blank values are one partition, the default one.
        partition = tuple(None if self._names_default_partition(key, value) else (type(value), repr(value))
                          for key, value in ((key, partition_spec.get(key)) for key in keys))
        if partition in self._overwritten_partitions:
            return
        for earlier in self._earlier_partition_paths(partition_spec):
            if earlier == own:
                continue
            # TEXT files keep no partition columns, also in earlier versions. Those in the earlier directory are the
            # partition's where no other partition has files there: no partition of this version names it, and the
            # path stopped at a null value only for the last key, if at all.
            depth = len(earlier.split('/')) if earlier else 0
            earlier_owned = self._file_format == Format.TEXT and (
                depth == len(keys) - 1 or depth == len(keys) and not self._names_written_directory(earlier))
            self._delete_partition_files(
                f"{location}/{earlier}" if earlier else location, partition_spec, verify=True,
                own_directory=earlier_owned)
        for other in self._other_floating_directories(partition_spec, own):
            self._delete_partition_files(f"{location}/{other}", partition_spec, verify=True, own_directory=True)
        self._overwritten_partitions.add(partition)

    def _earlier_partition_paths(self, partition_spec: dict) -> List[str]:
        """The directories where earlier versions wrote a partition's files.

        They named a directory with str() of the value, without escaping, and stopped the path at the first null
        value. The default partition holds null and blank values, so its earlier files are where the path stopped
        and in the directories of blank values, such as pt= for ''.
        """
        only_value = self._partition_only_value
        found, paths = [], ['']
        for key in self.table.partition_keys:
            value = partition_spec.get(key)
            if self._names_default_partition(key, value):
                found.extend(paths)
                prefix = '' if only_value else key + '='
                paths = [f"{path}/{name}" if path else name
                         for path in paths for name in self._directory_names(path)
                         if name.startswith(prefix) and self._earlier_name_is_blank(key, name[len(prefix):])]
                continue
            text = str(value)
            if only_value and text in ('.', '..'):
                return found
            component = text if only_value else f"{key}={text}"
            paths = [f"{path}/{component}" if path else component for path in paths]
        return found + paths

    def _earlier_name_is_blank(self, key, text: str) -> bool:
        """Whether an earlier version's directory name, str() of a value, names a blank value: ' ' names ' ' and
        b' ' names the bytes b' '."""
        if _is_null_or_whitespace_only(text):
            return True
        if not pyarrow.types.is_binary(PyarrowFieldParser.from_paimon_type(self._partition_types[key])):
            return False
        if re.fullmatch(r"b'.*'|b\".*\"", text, re.S) is None:
            return False
        try:
            value = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return False
        return isinstance(value, bytes) and self._names_default_partition(key, value)

    def _directory_names(self, path: str) -> List[str]:
        location = self._location()
        try:
            infos = self.table.file_io.list_status(f"{location}/{path}" if path else location)
        except Exception:
            return []
        return [info.path.rstrip('/').split('/')[-1] for info in infos if info.type == pafs.FileType.Directory]

    def _names_written_directory(self, path: str) -> bool:
        """Whether this version names a partition's directory so."""
        from pypaimon.execution.python.read.format_table_scan import _unescape_path_name
        for key, component in zip(self.table.partition_keys, path.split('/')):
            if not self._partition_only_value:
                prefix = _escape_partition_component(key) + '='
                if not component.startswith(prefix):
                    return False
                component = component[len(prefix):]
            text = _unescape_path_name(component)
            if text == self._default_part_value:
                continue
            data_type = self._partition_types[key]
            try:
                value = _partition_spec_value(text, data_type, legacy_ltz=self._legacy_partition_name)
                name = _partition_value_to_string(value, data_type, self._legacy_partition_name)
                if _is_null_or_whitespace_only(name) or _escape_partition_component(name) != component:
                    return False
            except Exception:
                return False
        return True

    def _other_floating_directories(self, partition_spec: dict, own: str) -> List[str]:
        """The other directories of a partition with a FLOAT or DOUBLE value.

        Java 19 changed Float and Double.toString, so Java 11 and Java 19+ name such a value differently, such as
        2.68873286E11 and 2.6887329E11, and reads take both as the value.
        """
        from pypaimon.execution.python.read.format_table_scan import _unescape_path_name
        keys = self.table.partition_keys
        if not any(pyarrow.types.is_floating(PyarrowFieldParser.from_paimon_type(self._partition_types[key]))
                   and partition_spec.get(key) is not None for key in keys):
            return []
        location = self._location()
        candidates = ['']
        for key, component in zip(keys, own.split('/')):
            data_type = self._partition_types[key]
            value = partition_spec.get(key)
            if value is None or not pyarrow.types.is_floating(PyarrowFieldParser.from_paimon_type(data_type)):
                candidates = [f"{path}/{component}" if path else component for path in candidates]
                continue
            prefix = '' if self._partition_only_value else _escape_partition_component(key) + '='
            found = []
            for path in candidates:
                try:
                    infos = self.table.file_io.list_status(f"{location}/{path}" if path else location)
                except Exception:
                    continue
                for info in infos:
                    name = info.path.rstrip('/').split('/')[-1]
                    if info.type != pafs.FileType.Directory or not name.startswith(prefix):
                        continue
                    text = _unescape_path_name(name[len(prefix):])
                    # The default partition name, such as 0, names the null partition, not a value.
                    if text == self._default_part_value:
                        continue
                    try:
                        parsed = _partition_spec_value(text, data_type, directory=True)
                    except ValueError:
                        continue
                    if _same_partition_value(parsed, value):
                        found.append(f"{path}/{name}" if path else name)
            candidates = found
        return [path for path in candidates if path != own]

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
        from pypaimon.execution.python.read.format_table_read import (
            _csv_skip_empty_lines, _read_file_to_arrow)
        if self._file_format == Format.TEXT:
            # A TEXT file holds only its text column, also in earlier versions.
            return None
        keys = self.table.partition_keys
        try:
            tbl = _read_file_to_arrow(
                self.table.file_io, FormatDataSplit(file_path=path, file_size=size), self._file_format, None,
                list(keys), self._partition_types, legacy_partition_name=self._legacy_partition_name,
                csv_skip_empty_lines=_csv_skip_empty_lines(self.table))
        except Exception as e:
            # Skipping the file would leave its rows next to the new ones, so the overwrite fails instead.
            raise RuntimeError(f"Cannot read {path} to check whether the overwrite replaces it") from e
        present = [key for key in keys if key in tbl.column_names]
        if not present:
            return None
        if len(present) != len(keys):
            return _UNKNOWN_PARTITION
        return list(zip(*[tbl[key].to_pylist() for key in keys]))

    def _is_partition(self, row: tuple, partition_spec: dict) -> bool:
        for key, value in zip(self.table.partition_keys, row):
            expected = partition_spec.get(key)
            # Null and blank values, such as b' ', name the default partition, whose files store null for the
            # blank ones.
            if self._names_default_partition(key, expected):
                if not self._names_default_partition(key, value):
                    return False
            elif not _same_partition_value(value, expected):
                return False
        return True

    def _names_default_partition(self, key, value) -> bool:
        return _is_null_or_whitespace_only(
            _partition_value_to_string(value, self._partition_types[key], self._legacy_partition_name))

    def _location(self) -> str:
        location = self.table.location()
        if location.startswith('file:'):
            location = self.table.file_io.to_filesystem_path(location)
        return location

    def _partition_dir(self, partition_spec: dict) -> tuple:
        """The partition directory names and the directory of a partition, raising if it cannot be written."""
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
        location = self._location()
        return components, f"{location}/{part_path}" if part_path else location

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
