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

from typing import Any, Dict, Iterator, List, Optional

import pyarrow

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.execution.python.write.partition_spec import _partition_spec_value, _UNREPRESENTABLE_PARTITION_VALUE
from pypaimon.schema.data_types import AtomicType, DataType, PyarrowFieldParser
from pypaimon.table.format.format_data_split import FormatDataSplit
from pypaimon.table.format.format_table import FormatTable, Format


def _text_format_schema_column(table: FormatTable) -> Optional[str]:
    """TEXT format: single data column name from schema (non-partition)."""
    if table.format() != Format.TEXT or not table.fields:
        return None
    data_names = [
        f.name for f in table.fields if f.name not in table.partition_keys
    ]
    return (
        data_names[0]
        if data_names
        else (table.field_names[0] if table.field_names else None)
    )


def _read_file_to_arrow(
    file_io: Any,
    split: FormatDataSplit,
    fmt: Format,
    partition_spec: Optional[Dict[str, str]],
    read_fields: Optional[List[str]],
    partition_key_types: Optional[Dict[str, DataType]] = None,
    text_column_name: Optional[str] = None,
    text_line_delimiter: str = "\n",
    legacy_partition_name: bool = True,
    csv_skip_empty_lines: bool = False,
) -> pyarrow.Table:
    path = split.data_path()
    csv_read_options = None
    if fmt == Format.CSV:
        import pyarrow.csv as csv
        csv_read_options = csv.ReadOptions(block_size=1 << 20)
    try:
        with file_io.new_input_stream(path) as stream:
            chunks = []
            while True:
                chunk = stream.read()
                if not chunk:
                    break
                chunks.append(
                    chunk if isinstance(chunk, bytes) else bytes(chunk)
                )
            data = b"".join(chunks)
    except Exception as e:
        raise RuntimeError(f"Failed to read {path}") from e

    if not data and fmt != Format.PARQUET:
        # Java's ORC reader reads a zero-byte file as empty, as Hive writes them.
        return pyarrow.table({})

    if fmt == Format.PARQUET:
        import io
        import pyarrow.parquet as pq
        data = (
            bytes(data) if not isinstance(data, bytes) else data
        )
        try:
            tbl = pq.read_table(io.BytesIO(data))
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.CSV:
        import pyarrow.csv as csv
        try:
            # Files leave the partition columns out, so a row whose only column in the file is null is an
            # empty line. Keep such lines as rows unless csv.skip-empty-lines is set, as Java does.
            tbl = csv.read_csv(
                pyarrow.BufferReader(data),
                read_options=csv_read_options,
                parse_options=csv.ParseOptions(ignore_empty_lines=csv_skip_empty_lines),
            )
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.JSON:
        import json
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        records = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if line:
                records.append(json.loads(line))
        if not records:
            return pyarrow.table({})
        tbl = pyarrow.Table.from_pylist(records)
    elif fmt == Format.ORC:
        import io
        import pyarrow.orc as orc
        data = bytes(data) if not isinstance(data, bytes) else data
        try:
            tbl = orc.read_table(io.BytesIO(data))
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.TEXT:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        lines = (
            text.rstrip(text_line_delimiter).split(text_line_delimiter)
            if text
            else []
        )
        if not lines:
            return pyarrow.table({})
        part_keys = set(partition_spec.keys()) if partition_spec else set()
        col_name = text_column_name if text_column_name else "value"
        if read_fields:
            for f in read_fields:
                if f not in part_keys:
                    col_name = f
                    break
        tbl = pyarrow.table({col_name: lines})
    else:
        raise ValueError(f"Format {fmt} read not implemented in Python")

    # Files of earlier versions keep the partition columns, as files of a table with only partition columns do.
    keeps_partition_columns = any(k in tbl.column_names for k in partition_key_types or {})
    for k in partition_key_types or partition_spec or {}:
        data_type = (partition_key_types or {}).get(k, AtomicType('STRING'))
        field = pyarrow.field(k, PyarrowFieldParser.from_paimon_type(data_type), nullable=data_type.nullable)
        if k in tbl.column_names:
            try:
                arr = tbl[k].cast(field.type)
            except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
                values = [_partition_spec_value(v, data_type, legacy_ltz=legacy_partition_name)
                          for v in tbl[k].to_pylist()]
                arr = pyarrow.array(values, type=field.type)
            tbl = tbl.set_column(tbl.schema.get_field_index(k), field, arr)
        elif partition_spec and k in partition_spec:
            v = partition_spec[k]
            value = _partition_spec_value(v, data_type, legacy_ltz=legacy_partition_name)
            if value is _UNREPRESENTABLE_PARTITION_VALUE:
                raise ValueError(f"Partition value '{v}' cannot be represented as {data_type} in Python")
            arr = pyarrow.array([value] * tbl.num_rows, type=field.type)
            tbl = tbl.append_column(field, arr)

    if split.row_filter is not None and partition_key_types:
        tbl = _filter_partition_rows(tbl, split, keeps_partition_columns, partition_key_types, legacy_partition_name)

    if read_fields and tbl.num_columns > 0:
        existing = [c for c in read_fields if c in tbl.column_names]
        if existing:
            tbl = tbl.select(existing)
    return tbl


def _filter_partition_rows(tbl: pyarrow.Table, split: FormatDataSplit, keeps_partition_columns: bool,
                           types: Dict[str, DataType], legacy_ltz: bool) -> pyarrow.Table:
    """The rows of a split that its partition filter selects.

    A file that keeps its partition columns, as files of earlier versions do, can hold values that differ from
    its directory's, so its rows are matched by their own values. The other files take their directory's.
    """
    if not keeps_partition_columns:
        return tbl if split.directory_matches else tbl.slice(0, 0)
    from pypaimon.execution.python.read.format_table_scan import _partition_filter_value
    try:
        values = {k: _partition_filter_value(v, types[k]) for k, v in split.row_filter.items()}
    except (ValueError, TypeError, OverflowError):
        return tbl.slice(0, 0)
    if any(k not in tbl.column_names for k in values):
        return tbl.slice(0, 0)
    matches = {}

    def match(key, value):
        # repr keeps apart the values that equality joins, such as 0.0 and -0.0, which name different partitions.
        cache_key = (key, type(value), repr(value))
        if cache_key not in matches:
            try:
                matches[cache_key] = _partition_filter_value(
                    value, types[key], legacy_ltz=legacy_ltz) == values[key]
            except (ValueError, TypeError, OverflowError):
                matches[cache_key] = False
        return matches[cache_key]

    columns = [tbl[k].to_pylist() for k in values]
    mask = [all(match(k, value) for k, value in zip(values, row)) for row in zip(*columns)]
    return tbl.filter(pyarrow.array(mask, type=pyarrow.bool_()))


def _csv_skip_empty_lines(table: FormatTable) -> bool:
    """Java's csv.skip-empty-lines, which defaults to false."""
    return table.options().get('csv.skip-empty-lines', 'false').lower() == 'true'


def _cast_to_declared_types(tbl: pyarrow.Table, table: FormatTable) -> pyarrow.Table:
    """Read the data columns of a CSV or JSON file with their declared types.

    These formats infer each file's types, so a column with only nulls infers the null type and cannot be
    concatenated with another file's. A column that does not cast keeps its inferred type, as before.
    """
    if table.format() not in (Format.CSV, Format.JSON):
        return tbl
    for field in table.fields:
        if field.name in table.partition_keys or field.name not in tbl.column_names:
            continue
        index = tbl.schema.get_field_index(field.name)
        arrow_type = PyarrowFieldParser.from_paimon_type(field.type)
        if tbl.schema.field(index).type == arrow_type:
            continue
        try:
            column = tbl.column(index).cast(arrow_type)
        except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
            continue
        tbl = tbl.set_column(index, pyarrow.field(field.name, arrow_type), column)
    return tbl


def _partition_key_types(
    table: FormatTable,
) -> Optional[Dict[str, DataType]]:
    """Build partition column name -> type from table schema."""
    if not table.partition_keys:
        return None
    result = {}
    for f in table.fields:
        if f.name in table.partition_keys:
            result[f.name] = f.type
    return result if result else None


def to_arrow(
    self,
    splits: List[FormatDataSplit],
) -> pyarrow.Table:
    read_fields = self.projection or self.table.field_names
    fmt = self.table.format()
    partition_key_types = _partition_key_types(self.table)
    legacy_partition_name = CoreOptions.from_dict(self.table.options()).options.get(
        CoreOptions.PARTITION_GENERATE_LEGACY_NAME)
    text_col = (
        _text_format_schema_column(self.table)
        if fmt == Format.TEXT
        else None
    )
    text_delim = (
        self.table.options().get("text.line-delimiter", "\n")
        if fmt == Format.TEXT
        else "\n"
    )
    tables = []
    nrows = 0
    for split in splits:
        t = _read_file_to_arrow(
            self.table.file_io,
            split,
            fmt,
            split.partition,
            read_fields,
            partition_key_types,
            text_column_name=text_col,
            text_line_delimiter=text_delim,
            legacy_partition_name=legacy_partition_name,
            csv_skip_empty_lines=_csv_skip_empty_lines(self.table),
        )
        t = _cast_to_declared_types(t, self.table)
        if t.num_rows > 0:
            tables.append(t)
            nrows += t.num_rows
            if self.limit is not None and nrows >= self.limit:
                if nrows > self.limit:
                    excess = nrows - self.limit
                    last = tables[-1]
                    tables[-1] = last.slice(0, last.num_rows - excess)
                break
    if not tables:
        fields = self.table.fields
        if read_fields:
            fields = [
                f for f in self.table.fields if f.name in read_fields
            ]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        return pyarrow.Table.from_pydict(
            {n: [] for n in schema.names},
            schema=schema,
        )
    out = pyarrow.concat_tables(tables)
    if self.limit is not None and out.num_rows > self.limit:
        out = out.slice(0, self.limit)
    return out


def to_iterator(
    self,
    splits: List[FormatDataSplit],
) -> Iterator[Any]:
    partition_key_types = _partition_key_types(self.table)
    legacy_partition_name = CoreOptions.from_dict(self.table.options()).options.get(
        CoreOptions.PARTITION_GENERATE_LEGACY_NAME)
    fmt = self.table.format()
    text_col = (
        _text_format_schema_column(self.table)
        if fmt == Format.TEXT
        else None
    )
    text_delim = (
        self.table.options().get("text.line-delimiter", "\n")
        if fmt == Format.TEXT
        else "\n"
    )
    n_yielded = 0
    for split in splits:
        if self.limit is not None and n_yielded >= self.limit:
            break
        t = _read_file_to_arrow(
            self.table.file_io,
            split,
            fmt,
            split.partition,
            self.projection or self.table.field_names,
            partition_key_types,
            text_column_name=text_col,
            text_line_delimiter=text_delim,
            legacy_partition_name=legacy_partition_name,
            csv_skip_empty_lines=_csv_skip_empty_lines(self.table),
        )
        t = _cast_to_declared_types(t, self.table)
        for batch in t.to_batches():
            for i in range(batch.num_rows):
                if self.limit is not None and n_yielded >= self.limit:
                    return
                yield batch.slice(i, 1)
                n_yielded += 1
