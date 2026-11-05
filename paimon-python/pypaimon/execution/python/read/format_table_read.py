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

from pypaimon.execution.python.format_table_json import JsonReader
from pypaimon.schema.data_types import PyarrowFieldParser
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
    partition_key_types: Optional[Dict[str, pyarrow.Field]] = None,
    text_column_name: Optional[str] = None,
    partition_data_types: Optional[Dict[str, Any]] = None,
    text_line_delimiter: str = "\n",
    text_reader: Optional[JsonReader] = None,
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
            tbl = csv.read_csv(
                pyarrow.BufferReader(data),
                read_options=csv_read_options,
            )
        except (pyarrow.ArrowException, OSError) as e:
            raise RuntimeError(f"Failed to read {path}") from e
    elif fmt == Format.JSON:
        tbl = text_reader.read(data)
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
        # A file above the partition depth has no partition values, so the table's keys name them.
        part_keys = set(partition_spec or ()) | set(partition_key_types or ())
        col_name = text_column_name if text_column_name else "value"
        if read_fields:
            for f in read_fields:
                if f not in part_keys:
                    col_name = f
                    break
        tbl = pyarrow.table({col_name: lines})
    else:
        raise ValueError(f"Format {fmt} read not implemented in Python")

    if partition_spec:
        for k, v in partition_spec.items():
            if k in tbl.column_names or (read_fields and k not in read_fields):
                continue
            # The table's field, with its nullability, as the columns of files that keep their partitions have.
            field = (partition_key_types or {}).get(k, pyarrow.field(k, pyarrow.string()))
            arr = pyarrow.array([v] * tbl.num_rows, type=pyarrow.string())
            if field.type != pyarrow.string():
                try:
                    arr = arr.cast(field.type)
                except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
                    # Arrow cannot cast every partition type from text, such as TIME; Java's conversion can.
                    from pypaimon.execution.python.write.partition_spec import (
                        _partition_spec_value, _UNREPRESENTABLE_PARTITION_VALUE)
                    value = _partition_spec_value(v, (partition_data_types or {})[k])
                    if value is _UNREPRESENTABLE_PARTITION_VALUE:
                        raise ValueError(f"Partition value '{v}' of '{k}' cannot be represented in Python")
                    arr = pyarrow.array([value] * tbl.num_rows, type=field.type)
            tbl = tbl.append_column(field, arr)

    if read_fields and tbl.num_columns > 0:
        existing = [c for c in read_fields if c in tbl.column_names]
        if existing:
            tbl = tbl.select(existing)
    return tbl


def _text_reader(table: FormatTable, read_fields: Optional[List[str]]) -> Optional[JsonReader]:
    if table.format() != Format.JSON:
        return None
    fields = {f.name: f for f in table.fields}
    data_fields = [f for f in table.fields if f.name not in table.partition_keys]
    # PyPaimon's files hold the partition columns too, Java's do not.
    partition_fields = [fields[key] for key in table.partition_keys]
    return JsonReader(data_fields + partition_fields, read_fields, table.options(), table.partition_keys)


def _partition_key_types(
    table: FormatTable,
) -> Optional[Dict[str, pyarrow.Field]]:
    """Build partition column name -> PyArrow field from table schema."""
    if not table.partition_keys:
        return None
    result = {}
    for f in table.fields:
        if f.name in table.partition_keys:
            result[f.name] = PyarrowFieldParser.from_paimon_field(f)
    return result if result else None


def to_arrow(
    self,
    splits: List[FormatDataSplit],
) -> pyarrow.Table:
    read_fields = self.projection
    fmt = self.table.format()
    partition_key_types = _partition_key_types(self.table)
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
    text_reader = _text_reader(self.table, read_fields)
    tables = []
    nrows = 0
    for split in splits:
        t = _read_file_to_arrow(
            self.table.file_io,
            split,
            fmt,
            split.partition,
            # Files leave the partition columns out, so the read restores the table's column order.
            read_fields or self.table.field_names,
            partition_key_types,
            text_column_name=text_col,
            text_line_delimiter=text_delim,
            text_reader=text_reader,
            partition_data_types={f.name: f.type for f in self.table.fields if f.name in self.table.partition_keys},
        )
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
    text_reader = _text_reader(self.table, self.projection)
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
            text_reader=text_reader,
            partition_data_types={f.name: f.type for f in self.table.fields if f.name in self.table.partition_keys},
        )
        for batch in t.to_batches():
            for i in range(batch.num_rows):
                if self.limit is not None and n_yielded >= self.limit:
                    return
                yield batch.slice(i, 1)
                n_yielded += 1
