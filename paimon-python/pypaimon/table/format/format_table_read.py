# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, Iterator, List, Optional

import pandas
import pyarrow

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
    partition_key_types: Optional[Dict[str, pyarrow.DataType]] = None,
    text_column_name: Optional[str] = None,
    text_line_delimiter: str = "\n",
) -> pyarrow.Table:
    path = split.data_path()
    csv_read_options = None
    if fmt == Format.CSV and hasattr(pyarrow, "csv"):
        csv_read_options = pyarrow.csv.ReadOptions(block_size=1 << 20)
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

    if not data or len(data) == 0:
        return pyarrow.table({})

    if fmt == Format.PARQUET:
        import io
        import pyarrow.parquet as pq
        data = (
            bytes(data) if not isinstance(data, bytes) else data
        )
        if len(data) < 4 or data[:4] != b"PAR1":
            return pyarrow.table({})
        try:
            tbl = pq.read_table(io.BytesIO(data))
        except pyarrow.ArrowInvalid:
            return pyarrow.table({})
    elif fmt == Format.CSV:
        if hasattr(pyarrow, "csv"):
            tbl = pyarrow.csv.read_csv(
                pyarrow.BufferReader(data),
                read_options=csv_read_options,
            )
        else:
            import io
            df = pandas.read_csv(io.BytesIO(data))
            tbl = pyarrow.Table.from_pandas(df)
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
        data = bytes(data) if not isinstance(data, bytes) else data
        if hasattr(pyarrow, "orc"):
            try:
                tbl = pyarrow.orc.read_table(io.BytesIO(data))
            except Exception:
                return pyarrow.table({})
        else:
            raise ValueError(
                "Format table read for ORC requires PyArrow with ORC support "
                "(pyarrow.orc)"
            )
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

    if partition_spec:
        for k, v in partition_spec.items():
            if k in tbl.column_names:
                continue
            pa_type = (
                partition_key_types.get(k, pyarrow.string())
                if partition_key_types
                else pyarrow.string()
            )
            arr = pyarrow.array([v] * tbl.num_rows, type=pyarrow.string())
            if pa_type != pyarrow.string():
                arr = arr.cast(pa_type)
            tbl = tbl.append_column(k, arr)

    if read_fields and tbl.num_columns > 0:
        existing = [c for c in read_fields if c in tbl.column_names]
        if existing:
            tbl = tbl.select(existing)
    return tbl


def _partition_key_types(
    table: FormatTable,
) -> Optional[Dict[str, pyarrow.DataType]]:
    """Build partition column name -> PyArrow type from table schema."""
    if not table.partition_keys:
        return None
    result = {}
    for f in table.fields:
        if f.name in table.partition_keys:
            pa_field = PyarrowFieldParser.from_paimon_field(f)
            result[f.name] = pa_field.type
    return result if result else None


class FormatTableRead:

    def __init__(
        self,
        table: FormatTable,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        self.table = table
        self.projection = projection
        self.limit = limit

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

    def to_pandas(self, splits: List[FormatDataSplit]) -> pandas.DataFrame:
        return self.to_arrow(splits).to_pandas()

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
        n_yielded = 0
        for split in splits:
            if self.limit is not None and n_yielded >= self.limit:
                break
            t = _read_file_to_arrow(
                self.table.file_io,
                split,
                fmt,
                split.partition,
                self.projection,
                partition_key_types,
                text_column_name=text_col,
                text_line_delimiter=text_delim,
            )
            for batch in t.to_batches():
                for i in range(batch.num_rows):
                    if self.limit is not None and n_yielded >= self.limit:
                        return
                    yield batch.slice(i, 1)
                    n_yielded += 1
