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

import io
import uuid
from collections import defaultdict
from typing import Dict, List, Optional

import pyarrow

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.format.format_commit_message import (
    FormatTableCommitMessage,
)
from pypaimon.table.format.format_table import (
    Format,
    FormatTable,
)


def _partition_path(
    partition_spec: dict, partition_keys: List[str], only_value: bool
) -> str:
    parts = []
    for k in partition_keys:
        v = partition_spec.get(k)
        if v is None:
            break
        parts.append(str(v) if only_value else f"{k}={v}")
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


class FormatTableWrite:
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
        self._data_file_prefix = "data-"
        self._suffix = {
            "parquet": ".parquet",
            "csv": ".csv",
            "json": ".json",
            "orc": ".orc",
            "text": ".txt",
        }.get(self._file_format.value, ".parquet")

    def write_arrow(self, data: pyarrow.Table) -> None:
        for batch in data.to_batches():
            self.write_arrow_batch(batch)

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
            part = _partition_from_row(data, partition_keys, i)
            parts_to_indices[part].append(i)
        for part_tuple, indices in parts_to_indices.items():
            part_spec = dict(zip(partition_keys, part_tuple))
            sub = data.take(pyarrow.array(indices))
            self._write_single_batch(sub, part_spec)

    def write_pandas(self, df) -> None:
        pa_schema = PyarrowFieldParser.from_paimon_schema(self.table.fields)
        batch = pyarrow.RecordBatch.from_pandas(df, schema=pa_schema)
        self.write_arrow_batch(batch)

    def _write_single_batch(
        self,
        data: pyarrow.RecordBatch,
        partition_spec: dict,
    ) -> None:
        if data.num_rows == 0:
            return
        location = self.table.location()
        partition_only_value = self._partition_only_value
        part_path = _partition_path(
            partition_spec,
            self.table.partition_keys,
            partition_only_value,
        )
        if part_path:
            dir_path = f"{location}/{part_path}"
        else:
            dir_path = location
        # When overwrite: clear partition dir only once per write session
        overwrite_this = (
            self._overwrite
            and dir_path not in self._overwritten_dirs
            and self.table.file_io.exists(dir_path)
        )
        if overwrite_this:
            should_delete = (
                not self._static_partitions
                or all(
                    str(partition_spec.get(k)) == str(v)
                    for k, v in self._static_partitions.items()
                )
            )
            if should_delete:
                from pypaimon.table.format.format_table_commit import (
                    _delete_data_files_in_path,
                )
                _delete_data_files_in_path(self.table.file_io, dir_path)
                self._overwritten_dirs.add(dir_path)
        self.table.file_io.check_or_mkdirs(dir_path)
        file_name = f"{self._data_file_prefix}{uuid.uuid4().hex}{self._suffix}"
        path = f"{dir_path}/{file_name}"

        fmt = self._file_format
        tbl = pyarrow.Table.from_batches([data])
        if fmt == Format.PARQUET:
            import pyarrow.parquet as pq
            buf = io.BytesIO()
            pq.write_table(tbl, buf, compression="zstd")
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
            partition_keys = self.table.partition_keys
            if partition_keys:
                data_cols = [
                    c for c in tbl.column_names if c not in partition_keys
                ]
                tbl = tbl.select(data_cols)
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
