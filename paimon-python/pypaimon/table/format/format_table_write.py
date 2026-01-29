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
from typing import List

import pyarrow

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.format.format_commit_message import FormatTableCommitMessage
from pypaimon.table.format.format_table import FormatTable, Format


def _partition_path(partition_spec: dict, partition_keys: List[str], only_value: bool) -> str:
    parts = []
    for k in partition_keys:
        v = partition_spec.get(k)
        if v is None:
            break
        parts.append(str(v) if only_value else f"{k}={v}")
    return "/".join(parts)


def _partition_from_row(
    row: pyarrow.RecordBatch,
    partition_keys: List[str],
    row_index: int,
) -> tuple:
    out = []
    for k in partition_keys:
        col = row.column(row.schema.get_field_index(k))
        val = col[row_index]
        if val is None or (hasattr(val, "as_py") and val.as_py() is None):
            out.append(None)
        else:
            out.append(val.as_py() if hasattr(val, "as_py") else val)
    return tuple(out)


class FormatTableWrite:
    """Batch write for format table: writes Arrow/Pandas to partition dirs as files."""

    def __init__(self, table: FormatTable, overwrite: bool = False):
        self.table = table
        self._overwrite = overwrite
        self._written_paths: List[str] = []
        self._partition_only_value = (
            table.options.get("format-table.partition-path-only-value", "false").lower() == "true"
        )
        self._file_format = table.format()
        self._data_file_prefix = "data-"
        self._suffix = {"parquet": ".parquet", "csv": ".csv", "json": ".json"}.get(
            self._file_format.value, ".parquet"
        )

    def write_arrow(self, data: pyarrow.Table) -> None:
        for batch in data.to_batches():
            self.write_arrow_batch(batch)

    def write_arrow_batch(self, data: pyarrow.RecordBatch) -> None:
        partition_keys = self.table.partition_keys
        if not partition_keys:
            part_spec = {}
            self._write_single_batch(data, part_spec)
            return
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
        if self._overwrite and self.table.file_io.exists(dir_path):
            from pypaimon.table.format.format_table_commit import _delete_data_files_in_path
            _delete_data_files_in_path(self.table.file_io, dir_path)
        self.table.file_io.check_or_mkdirs(dir_path)
        file_name = f"{self._data_file_prefix}{uuid.uuid4().hex}{self._suffix}"
        path = f"{dir_path}/{file_name}"

        fmt = self._file_format
        if fmt == Format.PARQUET:
            buf = io.BytesIO()
            pyarrow.parquet.write_table(
                pyarrow.Table.from_batches([data]),
                buf,
                compression="zstd",
            )
            raw = buf.getvalue()
        elif fmt == Format.CSV:
            buf = io.BytesIO()
            pyarrow.csv.write_csv(
                pyarrow.Table.from_batches([data]),
                buf,
            )
            raw = buf.getvalue()
        elif fmt == Format.JSON:
            tbl = pyarrow.Table.from_batches([data])
            import json
            lines = []
            for i in range(tbl.num_rows):
                row = {tbl.column_names[j]: tbl.column(j)[i].as_py() for j in range(tbl.num_columns)}
                lines.append(json.dumps(row) + "\n")
            raw = "".join(lines).encode("utf-8")
        else:
            raise ValueError(f"Format table write not implemented for {fmt}")

        with self.table.file_io.new_output_stream(path) as out:
            out.write(raw)

        self._written_paths.append(path)

    def prepare_commit(self) -> List[FormatTableCommitMessage]:
        return [FormatTableCommitMessage(written_paths=list(self._written_paths))]

    def close(self) -> None:
        pass
