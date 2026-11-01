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

import re
import struct
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO, supports_pread, pread
from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.push_down_utils import rewrite_predicate_indices
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields


class FormatMosaicReader(RecordBatchReader):

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 row_group_predicate: Optional[Predicate] = None):
        from mosaic import MosaicReader

        self._read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]
        self._batch_size = batch_size

        stream = file_io.new_input_stream(file_path)
        file_length = file_io.get_file_size(file_path)

        if supports_pread(stream):
            self._stream = stream

            def read_at(offset, length):
                return pread(stream, length, offset)
        else:
            self._stream = None
            file_data = stream.read()
            stream.close()
            file_length = len(file_data)

            def read_at(offset, length):
                return file_data[offset:offset + length]

        self._reader = MosaicReader.from_input_file(read_at, file_length)

        file_schema_names = set(f.name for f in self._reader.schema)
        self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
        self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

        if self.existing_fields:
            self._reader.project(self.existing_fields)

        if self.missing_fields:
            output_schema = PyarrowFieldParser.from_paimon_schema(read_fields)
            self._missing_out_fields = []
            for name in self.missing_fields:
                idx = output_schema.get_field_index(name)
                col_type = output_schema.field(idx).type if idx >= 0 else pa.null()
                nullable = not SpecialFields.is_system_field(name)
                self._missing_out_fields.append(pa.field(name, col_type, nullable=nullable))

        self._current_rg = 0
        self._num_row_groups = self._reader.num_row_groups
        self._current_batches = None

        self._predicate = push_down_predicate
        self._row_group_predicate = None
        if row_group_predicate is not None:
            try:
                self._row_group_predicate = rewrite_predicate_indices(
                    row_group_predicate, read_fields)
            except ValueError:
                pass

    def _next_row_group_batches(self):
        while self._current_rg < self._num_row_groups:
            if not self._matches_row_group(self._current_rg):
                self._current_rg += 1
                continue

            batch = self._reader.read_row_group(self._current_rg)
            self._current_rg += 1

            if batch.num_rows == 0:
                continue

            batch = self._fill_missing_fields(batch)

            if self._predicate is not None:
                dataset = ds.InMemoryDataset(pa.Table.from_batches([batch]))
                scanner = dataset.scanner(filter=self._predicate, batch_size=self._batch_size)
                return scanner.to_reader()
            else:
                return iter(pa.Table.from_batches([batch]).to_batches(
                    max_chunksize=self._batch_size))
        return None

    def _matches_row_group(self, row_group_index: int) -> bool:
        if self._row_group_predicate is None:
            return True

        try:
            row_count = self._reader.row_group_num_rows(row_group_index)
            stats_map = self._reader.get_row_group_statistics(row_group_index)
        except Exception:
            return True

        if not stats_map:
            return True

        try:
            stats = self._to_simple_stats(stats_map)
            return self._row_group_predicate.test_by_simple_stats(stats, row_count)
        except Exception:
            return True

    def _to_simple_stats(self, stats_map) -> SimpleStats:
        min_values = [None] * len(self._read_fields)
        max_values = [None] * len(self._read_fields)
        null_counts = [None] * len(self._read_fields)

        for i, field in enumerate(self._read_fields):
            stats = stats_map.get(field.name)
            if stats is None:
                continue

            null_counts[i] = stats.null_count
            if stats.has_min_max:
                min_values[i] = _convert_stats_value(stats.min, field.type)
                max_values[i] = _convert_stats_value(
                    stats.max, field.type, round_up_timestamp=True)

        return SimpleStats(
            GenericRow(min_values, self._read_fields),
            GenericRow(max_values, self._read_fields),
            null_counts)

    def _fill_missing_fields(self, batch: RecordBatch) -> RecordBatch:
        if not self.missing_fields:
            return batch

        all_columns = []
        out_fields = []
        for field_name in self._read_field_names:
            if field_name in self.existing_fields:
                col_idx = self.existing_fields.index(field_name)
                all_columns.append(batch.column(col_idx))
                out_fields.append(batch.schema.field(col_idx))
            else:
                miss_idx = self.missing_fields.index(field_name)
                out_field = self._missing_out_fields[miss_idx]
                all_columns.append(pa.nulls(batch.num_rows, type=out_field.type))
                out_fields.append(out_field)
        return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields))

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        while True:
            if self._current_batches is not None:
                try:
                    if hasattr(self._current_batches, 'read_next_batch'):
                        return self._current_batches.read_next_batch()
                    else:
                        return next(self._current_batches)
                except StopIteration:
                    self._current_batches = None

            self._current_batches = self._next_row_group_batches()
            if self._current_batches is None:
                return None

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        self._reader = None
        self._current_batches = None


_EPOCH = datetime(1970, 1, 1)


def _convert_stats_value(
        value: Optional[bytes], data_type, round_up_timestamp: bool = False) -> Any:
    if value is None:
        return None

    type_name = str(data_type).upper().strip()
    type_name = re.sub(r"\s+NOT\s+NULL$", "", type_name)
    type_name = re.sub(r"\s+NULL$", "", type_name)
    base_type = type_name.split("(", 1)[0].strip()

    if base_type in ("CHAR", "VARCHAR", "STRING"):
        return value.decode("utf-8")
    if base_type in ("BINARY", "VARBINARY", "BYTES", "BLOB"):
        return value
    if len(value) == 0:
        return None

    if base_type == "BOOLEAN":
        return value[0] != 0
    if base_type == "TINYINT":
        return struct.unpack(">b", value[:1])[0]
    if base_type == "SMALLINT":
        return struct.unpack(">h", value)[0]
    if base_type in ("INT", "INTEGER", "DATE", "TIME"):
        return struct.unpack(">i", value)[0]
    if base_type == "BIGINT":
        return struct.unpack(">q", value)[0]
    if base_type == "FLOAT":
        return struct.unpack(">f", value)[0]
    if base_type == "DOUBLE":
        return struct.unpack(">d", value)[0]
    if base_type in ("DECIMAL", "NUMERIC"):
        scale = _type_scale(type_name)
        return Decimal(int.from_bytes(value, byteorder="big", signed=True)).scaleb(-scale)
    if base_type in ("TIMESTAMP", "TIMESTAMP_LTZ"):
        precision = _type_precision(type_name, default=6)
        if precision <= 3:
            return _EPOCH + timedelta(milliseconds=struct.unpack(">q", value)[0])
        if precision <= 6:
            return _EPOCH + timedelta(microseconds=struct.unpack(">q", value)[0])
        millis, nanos_of_milli = struct.unpack(">qi", value)
        microseconds = nanos_of_milli // 1000
        if round_up_timestamp and nanos_of_milli % 1000 != 0:
            microseconds += 1
        return _EPOCH + timedelta(
            milliseconds=millis,
            microseconds=microseconds)

    return None


def _type_precision(type_name: str, default: int = 0) -> int:
    params = _type_params(type_name)
    if not params:
        return default
    return int(params[0])


def _type_scale(type_name: str) -> int:
    params = _type_params(type_name)
    if len(params) < 2:
        return 0
    return int(params[1])


def _type_params(type_name: str) -> List[str]:
    match = re.search(r"\(([^)]*)\)", type_name)
    if match is None:
        return []
    return [p.strip() for p in match.group(1).split(",") if p.strip()]
