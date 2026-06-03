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

from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO, supports_pread, pread
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class FormatMosaicReader(RecordBatchReader):

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024):
        from mosaic import MosaicReader

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

        if push_down_predicate is not None:
            self._predicate = push_down_predicate
        else:
            self._predicate = None

    def _next_row_group_batches(self):
        while self._current_rg < self._num_row_groups:
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
