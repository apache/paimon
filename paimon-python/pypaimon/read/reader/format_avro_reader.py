################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import List, Optional, Any

import fastavro
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


class FormatAvroReader(RecordBatchReader):
    """
    An ArrowBatchReader for reading Avro files using fastavro, filters records based on the
    provided predicate and projection, and converts Avro records to RecordBatch format.
    """

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str], full_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 4096):
        file_path_for_io = file_io.to_filesystem_path(file_path)
        self._file = file_io.filesystem.open_input_file(file_path_for_io)
        self._avro_reader = fastavro.reader(self._file)
        self._batch_size = batch_size
        self._push_down_predicate = push_down_predicate

        self._fields = read_fields
        full_fields_map = {field.name: field for field in full_fields}
        projected_data_fields = [full_fields_map[name] for name in read_fields]
        self._schema = PyarrowFieldParser.from_paimon_schema(projected_data_fields)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        pydict_data = {name: [] for name in self._fields}
        records_in_batch = 0

        for record in self._avro_reader:
            for col_name in self._fields:
                pydict_data[col_name].append(record.get(col_name))
            records_in_batch += 1
            if records_in_batch >= self._batch_size:
                break

        if records_in_batch == 0:
            return None
        if self._push_down_predicate is None:
            return pa.RecordBatch.from_pydict(pydict_data, self._schema)
        else:
            pa_batch = pa.Table.from_pydict(pydict_data, self._schema)
            dataset = ds.InMemoryDataset(pa_batch)
            scanner = dataset.scanner(filter=self._push_down_predicate)
            combine_chunks = scanner.to_table().combine_chunks()
            if combine_chunks.num_rows > 0:
                return combine_chunks.to_batches()[0]
            else:
                return None

    def close(self):
        if self._file:
            self._file.close()
            self._file = None
