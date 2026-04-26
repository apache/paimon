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
#  limitations under the License.
################################################################################

from typing import List, Optional, Any

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance_utils import to_lance_specified
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class FormatLanceReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Lance file using PyArrow,
    and filters it based on the provided predicate and projection.
    """

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024):
        """Initialize Lance reader."""
        import lance

        self._read_field_names = [f.name for f in read_fields]

        file_path_for_lance, storage_options = to_lance_specified(file_io, file_path)

        # Read once, then select existing columns in memory
        lance_reader = lance.file.LanceFileReader(
            file_path_for_lance,
            storage_options=storage_options)
        pa_table = lance_reader.read_all().to_table()
        file_schema_names = set(pa_table.schema.names)

        self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
        self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

        if self.existing_fields:
            pa_table = pa_table.select(self.existing_fields)

        # Precompute output schema for missing fields
        if self.missing_fields:
            output_schema = PyarrowFieldParser.from_paimon_schema(read_fields)
            self._missing_out_fields = []
            for name in self.missing_fields:
                idx = output_schema.get_field_index(name)
                col_type = output_schema.field(idx).type if idx >= 0 else pa.null()
                nullable = not SpecialFields.is_system_field(name)
                self._missing_out_fields.append(pa.field(name, col_type, nullable=nullable))

        if push_down_predicate is not None:
            in_memory_dataset = ds.InMemoryDataset(pa_table)
            scanner = in_memory_dataset.scanner(filter=push_down_predicate, batch_size=batch_size)
            self.reader = scanner.to_reader()
        else:
            self.reader = iter(pa_table.to_batches(max_chunksize=batch_size))

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            # Handle both scanner reader and iterator
            if hasattr(self.reader, 'read_next_batch'):
                batch = self.reader.read_next_batch()
            else:
                batch = next(self.reader)

            if not self.missing_fields:
                return batch

            # Reconstruct the batch with all fields in the correct order
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

        except StopIteration:
            return None

    def close(self):
        if self.reader is not None:
            self.reader = None
