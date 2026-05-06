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

from typing import List, Optional, Any, Set, Tuple

import pyarrow as pa
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class FormatVortexReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Vortex file,
    and filters it based on the provided predicate and projection.
    """

    # row_indices: from IndexedSplit (ANN vector search), discrete local row offsets within the file.
    # shard_range: from SlicedSplit (parallel shard scan), a contiguous [start, end) row range within the file.
    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 row_indices: Optional[List[int]] = None,
                 shard_range: Optional[Tuple[int, int]] = None,
                 predicate_fields: Optional[Set[str]] = None):
        import vortex

        from pypaimon.read.reader.vortex_utils import to_vortex_specified
        file_path_for_vortex, store_kwargs = to_vortex_specified(file_io, file_path)

        if store_kwargs:
            from vortex import store
            vortex_store = store.from_url(file_path_for_vortex, **store_kwargs)
            vortex_file = vortex_store.open()
        else:
            vortex_file = vortex.open(file_path_for_vortex)

        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]

        # Identify which fields exist in the file and which are missing
        file_schema_names = set(vortex_file.dtype.to_arrow_schema().names)
        self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
        self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

        columns_for_vortex = self.existing_fields if self.existing_fields else None

        # Try to convert Arrow predicate to Vortex expr for native push-down
        vortex_expr = None
        if push_down_predicate is not None:
            try:
                from vortex.arrow.expression import arrow_to_vortex
                arrow_schema = vortex_file.dtype.to_arrow_schema()
                vortex_expr = arrow_to_vortex(push_down_predicate, arrow_schema)
            except Exception:
                pass

        indices = None
        if row_indices is not None:
            indices = vortex.array(row_indices)
        elif shard_range is not None:
            indices = vortex.array(range(shard_range[0], shard_range[1]))

        self.record_batch_reader = vortex_file.scan(
            columns_for_vortex, expr=vortex_expr, indices=indices, batch_size=batch_size).to_arrow()

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

        # Collect predicate-referenced fields for targeted view type casting
        self._cast_fields = predicate_fields if predicate_fields and vortex_expr is not None else set()

    @staticmethod
    def _cast_view_types(batch: RecordBatch, target_fields: Set[str]) -> RecordBatch:
        """Cast string_view/binary_view columns to string/binary, only for target fields."""
        if not target_fields:
            return batch
        columns = []
        fields = []
        changed = False
        for i in range(batch.num_columns):
            col = batch.column(i)
            field = batch.schema.field(i)
            if field.name in target_fields:
                if col.type == pa.string_view():
                    col = col.cast(pa.utf8())
                    field = field.with_type(pa.utf8())
                    changed = True
                elif col.type == pa.binary_view():
                    col = col.cast(pa.binary())
                    field = field.with_type(pa.binary())
                    changed = True
            columns.append(col)
            fields.append(field)
        if changed:
            return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
        return batch

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            batch = next(self.record_batch_reader)
            batch = self._cast_view_types(batch, self._cast_fields)

            if not self.missing_fields:
                return batch

            def _type_for_missing(name: str) -> pa.DataType:
                if self._output_schema is not None:
                    idx = self._output_schema.get_field_index(name)
                    if idx >= 0:
                        return self._output_schema.field(idx).type
                return pa.null()

            missing_columns = [
                pa.nulls(batch.num_rows, type=_type_for_missing(name))
                for name in self.missing_fields
            ]

            # Reconstruct the batch with all fields in the correct order
            all_columns = []
            out_fields = []
            for field_name in self._read_field_names:
                if field_name in self.existing_fields:
                    column_idx = self.existing_fields.index(field_name)
                    all_columns.append(batch.column(column_idx))
                    out_fields.append(batch.schema.field(column_idx))
                else:
                    column_idx = self.missing_fields.index(field_name)
                    col_type = _type_for_missing(field_name)
                    all_columns.append(missing_columns[column_idx])
                    nullable = not SpecialFields.is_system_field(field_name)
                    out_fields.append(pa.field(field_name, col_type, nullable=nullable))
            return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields))

        except StopIteration:
            return None

    def close(self):
        self.record_batch_reader = None
