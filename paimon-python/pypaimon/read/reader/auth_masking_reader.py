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

import json
from typing import Callable, Dict, List, Optional

import pyarrow as pa

from pypaimon.common.predicate_json_parser import (
    _apply_predicate_transform,
    _collect_all_field_refs_from_transform,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.offset_row import OffsetRow


# ---------------------------------------------------------------------------
#  Adapters: row ↔ batch conversion
# ---------------------------------------------------------------------------

class RecordReaderToBatchAdapter(RecordBatchReader):
    """Convert a row-level RecordReader to a batch-level RecordBatchReader."""

    def __init__(self, inner, schema: pa.Schema, chunk_size: int = 65536, include_row_kind: bool = False):
        self._inner = inner
        self._schema = schema
        self._chunk_size = chunk_size
        self._exhausted = False
        self._pending_iterator = None
        self._include_row_kind = include_row_kind
        self.blob_field_indices = getattr(inner, 'blob_field_indices', None)
        self.vector_field_indices = getattr(inner, 'vector_field_indices', None)

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        if self._exhausted:
            return None
        row_tuples = []
        row_kinds = []
        while len(row_tuples) < self._chunk_size:
            if self._pending_iterator is not None:
                row = self._pending_iterator.next()
                while row is not None:
                    row_tuples.append(
                        row.row_tuple[row.offset:row.offset + row.arity])
                    if self._include_row_kind:
                        row_kinds.append(row.get_row_kind().to_string())
                    if len(row_tuples) >= self._chunk_size:
                        return self._flush(row_tuples, row_kinds)
                    row = self._pending_iterator.next()
                self._pending_iterator = None

            row_iterator = self._inner.read_batch()
            if row_iterator is None:
                self._exhausted = True
                break
            self._pending_iterator = row_iterator

        if not row_tuples:
            return None
        return self._flush(row_tuples, row_kinds)

    def _flush(self, row_tuples, row_kinds=None):
        columns_data = list(zip(*row_tuples))
        pydict = {
            name: list(col)
            for name, col in zip(self._schema.names, columns_data)
        }
        batch = pa.RecordBatch.from_pydict(pydict, schema=self._schema)
        if row_kinds:
            row_kind_array = pa.array(row_kinds, type=pa.string())
            row_kind_field = pa.field("_row_kind", pa.string())
            new_schema = pa.schema([row_kind_field] + list(batch.schema))
            columns = [row_kind_array] + [batch.column(i) for i in range(batch.num_columns)]
            batch = pa.RecordBatch.from_arrays(columns, schema=new_schema)
        return batch

    def close(self):
        self._inner.close()


class BatchToRecordReaderAdapter(RecordReader):
    """Convert a batch-level RecordBatchReader back to a row-level RecordReader."""

    def __init__(self, inner: RecordBatchReader):
        self._inner = inner

    def read_batch(self):
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        return _ArrowBatchIterator(batch)

    def close(self):
        self._inner.close()


class _ArrowBatchIterator(RecordIterator):

    def __init__(self, batch: pa.RecordBatch):
        self._batch = batch
        self._idx = 0
        self._has_rk = "_row_kind" in batch.schema.names
        if self._has_rk:
            self._rk_idx = batch.schema.get_field_index("_row_kind")
            self._data_cols = [j for j in range(batch.num_columns) if j != self._rk_idx]
        else:
            self._rk_idx = -1
            self._data_cols = list(range(batch.num_columns))

    def next(self):
        if self._idx >= self._batch.num_rows:
            return None
        row_tuple = tuple(
            self._batch.column(j)[self._idx].as_py()
            for j in self._data_cols
        )
        row = OffsetRow(row_tuple, 0, len(self._data_cols))
        if self._has_rk:
            from pypaimon.table.row.row_kind import RowKind
            kind_str = self._batch.column(self._rk_idx)[self._idx].as_py()
            row.set_row_kind_byte(RowKind.from_string(kind_str).value)
        self._idx += 1
        return row


# ---------------------------------------------------------------------------
#  Batch-level auth readers
# ---------------------------------------------------------------------------

class AuthFilterReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, filter_fn: Callable[[pa.RecordBatch], pa.Array]):
        self._inner = inner_reader
        self._filter_fn = filter_fn
        self.blob_field_indices = inner_reader.blob_field_indices
        self.vector_field_indices = inner_reader.vector_field_indices

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        mask = self._filter_fn(batch)
        return batch.filter(mask)

    def close(self):
        self._inner.close()


class AuthMaskingReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, masking_rules: Dict[str, str], read_fields: List):
        self._inner = inner_reader
        self._masking_rules = masking_rules
        self._read_fields = read_fields
        self.blob_field_indices = inner_reader.blob_field_indices
        self.vector_field_indices = inner_reader.vector_field_indices
        read_field_names = {f.name for f in read_fields}
        parsed = {}
        for col, tj in masking_rules.items():
            if col not in read_field_names:
                continue
            if not tj:
                continue
            transform = json.loads(tj)
            if transform is None:
                continue
            parsed[col] = transform
        self._parsed_rules = parsed
        for col_name, transform in self._parsed_rules.items():
            for ref_name in _collect_all_field_refs_from_transform(transform):
                if ref_name not in read_field_names:
                    raise RuntimeError(
                        f"Column masking refers to field '{ref_name}' which is not present "
                        f"in output row type. Available fields: {read_field_names}"
                    )

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        original_batch = batch
        masked_columns = {}
        for col_name, transform in self._parsed_rules.items():
            if col_name in original_batch.schema.names:
                col_idx = original_batch.schema.get_field_index(col_name)
                target_col_type = original_batch.schema.field(col_idx).type
                masked_columns[col_idx] = self._apply_masking_transform(transform, original_batch, target_col_type)
        for col_idx, masked_array in masked_columns.items():
            original_field = original_batch.schema.field(col_idx)
            batch = batch.set_column(
                col_idx,
                pa.field(original_field.name, masked_array.type, nullable=True),
                masked_array)
        return batch

    def close(self):
        self._inner.close()

    def _apply_masking_transform(
            self,
            transform: dict,
            original_batch: pa.RecordBatch,
            target_col_type: pa.DataType,
    ) -> pa.Array:
        return _apply_predicate_transform(
            transform, original_batch, null_type=target_col_type)


class ColumnProjectReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, columns: List[str]):
        self._inner = inner_reader
        self._columns = columns
        self.blob_field_indices = inner_reader.blob_field_indices
        self.vector_field_indices = inner_reader.vector_field_indices

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        columns = self._columns
        if "_row_kind" in batch.schema.names and "_row_kind" not in columns:
            columns = ["_row_kind"] + list(columns)
        return batch.select(columns)

    def close(self):
        self._inner.close()
