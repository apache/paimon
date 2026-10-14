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

from pypaimon.common.predicate_json_parser import _collect_all_field_refs_from_transform
from pypaimon.execution.python.read.predicate_json_parser import _apply_predicate_transform
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.utils.arrow_utils import row_kind_column_index, row_kind_field, with_no_row_kind, zero_column_batch


class RecordReaderToBatchAdapter(RecordBatchReader):

    def __init__(self, inner, schema: pa.Schema, chunk_size: int = 65536, include_row_kind: bool = False):
        self._inner = inner
        self._schema = schema if include_row_kind else with_no_row_kind(schema)
        self._chunk_size = chunk_size
        self._exhausted = False
        self._pending_iterator = None
        self._include_row_kind = include_row_kind
        self._adopt_blob_metadata(inner)

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
            self._refresh_blob_view_lookup(self._inner)

        if not row_tuples:
            return None
        return self._flush(row_tuples, row_kinds)

    def _flush(self, row_tuples, row_kinds=None):
        columns_data = list(zip(*row_tuples))
        pydict = {
            name: list(col)
            for name, col in zip(self._schema.names, columns_data)
        }
        batch = (pa.RecordBatch.from_pydict(pydict, schema=self._schema) if self._schema
                 else zero_column_batch(len(row_tuples), self._schema.metadata))
        if row_kinds:
            row_kind_array = pa.array(row_kinds, type=pa.string())
            new_schema = pa.schema([row_kind_field(batch.schema)] + list(batch.schema),
                                   metadata=batch.schema.metadata)
            columns = [row_kind_array] + [batch.column(i) for i in range(batch.num_columns)]
            batch = pa.RecordBatch.from_arrays(columns, schema=new_schema)
        return batch

    def close(self):
        self._inner.close()


class BatchToRecordReaderAdapter(RecordReader):
    """Convert a batch-level RecordBatchReader back to a row-level RecordReader."""

    def __init__(self, inner: RecordBatchReader):
        self._inner = inner
        self._adopt_blob_metadata(inner)

    def read_batch(self):
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        self._refresh_blob_view_lookup(self._inner)
        return _ArrowBatchIterator(
            batch,
            file_io=self.file_io,
            blob_field_indices=self.blob_field_indices,
            descriptor_field_indices=self.descriptor_field_indices,
            blob_view_lookup=self.blob_view_lookup,
            vector_field_indices=self.vector_field_indices,
            resolved_blob_field_indices=self.resolved_blob_field_indices,
            blob_base_path=self.blob_base_path,
        )

    def close(self):
        self._inner.close()


class _ArrowBatchIterator(RecordIterator):

    def __init__(self, batch: pa.RecordBatch,
                 file_io=None, blob_field_indices=None,
                 descriptor_field_indices=None, blob_view_lookup=None,
                 vector_field_indices=None, resolved_blob_field_indices=None,
                 blob_base_path=None):
        self._batch = batch
        self._idx = 0
        self._rk_idx = row_kind_column_index(batch.schema)
        self._has_rk = self._rk_idx >= 0
        if self._has_rk:
            self._data_cols = [j for j in range(batch.num_columns) if j != self._rk_idx]
        else:
            self._rk_idx = -1
            self._data_cols = list(range(batch.num_columns))
        self._file_io = file_io
        self._blob_field_indices = blob_field_indices
        self._descriptor_field_indices = descriptor_field_indices
        self._blob_view_lookup = blob_view_lookup
        self._vector_field_indices = vector_field_indices
        self._resolved_blob_field_indices = resolved_blob_field_indices
        self._blob_base_path = blob_base_path

    def next(self):
        if self._idx >= self._batch.num_rows:
            return None
        row_tuple = tuple(
            self._batch.column(j)[self._idx].as_py()
            for j in self._data_cols
        )
        row = OffsetRow(
            row_tuple, 0, len(self._data_cols),
            file_io=self._file_io,
            blob_field_indices=self._blob_field_indices,
            descriptor_field_indices=self._descriptor_field_indices,
            blob_view_lookup=self._blob_view_lookup,
            vector_field_indices=self._vector_field_indices,
            resolved_blob_field_indices=self._resolved_blob_field_indices,
            blob_base_path=self._blob_base_path)
        if self._has_rk:
            from pypaimon.table.row.row_kind import RowKind
            kind_str = self._batch.column(self._rk_idx)[self._idx].as_py()
            row.set_row_kind_byte(RowKind.from_string(kind_str).value)
        self._idx += 1
        return row


class AuthFilterReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, filter_fn: Callable[[pa.RecordBatch], pa.Array]):
        self._inner = inner_reader
        self._filter_fn = filter_fn
        self._adopt_metadata(inner_reader)

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        self._refresh_blob_view_lookup(self._inner)
        mask = self._filter_fn(batch)
        return batch.filter(mask)

    def close(self):
        self._inner.close()


def compile_auth_masking_rules(masking_rules: Dict[str, str], read_fields: List):
    """Parse and validate masking transforms for batch and native readers."""
    read_field_names = {f.name for f in read_fields}
    parsed = {}
    for col, transform_json in masking_rules.items():
        if col not in read_field_names or not transform_json:
            continue
        transform = json.loads(transform_json)
        if transform is not None:
            parsed[col] = transform
    for col_name, transform in parsed.items():
        for ref_name in _collect_all_field_refs_from_transform(transform):
            if ref_name not in read_field_names:
                raise RuntimeError(
                    f"Column masking refers to field '{ref_name}' which is not present "
                    f"in output row type. Available fields: {read_field_names}"
                )
    return parsed


def apply_auth_masking(batch: pa.RecordBatch, parsed_rules: Dict[str, dict]):
    """Apply compiled masking rules against the same original input batch."""
    original_batch = batch
    masked_columns = {}
    for col_name, transform in parsed_rules.items():
        if col_name in original_batch.schema.names:
            col_idx = original_batch.schema.get_field_index(col_name)
            target_col_type = original_batch.schema.field(col_idx).type
            masked_columns[col_idx] = _apply_predicate_transform(
                transform, original_batch, null_type=target_col_type)
    if not masked_columns:
        return batch
    columns = list(batch.columns)
    fields = list(batch.schema)
    for col_idx, masked_array in masked_columns.items():
        original_field = original_batch.schema.field(col_idx)
        columns[col_idx] = masked_array
        fields[col_idx] = original_field.with_type(masked_array.type).with_nullable(True)
    return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields, metadata=batch.schema.metadata))


class AuthMaskingReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, masking_rules: Dict[str, str], read_fields: List):
        self._inner = inner_reader
        self._masking_rules = masking_rules
        self._read_fields = read_fields
        self._adopt_metadata(inner_reader)
        self._parsed_rules = compile_auth_masking_rules(
            masking_rules, read_fields)

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        self._refresh_blob_view_lookup(self._inner)
        return apply_auth_masking(batch, self._parsed_rules)

    def close(self):
        self._inner.close()


class ColumnProjectReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, columns: List[str], source_fields=None):
        self._inner = inner_reader
        self._columns = columns
        self._adopt_metadata(inner_reader)
        if source_fields is not None:
            source_indices = {field.name: i for i, field in enumerate(source_fields)}
            for attr in ('blob_field_indices', 'descriptor_field_indices',
                         'resolved_blob_field_indices', 'vector_field_indices'):
                indices = getattr(self, attr)
                if indices is not None:
                    setattr(self, attr, {i for i, name in enumerate(columns)
                                         if source_indices[name] in indices})

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        self._refresh_blob_view_lookup(self._inner)
        columns = self._columns
        row_kind_index = row_kind_column_index(batch.schema)
        if row_kind_index >= 0:
            name = batch.schema.field(row_kind_index).name
            if name not in columns:
                columns = [name] + list(columns)
        if not columns:
            return zero_column_batch(batch.num_rows, batch.schema.metadata)
        return pa.RecordBatch.from_arrays(
            [batch.column(name) for name in columns],
            schema=pa.schema([batch.schema.field(name) for name in columns], metadata=batch.schema.metadata))

    def close(self):
        self._inner.close()
