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
import pyarrow.compute as pc

from pypaimon.common.predicate_json_parser import (
    _collect_all_field_refs_from_transform,
    _paimon_type_to_arrow,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class RecordReaderToBatchAdapter(RecordBatchReader):

    def __init__(self, inner, schema: pa.Schema, chunk_size: int = 65536, include_row_kind: bool = False):
        self._inner = inner
        self._schema = schema
        self._chunk_size = chunk_size
        self._exhausted = False
        self._pending_iterator = None
        self._include_row_kind = include_row_kind

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


class AuthFilterReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, filter_fn: Callable[[pa.RecordBatch], pa.Array]):
        self._inner = inner_reader
        self._filter_fn = filter_fn

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
        self._parsed_rules = {col: json.loads(tj) for col, tj in masking_rules.items()}
        read_field_names = {f.name for f in read_fields}
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
                masked_columns[col_idx] = self._apply_transform(transform, original_batch, target_col_type)
        for col_idx, masked_array in masked_columns.items():
            original_field = original_batch.schema.field(col_idx)
            if masked_array.type != original_field.type:
                masked_array = pc.cast(masked_array, original_field.type)
            batch = batch.set_column(
                col_idx, pa.field(original_field.name, original_field.type, nullable=True), masked_array)
        return batch

    def close(self):
        self._inner.close()

    def _apply_transform(
            self,
            transform: dict,
            original_batch: pa.RecordBatch,
            target_col_type: pa.DataType,
    ) -> pa.Array:
        name = transform["name"]

        if name == "NULL":
            return pa.nulls(len(original_batch), type=target_col_type)

        elif name == "FIELD_REF":
            ref_name = transform["fieldRef"]["name"]
            return original_batch.column(ref_name)

        elif name == "CAST":
            ref_name = transform["fieldRef"]["name"]
            source_col = original_batch.column(ref_name)
            target_type = _paimon_type_to_arrow(transform["type"])
            return pc.cast(source_col, target_type)

        elif name == "UPPER":
            input_col = self._resolve_input(transform["inputs"][0], original_batch)
            return pc.utf8_upper(input_col)

        elif name == "LOWER":
            input_col = self._resolve_input(transform["inputs"][0], original_batch)
            return pc.utf8_lower(input_col)

        elif name == "CONCAT":
            return self._apply_concat(transform["inputs"], original_batch)

        elif name == "CONCAT_WS":
            return self._apply_concat_ws(transform["inputs"], original_batch)

        raise ValueError(f"Unknown transform type: {name}")

    def _resolve_input(self, inp, original_batch: pa.RecordBatch) -> pa.Array:
        if isinstance(inp, dict):
            return original_batch.column(inp["name"])
        elif isinstance(inp, str):
            return pa.array([inp] * len(original_batch), type=pa.string())
        elif inp is None:
            return pa.nulls(len(original_batch), type=pa.string())
        return pa.array([str(inp)] * len(original_batch), type=pa.string())

    def _apply_concat(self, inputs: list, original_batch: pa.RecordBatch) -> pa.Array:
        resolved = [self._resolve_input(inp, original_batch) for inp in inputs]
        if not resolved:
            return pa.nulls(len(original_batch), type=pa.string())
        return pc.binary_join_element_wise(*resolved, "")

    def _apply_concat_ws(self, inputs: list, original_batch: pa.RecordBatch) -> pa.Array:
        if len(inputs) < 2:
            return pa.nulls(len(original_batch), type=pa.string())
        sep = self._resolve_input(inputs[0], original_batch)
        values = [self._resolve_input(inp, original_batch) for inp in inputs[1:]]
        return pc.binary_join_element_wise(*values, sep, null_handling='skip')


class ColumnProjectReader(RecordBatchReader):

    def __init__(self, inner_reader: RecordBatchReader, columns: List[str]):
        self._inner = inner_reader
        self._columns = columns

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
