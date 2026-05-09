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

from typing import List

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.reader.deduplicate_merge_function import \
    DeduplicateMergeFunction
from pypaimon.table.row.key_value import KeyValue
from pypaimon.write.writer.data_writer import DataWriter


class KeyValueDataWriter(DataWriter):
    """Data writer for primary key tables with system fields and sorting.

    On flush, applies the table's ``MergeFunction`` to fold rows that
    share a primary key down to a single row, mirroring Java
    ``SortBufferWriteBuffer.forEach`` /
    ``MergeIterator.advanceIfNeeded`` (paimon-core/.../mergetree/
    SortBufferWriteBuffer.java). This is what enforces the LSM "PK
    unique within a file" invariant the read-side ``raw_convertible``
    fast path relies on.
    """

    def __init__(self, table, partition, bucket, max_seq_number,
                 options=None, write_cols=None, merge_function=None):
        super().__init__(table, partition, bucket, max_seq_number,
                         options, write_cols)
        # Defaults to deduplicate so direct callers (tests / future code
        # paths that don't go through FileStoreWrite) don't accidentally
        # skip the merge step entirely.
        self._merge_function = merge_function or DeduplicateMergeFunction()

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        enhanced_data = self._add_system_fields(data)
        return pa.Table.from_batches([self._sort_by_primary_key(enhanced_data)])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        combined = pa.concat_tables([existing_data, new_data])
        return self._sort_by_primary_key(combined)

    def prepare_commit(self) -> List[DataFileMeta]:
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            self.pending_data = self._merge_pending_by_pk(self.pending_data)
        return super().prepare_commit()

    def _check_and_roll_if_needed(self):
        # Mirror Java MergeTreeWriter: collapse same-PK runs *before*
        # slicing for size, so each flushed file individually maintains
        # PK uniqueness even when a single buffer spans multiple files.
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            self.pending_data = self._merge_pending_by_pk(self.pending_data)
        super()._check_and_roll_if_needed()

    def _merge_pending_by_pk(self, data: pa.Table) -> pa.Table:
        """Fold same-PK runs in ``data`` using ``self._merge_function``.

        Mirrors Java ``MergeIterator.advanceIfNeeded``
        (SortBufferWriteBuffer.java:241-268). ``data`` is required to
        already be sorted by ``(primary_key, _SEQUENCE_NUMBER)`` --
        ``_process_data`` / ``_merge_data`` enforce that.
        """
        n = data.num_rows
        if n < 2:
            # Single-row buffer cannot have duplicates; sidestep the
            # row-by-row pyarrow round-trip in the common streaming case.
            return data

        rows = data.to_pylist()
        col_names = data.schema.names
        key_arity = len(self.trimmed_primary_keys)
        # System fields sit at indices [key_arity, key_arity + 1] (the
        # _SEQUENCE_NUMBER and _VALUE_KIND columns added by
        # _add_system_fields). Everything to the right is the value side.
        value_arity = len(col_names) - key_arity - 2

        merged_rows: List[dict] = []
        i = 0
        while i < n:
            j = i
            first_key = self._key_tuple(rows[i], col_names, key_arity)
            while j < n and \
                    self._key_tuple(rows[j], col_names, key_arity) == first_key:
                j += 1
            run = rows[i:j]
            self._merge_function.reset()
            for r in run:
                kv = KeyValue(key_arity, value_arity)
                kv.replace(self._row_to_tuple(r, col_names))
                self._merge_function.add(kv)
            result_kv = self._merge_function.get_result()
            if result_kv is not None:
                merged_rows.append(
                    self._kv_to_row(result_kv, col_names,
                                    key_arity, value_arity))
            i = j

        if not merged_rows:
            return data.slice(0, 0)
        return pa.Table.from_pylist(merged_rows, schema=data.schema)

    @staticmethod
    def _key_tuple(row: dict, col_names: List[str], key_arity: int) -> tuple:
        return tuple(row[col_names[i]] for i in range(key_arity))

    @staticmethod
    def _row_to_tuple(row: dict, col_names: List[str]) -> tuple:
        return tuple(row[name] for name in col_names)

    @staticmethod
    def _kv_to_row(kv: KeyValue, col_names: List[str],
                   key_arity: int, value_arity: int) -> dict:
        out = {}
        for i in range(key_arity):
            out[col_names[i]] = kv.key.get_field(i)
        out[col_names[key_arity]] = kv.sequence_number
        out[col_names[key_arity + 1]] = kv.value_row_kind_byte
        for i in range(value_arity):
            out[col_names[key_arity + 2 + i]] = kv.value.get_field(i)
        return out

    def _add_system_fields(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Add system fields: _KEY_{pk_key}, _SEQUENCE_NUMBER, _VALUE_KIND."""
        num_rows = data.num_rows

        new_arrays = []
        new_fields = []

        for pk_key in self.trimmed_primary_keys:
            if pk_key in data.schema.names:
                key_column = data.column(pk_key)
                new_arrays.append(key_column)
                src_field = data.schema.field(pk_key)
                new_fields.append(pa.field(f'_KEY_{pk_key}', src_field.type, nullable=src_field.nullable))

        sequence_column = pa.array([self.sequence_generator.next() for _ in range(num_rows)], type=pa.int64())
        new_arrays.append(sequence_column)
        new_fields.append(pa.field('_SEQUENCE_NUMBER', pa.int64(), nullable=False))

        # TODO: support real row kind here
        value_kind_column = pa.array([0] * num_rows, type=pa.int8())
        new_arrays.append(value_kind_column)
        new_fields.append(pa.field('_VALUE_KIND', pa.int8(), nullable=False))

        for i in range(data.num_columns):
            new_arrays.append(data.column(i))
            new_fields.append(data.schema.field(i))

        return pa.RecordBatch.from_arrays(new_arrays, schema=pa.schema(new_fields))

    def _sort_by_primary_key(self, data: pa.RecordBatch) -> pa.RecordBatch:
        sort_keys = [(key, 'ascending') for key in self.trimmed_primary_keys]
        if '_SEQUENCE_NUMBER' in data.schema.names:
            sort_keys.append(('_SEQUENCE_NUMBER', 'ascending'))

        sorted_indices = pc.sort_indices(data, sort_keys=sort_keys)
        sorted_batch = data.take(sorted_indices)
        return sorted_batch
