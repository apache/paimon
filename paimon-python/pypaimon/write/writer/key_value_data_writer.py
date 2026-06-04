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

from typing import List, Union

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.reader.deduplicate_merge_function import \
    DeduplicateMergeFunction
from pypaimon.table.row.key_value import KeyValue
from pypaimon.write.writer.data_writer import DataWriter


class KeyValueDataWriter(DataWriter):
    """Data writer for primary key tables with system fields and sorting.

    Accumulates incoming batches in ``pending_data`` without sorting or
    folding on the write path. Sort and ``MergeFunction``-based fold
    are deferred to flush time (``_flush_all``), where the result is
    roll-written into one or more data files. This enforces the LSM
    "PK unique within a file" invariant the read-side
    ``raw_convertible`` fast path relies on, while keeping per-write
    cost bounded.
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
        # No sort here: sorting once at flush is strictly cheaper than
        # per-batch sort + a final global sort. ``pending_data`` ends
        # up as a concat of unsorted batches; ``_flush_all`` sorts it
        # exactly once before folding.
        enhanced_data = self._add_system_fields(data)
        return pa.Table.from_batches([enhanced_data])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        # Plain concat. Sort + fold both run inside ``_flush_all`` so
        # N writes incur 1 sort instead of N sorts.
        return pa.concat_tables([existing_data, new_data])

    def prepare_commit(self) -> List[DataFileMeta]:
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            self._flush_all()
        # ``_flush_all`` leaves ``pending_data = None``, so super's
        # prepare_commit just returns ``committed_files``.
        return super().prepare_commit()

    def _check_and_roll_if_needed(self):
        # Buffer overflowed target_file_size: sort + fold + roll-write
        # the whole buffer as multiple files in one pass. Unlike the
        # base class's slice loop, we never keep a slice remainder in
        # ``pending_data`` -- flush empties the buffer outright.
        if (self.pending_data is not None
                and self.pending_data.num_rows > 0
                and self.pending_data.nbytes > self.target_file_size):
            self._flush_all()

    def close(self):
        # Override the base ``close`` because its straight
        # ``_write_data_to_file(pending_data)`` would land an unsorted,
        # un-folded buffer on disk -- violating the file-internal
        # PK-unique invariant. Route the final flush through
        # ``_flush_all`` so the contract holds even on the
        # close-without-prepare_commit path.
        try:
            if self.pending_data is not None and self.pending_data.num_rows > 0:
                self._flush_all()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                "Exception occurs when closing writer. Cleaning up.",
                exc_info=e)
            self.abort()
            raise e
        finally:
            self.pending_data = None

    def _flush_all(self) -> None:
        """Sort + fold the entire buffer, then roll-write as files.

        On return, ``pending_data is None`` and every flushed chunk
        has been recorded in ``committed_files``. The buffer is
        always fully drained per flush: no slice remainder is
        carried back into ``pending_data``.
        """
        if self.pending_data is None or self.pending_data.num_rows == 0:
            self.pending_data = None
            return
        sorted_data = self._sort_by_primary_key(self.pending_data)
        folded = self._merge_pending_by_pk(sorted_data)
        self.pending_data = None
        if folded.num_rows == 0:
            return
        self._roll_write(folded)

    def _roll_write(self, data: pa.Table) -> None:
        """Write ``data`` as one or more files, each <= target_file_size.

        ``data`` is required to be PK-unique (the fold guarantees
        that), so any slice of it is also PK-unique -- splitting for
        size does not violate the LSM file-internal invariant.
        Reuses ``_find_optimal_split_point`` / ``_write_data_to_file``
        from the base class.
        """
        while data.num_rows > 0:
            if data.nbytes <= self.target_file_size:
                self._write_data_to_file(data)
                return
            split_row = self._find_optimal_split_point(
                data, self.target_file_size)
            if split_row <= 0:
                # Single row already exceeds target_file_size; nothing
                # to gain from further slicing, write it as-is.
                self._write_data_to_file(data)
                return
            self._write_data_to_file(data.slice(0, split_row))
            data = data.slice(split_row)

    def _merge_pending_by_pk(self, data: pa.Table) -> pa.Table:
        """Fold same-PK runs in ``data`` using ``self._merge_function``.

        ``data`` is required to already be sorted by
        ``(primary_key, _SEQUENCE_NUMBER)``. ``_flush_all`` is the
        only caller and runs ``_sort_by_primary_key`` immediately
        before this method, so the precondition holds.

        NOTE(follow-up): the merge runs row-by-row over
        ``data.to_pydict()`` / ``pa.Table.from_pydict``. Arrow types
        with non-trivial Python representations (Decimal128 with
        specific precision/scale, timestamps with timezone or
        sub-millisecond units, durations, deeply nested structs) can
        drift across this round-trip. A columnar merge implementation
        would close the gap and is tracked as a follow-up; until
        then, partial-update on those types should be avoided in
        pypaimon.
        """
        n = data.num_rows
        if n < 2:
            # Single-row buffer cannot have duplicates; sidestep the
            # row-by-row pyarrow round-trip in the common streaming case.
            return data

        col_names = data.schema.names
        # ``to_pydict`` works on pyarrow >= 6 (Python 3.6 CI ships 6.0.1),
        # unlike ``to_pylist`` which only landed in pyarrow 7.
        col_dict = data.to_pydict()
        rows = [{name: col_dict[name][i] for name in col_names}
                for i in range(n)]
        key_arity = len(self.trimmed_primary_keys)
        # System fields sit at indices [key_arity, key_arity + 1] (the
        # _SEQUENCE_NUMBER and _VALUE_KIND columns added by
        # _add_system_fields). Everything to the right is the value side.
        value_arity = len(col_names) - key_arity - 2

        # Pool one ``KeyValue`` for the whole fold. Safe because:
        # - ``DeduplicateMergeFunction.add`` stores the kv reference; the
        #   reused instance always carries the most recent ``replace``,
        #   which is exactly the "latest wins" the engine wants.
        # - ``PartialUpdateMergeFunction.add`` also stores a reference,
        #   but ``get_result`` snapshots key + sequence into a fresh
        #   tuple before returning, so the consumed result is decoupled
        #   from any later ``replace`` on the pooled kv.
        # - ``FirstRowMergeFunction.add`` ``copy()``s the first kv, so it
        #   keeps the first row rather than tracking later ``replace``s on
        #   the pooled kv (which would otherwise yield the last row).
        # This drops per-row ``KeyValue``/``OffsetRow`` allocations and
        # the resulting GC churn on large buffers.
        pooled_kv = KeyValue(key_arity, value_arity)

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
                pooled_kv.replace(self._row_to_tuple(r, col_names))
                self._merge_function.add(pooled_kv)
            result_kv = self._merge_function.get_result()
            if result_kv is not None:
                merged_rows.append(
                    self._kv_to_row(result_kv, col_names,
                                    key_arity, value_arity))
            i = j

        if not merged_rows:
            return data.slice(0, 0)
        result_dict = {name: [r[name] for r in merged_rows]
                       for name in data.schema.names}
        return pa.Table.from_pydict(result_dict, schema=data.schema)

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

    def _sort_by_primary_key(
        self, data: Union[pa.RecordBatch, pa.Table]
    ) -> Union[pa.RecordBatch, pa.Table]:
        # pc.sort_indices + .take work uniformly over RecordBatch and
        # Table, so this serves both the per-batch entry path (legacy)
        # and the buffer-wide sort path (used by ``_flush_all``).
        sort_keys = [(key, 'ascending') for key in self.trimmed_primary_keys]
        if '_SEQUENCE_NUMBER' in data.schema.names:
            sort_keys.append(('_SEQUENCE_NUMBER', 'ascending'))

        sorted_indices = pc.sort_indices(data, sort_keys=sort_keys)
        return data.take(sorted_indices)
