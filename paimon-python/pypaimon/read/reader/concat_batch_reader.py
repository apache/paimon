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

import collections
from typing import Callable, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.reader.format_blob_reader import BlobRecordIterator
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.blob import Blob
from pypaimon.utils.range import Range

_MIN_BATCH_SIZE_TO_REFILL = 1024


class _BlobFileState:

    def __init__(
        self,
        file: DataFileMeta,
        supplier: Callable,
        selected_ranges: List[Range],
    ):
        self.file = file
        self.supplier = supplier
        self.selected_ranges = selected_ranges
        self.selected_count = sum(row_range.count() for row_range in selected_ranges)
        self.reader = None
        self.reader_initialized = False
        self.selected_range_index = 0
        self.selected_position_base = 0


class ConcatBatchReader(RecordBatchReader):

    def __init__(self, reader_suppliers: List[Callable], file_io=None,
                 blob_field_indices=None, vector_field_indices=None):
        self.queue: collections.deque[Callable] = collections.deque(reader_suppliers)
        self.current_reader: Optional[RecordBatchReader] = None
        self.file_io = file_io
        self.blob_field_indices = blob_field_indices
        self.vector_field_indices = vector_field_indices

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        while True:
            if self.current_reader is not None:
                batch = self.current_reader.read_arrow_batch()
                if batch is not None:
                    return batch
                self.current_reader.close()
                self.current_reader = None
            elif self.queue:
                supplier = self.queue.popleft()
                self.current_reader = supplier()
            else:
                return None

    def close(self) -> None:
        if self.current_reader:
            self.current_reader.close()
            self.current_reader = None
        self.queue.clear()


class MergeAllBatchReader(RecordBatchReader):
    """
    A reader that accepts multiple reader suppliers and concatenates all their arrow batches
    into one big batch. This is useful when you want to merge all data from multiple sources
    into a single batch for processing.
    """

    def __init__(self, reader_suppliers: List[Callable], batch_size: int = 1024):
        self.reader_suppliers = reader_suppliers
        self.merged_batch: Optional[RecordBatch] = None
        self.reader = None
        self._batch_size = batch_size

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        if self.reader:
            try:
                return self.reader.read_next_batch()
            except StopIteration:
                return None

        all_batches = []

        # Read all batches from all reader suppliers
        for supplier in self.reader_suppliers:
            reader = supplier()
            if reader is None:
                continue
            try:
                while True:
                    batch = reader.read_arrow_batch()
                    if batch is None:
                        break
                    all_batches.append(batch)
            finally:
                reader.close()

        # Concatenate all batches into one big batch
        if all_batches:
            # For PyArrow < 17.0.0, use Table.concat_tables approach
            # Convert batches to tables and concatenate
            tables = [pa.Table.from_batches([batch]) for batch in all_batches]
            if len(tables) == 1:
                # Single table, just get the first batch
                self.merged_batch = tables[0].to_batches()[0]
            else:
                # Multiple tables, concatenate them
                concatenated_table = pa.concat_tables(tables)
                # Convert back to a single batch by taking all batches and combining
                all_concatenated_batches = concatenated_table.to_batches()
                if len(all_concatenated_batches) == 1:
                    self.merged_batch = all_concatenated_batches[0]
                else:
                    # If still multiple batches, we need to manually combine them
                    # This shouldn't happen with concat_tables, but just in case
                    combined_arrays = []
                    for i in range(len(all_concatenated_batches[0].columns)):
                        column_arrays = [batch.column(i) for batch in all_concatenated_batches]
                        combined_arrays.append(pa.concat_arrays(column_arrays))
                    self.merged_batch = pa.RecordBatch.from_arrays(
                        combined_arrays,
                        schema=all_concatenated_batches[0].schema
                    )
        else:
            self.merged_batch = None
            return None
        dataset = ds.InMemoryDataset(self.merged_batch)
        self.reader = dataset.scanner(batch_size=self._batch_size).to_reader()
        return self.reader.read_next_batch()

    def close(self) -> None:
        self.merged_batch = None
        self.reader = None


class DataEvolutionMergeReader(RecordBatchReader):
    """
    This is a union reader which contains multiple inner readers, Each reader is responsible for reading one file.

    This reader, assembling multiple reader into one big and great reader, will merge the batches from all readers.

    For example, if rowOffsets is {0, 2, 0, 1, 2, 1} and fieldOffsets is {0, 0, 1, 1, 1, 0}, it means:
     - The first field comes from batch0, and it is at offset 0 in batch0.
     - The second field comes from batch2, and it is at offset 0 in batch2.
     - The third field comes from batch0, and it is at offset 1 in batch0.
     - The fourth field comes from batch1, and it is at offset 1 in batch1.
     - The fifth field comes from batch2, and it is at offset 1 in batch2.
     - The sixth field comes from batch1, and it is at offset 0 in batch1.
    """

    def __init__(
        self,
        row_offsets: List[int],
        field_offsets: List[int],
        readers: List[Optional[RecordBatchReader]],
        schema: pa.Schema,
    ):
        if row_offsets is None:
            raise ValueError("Row offsets must not be null")
        if field_offsets is None:
            raise ValueError("Field offsets must not be null")
        if len(row_offsets) != len(field_offsets):
            raise ValueError("Row offsets and field offsets must have the same length")
        if not row_offsets:
            raise ValueError("Row offsets must not be empty")
        if not readers or len(readers) < 1:
            raise ValueError("Readers should be more than 0")
        self.row_offsets = row_offsets
        self.field_offsets = field_offsets
        self.readers = readers
        self.schema = schema
        self._buffers: List[Optional[RecordBatch]] = [None] * len(readers)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batches: List[Optional[RecordBatch]] = [None] * len(self.readers)
        for i, reader in enumerate(self.readers):
            if reader is not None:
                if self._buffers[i] is not None:
                    remainder = self._buffers[i]
                    self._buffers[i] = None
                    if remainder.num_rows >= _MIN_BATCH_SIZE_TO_REFILL:
                        batches[i] = remainder
                    else:
                        new_batch = reader.read_arrow_batch()
                        if new_batch is not None and new_batch.num_rows > 0:
                            combined_arrays = [
                                pa.concat_arrays([remainder.column(j), new_batch.column(j)])
                                for j in range(remainder.num_columns)
                            ]
                            batches[i] = pa.RecordBatch.from_arrays(
                                combined_arrays, schema=remainder.schema
                            )
                        else:
                            batches[i] = remainder
                else:
                    batch = reader.read_arrow_batch()
                    if batch is None:
                        batches[i] = None
                    else:
                        batches[i] = batch
            else:
                batches[i] = None

        if not any(b is not None for b in batches):
            return None

        min_rows = min(b.num_rows for b in batches if b is not None)
        if min_rows == 0:
            return None

        columns = []
        for i in range(len(self.row_offsets)):
            batch_index = self.row_offsets[i]
            field_index = self.field_offsets[i]
            if batch_index >= 0 and batches[batch_index] is not None:
                columns.append(batches[batch_index].column(field_index).slice(0, min_rows))
            else:
                columns.append(pa.nulls(min_rows, type=self.schema.field(i).type))

        for i in range(len(self.readers)):
            if batches[i] is not None and batches[i].num_rows > min_rows:
                self._buffers[i] = batches[i].slice(min_rows, batches[i].num_rows - min_rows)

        return pa.RecordBatch.from_arrays(columns, schema=self.schema)

    def close(self) -> None:
        try:
            self._buffers = [None] * len(self.readers)
            for reader in self.readers:
                if reader is not None:
                    reader.close()
        except Exception as e:
            raise IOError("Failed to close inner readers") from e


class BlobFallbackBatchReader(RecordBatchReader):
    """Resolve blob placeholders by falling back through older blob versions."""

    def __init__(self, file_reader_suppliers: List[Tuple[DataFileMeta, Callable]],
                 field_name: str, output_type, row_ranges: Optional[List[Range]] = None,
                 blob_as_descriptor: bool = False, deletion_vector=None, batch_size: int = 1024,
                 blob_parallelism: int = 1):
        self._file_reader_suppliers = file_reader_suppliers
        self._field_name = field_name
        self._output_type = output_type
        self._row_ranges = Range.sort_and_merge_overlap(row_ranges) if row_ranges else None
        self._blob_as_descriptor = blob_as_descriptor
        self._is_array_blob = pa.types.is_list(output_type) or pa.types.is_large_list(output_type)
        self._is_map_blob = pa.types.is_map(output_type)
        self._data_field = DataField(
            0,
            field_name,
            PyarrowFieldParser.to_paimon_type(output_type, True),
        )
        if deletion_vector is None:
            self._deletion_vector_range = None
            self._deletion_vector = None
        else:
            self._deletion_vector_range, self._deletion_vector = deletion_vector
        self._returned = False
        self._batch_size = max(1, batch_size)
        self._blob_parallelism = max(1, blob_parallelism)
        self._file_io = None
        self._target_ranges = self._compute_target_ranges()
        self._target_range_index = 0
        self._next_row_id = (
            self._target_ranges[0].from_
            if self._target_ranges
            else None
        )
        self._file_states = []
        for file, supplier in self._file_reader_suppliers:
            selected_ranges = self._selected_ranges(file)
            if selected_ranges:
                self._file_states.append(_BlobFileState(file, supplier, selected_ranges))

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch_row_ids = self._next_batch_row_ids()
        if not batch_row_ids:
            return None

        resolve_blobs_concurrently = (
            self._blob_parallelism > 1 and not self._blob_as_descriptor
        )
        groups: Dict[int, Dict[int, Tuple[object, bool]]] = {}

        batch_first = batch_row_ids[0]
        batch_last = batch_row_ids[-1]
        for state in self._file_states:
            if not self._state_overlaps_batch(state, batch_first, batch_last):
                continue
            blob_values = self._read_blob_values(state, batch_row_ids)
            if not blob_values:
                continue
            group = groups.setdefault(state.file.max_sequence_number, {})
            if resolve_blobs_concurrently and self._file_io is None:
                self._file_io = state.reader._file_io
            for row_id, blob in blob_values.items():
                if row_id in group:
                    raise ValueError(
                        "Blob files within the same max sequence should not overlap."
                    )
                if blob is None:
                    group[row_id] = (None, False)
                elif (
                    blob is Blob.PLACE_HOLDER
                    or blob is Blob.ARRAY_PLACE_HOLDER
                    or blob is Blob.MAP_PLACE_HOLDER
                ):
                    group[row_id] = (None, True)
                elif self._is_map_blob:
                    if resolve_blobs_concurrently:
                        group[row_id] = (blob, False)
                    else:
                        group[row_id] = (self._map_value_for_arrow(blob), False)
                elif self._is_array_blob:
                    if resolve_blobs_concurrently:
                        group[row_id] = (blob, False)
                    else:
                        group[row_id] = (self._array_value_for_arrow(blob), False)
                else:
                    if self._blob_as_descriptor:
                        group[row_id] = (blob.to_descriptor().serialize(), False)
                    elif resolve_blobs_concurrently:
                        # Keep values lazy until fallback selects the newest
                        # non-placeholder version for each row. Otherwise older
                        # overridden BLOB versions would be read unnecessarily.
                        group[row_id] = (blob, False)
                    else:
                        group[row_id] = (blob.to_data(), False)
            if state.selected_range_index >= len(state.selected_ranges):
                self._close_state_reader(state)

        if not groups:
            return None

        result = []
        for row_id in batch_row_ids:
            found = False
            for max_sequence_number in sorted(groups.keys(), reverse=True):
                candidate = groups[max_sequence_number].get(row_id)
                if candidate is None:
                    continue
                value, is_placeholder = candidate
                if not is_placeholder:
                    result.append(value)
                    found = True
                    break
            if not found:
                raise ValueError("All blob files at the same row id store a placeholder.")

        if resolve_blobs_concurrently:
            result = self._resolve_selected_blobs(result)

        return pa.RecordBatch.from_arrays(
            [pa.array(result, type=self._output_type)],
            names=[self._field_name],
        )

    def _array_value_for_arrow(self, blob_array):
        result = []
        for blob in blob_array:
            if blob is None:
                result.append(None)
            elif self._blob_as_descriptor:
                result.append(blob.to_descriptor().serialize())
            else:
                result.append(blob.to_data())
        return result

    def _map_value_for_arrow(self, blob_map):
        if None in blob_map:
            raise ValueError(
                "MAP<X, BLOB> with null keys cannot be converted to a PyArrow Map."
            )
        result = {}
        for key, blob in blob_map.items():
            if blob is None:
                result[key] = None
            elif self._blob_as_descriptor:
                result[key] = blob.to_descriptor().serialize()
            else:
                result[key] = blob.to_data()
        return result

    def _resolve_selected_blobs(self, values: List[object]) -> List[object]:
        """Materialize selected scalar, array, or map BLOBs with the shared FileIO."""
        resolved = []
        indexed_blobs: List[Tuple[int, object, Blob]] = []
        for row_index, value in enumerate(values):
            if self._is_map_blob:
                if value is None:
                    resolved.append(None)
                    continue
                if None in value:
                    raise ValueError(
                        "MAP<X, BLOB> with null keys cannot be converted to a PyArrow Map."
                    )
                map_values = {}
                for key, blob in value.items():
                    if blob is None:
                        map_values[key] = None
                    else:
                        map_values[key] = None
                        indexed_blobs.append((row_index, key, blob))
                resolved.append(map_values)
            elif self._is_array_blob:
                if value is None:
                    resolved.append(None)
                    continue
                array_values = []
                for element_index, blob in enumerate(value):
                    if blob is None:
                        array_values.append(None)
                    else:
                        array_values.append(None)
                        indexed_blobs.append((row_index, element_index, blob))
                resolved.append(array_values)
            elif isinstance(value, Blob):
                resolved.append(None)
                indexed_blobs.append((row_index, None, value))
            else:
                resolved.append(value)

        if not indexed_blobs:
            return resolved

        bodies = self._file_io.read_blobs_concurrent(
            [blob for _, _, blob in indexed_blobs], self._blob_parallelism)
        for (row_index, slot, _), body in zip(indexed_blobs, bodies):
            if self._is_map_blob:
                resolved[row_index][slot] = body
            elif slot is None:
                resolved[row_index] = body
            else:
                resolved[row_index][slot] = body
        return resolved

    def _compute_target_ranges(self) -> List[Range]:
        ranges = Range.sort_and_merge_overlap([
            file.row_id_range()
            for file, _ in self._file_reader_suppliers
        ])
        if self._row_ranges is not None:
            ranges = Range.and_(ranges, self._row_ranges)
        return ranges

    def _selected_ranges(self, file: DataFileMeta) -> List[Range]:
        ranges = [file.row_id_range()]
        if self._row_ranges is not None:
            ranges = Range.and_(ranges, self._row_ranges)
        return ranges

    def _next_batch_row_ids(self) -> List[int]:
        batch_row_ids = []
        while (
            len(batch_row_ids) < self._batch_size
            and self._target_range_index < len(self._target_ranges)
        ):
            target_range = self._target_ranges[self._target_range_index]
            if self._next_row_id is None or self._next_row_id < target_range.from_:
                self._next_row_id = target_range.from_

            while (
                self._next_row_id <= target_range.to
                and len(batch_row_ids) < self._batch_size
            ):
                row_id = self._next_row_id
                self._next_row_id += 1
                if not self._is_deleted(row_id):
                    batch_row_ids.append(row_id)

            if self._next_row_id > target_range.to:
                self._target_range_index += 1
                self._next_row_id = (
                    self._target_ranges[self._target_range_index].from_
                    if self._target_range_index < len(self._target_ranges)
                    else None
                )

        return batch_row_ids

    def _is_deleted(self, row_id: int) -> bool:
        if self._deletion_vector is None:
            return False
        if not self._deletion_vector_range.contains(row_id):
            raise ValueError(
                f"Deletion vector range {self._deletion_vector_range} "
                f"should contain blob row id {row_id}."
            )
        return self._deletion_vector.is_deleted(
            row_id - self._deletion_vector_range.from_
        )

    def _state_overlaps_batch(
        self, state: _BlobFileState, batch_first: int, batch_last: int
    ) -> bool:
        selected_ranges = state.selected_ranges
        while (
            state.selected_range_index < len(selected_ranges)
            and selected_ranges[state.selected_range_index].to < batch_first
        ):
            state.selected_position_base += (
                selected_ranges[state.selected_range_index].count()
            )
            state.selected_range_index += 1
        if state.selected_range_index >= len(selected_ranges):
            # Batch row ids only move forward. Once the last selected range is
            # behind this batch, the reader can never be used again.
            self._close_state_reader(state)
            return False
        return selected_ranges[state.selected_range_index].from_ <= batch_last

    def _read_blob_values(
        self, state: _BlobFileState, batch_row_ids: List[int]
    ) -> Dict[int, object]:
        positions_and_row_ids = self._selected_positions_and_row_ids(
            state, batch_row_ids
        )
        if not positions_and_row_ids:
            return {}

        reader = self._reader_for_state(state)
        if reader is None:
            return {}

        try:
            blob_lengths = [reader.blob_lengths[pos] for pos, _ in positions_and_row_ids]
            blob_offsets = [reader.blob_offsets[pos] for pos, _ in positions_and_row_ids]
            iterator = BlobRecordIterator(
                reader._file_io,
                reader.file_path,
                blob_lengths,
                blob_offsets,
                self._data_field,
                reader._input_stream,
                blob_as_descriptor=(
                    self._blob_as_descriptor or self._blob_parallelism > 1
                ),
            )

            blobs = []
            for row in iterator:
                blobs.append(row.values[0])
            return {
                row_id: blob
                for (_, row_id), blob in zip(positions_and_row_ids, blobs)
            }
        except AttributeError as e:
            raise TypeError("Blob fallback reader expects FormatBlobReader suppliers.") from e

    @staticmethod
    def _selected_positions_and_row_ids(
        state: _BlobFileState, batch_row_ids: List[int]
    ) -> List[Tuple[int, int]]:
        selected_ranges = state.selected_ranges
        positions_and_row_ids = []
        for row_id in batch_row_ids:
            while (
                state.selected_range_index < len(selected_ranges)
                and selected_ranges[state.selected_range_index].to < row_id
            ):
                state.selected_position_base += (
                    selected_ranges[state.selected_range_index].count()
                )
                state.selected_range_index += 1
            if state.selected_range_index >= len(selected_ranges):
                break
            row_range = selected_ranges[state.selected_range_index]
            if row_range.from_ <= row_id <= row_range.to:
                positions_and_row_ids.append(
                    (
                        state.selected_position_base + row_id - row_range.from_,
                        row_id,
                    )
                )
        return positions_and_row_ids

    def _reader_for_state(self, state: _BlobFileState):
        if state.reader_initialized:
            return state.reader

        reader = state.supplier()
        if reader is None:
            state.reader_initialized = True
            return None
        actual_rows = len(reader.blob_lengths)
        expected_rows = state.selected_count
        if actual_rows != expected_rows:
            reader.close()
            raise ValueError(
                "Blob fallback reader returned an unexpected row count "
                f"for {state.file.file_name}: expect {expected_rows}, got "
                f"{actual_rows}."
            )

        state.reader = reader
        state.reader_initialized = True
        return reader

    @staticmethod
    def _close_state_reader(state: _BlobFileState) -> None:
        reader = state.reader
        state.reader = None
        state.reader_initialized = False
        if reader is not None:
            reader.close()

    def close(self) -> None:
        for state in self._file_states:
            self._close_state_reader(state)
