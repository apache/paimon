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

from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, PyarrowFieldParser, RowType)
from pypaimon.table.special_fields import SpecialFields


def _is_character_string_type(data_type) -> bool:
    if not isinstance(data_type, AtomicType):
        return False
    t = data_type.type.upper()
    return t == 'STRING' or t.startswith('VARCHAR') or t.startswith('CHAR')


def _unslice(array):
    """Re-materialize a sliced array so offsets/buffers start at zero.

    The list/map rebuilds below read ``offsets``/raw buffers directly; on a
    sliced array those still point into the parent storage, which either
    errors (list rebuild with a null mask) or silently misaligns rows (map
    rebuild from raw buffers)."""
    if array.offset == 0:
        return array
    return pa.concat_arrays([array])


def _to_string_values(array, data_type) -> list:
    """Render *array* as a list of per-row strings (None for NULL rows)."""
    if isinstance(data_type, (RowType, ArrayType, MapType)):
        return _constructed_to_string_array(array, data_type).to_pylist()
    return array.cast(pa.string(), safe=False).to_pylist()


def _constructed_to_string_array(array, file_type):
    """Render a struct/list/map array in the engine's string form:
    ROW -> ``{v1, v2}``, ARRAY -> ``[e1, e2]``, MAP -> ``{k1 -> v1, k2 -> v2}``.
    Sub-values are rendered recursively; a NULL sub-value renders as the
    literal ``null`` while a NULL container row stays NULL."""
    array = _unslice(array)
    valid = pc.is_valid(array).to_pylist()
    out = []
    if isinstance(file_type, RowType):
        children = [
            _to_string_values(array.field(i), sub.type)
            for i, sub in enumerate(file_type.fields)
        ]
        for i in range(len(array)):
            if not valid[i]:
                out.append(None)
                continue
            vals = [c[i] if c[i] is not None else 'null' for c in children]
            out.append('{' + ', '.join(vals) + '}')
    elif isinstance(file_type, ArrayType):
        values = _to_string_values(array.values, file_type.element)
        offsets = array.offsets.to_pylist()
        for i in range(len(array)):
            if not valid[i]:
                out.append(None)
                continue
            elems = [v if v is not None else 'null'
                     for v in values[offsets[i]:offsets[i + 1]]]
            out.append('[' + ', '.join(elems) + ']')
    elif isinstance(file_type, MapType):
        keys = _to_string_values(array.keys, file_type.key)
        items = _to_string_values(array.items, file_type.value)
        offsets = array.offsets.to_pylist()
        for i in range(len(array)):
            if not valid[i]:
                out.append(None)
                continue
            entries = [
                '{} -> {}'.format(
                    keys[j] if keys[j] is not None else 'null',
                    items[j] if items[j] is not None else 'null')
                for j in range(offsets[i], offsets[i + 1])
            ]
            out.append('{' + ', '.join(entries) + '}')
    else:
        raise ValueError(
            'Unsupported constructed type for string rendering: {}'.format(file_type))
    return pa.array(out, type=pa.string())


class DataFileBatchReader(RecordBatchReader):
    """
    Reads record batch from files of different formats
    """

    def __init__(self, format_reader: RecordBatchReader, index_mapping: List[int], partition_info: PartitionInfo,
                 system_primary_key: Optional[List[str]], fields: List[DataField],
                 max_sequence_number: int,
                 first_row_id: int,
                 row_tracking_enabled: bool,
                 system_fields: dict,
                 file_io: Optional[FileIO] = None,
                 row_id_offsets: Optional[List[int]] = None,
                 file_data_fields: Optional[List[DataField]] = None,
                 target_data_fields: Optional[List[DataField]] = None):
        self.format_reader = format_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info
        self.system_primary_key = system_primary_key
        self.schema_map = {field.name: field for field in PyarrowFieldParser.from_paimon_schema(fields)}
        self.row_tracking_enabled = row_tracking_enabled
        self.first_row_id = first_row_id
        self.row_id_offsets = row_id_offsets
        self._row_id_cursor = 0
        self.max_sequence_number = max_sequence_number
        self.system_fields = system_fields
        self.file_io = file_io
        # Per-file field-id normalization: map the physically-read columns
        # (the file's own field order/names) onto the latest read target by
        # field id, padding missing ids with NULL and recursing into nested
        # ROW / ARRAY<ROW> / MAP<.,ROW> sub-fields the same way. ``None`` when
        # there is no evolution to reconcile -- the common path stays zero-copy.
        self._normalize_plan = self._build_normalize_plan(file_data_fields, target_data_fields)

    @staticmethod
    def _build_normalize_plan(file_data_fields, target_data_fields):
        """Build a per-file field-id alignment plan.

        Returns a list of ``(pos, file_field, target_field)`` -- one per target
        field, in target order -- where ``pos`` is the column index in the
        physically-read batch carrying ``target_field`` (matched by field id),
        or -1 if the file does not contain that id (pad NULL). Returns ``None``
        when the file already matches the target exactly (no evolution), so the
        caller stays zero-copy.
        """
        if file_data_fields is None or target_data_fields is None:
            return None
        # Recursive equality covers nested sub-field changes too: any rename /
        # add / drop / type change at any depth makes the file != target.
        if file_data_fields == target_data_fields:
            return None
        file_id_to_pos = {f.id: i for i, f in enumerate(file_data_fields)}
        plan = []
        for target in target_data_fields:
            pos = file_id_to_pos.get(target.id, -1)
            file_field = file_data_fields[pos] if pos >= 0 else None
            plan.append((pos, file_field, target))
        return plan

    def _normalize_batch(self, record_batch: RecordBatch) -> RecordBatch:
        """Reorder/pad the physically-read batch onto the latest read target by
        field id, relabel columns to the latest names, and align nested ROW
        sub-fields by id. Missing ids become typed all-NULL columns."""
        if self._normalize_plan is None:
            return record_batch
        num_rows = record_batch.num_rows
        arrays = []
        names = []
        for pos, file_field, target_field in self._normalize_plan:
            target_pa_type = PyarrowFieldParser.from_paimon_type(target_field.type)
            if pos < 0:
                arrays.append(pa.nulls(num_rows, type=target_pa_type))
            else:
                arrays.append(self._align_array_by_id(
                    record_batch.column(pos), file_field.type, target_field.type))
            names.append(target_field.name)
        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _align_array_by_id(self, array, file_type, target_type):
        """Return *array* converted to *target_type*, matching ROW sub-fields by
        field id (reorder, pad missing with NULL, follow renames, cast changed
        types) recursively, transparently through ARRAY/MAP wrappers."""
        if isinstance(target_type, RowType) and isinstance(file_type, RowType):
            n = len(array)
            file_id_to_pos = {f.id: i for i, f in enumerate(file_type.fields)}
            children = []
            pa_fields = []
            for tsub in target_type.fields:
                p = file_id_to_pos.get(tsub.id, -1)
                if p < 0:
                    child = pa.nulls(n, type=PyarrowFieldParser.from_paimon_type(tsub.type))
                else:
                    child = self._align_array_by_id(
                        array.field(p), file_type.fields[p].type, tsub.type)
                children.append(child)
                pa_fields.append(pa.field(tsub.name, child.type, nullable=tsub.type.nullable))
            # Preserve the struct's own null mask; child values under a null
            # struct are irrelevant.
            return pa.StructArray.from_arrays(
                children, fields=pa_fields, mask=pc.is_null(array))
        if isinstance(target_type, ArrayType) and isinstance(file_type, ArrayType):
            array = _unslice(array)
            aligned_values = self._align_array_by_id(
                array.values, file_type.element, target_type.element)
            return pa.ListArray.from_arrays(
                array.offsets, aligned_values, mask=pc.is_null(array))
        if isinstance(target_type, MapType) and isinstance(file_type, MapType):
            array = _unslice(array)
            aligned_items = self._align_array_by_id(
                array.items, file_type.value, target_type.value)
            # MapArray.from_arrays cannot carry a null mask (a null map would
            # collapse to an empty one), so rebuild from buffers, reusing the
            # original validity/offset buffers and only swapping the value child.
            target_pa = PyarrowFieldParser.from_paimon_type(target_type)
            entries = pa.StructArray.from_arrays(
                [array.keys, aligned_items],
                fields=[target_pa.key_field, target_pa.item_field])
            return pa.Array.from_buffers(
                target_pa, len(array), array.buffers()[:2], children=[entries])
        # A constructed type changed to a character string: pyarrow cannot
        # cast struct/list/map to utf8 directly, so render the engine's
        # string form instead.
        if (isinstance(file_type, (RowType, ArrayType, MapType))
                and _is_character_string_type(target_type)):
            return _constructed_to_string_array(array, file_type)
        # Leaf / non-nested: cast to the target type when it differs.
        target_pa_type = PyarrowFieldParser.from_paimon_type(target_type)
        if array.type != target_pa_type:
            return array.cast(target_pa_type, safe=False)
        return array

    def read_arrow_batch(self, start_idx=None, end_idx=None) -> Optional[RecordBatch]:
        if isinstance(self.format_reader, FormatBlobReader):
            record_batch = self.format_reader.read_arrow_batch(start_idx, end_idx)
        else:
            record_batch = self.format_reader.read_arrow_batch()
        if record_batch is None:
            return None
        record_batch = self._normalize_batch(record_batch)

        if self.partition_info is None and self.index_mapping is None:
            # A file written under an older schema (e.g. before an INT -> BIGINT
            # promotion or a DECIMAL precision change) yields columns in the
            # data file's original types. Without reordering or partition padding
            # to rebuild the batch, those old types would otherwise leak through
            # here -- returning a type that depends on whether this read happens
            # to span newer-schema files, and failing to concatenate when it
            # does. Align them to the current read schema, mirroring the rebuild
            # path below.
            record_batch = self._align_batch_to_read_schema(
                record_batch.schema.names, record_batch.columns)
            if self.row_tracking_enabled and self.system_fields:
                record_batch = self._assign_row_tracking(record_batch)
            return record_batch

        inter_arrays = []
        inter_names = []
        num_rows = record_batch.num_rows

        if self.partition_info is not None:
            for i in range(self.partition_info.size()):
                if self.partition_info.is_partition_row(i):
                    partition_value, partition_field = self.partition_info.get_partition_value(i)
                    const_array = pa.repeat(partition_value, num_rows)
                    inter_arrays.append(const_array)
                    inter_names.append(partition_field.name)
                else:
                    real_index = self.partition_info.get_real_index(i)
                    if real_index < record_batch.num_columns:
                        inter_arrays.append(record_batch.column(real_index))
                        inter_names.append(record_batch.schema.field(real_index).name)
        else:
            inter_arrays = record_batch.columns
            inter_names = record_batch.schema.names

        if self.index_mapping is not None:
            mapped_arrays = []
            mapped_names = []
            for i, real_index in enumerate(self.index_mapping):
                if 0 <= real_index < len(inter_arrays):
                    mapped_arrays.append(inter_arrays[real_index])
                    mapped_names.append(inter_names[real_index])
                else:
                    null_array = pa.nulls(num_rows)
                    mapped_arrays.append(null_array)
                    mapped_names.append(f"null_col_{i}")

            if self.system_primary_key:
                for i in range(len(self.system_primary_key)):
                    if not mapped_names[i].startswith("_KEY_"):
                        mapped_names[i] = f"_KEY_{mapped_names[i]}"

            inter_arrays = mapped_arrays
            inter_names = mapped_names

        # Rebuild the batch typed by the read schema (carries 'not null' and
        # aligns old-schema column types).
        record_batch = self._align_batch_to_read_schema(inter_names, inter_arrays)

        # Handle row tracking fields
        if self.row_tracking_enabled and self.system_fields:
            record_batch = self._assign_row_tracking(record_batch)

        return record_batch

    def _align_batch_to_read_schema(self, names: List[str], arrays: list) -> RecordBatch:
        """Build a record batch for ``names``/``arrays`` typed by the read schema.

        Each known field is cast to the current read schema's type, which also
        carries the 'not null' property; unknown columns keep the array's own
        type. Columns whose type already matches are reused as-is, keeping the
        common (non-evolution) path zero-copy.

        Casts use ``safe=False`` to match Java ``CastExecutors`` semantics for
        the read-time conversions a user-approved schema evolution implies
        (e.g. DECIMAL scale-down or DOUBLE -> INT truncate rather than raise).
        Evolution legality is the writer's concern (``DataTypeCasts``); the read
        path only materializes the result.
        """
        out_arrays = []
        out_fields = []
        for name, array in zip(names, arrays):
            target_field = self.schema_map.get(name)
            if target_field is None:
                target_field = pa.field(name, array.type)
            elif array.type != target_field.type:
                array = array.cast(target_field.type, safe=False)
            out_arrays.append(array)
            out_fields.append(target_field)
        return pa.RecordBatch.from_arrays(out_arrays, schema=pa.schema(out_fields))

    def _assign_row_tracking(self, record_batch: RecordBatch) -> RecordBatch:
        """Assign row tracking meta fields (_ROW_ID and _SEQUENCE_NUMBER)."""
        arrays = list(record_batch.columns)

        # Handle _ROW_ID field
        if SpecialFields.ROW_ID.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.ROW_ID.name]
            if self.first_row_id is None:
                arrays[idx] = pa.nulls(record_batch.num_rows, type=pa.int64())
            elif self.row_id_offsets is not None:
                end = self._row_id_cursor + record_batch.num_rows
                row_ids = [self.first_row_id + o for o in self.row_id_offsets[self._row_id_cursor:end]]
                arrays[idx] = pa.array(row_ids, type=pa.int64())
                self._row_id_cursor = end
            else:
                row_id_range = range(self.first_row_id, self.first_row_id + record_batch.num_rows)
                arrays[idx] = pa.array(row_id_range, type=pa.int64())
                self.first_row_id += record_batch.num_rows

        # Handle _SEQUENCE_NUMBER field
        if SpecialFields.SEQUENCE_NUMBER.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.SEQUENCE_NUMBER.name]
            # Create a new array that fills with max_sequence_number
            arrays[idx] = pa.repeat(self.max_sequence_number, record_batch.num_rows)

        names = record_batch.schema.names
        table = None
        for i, name in enumerate(names):
            field = pa.field(
                name, arrays[i].type,
                nullable=record_batch.schema.field(name).nullable
            )
            if table is None:
                table = pa.table({name: arrays[i]}, schema=pa.schema([field]))
            else:
                table = table.append_column(field, arrays[i])
        return table.to_batches()[0]

    def close(self) -> None:
        self.format_reader.close()
