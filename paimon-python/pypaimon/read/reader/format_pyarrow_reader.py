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

from typing import Any, Dict, List, Optional, Set

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.variant_shredding import (
    VariantSchema,
    assemble_shredded_column,
    build_variant_schema,
    is_shredded_variant,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    DataField,
    MapType,
    MultisetType,
    PyarrowFieldParser,
    RowType,
)
from pypaimon.table.special_fields import SpecialFields


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.

    When a VARIANT column is stored in the shredded Parquet format (a struct with
    ``metadata``, ``value``, and ``typed_value`` fields), this reader transparently
    reconstructs the standard ``struct<value: binary, metadata: binary>`` representation.
    """

    def __init__(self, file_io: FileIO, file_format: str, file_path: str,
                 read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 options: CoreOptions = None,
                 nested_name_paths: Optional[List[List[str]]] = None,
                 predicate_field_names: Optional[Set[str]] = None):
        self._predicate_field_names = predicate_field_names or set()
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        self.dataset = ds.dataset(file_path_for_pyarrow, format=file_format, filesystem=file_io.filesystem)
        self._file_format = file_format
        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]

        if nested_name_paths is not None and len(nested_name_paths) != len(read_fields):
            raise ValueError(
                "nested_name_paths length {} does not match read_fields length {}".format(
                    len(nested_name_paths), len(read_fields)))
        self._nested_name_paths = nested_name_paths
        has_nested_path = bool(
            nested_name_paths and any(len(p) > 1 for p in nested_name_paths))
        self._has_nested_path = has_nested_path

        file_schema = self.dataset.schema
        if has_nested_path:
            self.existing_fields = []
            self.missing_fields = []
            for f, path in zip(read_fields, nested_name_paths):
                if _path_exists_in_arrow_schema(file_schema, path):
                    self.existing_fields.append(f.name)
                else:
                    self.missing_fields.append(f.name)
        else:
            file_schema_names = set(file_schema.names)
            self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
            self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

        self._variant_shredding_enabled = (
            options is None or options.variant_shredding_enabled())
        self._variant_schema_cache: Dict[pa.DataType, VariantSchema] = {}

        self._bounded_variant_read = (
            self._file_format == 'parquet' and self._has_projected_variant())
        if has_nested_path and not self._bounded_variant_read:
            existing_set = set(self.existing_fields)
            columns_dict = {}
            for f, path in zip(read_fields, nested_name_paths):
                if f.name in existing_set:
                    columns_dict[f.name] = ds.field(*path)
            self._scan_columns = columns_dict
        elif has_nested_path:
            self._scan_columns = None
        else:
            # Only pass existing fields to PyArrow scanner to avoid errors
            self._scan_columns = self.existing_fields
        self._scan_filter = push_down_predicate
        self._scan_batch_size = batch_size

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

        # Read projected VARIANT columns in bounded batches.
        self._parquet_file = None
        if self._bounded_variant_read:
            import pyarrow.parquet as pq
            # ParquetFile(filesystem=...) is unavailable in PyArrow 6.
            self._parquet_file = pq.ParquetFile(
                file_io.filesystem.open_input_file(file_path_for_pyarrow))
        if self._parquet_file is not None:
            self._raw_batches = self._iter_row_group_batches()
        else:
            reader = self.dataset.scanner(
                columns=self._scan_columns,
                filter=self._scan_filter,
                batch_size=self._scan_batch_size,
            ).to_reader()
            self._raw_batches = self._iter_reader_batches(reader)

    def _has_projected_variant(self) -> bool:
        return any(
            f.name in self.existing_fields
            and _contains_variant(f.type)
            for f in self.read_fields)

    @staticmethod
    def _iter_reader_batches(reader):
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                return

    def _iter_row_group_batches(self):
        columns = self._row_group_read_columns()
        for row_group in self._surviving_row_group_ids():
            for batch in self._parquet_file.iter_batches(
                    row_groups=[row_group],
                    columns=columns,
                    batch_size=self._scan_batch_size):
                if self._has_nested_path:
                    batches = [batch]
                    if self._scan_filter is not None:
                        table = ds.dataset(
                            pa.Table.from_batches([batch])
                        ).scanner(filter=self._scan_filter).to_table()
                        batches = table.to_batches()
                    for filtered in batches:
                        out = self._select_nested_fields(filtered)
                        if out.num_rows:
                            yield out
                    continue
                if self._scan_filter is None:
                    yield self._select_existing_fields(batch)
                    continue
                table = ds.dataset(
                    pa.Table.from_batches([batch])
                ).scanner(filter=self._scan_filter).to_table()
                if self.existing_fields:
                    table = table.select(self.existing_fields)
                for out in table.to_batches():
                    if out.num_rows:
                        yield out

    def _row_group_read_columns(self):
        if self._has_nested_path:
            existing = set(self.existing_fields)
            columns = []
            for field, path in zip(self.read_fields, self._nested_name_paths):
                if field.name in existing and path[0] not in columns:
                    columns.append(path[0])
        else:
            columns = list(self.existing_fields)
        if self._scan_filter is not None:
            file_names = set(self.dataset.schema.names)
            for name in self._predicate_field_names:
                if name in file_names and name not in columns:
                    columns.append(name)
        return columns

    def _select_existing_fields(self, batch):
        columns = []
        fields = []
        for name in self.existing_fields:
            index = batch.schema.get_field_index(name)
            if index < 0:
                raise KeyError("Field not found in batch: {}".format(name))
            columns.append(batch.column(index))
            fields.append(batch.schema.field(index))
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def _select_nested_fields(self, batch):
        columns = []
        names = []
        existing = set(self.existing_fields)
        for field, path in zip(self.read_fields, self._nested_name_paths):
            if field.name not in existing:
                continue
            index = batch.schema.get_field_index(path[0])
            if index < 0:
                raise KeyError("Field not found in batch: {}".format(path[0]))
            column = batch.column(index)
            for name in path[1:]:
                index = column.type.get_field_index(name)
                column = column.flatten()[index]
            columns.append(column)
            names.append(field.name)
        return pa.RecordBatch.from_arrays(columns, names=names)

    def _surviving_row_group_ids(self):
        total = self._parquet_file.num_row_groups
        if self._scan_filter is None:
            return range(total)
        try:
            ids = set()
            for fragment in self.dataset.get_fragments(
                    filter=self._scan_filter):
                for row_group in fragment.split_by_row_group(
                        self._scan_filter):
                    ids.update(info.id for info in row_group.row_groups)
            return sorted(ids)
        except Exception:
            return range(total)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = next(self._raw_batches, None)
        if batch is None:
            return None
        return self._post_process_batch(batch)

    def _post_process_batch(self, batch: RecordBatch) -> RecordBatch:
        if self._file_format == 'orc' and self._output_schema is not None:
            batch = self._cast_orc_time_columns(batch)

        if self._variant_shredding_enabled:
            batch = self._assemble_shredded_variants(batch)

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
                out_fields.append(
                    pa.field(field_name, col_type, nullable=nullable))
        return pa.RecordBatch.from_arrays(
            all_columns, schema=pa.schema(out_fields))

    def _assemble_shredded_variants(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        changed = False
        columns = list(batch.columns)
        fields = list(batch.schema)
        logical_types = {field.name: field.type for field in self.read_fields}

        for i, f in enumerate(fields):
            logical_type = logical_types.get(f.name)
            if logical_type is not None:
                new_col, column_changed = _assemble_variant_column(
                    columns[i], logical_type, self._variant_schema_cache)
            else:
                new_col, column_changed = columns[i], False
            if column_changed:
                columns[i] = new_col
                fields[i] = pa.field(f.name, new_col.type, nullable=f.nullable)
                changed = True

        if not changed:
            return batch
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def _cast_orc_time_columns(self, batch):
        """Cast int32 TIME columns back to time32('ms') when reading ORC.
        """
        columns = []
        fields = []
        changed = False
        for i, name in enumerate(batch.schema.names):
            col = batch.column(i)
            idx = self._output_schema.get_field_index(name)
            if idx >= 0 and pa.types.is_int32(col.type) \
                    and pa.types.is_time(self._output_schema.field(idx).type):
                col = col.cast(self._output_schema.field(idx).type)
                fields.append(self._output_schema.field(idx))
                changed = True
            else:
                fields.append(batch.schema.field(i))
            columns.append(col)
        if changed:
            return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
        return batch

    def close(self):
        self._raw_batches = None
        if self._parquet_file is not None:
            close = getattr(self._parquet_file, 'close', None)
            if close is not None:
                close()
            self._parquet_file = None


def _path_exists_in_arrow_schema(schema: pa.Schema, path: List[str]) -> bool:
    """Check whether a name path is fully resolvable in the given schema."""
    if not path:
        return False
    if path[0] not in schema.names:
        return False
    current_type = schema.field(path[0]).type
    for name in path[1:]:
        if not pa.types.is_struct(current_type):
            return False
        idx = current_type.get_field_index(name)
        if idx < 0:
            return False
        current_type = current_type[idx].type
    return True


def _contains_variant(data_type) -> bool:
    if isinstance(data_type, AtomicType):
        return data_type.type.upper() == 'VARIANT'
    if isinstance(data_type, (ArrayType, MultisetType)):
        return _contains_variant(data_type.element)
    if isinstance(data_type, MapType):
        return (_contains_variant(data_type.key)
                or _contains_variant(data_type.value))
    if isinstance(data_type, RowType):
        return any(_contains_variant(field.type) for field in data_type.fields)
    return False


def _assemble_variant_column(column, data_type, schema_cache):
    if isinstance(data_type, AtomicType):
        if (data_type.type.upper() != 'VARIANT'
                or not is_shredded_variant(column.type)):
            return column, False
        schema = schema_cache.get(column.type)
        if schema is None:
            schema = build_variant_schema(column.type)
            schema_cache[column.type] = schema
        return assemble_shredded_column(column, schema), True

    if isinstance(data_type, RowType) and pa.types.is_struct(column.type):
        logical_fields = {field.name: field.type for field in data_type.fields}
        columns = []
        fields = []
        changed = False
        for index, arrow_field in enumerate(column.type):
            child = column.field(index)
            logical_type = logical_fields.get(arrow_field.name)
            if logical_type is not None:
                child, child_changed = _assemble_variant_column(
                    child, logical_type, schema_cache)
                changed = changed or child_changed
            columns.append(child)
            fields.append(pa.field(
                arrow_field.name,
                child.type,
                nullable=arrow_field.nullable,
                metadata=arrow_field.metadata,
            ))
        if changed:
            mask = column.is_null() if column.null_count else None
            return pa.StructArray.from_arrays(
                columns, fields=fields, mask=mask), True
        return column, False

    if (isinstance(data_type, (ArrayType, MultisetType))
            and (pa.types.is_list(column.type)
                 or pa.types.is_large_list(column.type))):
        offsets, start, end = _normalized_offsets(column)
        values = column.values.slice(start, end - start)
        values, changed = _assemble_variant_column(
            values, data_type.element, schema_cache)
        if not changed:
            return column, False
        if pa.types.is_large_list(column.type):
            result = pa.LargeListArray.from_arrays(offsets, values)
            list_type = pa.large_list(pa.field(
                column.type.value_field.name,
                values.type,
                nullable=column.type.value_field.nullable,
                metadata=column.type.value_field.metadata,
            ))
        else:
            result = pa.ListArray.from_arrays(offsets, values)
            list_type = pa.list_(pa.field(
                column.type.value_field.name,
                values.type,
                nullable=column.type.value_field.nullable,
                metadata=column.type.value_field.metadata,
            ))
        return pa.Array.from_buffers(
            list_type,
            len(result),
            result.buffers()[:2],
            null_count=result.null_count,
            children=[values],
        ), True

    if isinstance(data_type, MapType) and pa.types.is_map(column.type):
        offsets, start, end = _normalized_offsets(column)
        keys = column.keys.slice(start, end - start)
        items = column.items.slice(start, end - start)
        keys, key_changed = _assemble_variant_column(
            keys, data_type.key, schema_cache)
        items, item_changed = _assemble_variant_column(
            items, data_type.value, schema_cache)
        if not key_changed and not item_changed:
            return column, False
        result = pa.MapArray.from_arrays(offsets, keys, items)
        map_type = pa.map_(
            pa.field(
                column.type.key_field.name,
                keys.type,
                nullable=False,
                metadata=column.type.key_field.metadata,
            ),
            pa.field(
                column.type.item_field.name,
                items.type,
                nullable=column.type.item_field.nullable,
                metadata=column.type.item_field.metadata,
            ),
            keys_sorted=getattr(column.type, 'keys_sorted', False),
        )
        entries = pa.StructArray.from_arrays(
            [keys, items], fields=[map_type.key_field, map_type.item_field])
        return pa.Array.from_buffers(
            map_type,
            len(result),
            result.buffers()[:2],
            null_count=result.null_count,
            children=[entries],
        ), True

    return column, False


def _normalized_offsets(column):
    offsets_array = getattr(column, 'offsets', None)
    if offsets_array is None:
        offsets_array = pa.Array.from_buffers(
            pa.int32(),
            len(column) + 1,
            [None, column.buffers()[1]],
            offset=column.offset,
        )
    raw_offsets = offsets_array.to_pylist()
    start = raw_offsets[0]
    end = raw_offsets[-1]
    offsets = [value - start for value in raw_offsets]
    for index, is_null in enumerate(column.is_null().to_pylist()):
        if is_null:
            offsets[index] = None
    return pa.array(offsets, type=offsets_array.type), start, end
