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

"""Java-compatible shared-shredding MAP conversion for data files."""

import math
from array import array
from collections import deque

import pyarrow as pa

from pypaimon.data.map_shared_shredding import (
    _normalized_offsets,
    shared_shredding_metadata,
)
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    MapType,
    MultisetType,
    RowType,
    VectorType,
)
from pypaimon.table.bucket_mode import BucketMode


_FIELD_MAPPING = "__field_mapping"
_OVERFLOW = "__overflow"
_PHYSICAL_COLUMN_PREFIX = "__col_"
_FIELD_ID_BASE = 2147483647 // 4
_FIELD_ID_DEPTH_LIMIT = 1 << 10
_CONVERSION_BYTES = 8 * 1024 * 1024


class MapSharedShreddingWriter:
    """Converts configured logical MAP columns before each file write."""

    def __init__(self, fields, options, file_format, changelog_format):
        self._options = options
        field_by_name = {field.name: field for field in fields}
        configured = self._configured_fields()
        self._fields = {}
        self._max_columns = {}
        self._policies = {}
        self._recent_widths = {}

        for name, field in field_by_name.items():
            if name in configured and not isinstance(field.type, MapType):
                raise ValueError(
                    "Column '{}' is configured with map.storage-layout but "
                    "its type is not MAP.".format(name))
            layout = options.map_storage_layout(name)
            if layout not in ("default", "shared-shredding"):
                raise ValueError(
                    "Unsupported MAP storage layout for '{}': {}".format(
                        name, layout))
            if layout != "shared-shredding":
                continue
            self._validate_field(field)
            self._fields[name] = field
            self._max_columns[name] = \
                options.map_shared_shredding_max_columns(name)
            self._policies[name] = \
                options.map_shared_shredding_column_placement_policy(name)
            self._recent_widths[name] = deque(maxlen=20)

        if configured.difference(field_by_name):
            name = sorted(configured.difference(field_by_name))[0]
            raise ValueError(
                "Column '{}' is configured with map.storage-layout but does "
                "not exist in table schema.".format(name))
        if not self._fields:
            return

        self._validate_format("file.format", file_format)
        self._validate_format("changelog.file.format", changelog_format)
        self._validate_compression("file.compression", options.file_compression())
        if options.bucket() == BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(
                "MAP shared-shredding does not support postpone bucket mode.")
        if any(_contains_type(field.type, _is_variant)
               for field in fields):
            raise ValueError(
                "MAP shared-shredding cannot be used with VARIANT fields.")
        if any(_contains_type(field.type, lambda t: isinstance(t, MultisetType))
               for field in fields):
            raise ValueError(
                "MAP shared-shredding cannot be used with MULTISET fields.")

    def is_active(self):
        return bool(self._fields)

    def write_parquet(self, file_io, path, data, compression, zstd_level):
        """Plan key metadata, then write bounded physical batches without retaining them."""
        import pyarrow.parquet as pq

        fields = list(data.schema)
        completed = {}
        converters = {}
        field_index = {field.name: index for index, field in enumerate(fields)}

        for name, logical_field in self._fields.items():
            index = field_index.get(name)
            if index is None:
                continue
            input_field = fields[index]
            if not pa.types.is_map(input_field.type):
                raise TypeError(
                    "Shared-shredding column '{}' must be a MAP.".format(name))
            num_columns = self._next_num_columns(name)
            converter = _MapFieldConverter(
                num_columns,
                self._policies[name],
                input_field.type.item_type,
                logical_field.type.value,
            )
            # Parquet's Arrow schema metadata must be known before opening the
            # writer. Scan only keys; reset placement for the actual write pass.
            for chunk in data.column(index).chunks:
                for start in range(0, len(chunk), 1024):
                    converter.analyze(chunk.slice(start, 1024))
            metadata = dict(input_field.metadata or {})
            metadata.update(shared_shredding_metadata(
                converter.name_to_id,
                converter.field_to_columns,
                converter.overflow_set,
                num_columns,
                converter.max_row_width,
                self._options.file_compression(),
            ))
            metadata.update(_field_id_metadata(logical_field.id))
            fields[index] = pa.field(
                name,
                converter.physical_type,
                nullable=input_field.nullable,
                metadata=metadata,
            )
            completed[name] = converter.max_row_width
            converters[index] = _MapFieldConverter(
                num_columns, self._policies[name], input_field.type.item_type,
                logical_field.type.value)

        schema = pa.schema(fields, metadata=data.schema.metadata)
        # Bound scratch space for slot mappings/indices as well as logical values.
        slots = sum(c.num_columns for c in converters.values())
        batch_rows = max(1, min(1024, _CONVERSION_BYTES // max(1, slots * 16)))
        kwargs = {'compression': compression}
        if compression.lower() == 'zstd':
            kwargs['compression_level'] = zstd_level
        try:
            with file_io.new_output_stream(path) as stream:
                with pq.ParquetWriter(stream, schema, **kwargs) as writer:
                    for batch in data.to_batches(max_chunksize=batch_rows):
                        for bounded in _bounded_batches(batch):
                            columns = list(bounded.columns)
                            for index, converter in converters.items():
                                columns[index] = converter.convert(columns[index])
                            physical = pa.Table.from_arrays(columns, schema=schema)
                            writer.write_table(physical)
                            del physical, columns
        except Exception:
            file_io.delete_quietly(path)
            raise
        return completed

    def file_completed(self, completed):
        for name, max_row_width in completed.items():
            self._recent_widths[name].append(max_row_width)

    def _next_num_columns(self, name):
        widths = self._recent_widths[name]
        max_columns = self._max_columns[name]
        if not widths:
            return max_columns
        ordered = sorted(widths)
        rank = max(1, min(int(math.ceil(0.90 * len(ordered))), len(ordered)))
        percentile = ordered[rank - 1]
        maximum = ordered[-1]
        if (maximum - percentile <= 4
                or maximum <= int(math.ceil(percentile * 1.25))):
            width = maximum
        else:
            width = percentile
        return max(1, min(width, max_columns))

    def _configured_fields(self):
        prefix = "fields."
        suffix = ".map.storage-layout"
        result = set()
        for key in self._options.options.to_map():
            if key.startswith(prefix) and key.endswith(suffix):
                result.add(key[len(prefix):-len(suffix)])
        return result

    @staticmethod
    def _validate_field(field):
        if not isinstance(field.type, MapType):
            raise ValueError(
                "Column '{}' is configured with map.storage-layout="
                "shared-shredding but its type is not MAP.".format(field.name))
        key_type = field.type.key
        if (not isinstance(key_type, AtomicType)
                or not key_type.type.upper().startswith(
                    ("STRING", "VARCHAR"))):
            raise ValueError(
                "Shared-shredding column '{}' must use STRING keys.".format(
                    field.name))
        if key_type.nullable:
            raise ValueError(
                "Shared-shredding MAP keys cannot be nullable for column '{}'."
                .format(field.name))
        if _contains_type(field.type.value, _is_blob):
            raise ValueError(
                "MAP shared-shredding cannot contain BLOB fields.")
        if _contains_type(field.type.value, lambda t: isinstance(t, VectorType)):
            raise ValueError(
                "MAP shared-shredding cannot contain VECTOR fields.")

    @staticmethod
    def _validate_format(option, file_format):
        if file_format and file_format.lower() != "parquet":
            raise ValueError(
                "PyPaimon MAP shared-shredding writes only support parquet, "
                "but {} is {}.".format(option, file_format))

    @staticmethod
    def _validate_compression(option, compression):
        if compression and compression.lower() not in ("none", "lz4", "zstd"):
            raise ValueError(
                "MAP shared-shredding only supports none/lz4/zstd compression, "
                "but {} is {}.".format(option, compression))


class _MapFieldConverter:

    def __init__(self, num_columns, policy, item_type, logical_item_type):
        self.num_columns = num_columns
        self.policy = policy
        self.name_to_id = {}
        self.field_to_columns = {}
        self.overflow_set = set()
        self.max_row_width = 0
        self._resident = [-1] * num_columns
        self._last_used = [0] * num_columns
        self._clock = 0
        self.physical_type = _physical_struct_type(
            num_columns, item_type, logical_item_type)

    def convert(self, column):
        offsets, start, end = _normalized_offsets(column)
        keys = column.keys.slice(start, end - start).to_pylist()
        values = _to_python_values(column.items.slice(start, end - start))
        nulls = column.is_null().to_pylist()
        mappings = array('i')
        mapping_offsets = [0]
        slots = {}
        overflows = []
        for row, is_null in enumerate(nulls):
            start, end = offsets[row:row + 2]
            if is_null:
                mapping_offsets.append(len(mappings))
                overflows.append(None)
                continue
            field_ids, mapping, overflow = self._place(keys[start:end])
            items = dict(zip(field_ids, values[start:end]))
            mappings.extend(mapping)
            mapping_offsets.append(len(mappings))
            for column_id, field_id in enumerate(mapping):
                if field_id >= 0:
                    if column_id not in slots:
                        slots[column_id] = [None] * len(column)
                    slots[column_id][row] = items[field_id]
            overflows.append([(field_id, items[field_id]) for field_id in overflow]
                             if overflow else None)
        children = [pa.ListArray.from_arrays(
            pa.array(mapping_offsets, type=pa.int32()),
            pa.array(mappings, type=pa.int32()))]
        empty = None
        for column_id in range(self.num_columns):
            value_type = self.physical_type[column_id + 1].type
            if column_id in slots:
                children.append(pa.array(slots.pop(column_id), type=value_type))
            else:
                if empty is None:
                    empty = pa.array([None] * len(column), type=value_type)
                children.append(empty)
        children.append(pa.array(overflows, type=self.physical_type[-1].type))
        return pa.StructArray.from_arrays(
            children, fields=list(self.physical_type),
            mask=column.is_null() if column.null_count else None)

    def analyze(self, column):
        offsets, start, end = _normalized_offsets(column)
        keys = column.keys.slice(start, end - start).to_pylist()
        for row, is_null in enumerate(column.is_null().to_pylist()):
            if not is_null:
                self._place(keys[offsets[row]:offsets[row + 1]])

    def _place(self, keys):
        field_ids = []
        for key in keys:
            if key is None:
                raise ValueError("Shared-shredding MAP keys cannot be null")
            if not isinstance(key, str):
                raise TypeError("Shared-shredding MAP keys must be strings")
            field_id = self.name_to_id.setdefault(key, len(self.name_to_id))
            field_ids.append(field_id)

        mapping, overflow = self._allocate(field_ids)
        for column_id, field_id in enumerate(mapping):
            if field_id >= 0:
                self.field_to_columns.setdefault(field_id, set()).add(column_id)
        self.overflow_set.update(overflow)
        self.max_row_width = max(self.max_row_width, len(field_ids))
        return field_ids, mapping, overflow

    def _allocate(self, field_ids):
        if self.policy == "plain":
            ordered = field_ids
            return self._leading(ordered)
        if self.policy == "sequential":
            return self._leading(sorted(field_ids))
        return self._lru(field_ids)

    def _leading(self, field_ids):
        mapping = [-1] * self.num_columns
        for index, field_id in enumerate(field_ids[:self.num_columns]):
            mapping[index] = field_id
        return mapping, list(field_ids[self.num_columns:])

    def _lru(self, field_ids):
        mapping = [-1] * self.num_columns
        next_resident = list(self._resident)
        used = [False] * self.num_columns
        unassigned = []
        for field_id in sorted(field_ids):
            try:
                column_id = self._resident.index(field_id)
            except ValueError:
                unassigned.append(field_id)
                continue
            used[column_id] = True
            mapping[column_id] = field_id

        overflow = []
        for field_id in unassigned:
            column_id = self._select_lru_column(used, next_resident)
            if column_id < 0:
                overflow.append(field_id)
                continue
            used[column_id] = True
            mapping[column_id] = field_id
            next_resident[column_id] = field_id

        touched = False
        for column_id, field_id in enumerate(mapping):
            if field_id >= 0:
                self._last_used[column_id] = self._clock
                touched = True
        if touched:
            self._clock += 1
        self._resident = next_resident
        return mapping, overflow

    def _select_lru_column(self, used, resident):
        selected = -1
        selected_last_used = None
        for column_id in range(self.num_columns):
            if used[column_id]:
                continue
            if resident[column_id] < 0:
                return column_id
            last_used = self._last_used[column_id]
            if selected < 0 or last_used < selected_last_used:
                selected = column_id
                selected_last_used = last_used
        return selected


def _bounded_batches(batch):
    # One oversized row is indivisible; bound the other logical batches before
    # expanding values into Python objects. The caller's input buffer is unchanged.
    if batch.nbytes > _CONVERSION_BYTES and batch.num_rows > 1:
        middle = batch.num_rows // 2
        yield from _bounded_batches(batch.slice(0, middle))
        yield from _bounded_batches(batch.slice(middle))
    else:
        yield batch


def _to_python_values(column):
    """Avoid Arrow 6 MAP scalars, including MAPs nested in ROW/ARRAY values."""
    data_type = column.type
    if pa.types.is_struct(data_type):
        children = [_to_python_values(column.field(i))
                    for i in range(len(data_type))]
        names = [field.name for field in data_type]
        values = [dict(zip(names, (child[i] for child in children)))
                  for i in range(len(column))]
    elif (pa.types.is_map(data_type) or pa.types.is_list(data_type)
          or pa.types.is_large_list(data_type)):
        offsets, start, end = _normalized_offsets(column)
        if pa.types.is_map(data_type):
            keys = _to_python_values(column.keys.slice(start, end - start))
            items = _to_python_values(column.items.slice(start, end - start))
            children = list(zip(keys, items))
        else:
            children = _to_python_values(column.values.slice(start, end - start))
        values = [children[offsets[i]:offsets[i + 1]] for i in range(len(column))]
    elif pa.types.is_fixed_size_list(data_type):
        size = data_type.list_size
        children = _to_python_values(column.values.slice(
            column.offset * size, len(column) * size))
        values = [children[i * size:(i + 1) * size] for i in range(len(column))]
    else:
        return column.to_pylist()
    return [None if is_null else value
            for is_null, value in zip(column.is_null().to_pylist(), values)]


def _physical_struct_type(num_columns, item_type, logical_item_type):
    mapping_type = pa.list_(pa.field(
        "item",
        pa.int32(),
        metadata=_field_id_metadata(_array_element_id(0, 1)),
    ))
    fields = [pa.field(
        _FIELD_MAPPING,
        mapping_type,
        metadata=_field_id_metadata(0),
    )]
    for column_id in range(num_columns):
        fields.append(_field_with_ids(
            _PHYSICAL_COLUMN_PREFIX + str(column_id),
            item_type,
            True,
            logical_item_type,
            column_id + 1,
        ))
    overflow_id = num_columns + 1
    overflow_type = _map_type_with_ids(
        pa.int32(), item_type, logical_item_type, overflow_id, 0)
    fields.append(pa.field(
        _OVERFLOW,
        overflow_type,
        metadata=_field_id_metadata(overflow_id),
    ))
    return pa.struct(fields)


def _field_with_ids(name, arrow_type, nullable, logical_type, field_id):
    return pa.field(
        name,
        _type_with_ids(arrow_type, logical_type, field_id, 0),
        nullable=nullable,
        metadata=_field_id_metadata(field_id),
    )


def _type_with_ids(arrow_type, logical_type, field_id, depth):
    if isinstance(logical_type, RowType) and pa.types.is_struct(arrow_type):
        arrow_by_name = {field.name: field for field in arrow_type}
        return pa.struct([
            _field_with_ids(
                field.name,
                arrow_by_name[field.name].type,
                arrow_by_name[field.name].nullable,
                field.type,
                field.id,
            )
            for field in logical_type.fields
        ])
    if isinstance(logical_type, (ArrayType, VectorType)):
        child_id = _array_element_id(field_id, depth + 1)
        value_field = pa.field(
            "item",
            _type_with_ids(
                arrow_type.value_type,
                logical_type.element,
                field_id,
                depth + 1,
            ),
            nullable=logical_type.element.nullable,
            metadata=_field_id_metadata(child_id),
        )
        if isinstance(logical_type, VectorType):
            return pa.list_(value_field, logical_type.length)
        return pa.list_(value_field)
    if isinstance(logical_type, MapType):
        return _map_type_with_ids(
            arrow_type.key_type,
            arrow_type.item_type,
            logical_type.value,
            field_id,
            depth,
        )
    return arrow_type


def _map_type_with_ids(
        key_type, item_type, logical_item_type, field_id, depth):
    key = pa.field(
        "key",
        key_type,
        nullable=False,
        metadata=_field_id_metadata(_map_key_id(field_id, depth + 1)),
    )
    item = pa.field(
        "value",
        _type_with_ids(
            item_type, logical_item_type, field_id, depth + 1),
        nullable=logical_item_type.nullable,
        metadata=_field_id_metadata(_map_value_id(field_id, depth + 1)),
    )
    return pa.map_(key, item)


def _field_id_metadata(field_id):
    value = str(field_id).encode("utf-8")
    return {b"PARQUET:field_id": value, b"paimon.id": value}


def _array_element_id(field_id, depth):
    return _FIELD_ID_BASE + field_id * _FIELD_ID_DEPTH_LIMIT + depth


def _map_key_id(field_id, depth):
    return _FIELD_ID_BASE - field_id * _FIELD_ID_DEPTH_LIMIT - depth


def _map_value_id(field_id, depth):
    return _FIELD_ID_BASE + field_id * _FIELD_ID_DEPTH_LIMIT + depth


def _contains_type(data_type, predicate):
    if predicate(data_type):
        return True
    if isinstance(data_type, RowType):
        return any(_contains_type(field.type, predicate)
                   for field in data_type.fields)
    if isinstance(data_type, (ArrayType, VectorType, MultisetType)):
        return _contains_type(data_type.element, predicate)
    if isinstance(data_type, MapType):
        return (_contains_type(data_type.key, predicate)
                or _contains_type(data_type.value, predicate))
    return False


def _is_variant(data_type):
    return (isinstance(data_type, AtomicType)
            and data_type.type.upper() == "VARIANT")


def _is_blob(data_type):
    return (isinstance(data_type, AtomicType)
            and data_type.type.upper() == "BLOB")
