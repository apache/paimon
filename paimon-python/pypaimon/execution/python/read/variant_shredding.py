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

"""Python execution for physical shredded columns."""

import struct
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple
import numpy as np
import pyarrow as pa
from pypaimon.data.generic_variant import GenericVariant, _Type, _variant_get_type
from pypaimon.data._variant_binary import (
    _OBJECT, _U8_MAX, _U32_SIZE, _get_int_size, _object_header,
    _primitive_header, _read_unsigned,
)

from pypaimon.data.variant_shredding import (
    VARIANT_ARROW_TYPE,
    ObjectField,
    VariantSchema,
    _build_array_value,
    _build_object_value,
    _encode_scalar_to_value_bytes,
    parse_metadata_dict,
)


_NULL_TYPE_ID = 0
_TRUE_TYPE_ID = 1
_FALSE_TYPE_ID = 2
_NULL_VALUE_BYTES: bytes = bytes([_primitive_header(_NULL_TYPE_ID)])
# Use the bulk decoder only when enough fields amortize the array allocations.
_MIN_BULK_OBJECT_FIELDS = 256


def _metadata_id_to_name(metadata: bytes) -> Dict[int, str]:
    return {key_id: name for name, key_id in parse_metadata_dict(metadata).items()}


def _read_object_ints(data: bytes, start: int, count: int, width: int):
    if width == 1:
        return data[start:start + count]
    if width in (2, 4) and start + count * width <= len(data):
        kind = 'H' if width == 2 else 'I'
        return struct.unpack_from('<{}{}'.format(count, kind), data, start)
    return tuple(_read_unsigned(data, start + i * width, width) for i in range(count))


def _extract_overflow_fields(overflow_bytes: bytes) -> List[Tuple[int, bytes]]:
    """Parse an overflow binary (a variant object) into ``(key_id, value_bytes)`` pairs.

    The overflow binary contains fields that were NOT shredded — they remain
    encoded as a compact variant object.
    """
    if not overflow_bytes:
        return []

    b = overflow_bytes[0]
    basic_type = b & 0x3
    if basic_type != _OBJECT:
        return []

    type_info = (b >> 2) & 0x3F
    large_size = bool((type_info >> 4) & 0x1)
    size_bytes = _U32_SIZE if large_size else 1
    size = _read_unsigned(overflow_bytes, 1, size_bytes)
    if size == 0:
        return []

    id_size = ((type_info >> 2) & 0x3) + 1
    offset_size = (type_info & 0x3) + 1
    id_start = 1 + size_bytes
    offset_start = id_start + size * id_size
    data_start = offset_start + (size + 1) * offset_size

    # The Parquet variant spec stores field offsets in the same order as the id_table,
    # but the DATA section may be laid out in a different order (e.g. GenericVariantBuilder
    # sorts the id/offset tables alphabetically while writing data in insertion order).
    # We must sort by offset to determine each field's byte boundaries correctly.
    #
    # Example: variant {"b": 2, "a": 1} written by GenericVariantBuilder
    #   id_table:     [id_a=0,  id_b=1]    (alphabetical order)
    #   offset_table: [off_a=2, off_b=0]   (id-order: a first, then b)
    #   data section: [enc(2),  enc(1)]    (insertion order: b first, then a)
    #   So pairs = [(id_a=0, off=2), (id_b=1, off=0)]
    #   Sorted by offset: [(id_b=1, off=0), (id_a=0, off=2)]
    #   Boundaries:        [0, 2, sentinel] → enc(2)=data[0:2], enc(1)=data[2:sentinel]
    key_ids = _read_object_ints(overflow_bytes, id_start, size, id_size)
    offsets = _read_object_ints(overflow_bytes, offset_start, size + 1, offset_size)

    # Sort by offset so that adjacent entries define contiguous data boundaries.
    # Track by original index (always unique) to avoid dict key collisions when
    # two fields share the same offset (malformed data).
    sorted_indices = sorted(range(size), key=offsets.__getitem__)

    # end_by_orig[i] = end offset for key_ids[i]
    end_by_orig = [0] * size
    end = offsets[size]
    for orig_idx in reversed(sorted_indices):
        end_by_orig[orig_idx] = end
        end = offsets[orig_idx]

    fields: List[Tuple[int, bytes]] = []
    for orig_idx, key_id in enumerate(key_ids):
        off = offsets[orig_idx]
        end = end_by_orig[orig_idx]
        field_bytes = bytes(overflow_bytes[data_start + off:data_start + end])
        fields.append((key_id, field_bytes))
    return fields


def _split_object_fields(
    fields: List[Tuple[int, bytes]],
    id_to_name: Dict[int, str],
    shredded_names: Set[str],
):
    overflow_pairs = []
    shredded_by_name = {}
    previous_name = None
    ordered = True
    for kid, field_bytes in fields:
        name = id_to_name.get(kid)
        if name is None:
            raise ValueError('MALFORMED_VARIANT: key id out of range')
        if previous_name is not None and name < previous_name:
            ordered = False
        previous_name = name
        if name in shredded_names:
            shredded_by_name[name] = field_bytes
        else:
            overflow_pairs.append((kid, field_bytes))

    if not ordered:
        overflow_pairs.sort(key=lambda item: id_to_name[item[0]].encode('utf-8'))
    return overflow_pairs, shredded_by_name


def rebuild_value(
    row: dict,
    schema: VariantSchema,
    key_dict: Dict[str, int],
) -> Optional[bytes]:
    """Reconstruct variant value bytes from a shredded sub-row dict.

    Args:
        row:      Python dict from ``PyArrow StructScalar.as_py()``.  Keys are
                  ``'value'`` and/or ``'typed_value'``.
        schema:   VariantSchema for this level.
        key_dict: ``{key_name: key_id}`` parsed from the top-level metadata.

    Returns:
        Variant value bytes, or ``None`` if the field is absent (both
        ``typed_value`` and ``value`` are null — "missing from this row").
    """
    typed_value = row.get('typed_value') if schema.typed_idx >= 0 else None
    overflow = row.get('value') if schema.value_idx >= 0 else None

    # if both null → field is absent in this row
    if typed_value is None and overflow is None:
        return None

    # if typed_value is null → use overflow bytes directly
    if typed_value is None:
        return bytes(overflow)

    if schema.scalar_arrow_type is not None:
        return _encode_scalar_to_value_bytes(typed_value, schema.scalar_arrow_type)

    if schema.object_fields is not None:
        return _rebuild_object(typed_value, schema, key_dict, overflow)

    if schema.array_schema is not None:
        return _rebuild_array(typed_value, schema.array_schema, key_dict)

    # No sub-schema for typed_value; fall back to overflow
    return bytes(overflow) if overflow is not None else None


def _rebuild_object(
    typed_value: dict,
    schema: VariantSchema,
    key_dict: Dict[str, int],
    overflow_bytes: Optional[bytes],
) -> bytes:
    """Rebuild an object variant from shredded object sub-fields."""
    fields: List[Tuple[int, bytes]] = []

    for obj_field in schema.object_fields:
        fname = obj_field.field_name
        sub_row = typed_value.get(fname) if isinstance(typed_value, dict) else None
        if sub_row is None:
            continue

        field_value = rebuild_value(sub_row, obj_field.schema, key_dict)
        if field_value is None:
            continue

        key_id = key_dict.get(fname)
        if key_id is None:
            # Key not found in metadata — data integrity issue, skip
            continue
        fields.append((key_id, field_value))

    # Merge with overflow (fields not shredded)
    if overflow_bytes:
        fields.extend(_extract_overflow_fields(bytes(overflow_bytes)))

    return _build_object_value(fields, key_dict)


def _rebuild_array(
    typed_value,
    element_schema: VariantSchema,
    key_dict: Dict[str, int],
) -> bytes:
    """Rebuild an array variant from a shredded list."""
    if typed_value is None:
        typed_value = []

    element_bytes_list: List[bytes] = []
    for element_row in typed_value:
        if element_row is None:
            element_bytes_list.append(_NULL_VALUE_BYTES)
        elif isinstance(element_row, dict):
            eb = rebuild_value(element_row, element_schema, key_dict)
            element_bytes_list.append(eb if eb is not None else _NULL_VALUE_BYTES)
        else:
            # Scalar element directly (no surrounding {value, typed_value} struct)
            if element_schema.scalar_arrow_type is not None:
                eb = _encode_scalar_to_value_bytes(element_row, element_schema.scalar_arrow_type)
            else:
                eb = _NULL_VALUE_BYTES
            element_bytes_list.append(eb)

    return _build_array_value(element_bytes_list)


def rebuild(
    row: dict,
    schema: VariantSchema,
    key_dict: Optional[Dict[str, int]] = None,
) -> Tuple[bytes, bytes]:
    """Reconstruct ``(value_bytes, metadata_bytes)`` from a top-level shredded row.

    Args:
        row:      Top-level shredded row dict (contains ``'metadata'``,
                  ``'value'``, and/or ``'typed_value'`` keys).
        schema:   Top-level VariantSchema (from ``build_variant_schema()``).
        key_dict: Optional pre-parsed ``{key_name: key_id}`` mapping.  If None,
                  it is parsed from ``row['metadata']`` automatically.

    Returns:
        ``(value_bytes, metadata_bytes)`` suitable for constructing a standard
        VARIANT ``struct<value: binary, metadata: binary>`` row.
    """
    raw_metadata = row.get('metadata')
    if raw_metadata is None:
        raise ValueError("Shredded VARIANT row missing 'metadata' field")
    metadata = bytes(raw_metadata)

    if key_dict is None:
        key_dict = parse_metadata_dict(metadata)

    value_bytes = rebuild_value(row, schema, key_dict)
    if value_bytes is None:
        value_bytes = _NULL_VALUE_BYTES

    return value_bytes, metadata


def assemble_shredded_column(column: pa.Array, schema: VariantSchema) -> pa.Array:
    """Convert a shredded VARIANT column to standard ``struct<value, metadata>``.

    Args:
        column: A PyArrow Array whose type is a shredded VARIANT struct.
        schema: VariantSchema built from the column's type.

    Returns:
        A ``pa.StructArray`` with type ``struct<value: binary, metadata: binary>``.
    """
    rows = column.to_pylist()
    assembled = []
    # cache parsed key dicts keyed by metadata bytes; most files share one metadata
    key_dict_cache: Dict[bytes, Dict[str, int]] = {}

    for row in rows:
        if row is None:
            assembled.append(None)
            continue

        raw_meta = row.get('metadata')
        if raw_meta is None:
            assembled.append(None)
            continue

        metadata = bytes(raw_meta)
        if metadata not in key_dict_cache:
            key_dict_cache[metadata] = parse_metadata_dict(metadata)

        value_bytes = rebuild_value(row, schema, key_dict_cache[metadata])
        if value_bytes is None:
            value_bytes = _NULL_VALUE_BYTES

        assembled.append({'value': value_bytes, 'metadata': metadata})

    return pa.array(assembled, type=VARIANT_ARROW_TYPE)


def _try_shred_scalar(field_bytes: bytes, metadata: bytes, arrow_type: pa.DataType):
    kind = _variant_get_type(field_bytes, 0)
    if kind == _Type.DOUBLE:
        compatible = pa.types.is_float64(arrow_type)
    elif kind == _Type.FLOAT:
        compatible = pa.types.is_float32(arrow_type)
    elif kind == _Type.LONG:
        compatible = pa.types.is_integer(arrow_type)
    elif kind == _Type.BOOLEAN:
        compatible = pa.types.is_boolean(arrow_type)
    elif kind == _Type.STRING:
        compatible = pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)
    elif kind == _Type.BINARY:
        compatible = pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type)
    elif kind == _Type.DATE:
        compatible = pa.types.is_date32(arrow_type)
    elif kind == _Type.DECIMAL:
        compatible = pa.types.is_decimal(arrow_type)
    elif kind in (_Type.TIMESTAMP, _Type.TIMESTAMP_NTZ):
        compatible = (pa.types.is_timestamp(arrow_type)
                      and bool(arrow_type.tz) == (kind == _Type.TIMESTAMP))
    else:
        # JSON null stays in residual; two null columns mean absent.
        return None
    if not compatible:
        return None

    value = GenericVariant(field_bytes, metadata).to_python()
    if kind == _Type.LONG:
        bits = arrow_type.bit_width
        lower = 0 if pa.types.is_unsigned_integer(arrow_type) else -(1 << (bits - 1))
        if not lower <= value < lower + (1 << bits):
            return None
    elif kind in (_Type.DECIMAL, _Type.TIMESTAMP, _Type.TIMESTAMP_NTZ):
        # Keep Arrow validation for scale, timestamp precision and overflow.
        value = pa.scalar(value, type=arrow_type).as_py()
        if _encode_scalar_to_value_bytes(value, arrow_type) != field_bytes:
            return None
    return value


def _decompose_field_bytes(
    field_bytes: bytes,
    schema: VariantSchema,
    metadata: bytes,
    id_to_name: Optional[Dict[int, str]] = None,
) -> dict:
    """Decompose a variant field's value bytes into a ``{value, typed_value}`` sub-struct dict.

    This is the write-direction counterpart of ``rebuild_value()``.

    Args:
        field_bytes: Variant value bytes for a single field extracted from the
                     original variant binary.
        schema:      VariantSchema describing how this field should be shredded.
        metadata:    Top-level metadata bytes (used for key ID lookups in nested objects).

    Returns:
        A Python dict ``{'value': bytes_or_none, 'typed_value': val_or_dict_or_none}``
        suitable for building a PyArrow struct array row.
    """
    if schema.scalar_arrow_type is not None:
        try:
            typed_val = _try_shred_scalar(field_bytes, metadata, schema.scalar_arrow_type)
        except Exception:
            typed_val = None
        return {'value': field_bytes if typed_val is None else None, 'typed_value': typed_val}

    if schema.object_fields is not None:
        if not field_bytes or (field_bytes[0] & 0x3) != _OBJECT:
            return {'value': field_bytes, 'typed_value': None}

        all_sub = _extract_overflow_fields(field_bytes)
        if id_to_name is None:
            id_to_name = _metadata_id_to_name(metadata)
        shredded_names = schema.object_schema_map
        if shredded_names is None:
            shredded_names = {of.field_name for of in schema.object_fields}

        overflow_pairs, shredded_by_name = _split_object_fields(
            all_sub, id_to_name, shredded_names)

        typed_value: Dict[str, dict] = {}
        for of in schema.object_fields:
            fname = of.field_name
            if fname in shredded_by_name:
                typed_value[fname] = _decompose_field_bytes(
                    shredded_by_name[fname], of.schema, metadata, id_to_name
                )
            else:
                typed_value[fname] = {'value': None, 'typed_value': None}

        overflow_bytes = (_build_object_value(overflow_pairs, already_sorted=True)
                          if overflow_pairs else None)
        return {'value': overflow_bytes, 'typed_value': typed_value}

    # No shredding sub-schema: treat field bytes as overflow
    return {'value': field_bytes, 'typed_value': None}


def _read_object_int_array(data, start, count, width):
    """Read the little-endian IDs or offsets without allocating Python integers."""
    if width != 3:
        return np.frombuffer(data, dtype='<u{}'.format(width), count=count, offset=start)
    parts = np.frombuffer(data, dtype='u1', count=count * width, offset=start).reshape(-1, width)
    return (parts[:, 0].astype('u4')
            | (parts[:, 1].astype('u4') << 8)
            | (parts[:, 2].astype('u4') << 16))


def _pack_object_ints(values, width):
    if width != 3:
        return values.astype('<u{}'.format(width), copy=False).tobytes()
    return values.astype('<u4', copy=False).view('u1').reshape(-1, 4)[:, :3].tobytes()


def _copy_object_residual(data, ids, starts, ends):
    """Encode IDs and new offsets, then copy adjacent cold values in one slice."""
    offsets = np.empty(len(ids) + 1, dtype='<u4')
    offsets[0] = 0
    np.cumsum(ends - starts, out=offsets[1:])
    id_width = _get_int_size(int(ids.max()))
    offset_width = _get_int_size(int(offsets[-1]))
    large = len(ids) > _U8_MAX
    size_width = _U32_SIZE if large else 1
    header = bytes([_object_header(large, id_width, offset_width)])
    parts = [
        header,
        len(ids).to_bytes(size_width, 'little'),
        _pack_object_ints(ids, id_width),
        _pack_object_ints(offsets, offset_width),
    ]
    breaks = np.flatnonzero(ends[:-1] != starts[1:])
    left = starts[np.r_[0, breaks + 1]].tolist()
    right = ends[np.r_[breaks, len(ids) - 1]].tolist()
    parts.extend(data[lo:hi] for lo, hi in zip(left, right))
    return b''.join(parts)


def _split_wide_object(value, size, lookup):
    """Extract hot fields and encode the residual in the existing field order.

    Return None for layouts outside the bulk path so the original decoder
    keeps handling their ordering, bounds, and error semantics.
    """
    names, ranks, hot_ids = lookup
    header = value[0]
    size_width = _U32_SIZE if header & 0x40 else 1
    id_width = ((header >> 4) & 3) + 1
    offset_width = ((header >> 2) & 3) + 1
    ids_start = 1 + size_width
    offsets_start = ids_start + size * id_width
    data_start = offsets_start + (size + 1) * offset_width
    if data_start > len(value):
        return None

    ids = _read_object_int_array(value, ids_start, size, id_width)
    if int(ids.max()) >= len(ranks):
        return None
    key_ranks = ranks[ids]
    if np.any(key_ranks[1:] <= key_ranks[:-1]):
        return None

    offsets = _read_object_int_array(value, offsets_start, size + 1, offset_width)
    starts = offsets[:-1]
    # Key order and physical data order need not match. Find value boundaries
    # in data order, then restore key order for the residual writer.
    order = np.argsort(starts, kind='stable')
    sorted_starts = starts[order]
    if (int(sorted_starts[0]) != 0
            or int(offsets[-1]) != len(value) - data_start
            or np.any(sorted_starts[1:] <= sorted_starts[:-1])
            or int(sorted_starts[-1]) >= int(offsets[-1])):
        return None
    ends = np.empty_like(starts)
    ends[order[:-1]] = sorted_starts[1:]
    ends[order[-1]] = offsets[-1]

    hot = hot_ids[ids]
    shredded = {
        names[int(ids[i])]: value[data_start + int(starts[i]):data_start + int(ends[i])]
        for i in np.flatnonzero(hot)
    }
    cold = ~hot
    residual = (_copy_object_residual(memoryview(value)[data_start:], ids[cold], starts[cold], ends[cold])
                if np.any(cold) else None)
    return residual, shredded


def _decompose_object_fields(obj_fields, shredded_by_name, metadata, id_to_name):
    return {
        field.field_name: (
            _decompose_field_bytes(
                shredded_by_name[field.field_name], field.schema, metadata, id_to_name)
            if field.field_name in shredded_by_name else {'value': None, 'typed_value': None}
        )
        for field in obj_fields
    }


def decompose_variant(
    gv: 'GenericVariant',
    obj_fields: List[ObjectField],
    shredded_names: Optional[Set[str]] = None,
    id_to_name: Optional[Dict[int, str]] = None,
) -> dict:
    """Decompose a ``GenericVariant`` into a shredded row dict for writing.

    This is the inverse of ``rebuild()`` / ``rebuild_value()`` — it takes a
    fully-encoded variant and splits it into the shredded ``{metadata, value,
    typed_value}`` structure that Parquet shredding expects.

    Args:
        gv:         The ``GenericVariant`` to decompose.
        obj_fields: List of ``ObjectField`` from ``parse_shredding_schema_option()``,
                    describing which top-level object keys to shred.

    Returns:
        A Python dict matching the Arrow type produced by
        ``shredding_schema_to_arrow_type(obj_fields)``.
    """
    metadata = gv.metadata()
    value_bytes = gv.value()

    # Non-object variants cannot be shredded: put everything in overflow and set
    # typed_value to NULL so the Java reader falls through to the overflow path.
    if not value_bytes or (value_bytes[0] & 0x3) != _OBJECT:
        return {'metadata': metadata, 'value': value_bytes, 'typed_value': None}

    all_fields = _extract_overflow_fields(value_bytes)
    if id_to_name is None:
        id_to_name = _metadata_id_to_name(metadata)
    if shredded_names is None:
        shredded_names = {of.field_name for of in obj_fields}

    overflow_pairs, shredded_by_name = _split_object_fields(
        all_fields, id_to_name, shredded_names)

    typed_value = _decompose_object_fields(obj_fields, shredded_by_name, metadata, id_to_name)
    overflow_bytes = (_build_object_value(overflow_pairs, already_sorted=True)
                      if overflow_pairs else None)
    return {'metadata': metadata, 'value': overflow_bytes, 'typed_value': typed_value}


def shred_variant_column(
    column: pa.Array,
    obj_fields: List[ObjectField],
    target_type: pa.StructType,
) -> pa.Array:
    """Convert a standard VARIANT column to its shredded representation.

    Args:
        column:      A ``pa.Array`` of type ``struct<value: binary, metadata: binary>``
                     (the standard Paimon VARIANT layout).
        obj_fields:  ``[ObjectField, ...]`` from ``parse_shredding_schema_option()``.
        target_type: The Arrow struct type from ``shredding_schema_to_arrow_type(obj_fields)``.

    Returns:
        A ``pa.StructArray`` with ``target_type`` suitable for writing to Parquet
        in the shredded format.
    """
    from pypaimon.data.generic_variant import GenericVariant

    rows = column.to_pylist()
    shredded_names = {of.field_name for of in obj_fields}

    @lru_cache(maxsize=16)
    def metadata_names(metadata: bytes) -> Dict[int, str]:
        return _metadata_id_to_name(metadata)

    @lru_cache(maxsize=16)
    def wide_object_lookup(metadata):
        id_to_name = metadata_names(metadata)
        if any(index not in id_to_name for index in range(len(id_to_name))):
            return None
        names = tuple(id_to_name[index] for index in range(len(id_to_name)))
        ranks = np.empty(len(names), dtype='i4')
        for rank, index in enumerate(sorted(range(len(names)), key=names.__getitem__)):
            ranks[index] = rank
        return names, ranks, np.asarray([name in shredded_names for name in names], dtype=bool)

    result = []
    for row in rows:
        if row is None:
            result.append(None)
            continue

        gv = GenericVariant.from_arrow_struct(row)
        value = gv.value()
        metadata = gv.metadata()
        id_to_name = None
        if value and (value[0] & 0x3) == _OBJECT:
            id_to_name = metadata_names(metadata)
            size = _read_unsigned(value, 1, _U32_SIZE if value[0] & 0x40 else 1)
            if size >= _MIN_BULK_OBJECT_FIELDS:
                lookup = wide_object_lookup(metadata)
                parts = _split_wide_object(value, size, lookup) if lookup is not None else None
                if parts is not None:
                    residual, shredded = parts
                    typed = _decompose_object_fields(obj_fields, shredded, metadata, id_to_name)
                    result.append({'metadata': metadata, 'value': residual, 'typed_value': typed})
                    continue
        result.append(decompose_variant(gv, obj_fields, shredded_names, id_to_name))
    return pa.array(result, type=target_type)
