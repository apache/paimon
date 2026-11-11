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

from typing import Dict, List, Optional, Tuple
import pyarrow as pa
from pypaimon.data.generic_variant import GenericVariant, _Type, _variant_get_type
from pypaimon.data._variant_binary import _OBJECT, _U32_SIZE, _read_unsigned, _primitive_header

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
    pairs: List[Tuple[int, int]] = []
    for i in range(size):
        key_id = _read_unsigned(overflow_bytes, id_start + i * id_size, id_size)
        off = _read_unsigned(overflow_bytes, offset_start + i * offset_size, offset_size)
        pairs.append((key_id, off))

    sentinel = _read_unsigned(overflow_bytes, offset_start + size * offset_size, offset_size)

    # Sort by offset so that adjacent entries define contiguous data boundaries.
    # Track by original index (always unique) to avoid dict key collisions when
    # two fields share the same offset (malformed data).
    indexed_pairs = sorted(enumerate(pairs), key=lambda x: x[1][1])
    boundaries = [ip[1][1] for ip in indexed_pairs] + [sentinel]

    # end_by_orig[i] = end offset for pairs[i]
    end_by_orig = [0] * size
    for rank, (orig_idx, _) in enumerate(indexed_pairs):
        end_by_orig[orig_idx] = boundaries[rank + 1]

    fields: List[Tuple[int, bytes]] = []
    for orig_idx, (key_id, off) in enumerate(pairs):
        end = end_by_orig[orig_idx]
        field_bytes = bytes(overflow_bytes[data_start + off:data_start + end])
        fields.append((key_id, field_bytes))
    return fields


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
        key_dict = parse_metadata_dict(metadata)
        id_to_name = {v: k for k, v in key_dict.items()}
        shredded_names = {of.field_name for of in schema.object_fields}

        overflow_pairs = [
            (kid, fb) for kid, fb in all_sub
            if id_to_name.get(kid) not in shredded_names
        ]
        shredded_by_name = {
            id_to_name[kid]: fb for kid, fb in all_sub
            if kid in id_to_name and id_to_name[kid] in shredded_names
        }

        typed_value: Dict[str, dict] = {}
        for of in schema.object_fields:
            fname = of.field_name
            if fname in shredded_by_name:
                typed_value[fname] = _decompose_field_bytes(
                    shredded_by_name[fname], of.schema, metadata
                )
            else:
                typed_value[fname] = {'value': None, 'typed_value': None}

        overflow_bytes = (_build_object_value(overflow_pairs, key_dict)
                          if overflow_pairs else None)
        return {'value': overflow_bytes, 'typed_value': typed_value}

    # No shredding sub-schema: treat field bytes as overflow
    return {'value': field_bytes, 'typed_value': None}


def decompose_variant(
    gv: 'GenericVariant',
    obj_fields: List[ObjectField],
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
    key_dict = parse_metadata_dict(metadata)
    id_to_name = {v: k for k, v in key_dict.items()}
    shredded_names = {of.field_name for of in obj_fields}

    overflow_pairs = [
        (kid, fb) for kid, fb in all_fields
        if id_to_name.get(kid) not in shredded_names
    ]
    shredded_by_name = {
        id_to_name[kid]: fb for kid, fb in all_fields
        if kid in id_to_name and id_to_name[kid] in shredded_names
    }

    typed_value = {}
    for of in obj_fields:
        fname = of.field_name
        if fname in shredded_by_name:
            typed_value[fname] = _decompose_field_bytes(
                shredded_by_name[fname], of.schema, metadata
            )
        else:
            typed_value[fname] = {'value': None, 'typed_value': None}

    overflow_bytes = (_build_object_value(overflow_pairs, key_dict)
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
    result = []
    for row in rows:
        if row is None:
            result.append(None)
        else:
            gv = GenericVariant.from_arrow_struct(row)
            result.append(decompose_variant(gv, obj_fields))
    return pa.array(result, type=target_type)
