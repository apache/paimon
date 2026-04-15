################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################

"""Shredded Parquet VARIANT support for pypaimon.

Paimon stores VARIANT columns in Parquet using a "shredded" format that enables
efficient sub-field reading and predicate pushdown.  This module provides:

  1. VariantSchema / build_variant_schema()
     Parse the PyArrow struct type of a shredded VARIANT column into a tree
     that mirrors the Java ``VariantSchema`` class.

  2. rebuild_value() / rebuild()
     Reconstruct standard ``(value: bytes, metadata: bytes)`` VARIANT binary
     from a shredded row dict.  Mirrors ``ShreddingUtils.rebuild()`` in Java.

  3. assemble_shredded_column()
     High-level helper used by ``FormatPyArrowReader`` to post-process batches
     that contain shredded VARIANT columns.

  4. is_shredded_variant()
     Detect shredded VARIANT columns in a file schema.

Shredded column layout (Parquet GROUP → PyArrow struct):
  - ``metadata``: binary — the top-level key dictionary
  - ``value``:    binary (optional) — overflow bytes for un-shredded fields
  - ``typed_value``: struct — per-field typed sub-columns (shredded fields)

Each field inside ``typed_value`` has the same ``{value, typed_value}``
structure recursively.  No ``field_`` prefix is used; sub-column names are
the exact variant key names.
"""

import datetime
import decimal as _decimal
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

# ---------------------------------------------------------------------------
# Variant binary constants (mirror generic_variant.py)
# ---------------------------------------------------------------------------

_PRIMITIVE = 0
_SHORT_STR = 1
_OBJECT = 2
_ARRAY = 3

_NULL_TYPE_ID = 0
_TRUE_TYPE_ID = 1
_FALSE_TYPE_ID = 2

_U8_MAX = 255
_U32_SIZE = 4
_VERSION = 1
_VERSION_MASK = 0x0F

_NULL_VALUE_BYTES: bytes = bytes([((_NULL_TYPE_ID << 2) | _PRIMITIVE)])


# ---------------------------------------------------------------------------
# Low-level binary helpers
# ---------------------------------------------------------------------------

def _read_unsigned(data: bytes, pos: int, n: int) -> int:
    return int.from_bytes(data[pos:pos + n], 'little', signed=False)


def _get_int_size(value: int) -> int:
    if value <= 0xFF:
        return 1
    if value <= 0xFFFF:
        return 2
    if value <= 0xFFFFFF:
        return 3
    return 4


def _append_le(buf: bytearray, value: int, n: int) -> None:
    buf.extend(value.to_bytes(n, 'little'))


def _primitive_header(type_id: int) -> int:
    return (type_id << 2) | _PRIMITIVE


def _object_header(large_size: bool, id_size: int, offset_size: int) -> int:
    return (
        ((1 if large_size else 0) << 6)
        | ((id_size - 1) << 4)
        | ((offset_size - 1) << 2)
        | _OBJECT
    )


def _array_header(large_size: bool, offset_size: int) -> int:
    return (
        ((1 if large_size else 0) << 4)
        | ((offset_size - 1) << 2)
        | _ARRAY
    )


# ---------------------------------------------------------------------------
# Metadata parsing
# ---------------------------------------------------------------------------

def parse_metadata_dict(metadata: bytes) -> Dict[str, int]:
    """Parse variant metadata bytes into a ``{key_name: key_id}`` mapping.

    The top-level metadata is shared across all shredded sub-fields.  We parse
    it once and pass the dict down to ``rebuild_value()`` so every recursive
    call can look up key IDs without re-parsing.
    """
    if not metadata or len(metadata) < 1:
        return {}
    if (metadata[0] & _VERSION_MASK) != _VERSION:
        raise ValueError('MALFORMED_VARIANT: invalid metadata version')
    offset_size = ((metadata[0] >> 6) & 0x3) + 1
    if len(metadata) < 1 + offset_size:
        return {}
    dict_size = _read_unsigned(metadata, 1, offset_size)
    if dict_size == 0:
        return {}
    string_start = 1 + (dict_size + 2) * offset_size
    result: Dict[str, int] = {}
    for key_id in range(dict_size):
        off = _read_unsigned(metadata, 1 + (key_id + 1) * offset_size, offset_size)
        next_off = _read_unsigned(metadata, 1 + (key_id + 2) * offset_size, offset_size)
        key = metadata[string_start + off:string_start + next_off].decode('utf-8')
        result[key] = key_id
    return result


# ---------------------------------------------------------------------------
# VariantSchema / build_variant_schema
# ---------------------------------------------------------------------------

@dataclass
class ObjectField:
    """One shredded field inside a ``typed_value`` object group."""
    field_name: str         # exact variant key name, no "field_" prefix
    schema: 'VariantSchema'


@dataclass
class VariantSchema:
    """Describes the shredding layout of a VARIANT column or sub-column.

    Mirrors the Java ``VariantSchema`` class.  Indices are positions within
    the PyArrow struct type that was parsed via ``build_variant_schema()``.

    For a plain (un-shredded) VARIANT:
      ``metadata_idx >= 0``, ``value_idx >= 0``, ``typed_idx < 0``.
    """
    typed_idx: int = -1
    value_idx: int = -1
    metadata_idx: int = -1
    num_fields: int = 0
    scalar_arrow_type: Optional[pa.DataType] = None
    object_fields: Optional[List[ObjectField]] = None
    object_schema_map: Optional[Dict[str, int]] = None
    array_schema: Optional['VariantSchema'] = None

    def is_unshredded(self) -> bool:
        """Return True if this is a plain (non-shredded) VARIANT layout."""
        return self.metadata_idx >= 0 and self.typed_idx < 0


def is_shredded_variant(pa_type: pa.DataType) -> bool:
    """Return True if *pa_type* is a shredded Parquet VARIANT struct.

    A shredded VARIANT column has three top-level fields: ``metadata``,
    ``value`` (overflow), and ``typed_value`` (shredded sub-columns).
    """
    if not pa.types.is_struct(pa_type):
        return False
    names = {pa_type[i].name for i in range(pa_type.num_fields)}
    return 'metadata' in names and 'value' in names and 'typed_value' in names


def build_variant_schema(pa_type: pa.StructType) -> VariantSchema:
    """Parse a PyArrow struct type into a ``VariantSchema`` tree.

    Works for both the top-level VARIANT column struct (which has ``metadata``
    in addition to ``value`` / ``typed_value``) and sub-field structs (which
    only have ``value`` / ``typed_value``).
    """
    schema = VariantSchema(num_fields=pa_type.num_fields)
    for i in range(pa_type.num_fields):
        f = pa_type[i]
        if f.name == 'metadata':
            schema.metadata_idx = i
        elif f.name == 'value':
            schema.value_idx = i
        elif f.name == 'typed_value':
            schema.typed_idx = i
            schema = _parse_typed_value_field(schema, f.type)
    return schema


def _parse_typed_value_field(schema: VariantSchema, tv_type: pa.DataType) -> VariantSchema:
    """Fill in the shredding details for a ``typed_value`` field."""
    if pa.types.is_struct(tv_type):
        object_fields: List[ObjectField] = []
        for j in range(tv_type.num_fields):
            sub_f = tv_type.field(j)
            if pa.types.is_struct(sub_f.type):
                sub_schema = build_variant_schema(sub_f.type)
            else:
                # Scalar typed_value embedded directly (no surrounding struct)
                sub_schema = VariantSchema(
                    typed_idx=0, num_fields=1, scalar_arrow_type=sub_f.type
                )
            object_fields.append(ObjectField(sub_f.name, sub_schema))
        schema.object_fields = object_fields
        schema.object_schema_map = {
            of.field_name: idx for idx, of in enumerate(object_fields)
        }
    elif pa.types.is_list(tv_type) or pa.types.is_large_list(tv_type):
        elem_type = tv_type.value_type
        if pa.types.is_struct(elem_type):
            schema.array_schema = build_variant_schema(elem_type)
        else:
            schema.array_schema = VariantSchema(
                typed_idx=0, num_fields=1, scalar_arrow_type=elem_type
            )
    else:
        schema.scalar_arrow_type = tv_type
    return schema


# ---------------------------------------------------------------------------
# Scalar encoding: Arrow typed value → variant binary bytes
# ---------------------------------------------------------------------------

def _encode_scalar_to_value_bytes(typed_value, arrow_type: pa.DataType) -> bytes:
    """Encode a typed Python scalar (from PyArrow .as_py()) to variant value bytes."""
    from pypaimon.data.generic_variant import _GenericVariantBuilder  # local import avoids circular
    builder = _GenericVariantBuilder()
    _append_scalar(builder, typed_value, arrow_type)
    gv = builder.result()
    # _pos == 0 so value() returns all bytes
    return gv.value()


def _append_scalar(builder, value, arrow_type: pa.DataType) -> None:
    """Dispatch a typed Python scalar into the appropriate builder method."""
    if value is None:
        builder.append_null()
        return

    if pa.types.is_boolean(arrow_type):
        builder.append_boolean(bool(value))
    elif pa.types.is_integer(arrow_type):
        builder.append_long(int(value))
    elif pa.types.is_float64(arrow_type):
        builder.append_double(float(value))
    elif pa.types.is_float32(arrow_type):
        builder.append_float(float(value))
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        builder.append_string(str(value))
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        builder.append_binary(bytes(value))
    elif pa.types.is_date32(arrow_type):
        # PyArrow converts date32 to datetime.date
        if isinstance(value, datetime.date):
            days = (value - datetime.date(1970, 1, 1)).days
        else:
            days = int(value)
        builder.append_date(days)
    elif pa.types.is_timestamp(arrow_type):
        # PyArrow converts timestamp to datetime.datetime
        if isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                micros = int((value - epoch).total_seconds() * 1_000_000)
                builder.append_timestamp(micros)
            else:
                epoch = datetime.datetime(1970, 1, 1)
                micros = int((value - epoch).total_seconds() * 1_000_000)
                builder.append_timestamp_ntz(micros)
        else:
            builder.append_timestamp_ntz(int(value))
    elif pa.types.is_decimal(arrow_type):
        if isinstance(value, _decimal.Decimal):
            builder.append_decimal(value)
        else:
            builder.append_decimal(_decimal.Decimal(str(value)))
    else:
        # Fallback: encode as string
        builder.append_string(str(value))


# ---------------------------------------------------------------------------
# Object / array binary construction
# ---------------------------------------------------------------------------

def _build_object_value(fields: List[Tuple[int, bytes]]) -> bytes:
    """Build object variant value bytes from ``(key_id, value_bytes)`` pairs.

    The variant spec requires fields sorted by key_id.
    """
    if not fields:
        # Empty object: header + size=0 + one zero-offset sentinel
        buf = bytearray()
        buf.append(_object_header(False, 1, 1))
        buf.append(0)       # size = 0
        buf.append(0)       # offset[0] = 0 (sentinel)
        return bytes(buf)

    fields = sorted(fields, key=lambda f: f[0])
    size = len(fields)
    data = b''.join(vb for _, vb in fields)
    data_size = len(data)

    large_size = size > _U8_MAX
    size_bytes = _U32_SIZE if large_size else 1
    max_id = max(kid for kid, _ in fields)
    id_size = _get_int_size(max_id)
    offset_size = _get_int_size(data_size) if data_size > 0 else 1

    buf = bytearray()
    buf.append(_object_header(large_size, id_size, offset_size))
    _append_le(buf, size, size_bytes)

    for kid, _ in fields:
        _append_le(buf, kid, id_size)

    offset = 0
    for _, vb in fields:
        _append_le(buf, offset, offset_size)
        offset += len(vb)
    _append_le(buf, offset, offset_size)   # sentinel = total data size

    buf.extend(data)
    return bytes(buf)


def _build_array_value(element_bytes_list: List[bytes]) -> bytes:
    """Build array variant value bytes from per-element value bytes."""
    size = len(element_bytes_list)
    data = b''.join(element_bytes_list)
    data_size = len(data)

    large_size = size > _U8_MAX
    size_bytes = _U32_SIZE if large_size else 1
    offset_size = _get_int_size(data_size) if data_size > 0 else 1

    buf = bytearray()
    buf.append(_array_header(large_size, offset_size))
    _append_le(buf, size, size_bytes)

    offset = 0
    for eb in element_bytes_list:
        _append_le(buf, offset, offset_size)
        offset += len(eb)
    _append_le(buf, offset, offset_size)   # sentinel

    buf.extend(data)
    return bytes(buf)


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


# ---------------------------------------------------------------------------
# Core rebuild algorithm (mirrors ShreddingUtils.rebuild() in Java)
# ---------------------------------------------------------------------------

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

    return _build_object_value(fields)


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


# ---------------------------------------------------------------------------
# High-level column assembly
# ---------------------------------------------------------------------------

#: The canonical VARIANT Arrow type (struct<value: binary NOT NULL,
#: metadata: binary NOT NULL>).
VARIANT_ARROW_TYPE = pa.struct([
    pa.field('value', pa.binary(), nullable=False),
    pa.field('metadata', pa.binary(), nullable=False),
])


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


# ---------------------------------------------------------------------------
# Write-side shredding
# ---------------------------------------------------------------------------

def _paimon_type_str_to_arrow(type_str: str) -> pa.DataType:
    """Map a Paimon SQL type string to the Arrow type used in ``typed_value``."""
    from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
    try:
        return PyarrowFieldParser.from_paimon_type(AtomicType(type_str.upper()))
    except Exception:
        return pa.binary()


def _parse_sub_schema(type_def) -> VariantSchema:
    """Recursively parse a Paimon type definition (string or dict) into a VariantSchema.

    The resulting schema is for a sub-field struct, so ``value_idx=0``,
    ``typed_idx=1``, no ``metadata_idx``.
    """
    if isinstance(type_def, str):
        # Scalar leaf: e.g. "BIGINT", "VARCHAR", "DOUBLE"
        arrow_type = _paimon_type_str_to_arrow(type_def)
        return VariantSchema(
            value_idx=0,
            typed_idx=1,
            num_fields=2,
            scalar_arrow_type=arrow_type,
        )

    if not isinstance(type_def, dict):
        return VariantSchema(value_idx=0, num_fields=1)

    kind = type_def.get('type', '').upper()

    if kind == 'ROW':
        sub_fields_def = type_def.get('fields', [])
        object_fields: List[ObjectField] = []
        for f in sub_fields_def:
            fname = f.get('name', '')
            ftype = f.get('type', 'BINARY')
            sub_schema = _parse_sub_schema(ftype)
            object_fields.append(ObjectField(fname, sub_schema))
        schema = VariantSchema(
            value_idx=0,
            typed_idx=1,
            num_fields=2,
            object_fields=object_fields,
            object_schema_map={of.field_name: i for i, of in enumerate(object_fields)},
        )
        return schema

    if kind == 'ARRAY':
        elem_type = type_def.get('element', 'BINARY')
        elem_schema = _parse_sub_schema(elem_type)
        return VariantSchema(
            value_idx=0,
            typed_idx=1,
            num_fields=2,
            array_schema=elem_schema,
        )

    # Fallback: treat as scalar using the type string
    arrow_type = _paimon_type_str_to_arrow(kind)
    return VariantSchema(
        value_idx=0,
        typed_idx=1,
        num_fields=2,
        scalar_arrow_type=arrow_type,
    )


def parse_shredding_schema_option(json_str: str) -> Dict[str, List[ObjectField]]:
    """Parse the ``variant.shreddingSchema`` option value.

    Args:
        json_str: JSON-encoded Paimon ROW type where each top-level field
                  corresponds to a VARIANT column name, and its type is a ROW
                  listing the sub-fields to shred.

    Returns:
        ``{variant_col_name: [ObjectField, ...]}`` mapping.

    Raises:
        ValueError: if the JSON is invalid or the top-level type is not ROW.
    """
    import json as _json
    data = _json.loads(json_str)
    if data.get('type', '').upper() != 'ROW':
        raise ValueError(
            f"variant.shreddingSchema must be a JSON-encoded ROW type, got: {data.get('type')}"
        )

    result: Dict[str, List[ObjectField]] = {}
    for field_def in data.get('fields', []):
        col_name = field_def.get('name', '')
        col_type = field_def.get('type', {})

        if isinstance(col_type, dict) and col_type.get('type', '').upper() == 'ROW':
            sub_fields_def = col_type.get('fields', [])
            obj_fields: List[ObjectField] = []
            for sf in sub_fields_def:
                fname = sf.get('name', '')
                ftype = sf.get('type', 'BINARY')
                sub_schema = _parse_sub_schema(ftype)
                obj_fields.append(ObjectField(fname, sub_schema))
            result[col_name] = obj_fields
        else:
            # Top-level field type is not a ROW — skip (can't shred a non-object schema)
            pass

    return result


def _fid(field_id: int) -> dict:
    """Return PyArrow field metadata dict that sets the Parquet field ID.

    PyArrow respects the ``PARQUET:field_id`` key when writing Parquet, which
    ensures ``parquetType.getId()`` returns a non-null value on the Java reader
    side (``ParquetSchemaConverter.convertToPaimonField`` calls
    ``parquetType.getId().intValue()`` unconditionally).
    """
    return {b'PARQUET:field_id': str(field_id).encode()}


def _leaf_arrow_type_for_write(schema: VariantSchema) -> pa.DataType:
    """Return the Arrow type for the ``typed_value`` leaf of a sub-field struct.

    Used by ``sub_field_output_type``; field IDs are NOT embedded here since the
    result describes output column types, not Parquet-serialised fields.
    """
    if schema.scalar_arrow_type is not None:
        return schema.scalar_arrow_type
    if schema.object_fields is not None:
        return pa.struct([
            pa.field(of.field_name, pa.struct([
                pa.field('value', pa.binary(), nullable=True),
                pa.field('typed_value', _leaf_arrow_type_for_write(of.schema), nullable=True),
            ]), nullable=True)
            for of in schema.object_fields
        ])
    return pa.binary()


def _leaf_arrow_type_for_write_with_ids(schema: VariantSchema) -> pa.DataType:
    """Like ``_leaf_arrow_type_for_write`` but embeds ``PARQUET:field_id`` metadata.

    Used by ``shredding_schema_to_arrow_type`` so that Parquet field IDs are
    present in every nested field of the written file.
    """
    if schema.scalar_arrow_type is not None:
        return schema.scalar_arrow_type
    if schema.object_fields is not None:
        inner_fields = []
        for i, of in enumerate(schema.object_fields):
            inner_sub = pa.struct([
                pa.field('value', pa.binary(), nullable=True, metadata=_fid(0)),
                pa.field(
                    'typed_value',
                    _leaf_arrow_type_for_write_with_ids(of.schema),
                    nullable=True,
                    metadata=_fid(1),
                ),
            ])
            inner_fields.append(
                pa.field(of.field_name, inner_sub, nullable=False, metadata=_fid(i + 1))
            )
        return pa.struct(inner_fields)
    return pa.binary()


def shredding_schema_to_arrow_type(obj_fields: List[ObjectField]) -> pa.StructType:
    """Convert an ``[ObjectField]`` list into the PyArrow struct type for a shredded column.

    The produced type is the canonical Parquet shredding layout::

        struct<
            metadata: binary NOT NULL,   (field_id=0)
            value:    binary,             (field_id=1)
            typed_value: struct<          (field_id=2)
                field_a: struct<          (field_id=1, NOT NULL)
                    value: binary,        (field_id=0)
                    typed_value: <type_a> (field_id=1)
                >,
                ...
            >
        >

    ``PARQUET:field_id`` metadata is embedded on every field so that the Java
    ``ParquetSchemaConverter.convertToPaimonField`` can call
    ``parquetType.getId().intValue()`` without a NullPointerException.

    Sub-field structs within ``typed_value`` are marked NOT NULL (``nullable=False``)
    to match the Java shredding schema where each named sub-field carries a
    ``.notNull()`` annotation.
    """
    sub_field_defs = []
    for i, of in enumerate(obj_fields):
        typed_val_type = _leaf_arrow_type_for_write_with_ids(of.schema)
        sub_struct = pa.struct([
            pa.field('value', pa.binary(), nullable=True, metadata=_fid(0)),
            pa.field('typed_value', typed_val_type, nullable=True, metadata=_fid(1)),
        ])
        # Java's variantShreddingSchema marks each named sub-field as .notNull()
        sub_field_defs.append(pa.field(of.field_name, sub_struct, nullable=False, metadata=_fid(i + 1)))

    return pa.struct([
        pa.field('metadata', pa.binary(), nullable=False, metadata=_fid(0)),
        pa.field('value', pa.binary(), nullable=True, metadata=_fid(1)),
        pa.field('typed_value', pa.struct(sub_field_defs), nullable=True, metadata=_fid(2)),
    ])


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
    from pypaimon.data.generic_variant import GenericVariant

    if schema.scalar_arrow_type is not None:
        try:
            py_val = GenericVariant(field_bytes, metadata).to_python()
        except Exception:
            return {'value': field_bytes, 'typed_value': None}
        return {'value': None, 'typed_value': py_val}

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

        overflow_bytes = _build_object_value(overflow_pairs) if overflow_pairs else None
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

    overflow_bytes = _build_object_value(overflow_pairs) if overflow_pairs else None
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
