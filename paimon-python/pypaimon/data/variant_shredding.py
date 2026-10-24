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

"""Shredded VARIANT schemas, metadata and shared binary value construction."""

import datetime
import decimal as _decimal
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pyarrow as pa
from pypaimon.data._variant_binary import (
    _U8_MAX,
    _U32_SIZE,
    _VERSION,
    _VERSION_MASK,
    _read_unsigned,
    _get_int_size,
    _object_header,
    _array_header,
)


def _append_le(buf: bytearray, value: int, n: int) -> None:
    buf.extend(value.to_bytes(n, 'little'))


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
            sub_f = tv_type[j]
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
    if pa.types.is_decimal(arrow_type) and arrow_type.scale < 0:
        raise ValueError('VARIANT decimal scale must be non-negative')
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
            if (arrow_type.unit == 'ns'
                    and getattr(value, 'nanosecond', 0) != 0):
                raise ValueError(
                    "VARIANT timestamps require microsecond-aligned values")
            if value.tzinfo is not None:
                epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                delta = value - epoch
                micros = (
                    (delta.days * 86400 + delta.seconds) * 1_000_000
                    + delta.microseconds
                )
                builder.append_timestamp(micros)
            else:
                epoch = datetime.datetime(1970, 1, 1)
                delta = value - epoch
                micros = (
                    (delta.days * 86400 + delta.seconds) * 1_000_000
                    + delta.microseconds
                )
                builder.append_timestamp_ntz(micros)
        else:
            raw_value = int(value)
            if arrow_type.unit == 'ns':
                if raw_value % 1000:
                    raise ValueError(
                        "VARIANT timestamps require microsecond-aligned values")
                micros = raw_value // 1000
            else:
                micros = raw_value * {'s': 1_000_000, 'ms': 1000, 'us': 1}[
                    arrow_type.unit]
            builder.append_timestamp_ntz(micros)
    elif pa.types.is_decimal(arrow_type):
        decimal = (
            value if isinstance(value, _decimal.Decimal)
            else _decimal.Decimal(str(value))
        )
        sign, digits, exponent = decimal.as_tuple()
        unscaled = int(''.join(str(digit) for digit in digits))
        if sign:
            unscaled = -unscaled
        shift = exponent + arrow_type.scale
        if shift < 0 and unscaled % (10 ** -shift):
            raise ValueError(
                f'{decimal} does not have Arrow scale {arrow_type.scale}')
        unscaled = (
            unscaled * (10 ** shift) if shift >= 0
            else unscaled // (10 ** -shift)
        )
        scale = arrow_type.scale
        precision = max(1, len(str(abs(unscaled))))
        if precision > arrow_type.precision:
            raise ValueError(
                f'{decimal} exceeds Arrow precision {arrow_type.precision}')
        builder.append_decimal_unscaled(unscaled, precision, scale)
    else:
        # Fallback: encode as string
        builder.append_string(str(value))


def _build_object_value(
    fields: List[Tuple[int, bytes]],
    key_dict: Optional[Dict[str, int]] = None,
) -> bytes:
    """Build object variant value bytes from ``(key_id, value_bytes)`` pairs.

    Variant object fields are ordered by key name, not metadata key ID. The
    metadata dictionary may assign IDs in a different order.
    """
    if not fields:
        # Empty object: header + size=0 + one zero-offset sentinel
        buf = bytearray()
        buf.append(_object_header(False, 1, 1))
        buf.append(0)       # size = 0
        buf.append(0)       # offset[0] = 0 (sentinel)
        return bytes(buf)

    if key_dict is None:
        fields = sorted(fields, key=lambda f: f[0])
    else:
        id_to_name = {key_id: name for name, key_id in key_dict.items()}
        fields = sorted(
            fields,
            key=lambda f: id_to_name[f[0]].encode('utf-8'),
        )
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


VARIANT_ARROW_TYPE = pa.struct([
    pa.field('value', pa.binary(), nullable=False),
    pa.field('metadata', pa.binary(), nullable=False),
])


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
