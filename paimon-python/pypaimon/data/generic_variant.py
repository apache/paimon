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

"""Python implementation of Paimon GenericVariant — a thin storage container.

Paimon's VARIANT type is stored as two byte arrays:
    value    – encodes the structure and leaf values (Parquet Variant binary spec)
    metadata – key dictionary for object field names

pypaimon exposes VARIANT columns as Arrow struct<value: binary NOT NULL,
metadata: binary NOT NULL>.  This class is a convenience wrapper that lets
Python code construct (for writing) or inspect (for debugging) those bytes.

Variant semantics such as path extraction, type casting, and JSONPath queries
are the responsibility of the compute engine or application layer, not of
pypaimon (the storage layer).

Primary entry points:
    GenericVariant.from_json(json_str)     – build from a JSON string (for writing)
    GenericVariant.from_python(obj)        – build from a Python object (for writing)
    GenericVariant(value, metadata)        – wrap raw bytes read from a VARIANT column
    GenericVariant.from_arrow_struct(d)    – wrap a row dict from a VARIANT Arrow column
    GenericVariant.to_arrow_array(variants)– convert a list to a PyArrow StructArray

Inspection helpers (for debugging/testing):
    v.to_json()   – decode back to a JSON string
    v.to_python() – decode to native Python objects
    v.value()     – raw value bytes
    v.metadata()  – raw metadata bytes
"""

import base64
import datetime
import decimal as _decimal
import enum
import json as _json
import struct

# ---------------------------------------------------------------------------
# Constants (matching GenericVariantUtil.java)
# ---------------------------------------------------------------------------

_PRIMITIVE = 0
_SHORT_STR = 1
_OBJECT = 2
_ARRAY = 3

_NULL = 0
_TRUE = 1
_FALSE = 2
_INT1 = 3
_INT2 = 4
_INT4 = 5
_INT8 = 6
_DOUBLE = 7
_DECIMAL4 = 8
_DECIMAL8 = 9
_DECIMAL16 = 10
_DATE = 11
_TIMESTAMP = 12
_TIMESTAMP_NTZ = 13
_FLOAT = 14
_BINARY = 15
_LONG_STR = 16
_UUID = 20

_VERSION = 1
_VERSION_MASK = 0x0F
_SIZE_LIMIT = 128 * 1024 * 1024
_MAX_SHORT_STR_SIZE = 0x3F   # 63
_U8_MAX = 255
_U16_MAX = 65535
_U24_MAX = 16777215
_U32_SIZE = 4
_MAX_DECIMAL4_PRECISION = 9
_MAX_DECIMAL8_PRECISION = 18
_MAX_DECIMAL16_PRECISION = 38

# Epoch for date/timestamp conversions (used by to_json / to_python)
_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DT_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_DT_NTZ = datetime.datetime(1970, 1, 1)


class _Type(enum.Enum):
    """Internal high-level variant value types (many-to-one from wire types)."""
    OBJECT = 'OBJECT'
    ARRAY = 'ARRAY'
    NULL = 'NULL'
    BOOLEAN = 'BOOLEAN'
    LONG = 'LONG'
    STRING = 'STRING'
    DOUBLE = 'DOUBLE'
    DECIMAL = 'DECIMAL'
    DATE = 'DATE'
    TIMESTAMP = 'TIMESTAMP'
    TIMESTAMP_NTZ = 'TIMESTAMP_NTZ'
    FLOAT = 'FLOAT'
    BINARY = 'BINARY'
    UUID = 'UUID'


_PRIMITIVE_TYPE_MAP = {
    _NULL: _Type.NULL,
    _TRUE: _Type.BOOLEAN, _FALSE: _Type.BOOLEAN,
    _INT1: _Type.LONG, _INT2: _Type.LONG, _INT4: _Type.LONG, _INT8: _Type.LONG,
    _DOUBLE: _Type.DOUBLE,
    _DECIMAL4: _Type.DECIMAL, _DECIMAL8: _Type.DECIMAL, _DECIMAL16: _Type.DECIMAL,
    _DATE: _Type.DATE,
    _TIMESTAMP: _Type.TIMESTAMP,
    _TIMESTAMP_NTZ: _Type.TIMESTAMP_NTZ,
    _FLOAT: _Type.FLOAT,
    _BINARY: _Type.BINARY,
    _LONG_STR: _Type.STRING,
    _UUID: _Type.UUID,
}
_PRIMITIVE_FIXED_SIZES = {
    _NULL: 1, _TRUE: 1, _FALSE: 1,
    _INT1: 2, _INT2: 3, _INT4: 5, _INT8: 9,
    _DOUBLE: 9, _FLOAT: 5, _DATE: 5,
    _TIMESTAMP: 9, _TIMESTAMP_NTZ: 9,
    _DECIMAL4: 6, _DECIMAL8: 10, _DECIMAL16: 18,
    _UUID: 17,
}
_LONG_FAMILY_SIZES = {
    _INT1: 1, _INT2: 2, _INT4: 4, _INT8: 8,
    _DATE: 4, _TIMESTAMP: 8, _TIMESTAMP_NTZ: 8,
}


# ---------------------------------------------------------------------------
# Low-level binary utilities
# ---------------------------------------------------------------------------

def _read_unsigned(data, pos, n):
    return int.from_bytes(data[pos:pos + n], 'little', signed=False)


def _read_signed(data, pos, n):
    return int.from_bytes(data[pos:pos + n], 'little', signed=True)


def _write_le(buf, pos, value, n):
    buf[pos:pos + n] = value.to_bytes(n, 'little')


def _get_int_size(value):
    if value <= _U8_MAX:
        return 1
    if value <= _U16_MAX:
        return 2
    if value <= _U24_MAX:
        return 3
    return 4


def _primitive_header(type_id):
    return (type_id << 2) | _PRIMITIVE


def _short_str_header(size):
    return (size << 2) | _SHORT_STR


def _object_header(large_size, id_size, offset_size):
    return (
        ((1 if large_size else 0) << 6)
        | ((id_size - 1) << 4)
        | ((offset_size - 1) << 2)
        | _OBJECT
    )


def _array_header(large_size, offset_size):
    return (
        ((1 if large_size else 0) << 4)
        | ((offset_size - 1) << 2)
        | _ARRAY
    )


def _variant_get_type(value, pos):
    b = value[pos]
    basic_type = b & 0x3
    type_info = (b >> 2) & 0x3F
    if basic_type == _SHORT_STR:
        return _Type.STRING
    if basic_type == _OBJECT:
        return _Type.OBJECT
    if basic_type == _ARRAY:
        return _Type.ARRAY
    t = _PRIMITIVE_TYPE_MAP.get(type_info)
    if t is None:
        raise ValueError(f'Unknown primitive variant type id: {type_info}')
    return t


def _value_size(value, pos):
    b = value[pos]
    basic_type = b & 0x3
    type_info = (b >> 2) & 0x3F
    if basic_type == _SHORT_STR:
        return 1 + type_info
    if basic_type == _OBJECT:
        return _handle_object(
            value, pos,
            lambda size, id_size, offset_size, id_start, offset_start, data_start: (
                data_start - pos + _read_unsigned(
                    value, offset_start + size * offset_size, offset_size)
            )
        )
    if basic_type == _ARRAY:
        return _handle_array(
            value, pos,
            lambda size, offset_size, offset_start, data_start: (
                data_start - pos + _read_unsigned(
                    value, offset_start + size * offset_size, offset_size)
            )
        )
    size = _PRIMITIVE_FIXED_SIZES.get(type_info)
    if size is not None:
        return size
    if type_info in (_BINARY, _LONG_STR):
        return 1 + _U32_SIZE + _read_unsigned(value, pos + 1, _U32_SIZE)
    raise ValueError(f'Unknown primitive type id: {type_info}')


def _handle_object(value, pos, handler):
    b = value[pos]
    type_info = (b >> 2) & 0x3F
    large_size = bool((type_info >> 4) & 0x1)
    size_bytes = _U32_SIZE if large_size else 1
    size = _read_unsigned(value, pos + 1, size_bytes)
    id_size = ((type_info >> 2) & 0x3) + 1
    offset_size = (type_info & 0x3) + 1
    id_start = pos + 1 + size_bytes
    offset_start = id_start + size * id_size
    data_start = offset_start + (size + 1) * offset_size
    return handler(size, id_size, offset_size, id_start, offset_start, data_start)


def _handle_array(value, pos, handler):
    b = value[pos]
    type_info = (b >> 2) & 0x3F
    large_size = bool((type_info >> 2) & 0x1)
    size_bytes = _U32_SIZE if large_size else 1
    size = _read_unsigned(value, pos + 1, size_bytes)
    offset_size = (type_info & 0x3) + 1
    offset_start = pos + 1 + size_bytes
    data_start = offset_start + (size + 1) * offset_size
    return handler(size, offset_size, offset_start, data_start)


def _get_metadata_key(metadata, key_id):
    offset_size = ((metadata[0] >> 6) & 0x3) + 1
    dict_size = _read_unsigned(metadata, 1, offset_size)
    if key_id >= dict_size:
        raise ValueError('MALFORMED_VARIANT: key id out of range')
    string_start = 1 + (dict_size + 2) * offset_size
    offset = _read_unsigned(metadata, 1 + (key_id + 1) * offset_size, offset_size)
    next_offset = _read_unsigned(metadata, 1 + (key_id + 2) * offset_size, offset_size)
    return metadata[string_start + offset:string_start + next_offset].decode('utf-8')


# ---------------------------------------------------------------------------
# _GenericVariantBuilder (for from_json / from_python)
# ---------------------------------------------------------------------------

class _GenericVariantBuilder:
    """Builds a GenericVariant from Python values or JSON strings."""

    def __init__(self):
        self._buf = bytearray(128)
        self._pos = 0
        self._dict = {}
        self._keys = []

    def _get_or_add_key(self, key):
        if key not in self._dict:
            kid = len(self._keys)
            self._dict[key] = kid
            self._keys.append(key.encode('utf-8'))
        return self._dict[key]

    def _ensure(self, n):
        needed = self._pos + n
        if needed > len(self._buf):
            new_cap = max(needed, len(self._buf) * 2)
            new_buf = bytearray(new_cap)
            new_buf[:self._pos] = self._buf[:self._pos]
            self._buf = new_buf

    def _write_byte(self, b):
        self._ensure(1)
        self._buf[self._pos] = b & 0xFF
        self._pos += 1

    def _write_le(self, value, n):
        self._ensure(n)
        _write_le(self._buf, self._pos, value, n)
        self._pos += n

    def append_null(self):
        self._write_byte(_primitive_header(_NULL))

    def append_boolean(self, b):
        self._write_byte(_primitive_header(_TRUE if b else _FALSE))

    def append_long(self, n):
        if -(1 << 7) <= n < (1 << 7):
            self._write_byte(_primitive_header(_INT1))
            self._write_le(n & 0xFF, 1)
        elif -(1 << 15) <= n < (1 << 15):
            self._write_byte(_primitive_header(_INT2))
            self._write_le(n & 0xFFFF, 2)
        elif -(1 << 31) <= n < (1 << 31):
            self._write_byte(_primitive_header(_INT4))
            self._write_le(n & 0xFFFFFFFF, 4)
        else:
            self._write_byte(_primitive_header(_INT8))
            self._write_le(n & 0xFFFFFFFFFFFFFFFF, 8)

    def append_double(self, d):
        self._write_byte(_primitive_header(_DOUBLE))
        self._ensure(8)
        struct.pack_into('<d', self._buf, self._pos, d)
        self._pos += 8

    def append_float(self, f):
        self._write_byte(_primitive_header(_FLOAT))
        self._ensure(4)
        struct.pack_into('<f', self._buf, self._pos, f)
        self._pos += 4

    def append_decimal(self, d):
        d = d.normalize()
        sign, digits, exponent = d.as_tuple()
        if exponent > 0:
            raise ValueError(
                f'append_decimal requires a non-positive exponent (got {d!r}); '
                'use append_double() for Decimal values with positive exponents'
            )
        unscaled = int(''.join(str(x) for x in digits))
        if sign:
            unscaled = -unscaled
        scale = -exponent if exponent < 0 else 0
        precision = len(digits)

        if scale <= _MAX_DECIMAL4_PRECISION and precision <= _MAX_DECIMAL4_PRECISION:
            self._write_byte(_primitive_header(_DECIMAL4))
            self._write_byte(scale)
            self._write_le(unscaled & 0xFFFFFFFF, 4)
        elif scale <= _MAX_DECIMAL8_PRECISION and precision <= _MAX_DECIMAL8_PRECISION:
            self._write_byte(_primitive_header(_DECIMAL8))
            self._write_byte(scale)
            self._write_le(unscaled & 0xFFFFFFFFFFFFFFFF, 8)
        else:
            self._write_byte(_primitive_header(_DECIMAL16))
            self._write_byte(scale)
            self._ensure(16)
            raw = unscaled.to_bytes(16, 'little', signed=True)
            self._buf[self._pos:self._pos + 16] = raw
            self._pos += 16

    def append_string(self, s):
        text = s.encode('utf-8')
        if len(text) <= _MAX_SHORT_STR_SIZE:
            self._write_byte(_short_str_header(len(text)))
        else:
            self._write_byte(_primitive_header(_LONG_STR))
            self._write_le(len(text), _U32_SIZE)
        self._ensure(len(text))
        self._buf[self._pos:self._pos + len(text)] = text
        self._pos += len(text)

    def append_binary(self, b):
        self._write_byte(_primitive_header(_BINARY))
        self._write_le(len(b), _U32_SIZE)
        self._ensure(len(b))
        self._buf[self._pos:self._pos + len(b)] = b
        self._pos += len(b)

    def append_date(self, days_since_epoch):
        self._write_byte(_primitive_header(_DATE))
        self._write_le(days_since_epoch & 0xFFFFFFFF, 4)

    def append_timestamp(self, micros_since_epoch):
        self._write_byte(_primitive_header(_TIMESTAMP))
        self._write_le(micros_since_epoch & 0xFFFFFFFFFFFFFFFF, 8)

    def append_timestamp_ntz(self, micros_since_epoch):
        self._write_byte(_primitive_header(_TIMESTAMP_NTZ))
        self._write_le(micros_since_epoch & 0xFFFFFFFFFFFFFFFF, 8)

    def _finish_writing_object(self, start, fields):
        fields.sort(key=lambda f: f[0])
        for i in range(1, len(fields)):
            if fields[i][0] == fields[i - 1][0]:
                raise ValueError('Duplicate key in variant object')

        size = len(fields)
        data_size = self._pos - start
        large_size = size > _U8_MAX
        size_bytes = _U32_SIZE if large_size else 1
        max_id = max((f[1] for f in fields), default=0)
        id_size = _get_int_size(max_id)
        offset_size = _get_int_size(data_size)
        header_size = 1 + size_bytes + size * id_size + (size + 1) * offset_size

        self._ensure(header_size)
        dst = start + header_size
        src = start
        self._buf[dst:dst + data_size] = self._buf[src:src + data_size]
        self._pos += header_size

        self._buf[start] = _object_header(large_size, id_size, offset_size)
        _write_le(self._buf, start + 1, size, size_bytes)
        id_start = start + 1 + size_bytes
        offset_start = id_start + size * id_size
        for i, (_, fid, offset) in enumerate(fields):
            _write_le(self._buf, id_start + i * id_size, fid, id_size)
            _write_le(self._buf, offset_start + i * offset_size, offset, offset_size)
        _write_le(self._buf, offset_start + size * offset_size, data_size, offset_size)

    def _finish_writing_array(self, start, offsets):
        size = len(offsets)
        data_size = self._pos - start
        large_size = size > _U8_MAX
        size_bytes = _U32_SIZE if large_size else 1
        offset_size = _get_int_size(data_size)
        header_size = 1 + size_bytes + (size + 1) * offset_size

        self._ensure(header_size)
        dst = start + header_size
        self._buf[dst:dst + data_size] = self._buf[start:start + data_size]
        self._pos += header_size

        self._buf[start] = _array_header(large_size, offset_size)
        _write_le(self._buf, start + 1, size, size_bytes)
        offset_start = start + 1 + size_bytes
        for i, off in enumerate(offsets):
            _write_le(self._buf, offset_start + i * offset_size, off, offset_size)
        _write_le(self._buf, offset_start + size * offset_size, data_size, offset_size)

    def build_python(self, obj):
        """Recursively encode a Python value into the variant binary buffer."""
        if obj is None:
            self.append_null()
        elif isinstance(obj, bool):
            self.append_boolean(obj)
        elif isinstance(obj, int):
            self.append_long(obj)
        elif isinstance(obj, float):
            self.append_double(obj)
        elif isinstance(obj, _decimal.Decimal):
            self._try_decimal_or_double(obj)
        elif isinstance(obj, str):
            self.append_string(obj)
        elif isinstance(obj, dict):
            fields = []
            start = self._pos
            for key, val in obj.items():
                fid = self._get_or_add_key(key)
                offset = self._pos - start
                fields.append((key, fid, offset))
                self.build_python(val)
            self._finish_writing_object(start, fields)
        elif isinstance(obj, (list, tuple)):
            elem_offsets = []
            start = self._pos
            for val in obj:
                elem_offsets.append(self._pos - start)
                self.build_python(val)
            self._finish_writing_array(start, elem_offsets)
        elif isinstance(obj, bytes):
            self.append_binary(obj)
        else:
            raise TypeError(f'Unsupported Python type for variant encoding: {type(obj).__name__}')

    def _try_decimal_or_double(self, d):
        try:
            sign, digits, exponent = d.as_tuple()
            if exponent > 0:
                self.append_double(float(d))
                return
            scale = -exponent if exponent < 0 else 0
            precision = len(digits)
            if scale <= _MAX_DECIMAL16_PRECISION and precision <= _MAX_DECIMAL16_PRECISION:
                self.append_decimal(d)
                return
        except (ArithmeticError, ValueError):
            pass
        self.append_double(float(d))

    def result(self):
        """Build metadata and return the completed GenericVariant."""
        n_keys = len(self._keys)
        total_str_size = sum(len(k) for k in self._keys)
        max_size = max(total_str_size, n_keys, 0)
        offset_size = _get_int_size(max_size) if max_size > 0 else 1

        offset_start = 1 + offset_size
        string_start = offset_start + (n_keys + 1) * offset_size
        metadata_size = string_start + total_str_size

        metadata = bytearray(metadata_size)
        metadata[0] = _VERSION | ((offset_size - 1) << 6)
        _write_le(metadata, 1, n_keys, offset_size)

        current_offset = 0
        for i, key_bytes in enumerate(self._keys):
            _write_le(metadata, offset_start + i * offset_size, current_offset, offset_size)
            klen = len(key_bytes)
            metadata[string_start + current_offset:string_start + current_offset + klen] = key_bytes
            current_offset += klen
        _write_le(metadata, offset_start + n_keys * offset_size, current_offset, offset_size)

        return GenericVariant(bytes(self._buf[:self._pos]), bytes(metadata))


# ---------------------------------------------------------------------------
# GenericVariant
# ---------------------------------------------------------------------------

class GenericVariant:
    """Storage container for a Paimon/Parquet VARIANT value.

    A VARIANT value is stored as two byte arrays:
        value    – encoded payload (Parquet Variant binary spec)
        metadata – key dictionary for object field names

    pypaimon exposes VARIANT columns as Arrow struct arrays with exactly these
    two fields.  This class helps Python code build or inspect those bytes.

    **Writing example**::

        import pyarrow as pa
        from pypaimon.data.generic_variant import GenericVariant

        gv1 = GenericVariant.from_json('{"age": 30, "city": "Beijing"}')
        gv2 = GenericVariant.from_json('[1, 2, 3]')
        # Create an Arrow StructArray ready for writing
        col = GenericVariant.to_arrow_array([gv1, gv2, None])
        table = pa.table({'id': [1, 2, 3], 'payload': col})

    **Reading example**::

        result = table_read.to_arrow(splits)
        # 'payload' column is struct<value: binary, metadata: binary>
        for row in result.column('payload').to_pylist():
            if row is not None:
                gv = GenericVariant.from_arrow_struct(row)
                print(gv.to_json())   # e.g. '{"age":30,"city":"Beijing"}'
                print(gv.to_python()) # e.g. {'age': 30, 'city': 'Beijing'}
    """

    __slots__ = ('_value', '_metadata', '_pos')

    def __init__(self, value: bytes, metadata: bytes, _pos: int = 0):
        self._value = bytes(value)
        self._metadata = bytes(metadata)
        self._pos = _pos
        if len(metadata) < 1 or (metadata[0] & _VERSION_MASK) != _VERSION:
            raise ValueError('MALFORMED_VARIANT: invalid metadata version')

    # -- constructors --

    @classmethod
    def from_json(cls, json_str: str) -> 'GenericVariant':
        """Parse a JSON string and encode it as VARIANT binary bytes.

        Use this when writing VARIANT data from Python::

            gv = GenericVariant.from_json('{"name": "Alice", "age": 30}')
            col = GenericVariant.to_arrow_array([gv])
        """
        obj = _json.loads(json_str, parse_float=_decimal.Decimal)
        builder = _GenericVariantBuilder()
        builder.build_python(obj)
        return builder.result()

    @classmethod
    def from_python(cls, obj) -> 'GenericVariant':
        """Encode a Python object (dict / list / str / int / float / bool / None) as VARIANT.

        Use this when writing VARIANT data from Python::

            gv = GenericVariant.from_python({'name': 'Alice', 'age': 30})
            col = GenericVariant.to_arrow_array([gv])
        """
        builder = _GenericVariantBuilder()
        builder.build_python(obj)
        return builder.result()

    @classmethod
    def from_arrow_struct(cls, d: dict) -> 'GenericVariant':
        """Wrap raw bytes from a PyArrow VARIANT struct: {'value': bytes, 'metadata': bytes}.

        Use this on the read path after reading a VARIANT column::

            for row in result.column("payload").to_pylist():
                if row is not None:
                    gv = GenericVariant.from_arrow_struct(row)
                    print(gv.to_json())
        """
        return cls(bytes(d['value']), bytes(d['metadata']))

    @classmethod
    def to_arrow_array(cls, variants):
        """Convert a list of GenericVariant (or None) to a PyArrow StructArray.

        The returned array has the canonical VARIANT layout::

            struct<value: binary NOT NULL, metadata: binary NOT NULL>

        Pass None in the list to represent a SQL NULL (absent VARIANT value).

        Example::

            gv1 = GenericVariant.from_json('{"age":30}')
            gv2 = GenericVariant.from_json('[1,2,3]')
            col = GenericVariant.to_arrow_array([gv1, gv2, None])
            table = pa.table({'id': [1, 2, 3], 'payload': col})
        """
        import pyarrow as _pa

        values = []
        metadatas = []
        mask = []
        for v in variants:
            if v is None:
                values.append(b'')
                metadatas.append(b'')
                mask.append(True)
            else:
                values.append(v.value())
                metadatas.append(v.metadata())
                mask.append(False)

        variant_type = _pa.struct([
            _pa.field('value', _pa.binary(), nullable=False),
            _pa.field('metadata', _pa.binary(), nullable=False),
        ])
        return _pa.StructArray.from_arrays(
            [_pa.array(values, type=_pa.binary()),
             _pa.array(metadatas, type=_pa.binary())],
            fields=[variant_type[0], variant_type[1]],
            mask=_pa.array(mask, type=_pa.bool_()),
        )

    # -- raw bytes --

    def value(self) -> bytes:
        """Return the value payload bytes."""
        if self._pos == 0:
            return self._value
        size = _value_size(self._value, self._pos)
        return self._value[self._pos:self._pos + size]

    def metadata(self) -> bytes:
        """Return the metadata (key-dictionary) bytes."""
        return self._metadata

    # -- inspection helpers (for debugging / testing) --

    def to_json(self) -> str:
        """Decode the variant to a JSON string.

        Useful for debugging and testing.  Variant semantics and path-based
        queries are the responsibility of the application layer.
        """
        parts = []
        self._to_json_impl(self._value, self._metadata, self._pos, parts)
        return ''.join(parts)

    def _to_json_impl(self, value, metadata, pos, parts):
        vtype = _variant_get_type(value, pos)
        if vtype == _Type.OBJECT:
            def _render(size, id_size, offset_size, id_start, offset_start, data_start):
                parts.append('{')
                for i in range(size):
                    fid = _read_unsigned(value, id_start + id_size * i, id_size)
                    key = _get_metadata_key(metadata, fid)
                    offset = _read_unsigned(
                        value, offset_start + offset_size * i, offset_size)
                    if i != 0:
                        parts.append(',')
                    parts.append(_json.dumps(key))
                    parts.append(':')
                    self._to_json_impl(value, metadata, data_start + offset, parts)
                parts.append('}')
            _handle_object(value, pos, _render)
        elif vtype == _Type.ARRAY:
            def _render_arr(size, offset_size, offset_start, data_start):
                parts.append('[')
                for i in range(size):
                    offset = _read_unsigned(
                        value, offset_start + offset_size * i, offset_size)
                    if i != 0:
                        parts.append(',')
                    self._to_json_impl(value, metadata, data_start + offset, parts)
                parts.append(']')
            _handle_array(value, pos, _render_arr)
        else:
            b = value[pos]
            basic_type = b & 0x3
            type_info = (b >> 2) & 0x3F
            if vtype == _Type.NULL:
                parts.append('null')
            elif vtype == _Type.BOOLEAN:
                parts.append('true' if type_info == _TRUE else 'false')
            elif vtype == _Type.LONG:
                n = _LONG_FAMILY_SIZES.get(type_info)
                parts.append(str(_read_signed(value, pos + 1, n)))
            elif vtype == _Type.STRING:
                if basic_type == _SHORT_STR:
                    s = value[pos + 1:pos + 1 + type_info].decode('utf-8')
                else:
                    length = _read_unsigned(value, pos + 1, _U32_SIZE)
                    s = value[pos + 1 + _U32_SIZE:pos + 1 + _U32_SIZE + length].decode('utf-8')
                parts.append(_json.dumps(s))
            elif vtype == _Type.DOUBLE:
                d = struct.unpack_from('<d', value, pos + 1)[0]
                parts.append(_json.dumps(d) if d == d and d not in (float('inf'), float('-inf'))
                             else _json.dumps(str(d)))
            elif vtype == _Type.FLOAT:
                f = struct.unpack_from('<f', value, pos + 1)[0]
                parts.append(_json.dumps(float(f)) if f == f and f not in (float('inf'), float('-inf'))
                             else _json.dumps(str(f)))
            elif vtype == _Type.DECIMAL:
                scale = value[pos + 1] & 0xFF
                if type_info == _DECIMAL4:
                    unscaled = _read_signed(value, pos + 2, 4)
                elif type_info == _DECIMAL8:
                    unscaled = _read_signed(value, pos + 2, 8)
                else:
                    raw = bytes(value[pos + 2:pos + 18])
                    unscaled = int.from_bytes(raw, 'little', signed=True)
                d = _decimal.Decimal(unscaled) / (_decimal.Decimal(10) ** scale)
                parts.append(str(d.normalize()))
            elif vtype == _Type.DATE:
                days = _read_signed(value, pos + 1, 4)
                parts.append(_json.dumps(str(_EPOCH_DATE + datetime.timedelta(days=days))))
            elif vtype == _Type.TIMESTAMP:
                micros = _read_signed(value, pos + 1, 8)
                dt = _EPOCH_DT_UTC + datetime.timedelta(microseconds=micros)
                parts.append(_json.dumps(dt.strftime('%Y-%m-%d %H:%M:%S.%f+00:00')))
            elif vtype == _Type.TIMESTAMP_NTZ:
                micros = _read_signed(value, pos + 1, 8)
                dt = _EPOCH_DT_NTZ + datetime.timedelta(microseconds=micros)
                parts.append(_json.dumps(dt.strftime('%Y-%m-%d %H:%M:%S.%f')))
            elif vtype == _Type.BINARY:
                length = _read_unsigned(value, pos + 1, _U32_SIZE)
                b_val = bytes(value[pos + 1 + _U32_SIZE:pos + 1 + _U32_SIZE + length])
                parts.append(_json.dumps(base64.b64encode(b_val).decode('ascii')))

    def to_python(self):
        """Decode the variant to native Python objects.

        Object  → dict
        Array   → list
        Boolean → bool
        Integer → int
        Double/Float → float
        Decimal → decimal.Decimal
        String  → str
        Date    → datetime.date
        Timestamp → datetime.datetime (UTC-aware)
        Timestamp_NTZ → datetime.datetime (naive)
        Binary  → bytes
        Null    → None

        Useful for debugging and testing.  Variant semantics and path-based
        queries are the responsibility of the application layer.
        """
        return self._to_python_impl(self._value, self._metadata, self._pos)

    def _to_python_impl(self, value, metadata, pos):
        vtype = _variant_get_type(value, pos)
        b = value[pos]
        basic_type = b & 0x3
        type_info = (b >> 2) & 0x3F

        if vtype == _Type.NULL:
            return None
        if vtype == _Type.BOOLEAN:
            return type_info == _TRUE
        if vtype == _Type.LONG:
            n = _LONG_FAMILY_SIZES.get(type_info)
            return _read_signed(value, pos + 1, n)
        if vtype == _Type.DOUBLE:
            return struct.unpack_from('<d', value, pos + 1)[0]
        if vtype == _Type.FLOAT:
            return float(struct.unpack_from('<f', value, pos + 1)[0])
        if vtype == _Type.DECIMAL:
            scale = value[pos + 1] & 0xFF
            if type_info == _DECIMAL4:
                unscaled = _read_signed(value, pos + 2, 4)
            elif type_info == _DECIMAL8:
                unscaled = _read_signed(value, pos + 2, 8)
            else:
                raw = bytes(value[pos + 2:pos + 18])
                unscaled = int.from_bytes(raw, 'little', signed=True)
            return _decimal.Decimal(unscaled) / (_decimal.Decimal(10) ** scale)
        if vtype == _Type.STRING:
            if basic_type == _SHORT_STR:
                return value[pos + 1:pos + 1 + type_info].decode('utf-8')
            length = _read_unsigned(value, pos + 1, _U32_SIZE)
            return value[pos + 1 + _U32_SIZE:pos + 1 + _U32_SIZE + length].decode('utf-8')
        if vtype == _Type.DATE:
            days = _read_signed(value, pos + 1, 4)
            return _EPOCH_DATE + datetime.timedelta(days=days)
        if vtype == _Type.TIMESTAMP:
            micros = _read_signed(value, pos + 1, 8)
            return _EPOCH_DT_UTC + datetime.timedelta(microseconds=micros)
        if vtype == _Type.TIMESTAMP_NTZ:
            micros = _read_signed(value, pos + 1, 8)
            return _EPOCH_DT_NTZ + datetime.timedelta(microseconds=micros)
        if vtype == _Type.BINARY:
            length = _read_unsigned(value, pos + 1, _U32_SIZE)
            return bytes(value[pos + 1 + _U32_SIZE:pos + 1 + _U32_SIZE + length])
        if vtype == _Type.OBJECT:
            def _build_dict(size, id_size, offset_size, id_start, offset_start, data_start):
                result = {}
                for i in range(size):
                    fid = _read_unsigned(value, id_start + id_size * i, id_size)
                    key = _get_metadata_key(metadata, fid)
                    offset = _read_unsigned(value, offset_start + offset_size * i, offset_size)
                    result[key] = self._to_python_impl(value, metadata, data_start + offset)
                return result
            return _handle_object(value, pos, _build_dict)
        if vtype == _Type.ARRAY:
            def _build_list(size, offset_size, offset_start, data_start):
                result = []
                for i in range(size):
                    offset = _read_unsigned(value, offset_start + offset_size * i, offset_size)
                    result.append(self._to_python_impl(value, metadata, data_start + offset))
                return result
            return _handle_array(value, pos, _build_list)
        return None

    # -- dunder --

    def __repr__(self) -> str:
        return f'GenericVariant({self.to_json()!r})'

    def __str__(self) -> str:
        return self.to_json()

    def __eq__(self, other) -> bool:
        if not isinstance(other, GenericVariant):
            return NotImplemented
        return self.value() == other.value() and self._metadata == other._metadata

    def __hash__(self):
        return hash((self.value(), self._metadata))
