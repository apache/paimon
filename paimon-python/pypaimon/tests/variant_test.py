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
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""Unit tests for VARIANT type support in pypaimon.

Covers the user-facing features described in the Python API documentation:

  - Plain VARIANT:    ordinary write (GenericVariant → Parquet) and read.
  - Shredded VARIANT: write-side decomposition (shred_variant_column) and
                      read-side transparent assembly (assemble_shredded_column).

Sections
--------
1.  Type-system layer        – schema parsing, Paimon ↔ Arrow type mapping.
2.  GenericVariant container – construction, inspection, JSON round-trip.
3.  Plain VARIANT I/O        – PyArrow-level Parquet write/read sanity check.
4.  Shredded schema utils    – is_shredded_variant, parse_metadata_dict,
                               build_variant_schema.
5.  Binary encoding helpers  – _encode_scalar_to_value_bytes, _build_object_value,
                               _build_array_value.
6.  Read-path assembly       – rebuild_value, rebuild, assemble_shredded_column.
7.  Write-path shredding     – parse_shredding_schema_option,
                               shredding_schema_to_arrow_type, decompose_variant.
8.  Full-chain via Paimon API – write then read through CatalogFactory / Schema /
                               write_arrow / to_arrow for both plain VARIANT and
                               shredded VARIANT.
"""

import io
import json
import os
import shutil
import struct as _struct
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.variant_shredding import (
    VARIANT_ARROW_TYPE,
    VariantSchema,
    _NULL_VALUE_BYTES,
    _build_array_value,
    _build_object_value,
    _encode_scalar_to_value_bytes,
    assemble_shredded_column,
    build_variant_schema,
    decompose_variant,
    is_shredded_variant,
    parse_metadata_dict,
    parse_shredding_schema_option,
    rebuild,
    rebuild_value,
    shredding_schema_to_arrow_type,
)
from pypaimon.schema.data_types import (
    AtomicType,
    DataField,
    DataTypeParser,
    PyarrowFieldParser,
    RowType,
    is_variant_struct,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _variant_arrow_type() -> pa.StructType:
    """The canonical Arrow representation of a plain VARIANT column."""
    return pa.struct([
        pa.field('value', pa.binary(), nullable=False),
        pa.field('metadata', pa.binary(), nullable=False),
    ])


def _make_metadata(*keys: str) -> bytes:
    """Build variant metadata bytes.  With no keys returns the minimal header."""
    if keys:
        return GenericVariant.from_python({k: None for k in keys}).metadata()
    return b'\x01\x00'  # version=1, dict_size=0


def _scalar_sub_struct(arrow_type: pa.DataType) -> pa.StructType:
    """Return struct<value: binary, typed_value: arrow_type> for one scalar sub-field."""
    return pa.struct([
        pa.field('value', pa.binary(), nullable=True),
        pa.field('typed_value', arrow_type, nullable=True),
    ])


def _object_shredded_type(*key_type_pairs) -> pa.StructType:
    """Build a top-level shredded VARIANT struct with the given (key, arrow_type) sub-fields."""
    tv_fields = [
        pa.field(name, _scalar_sub_struct(atype), nullable=True)
        for name, atype in key_type_pairs
    ]
    return pa.struct([
        pa.field('metadata', pa.binary(), nullable=False),
        pa.field('value', pa.binary(), nullable=True),
        pa.field('typed_value', pa.struct(tv_fields), nullable=True),
    ])


def _schema_json(col_name: str, sub_fields) -> str:
    """Build a variant.shreddingSchema JSON string for the given column and sub-fields.

    Args:
        col_name: VARIANT column name.
        sub_fields: list of (field_name, paimon_type_str) pairs, e.g.
                    [('age', 'BIGINT'), ('city', 'VARCHAR')].
    """
    return json.dumps({
        'type': 'ROW',
        'fields': [
            {
                'id': 0,
                'name': col_name,
                'type': {
                    'type': 'ROW',
                    'fields': [
                        {'id': i, 'name': name, 'type': dtype}
                        for i, (name, dtype) in enumerate(sub_fields)
                    ],
                },
            }
        ],
    })


# ===========================================================================
# 1. Type-system layer
# ===========================================================================

class TestVariantSchemaParsing(unittest.TestCase):

    def test_parse_variant_keyword(self):
        dt = DataTypeParser.parse_atomic_type_sql_string('VARIANT')
        self.assertIsInstance(dt, AtomicType)
        self.assertEqual(dt.type, 'VARIANT')
        self.assertTrue(dt.nullable)

    def test_parse_variant_not_null(self):
        dt = DataTypeParser.parse_atomic_type_sql_string('VARIANT NOT NULL')
        self.assertIsInstance(dt, AtomicType)
        self.assertFalse(dt.nullable)

    def test_variant_to_dict_roundtrip(self):
        dt = AtomicType('VARIANT')
        restored = DataTypeParser.parse_data_type(dt.to_dict())
        self.assertEqual(dt, restored)

    def test_variant_str(self):
        self.assertEqual(str(AtomicType('VARIANT')), 'VARIANT')
        self.assertEqual(str(AtomicType('VARIANT', nullable=False)), 'VARIANT NOT NULL')


class TestVariantFromPaimonType(unittest.TestCase):

    def _arrow_type(self):
        return PyarrowFieldParser.from_paimon_type(AtomicType('VARIANT'))

    def test_returns_struct(self):
        self.assertTrue(pa.types.is_struct(self._arrow_type()))
        self.assertEqual(self._arrow_type().num_fields, 2)

    def test_field_names(self):
        t = self._arrow_type()
        self.assertEqual(t.field(0).name, 'value')
        self.assertEqual(t.field(1).name, 'metadata')

    def test_field_types_are_binary(self):
        t = self._arrow_type()
        self.assertTrue(pa.types.is_binary(t.field(0).type))
        self.assertTrue(pa.types.is_binary(t.field(1).type))

    def test_fields_not_nullable(self):
        t = self._arrow_type()
        self.assertFalse(t.field(0).nullable)
        self.assertFalse(t.field(1).nullable)

    def test_from_paimon_field(self):
        df = DataField(id=0, name='payload', type=AtomicType('VARIANT'))
        pa_field = PyarrowFieldParser.from_paimon_field(df)
        self.assertEqual(pa_field.name, 'payload')
        self.assertTrue(pa.types.is_struct(pa_field.type))
        self.assertTrue(pa_field.nullable)

    def test_from_paimon_schema(self):
        fields = [
            DataField(id=0, name='id', type=AtomicType('BIGINT')),
            DataField(id=1, name='payload', type=AtomicType('VARIANT')),
        ]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        self.assertEqual(schema.field('payload').type, _variant_arrow_type())


class TestVariantToPaimonType(unittest.TestCase):

    def test_is_variant_struct_positive(self):
        self.assertTrue(is_variant_struct(_variant_arrow_type()))

    def test_is_variant_struct_wrong_names(self):
        st = pa.struct([
            pa.field('val', pa.binary(), nullable=False),
            pa.field('meta', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_is_variant_struct_nullable_fields(self):
        st = pa.struct([
            pa.field('value', pa.binary(), nullable=True),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_is_variant_struct_wrong_types(self):
        st = pa.struct([
            pa.field('value', pa.string(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_is_variant_struct_extra_fields(self):
        st = pa.struct([
            pa.field('value', pa.binary(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
            pa.field('typed_value', pa.int64(), nullable=True),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_to_paimon_type_variant(self):
        result = PyarrowFieldParser.to_paimon_type(_variant_arrow_type(), nullable=True)
        self.assertIsInstance(result, AtomicType)
        self.assertEqual(result.type, 'VARIANT')
        self.assertTrue(result.nullable)

    def test_to_paimon_type_variant_not_null(self):
        result = PyarrowFieldParser.to_paimon_type(_variant_arrow_type(), nullable=False)
        self.assertFalse(result.nullable)

    def test_ordinary_struct_maps_to_row_type(self):
        st = pa.struct([pa.field('a', pa.int32()), pa.field('b', pa.string())])
        result = PyarrowFieldParser.to_paimon_type(st, nullable=True)
        self.assertIsInstance(result, RowType)

    def test_struct_same_names_different_types_is_row_type(self):
        st = pa.struct([
            pa.field('value', pa.string(), nullable=False),
            pa.field('metadata', pa.string(), nullable=False),
        ])
        result = PyarrowFieldParser.to_paimon_type(st, nullable=True)
        self.assertIsInstance(result, RowType)


class TestVariantSchemaRoundTrip(unittest.TestCase):

    def test_paimon_to_arrow_to_paimon(self):
        original = DataField(id=0, name='v', type=AtomicType('VARIANT'))
        pa_field = PyarrowFieldParser.from_paimon_field(original)
        restored = PyarrowFieldParser.to_paimon_type(pa_field.type, pa_field.nullable)
        self.assertIsInstance(restored, AtomicType)
        self.assertEqual(restored.type, 'VARIANT')

    def test_mixed_schema_round_trip(self):
        fields = [
            DataField(id=0, name='id', type=AtomicType('BIGINT')),
            DataField(id=1, name='payload', type=AtomicType('VARIANT')),
            DataField(id=2, name='ts', type=AtomicType('TIMESTAMP(6)')),
        ]
        pa_schema = PyarrowFieldParser.from_paimon_schema(fields)
        restored = PyarrowFieldParser.to_paimon_schema(pa_schema)

        self.assertEqual(restored[1].name, 'payload')
        self.assertIsInstance(restored[1].type, AtomicType)
        self.assertEqual(restored[1].type.type, 'VARIANT')
        self.assertEqual(restored[2].name, 'ts')


# ===========================================================================
# 2. GenericVariant container
# ===========================================================================

class TestGenericVariantContainer(unittest.TestCase):

    def test_from_json_returns_instance(self):
        gv = GenericVariant.from_json('{"age":30}')
        self.assertIsInstance(gv, GenericVariant)
        self.assertIsInstance(gv.value(), bytes)
        self.assertIsInstance(gv.metadata(), bytes)
        self.assertGreater(len(gv.value()), 0)
        self.assertGreater(len(gv.metadata()), 0)

    def test_from_python_returns_instance(self):
        gv = GenericVariant.from_python({'a': 1, 'b': 'hello'})
        self.assertIsInstance(gv, GenericVariant)
        self.assertIsInstance(gv.value(), bytes)

    def test_from_arrow_struct_roundtrip(self):
        original = GenericVariant.from_json('{"x":1,"y":2}')
        restored = GenericVariant.from_arrow_struct(
            {'value': original.value(), 'metadata': original.metadata()})
        self.assertEqual(restored.value(), original.value())
        self.assertEqual(restored.metadata(), original.metadata())

    def test_to_json_roundtrip(self):
        json_str = '{"age":30,"city":"Beijing"}'
        gv = GenericVariant.from_json(json_str)
        parsed = json.loads(gv.to_json())
        self.assertEqual(parsed, json.loads(json_str))

    def test_to_python_object(self):
        gv = GenericVariant.from_json('{"age":30,"city":"Beijing"}')
        result = gv.to_python()
        self.assertEqual(result, {'age': 30, 'city': 'Beijing'})

    def test_to_python_array(self):
        gv = GenericVariant.from_json('[1,2,3]')
        self.assertEqual(gv.to_python(), [1, 2, 3])

    def test_to_python_null(self):
        gv = GenericVariant.from_json('null')
        self.assertIsNone(gv.to_python())

    def test_to_python_string(self):
        gv = GenericVariant.from_json('"hello"')
        self.assertEqual(gv.to_python(), 'hello')

    def test_to_python_number(self):
        gv = GenericVariant.from_json('42')
        self.assertEqual(gv.to_python(), 42)

    def test_from_python_none(self):
        gv = GenericVariant.from_python(None)
        self.assertIsNone(gv.to_python())

    def test_from_python_nested(self):
        obj = {'user': {'name': 'Alice', 'scores': [10, 20, 30]}, 'active': True}
        gv = GenericVariant.from_python(obj)
        result = gv.to_python()
        self.assertEqual(result['user']['name'], 'Alice')
        self.assertEqual(result['user']['scores'], [10, 20, 30])
        self.assertTrue(result['active'])

    def test_equality(self):
        gv1 = GenericVariant.from_json('{"a":1}')
        gv2 = GenericVariant.from_json('{"a":1}')
        self.assertEqual(gv1, gv2)

    def test_repr_and_str(self):
        gv = GenericVariant.from_json('"hello"')
        self.assertIn('hello', repr(gv))
        self.assertIn('hello', str(gv))


class TestToArrowArray(unittest.TestCase):

    def test_basic(self):
        gv1 = GenericVariant.from_json('{"a":1}')
        gv2 = GenericVariant.from_json('[1,2]')
        arr = GenericVariant.to_arrow_array([gv1, gv2])
        self.assertIsInstance(arr, pa.StructArray)
        self.assertEqual(len(arr), 2)
        restored = GenericVariant.from_arrow_struct(arr[0].as_py())
        self.assertEqual(restored.to_python(), {'a': 1})

    def test_with_nulls(self):
        arr = GenericVariant.to_arrow_array([GenericVariant.from_json('42'), None])
        self.assertEqual(len(arr), 2)
        self.assertIsNotNone(arr[0].as_py())
        self.assertIsNone(arr[1].as_py())

    def test_empty(self):
        self.assertEqual(len(GenericVariant.to_arrow_array([])), 0)

    def test_arrow_type(self):
        gv = GenericVariant.from_json('true')
        arr = GenericVariant.to_arrow_array([gv])
        self.assertEqual(arr.type, _variant_arrow_type())


class TestJsonRoundtrip(unittest.TestCase):

    def _check(self, json_str):
        gv = GenericVariant.from_json(json_str)
        self.assertEqual(json.loads(gv.to_json()), json.loads(json_str))

    def test_nested_object_array(self):
        self._check('{"users":[{"name":"Alice","age":30},{"name":"Bob","age":25}]}')

    def test_deep_nesting(self):
        self._check('{"a":{"b":{"c":{"d":42}}}}')

    def test_array_of_objects(self):
        self._check('[{"x":1},{"x":2},{"x":3}]')

    def test_all_primitive_types(self):
        self._check('{"n":null,"b":true,"i":42,"s":"hello","f":1.5}')

    def test_empty_object(self):
        gv = GenericVariant.from_json('{}')
        self.assertEqual(gv.to_python(), {})

    def test_empty_array(self):
        gv = GenericVariant.from_json('[]')
        self.assertEqual(gv.to_python(), [])


# ===========================================================================
# 3. Plain VARIANT — PyArrow-level Parquet sanity check
#    (verifies the physical struct<value, metadata> layout works in Parquet)
# ===========================================================================

def _make_variant_bytes(json_str: str) -> bytes:
    """Produce a minimal VARIANT value payload encoding a long-string primitive."""
    payload = json_str.encode('utf-8')
    return _struct.pack('<B', 0x15) + _struct.pack('<I', len(payload)) + payload


class TestVariantParquetCycle(unittest.TestCase):

    def test_write_and_read_parquet_values(self):
        """Write struct<value, metadata> bytes to Parquet and verify schema + values."""
        meta = _make_metadata()
        payload_col = pa.array(
            [{'value': _make_variant_bytes('{"key":"hello"}'), 'metadata': meta},
             {'value': _make_variant_bytes('42'), 'metadata': meta}],
            type=_variant_arrow_type(),
        )
        original = pa.table({'id': [1, 2], 'payload': payload_col})
        buf = io.BytesIO()
        pq.write_table(original, buf)
        buf.seek(0)
        restored = pq.read_table(buf)

        self.assertEqual(restored.schema.field('payload').type, _variant_arrow_type())
        self.assertEqual(restored.num_rows, 2)
        row0 = restored.column('payload')[0].as_py()
        self.assertEqual(row0['value'], _make_variant_bytes('{"key":"hello"}'))
        self.assertEqual(row0['metadata'], meta)

    def test_null_variant_row(self):
        """SQL-NULL VARIANT row survives Parquet round-trip as None."""
        payload_col = pa.array(
            [None, {'value': _make_variant_bytes('true'), 'metadata': _make_metadata()}],
            type=_variant_arrow_type(),
        )
        buf = io.BytesIO()
        pq.write_table(pa.table({'id': [1, 2], 'payload': payload_col}), buf)
        buf.seek(0)
        restored = pq.read_table(buf)
        self.assertIsNone(restored.column('payload')[0].as_py())
        self.assertIsNotNone(restored.column('payload')[1].as_py())

    def test_to_arrow_array_roundtrip_parquet(self):
        """GenericVariant.to_arrow_array() produces bytes that survive a Parquet cycle."""
        gvs = [
            GenericVariant.from_python({'score': 1}),
            GenericVariant.from_python({'score': 2}),
            None,
        ]
        col = GenericVariant.to_arrow_array(gvs)
        buf = io.BytesIO()
        pq.write_table(pa.table({'v': col}), buf)
        buf.seek(0)
        restored = pq.read_table(buf).column('v')
        self.assertEqual(
            GenericVariant.from_arrow_struct(restored[0].as_py()).to_python(), {'score': 1})
        self.assertEqual(
            GenericVariant.from_arrow_struct(restored[1].as_py()).to_python(), {'score': 2})
        self.assertIsNone(restored[2].as_py())


# ===========================================================================
# 4. Shredded VARIANT — schema detection and parsing
# ===========================================================================

class TestIsShredded(unittest.TestCase):

    def test_shredded_is_detected(self):
        t = _object_shredded_type(('age', pa.int64()))
        self.assertTrue(is_shredded_variant(t))

    def test_plain_variant_not_shredded(self):
        plain = pa.struct([
            pa.field('value', pa.binary(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_shredded_variant(plain))

    def test_arbitrary_struct_not_shredded(self):
        t = pa.struct([pa.field('a', pa.int32()), pa.field('b', pa.string())])
        self.assertFalse(is_shredded_variant(t))

    def test_non_struct_not_shredded(self):
        self.assertFalse(is_shredded_variant(pa.binary()))
        self.assertFalse(is_shredded_variant(pa.int64()))

    def test_missing_typed_value_not_shredded(self):
        t = pa.struct([
            pa.field('metadata', pa.binary()),
            pa.field('value', pa.binary()),
        ])
        self.assertFalse(is_shredded_variant(t))


class TestParseMetadataDict(unittest.TestCase):

    def test_single_key(self):
        meta = _make_metadata('age')
        d = parse_metadata_dict(meta)
        self.assertIn('age', d)
        self.assertEqual(d['age'], 0)

    def test_multiple_keys(self):
        meta = _make_metadata('age', 'name', 'city')
        d = parse_metadata_dict(meta)
        self.assertEqual(set(d.keys()), {'age', 'name', 'city'})
        self.assertEqual(d['age'], 0)
        self.assertEqual(d['name'], 1)
        self.assertEqual(d['city'], 2)

    def test_empty_metadata(self):
        meta = b'\x01\x00'
        d = parse_metadata_dict(meta)
        self.assertEqual(d, {})

    def test_empty_bytes(self):
        d = parse_metadata_dict(b'')
        self.assertEqual(d, {})


class TestBuildVariantSchema(unittest.TestCase):

    def test_simple_scalar(self):
        t = _object_shredded_type(('age', pa.int64()), ('name', pa.string()))
        schema = build_variant_schema(t)
        self.assertEqual(schema.metadata_idx, 0)
        self.assertEqual(schema.value_idx, 1)
        self.assertEqual(schema.typed_idx, 2)
        self.assertIsNotNone(schema.object_fields)
        self.assertEqual(len(schema.object_fields), 2)
        self.assertEqual(schema.object_fields[0].field_name, 'age')
        self.assertEqual(schema.object_fields[1].field_name, 'name')

    def test_sub_field_scalar_type(self):
        t = _object_shredded_type(('age', pa.int64()))
        schema = build_variant_schema(t)
        age_schema = schema.object_fields[0].schema
        self.assertEqual(age_schema.scalar_arrow_type, pa.int64())
        self.assertEqual(age_schema.value_idx, 0)
        self.assertEqual(age_schema.typed_idx, 1)

    def test_object_schema_map(self):
        t = _object_shredded_type(('age', pa.int64()), ('name', pa.string()))
        schema = build_variant_schema(t)
        self.assertIn('age', schema.object_schema_map)
        self.assertIn('name', schema.object_schema_map)

    def test_is_unshredded_for_plain_variant(self):
        schema = VariantSchema(metadata_idx=0, value_idx=1)
        self.assertTrue(schema.is_unshredded())

    def test_not_unshredded_for_shredded_schema(self):
        t = _object_shredded_type(('age', pa.int64()))
        schema = build_variant_schema(t)
        self.assertFalse(schema.is_unshredded())

    def test_no_typed_value_field(self):
        plain = pa.struct([
            pa.field('metadata', pa.binary(), nullable=False),
            pa.field('value', pa.binary(), nullable=True),
        ])
        schema = build_variant_schema(plain)
        self.assertEqual(schema.metadata_idx, 0)
        self.assertEqual(schema.value_idx, 1)
        self.assertEqual(schema.typed_idx, -1)
        self.assertTrue(schema.is_unshredded())


# ===========================================================================
# 5. Binary encoding helpers
# ===========================================================================

class TestEncodeScalar(unittest.TestCase):

    def _roundtrip(self, json_str: str, arrow_type: pa.DataType):
        """Encode a scalar to bytes via the Arrow type, then decode via GenericVariant."""
        gv_orig = GenericVariant.from_json(json_str)
        typed_value = pa.array([gv_orig.to_python()], type=arrow_type).to_pylist()[0]
        value_bytes = _encode_scalar_to_value_bytes(typed_value, arrow_type)
        gv = GenericVariant(value_bytes, b'\x01\x00')
        return gv.to_python()

    def test_int(self):
        self.assertEqual(self._roundtrip('42', pa.int64()), 42)

    def test_float(self):
        value_bytes = _encode_scalar_to_value_bytes(3.14, pa.float64())
        gv = GenericVariant(value_bytes, b'\x01\x00')
        self.assertAlmostEqual(gv.to_python(), 3.14, places=5)

    def test_bool_true(self):
        self.assertEqual(self._roundtrip('true', pa.bool_()), True)

    def test_bool_false(self):
        self.assertEqual(self._roundtrip('false', pa.bool_()), False)

    def test_string(self):
        self.assertEqual(self._roundtrip('"hello"', pa.string()), 'hello')

    def test_null(self):
        value_bytes = _encode_scalar_to_value_bytes(None, pa.int64())
        gv = GenericVariant(value_bytes, b'\x01\x00')
        self.assertIsNone(gv.to_python())


class TestBuildBinary(unittest.TestCase):

    def test_build_object_empty(self):
        obj_bytes = _build_object_value([])
        gv = GenericVariant(obj_bytes, b'\x01\x00')
        self.assertEqual(gv.to_python(), {})

    def test_build_object_one_field(self):
        meta = _make_metadata('age')
        key_dict = parse_metadata_dict(meta)
        age_val = _encode_scalar_to_value_bytes(30, pa.int64())
        obj_bytes = _build_object_value([(key_dict['age'], age_val)])
        gv = GenericVariant(obj_bytes, meta)
        self.assertEqual(gv.to_python(), {'age': 30})

    def test_build_array_empty(self):
        arr_bytes = _build_array_value([])
        gv = GenericVariant(arr_bytes, b'\x01\x00')
        self.assertEqual(gv.to_python(), [])

    def test_build_array_three_ints(self):
        elem1 = _encode_scalar_to_value_bytes(1, pa.int64())
        elem2 = _encode_scalar_to_value_bytes(2, pa.int64())
        elem3 = _encode_scalar_to_value_bytes(3, pa.int64())
        arr_bytes = _build_array_value([elem1, elem2, elem3])
        gv = GenericVariant(arr_bytes, b'\x01\x00')
        self.assertEqual(gv.to_python(), [1, 2, 3])

    def test_build_array_with_null(self):
        elem1 = _encode_scalar_to_value_bytes(1, pa.int64())
        arr_bytes = _build_array_value([elem1, _NULL_VALUE_BYTES])
        gv = GenericVariant(arr_bytes, b'\x01\x00')
        result = gv.to_python()
        self.assertEqual(result[0], 1)
        self.assertIsNone(result[1])


# ===========================================================================
# 6. Read-path assembly
# ===========================================================================

class TestRebuildValue(unittest.TestCase):
    """Test the core rebuild_value() function for various shredding scenarios."""

    def _age_schema(self) -> VariantSchema:
        """Schema for a sub-field struct {value: binary, typed_value: int64}."""
        return build_variant_schema(_scalar_sub_struct(pa.int64()))

    def _metadata_and_dict(self, *keys: str):
        meta = _make_metadata(*keys)
        return meta, parse_metadata_dict(meta)

    def test_scalar_typed_value(self):
        schema = self._age_schema()
        row = {'value': None, 'typed_value': 30}
        result = rebuild_value(row, schema, {})
        gv = GenericVariant(result, b'\x01\x00')
        self.assertEqual(gv.to_python(), 30)

    def test_scalar_overflow(self):
        """When typed_value is None, fall back to overflow value bytes."""
        age_val = _encode_scalar_to_value_bytes(42, pa.int64())
        schema = self._age_schema()
        row = {'value': age_val, 'typed_value': None}
        result = rebuild_value(row, schema, {})
        gv = GenericVariant(result, b'\x01\x00')
        self.assertEqual(gv.to_python(), 42)

    def test_both_null_returns_none(self):
        """Both value and typed_value being None signals an absent field."""
        schema = self._age_schema()
        row = {'value': None, 'typed_value': None}
        self.assertIsNone(rebuild_value(row, schema, {}))

    def test_object_with_shredded_fields(self):
        meta, key_dict = self._metadata_and_dict('age', 'name')
        t = _object_shredded_type(('age', pa.int64()), ('name', pa.string()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': None,
            'typed_value': {
                'age': {'value': None, 'typed_value': 30},
                'name': {'value': None, 'typed_value': 'Alice'},
            },
        }
        value_bytes, _ = rebuild(row, schema, key_dict)
        gv = GenericVariant(value_bytes, meta)
        result = gv.to_python()
        self.assertEqual(result['age'], 30)
        self.assertEqual(result['name'], 'Alice')

    def test_object_absent_field_skipped(self):
        """A sub-field with both value=None and typed_value=None is omitted from output."""
        meta, key_dict = self._metadata_and_dict('age', 'name')
        t = _object_shredded_type(('age', pa.int64()), ('name', pa.string()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': None,
            'typed_value': {
                'age': {'value': None, 'typed_value': 25},
                'name': {'value': None, 'typed_value': None},  # absent
            },
        }
        value_bytes, _ = rebuild(row, schema, key_dict)
        gv = GenericVariant(value_bytes, meta)
        result = gv.to_python()
        self.assertEqual(result['age'], 25)
        self.assertNotIn('name', result)

    def test_object_with_overflow(self):
        """Fields not in typed_value are preserved from overflow bytes."""
        original = GenericVariant.from_json('{"age": 30, "extra": "overflow_val"}')
        overflow_bytes = original.value()
        meta = original.metadata()
        key_dict = parse_metadata_dict(meta)

        t = _object_shredded_type(('age', pa.int64()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': overflow_bytes,
            'typed_value': {
                'age': {'value': None, 'typed_value': 30},
            },
        }
        value_bytes = rebuild_value(row, schema, key_dict)
        gv = GenericVariant(value_bytes, meta)
        result = gv.to_python()
        self.assertEqual(result['age'], 30)
        self.assertIn('extra', result)

    def test_typed_value_null_uses_overflow(self):
        """If typed_value struct is None for the whole row, full overflow bytes are used."""
        original = GenericVariant.from_json('{"age": 99}')
        meta = original.metadata()
        key_dict = parse_metadata_dict(meta)

        t = _object_shredded_type(('age', pa.int64()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': original.value(),
            'typed_value': None,  # typed_value absent for this row
        }
        value_bytes, _ = rebuild(row, schema, key_dict)
        gv = GenericVariant(value_bytes, meta)
        self.assertEqual(gv.to_python()['age'], 99)

    def test_rebuild_matches_direct_variant(self):
        """Bytes rebuilt from shredded form must equal bytes from GenericVariant.from_json."""
        original_json = '{"score": 42, "tag": "test"}'
        original_gv = GenericVariant.from_json(original_json)
        meta = original_gv.metadata()
        key_dict = parse_metadata_dict(meta)

        t = _object_shredded_type(('score', pa.int64()), ('tag', pa.string()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': None,
            'typed_value': {
                'score': {'value': None, 'typed_value': 42},
                'tag': {'value': None, 'typed_value': 'test'},
            },
        }
        value_bytes, _ = rebuild(row, schema, key_dict)
        gv_rebuilt = GenericVariant(value_bytes, meta)
        self.assertEqual(gv_rebuilt.to_python(), original_gv.to_python())


class TestRebuild(unittest.TestCase):

    def test_missing_metadata_raises(self):
        schema = VariantSchema()
        with self.assertRaises(ValueError):
            rebuild({'value': None, 'typed_value': None}, schema)

    def test_basic_roundtrip(self):
        meta = _make_metadata('x')
        key_dict = parse_metadata_dict(meta)
        t = _object_shredded_type(('x', pa.int64()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': None,
            'typed_value': {'x': {'value': None, 'typed_value': 7}},
        }
        value_bytes, ret_meta = rebuild(row, schema, key_dict)
        self.assertEqual(ret_meta, meta)
        gv = GenericVariant(value_bytes, ret_meta)
        self.assertEqual(gv.to_python(), {'x': 7})

    def test_auto_parse_key_dict(self):
        """When key_dict is None, rebuild() parses it automatically from the metadata field."""
        meta = _make_metadata('y')
        t = _object_shredded_type(('y', pa.string()))
        schema = build_variant_schema(t)
        row = {
            'metadata': meta,
            'value': None,
            'typed_value': {'y': {'value': None, 'typed_value': 'hi'}},
        }
        value_bytes, _ = rebuild(row, schema)
        gv = GenericVariant(value_bytes, meta)
        self.assertEqual(gv.to_python(), {'y': 'hi'})


class TestAssembleShreddedColumn(unittest.TestCase):

    def test_basic_two_rows(self):
        """assemble_shredded_column converts a shredded Arrow column to plain VARIANT."""
        meta = _make_metadata('age', 'name')
        t = _object_shredded_type(('age', pa.int64()), ('name', pa.string()))
        rows = [
            {'metadata': meta, 'value': None, 'typed_value': {
                'age': {'value': None, 'typed_value': 30},
                'name': {'value': None, 'typed_value': 'Alice'},
            }},
            {'metadata': meta, 'value': None, 'typed_value': {
                'age': {'value': None, 'typed_value': 25},
                'name': {'value': None, 'typed_value': 'Bob'},
            }},
        ]
        col = pa.array(rows, type=t)
        schema = build_variant_schema(col.type)
        result = assemble_shredded_column(col, schema)

        self.assertEqual(result.type, VARIANT_ARROW_TYPE)
        self.assertEqual(len(result), 2)

        gv0 = GenericVariant.from_arrow_struct(result[0].as_py())
        self.assertEqual(gv0.to_python(), {'age': 30, 'name': 'Alice'})

        gv1 = GenericVariant.from_arrow_struct(result[1].as_py())
        self.assertEqual(gv1.to_python(), {'age': 25, 'name': 'Bob'})


# ===========================================================================
# 7. Write-path shredding
# ===========================================================================

class TestShreddingWrite(unittest.TestCase):
    """Tests for the static-mode write shredding API."""

    def _obj_fields_for(self, col_name, sub_fields):
        """Helper: build obj_fields for a single column from (name, type) pairs."""
        return parse_shredding_schema_option(_schema_json(col_name, sub_fields))[col_name]

    # -----------------------------------------------------------------------
    # parse_shredding_schema_option
    # -----------------------------------------------------------------------

    def test_parse_single_scalar_field(self):
        result = parse_shredding_schema_option(json.dumps({
            'type': 'ROW',
            'fields': [
                {'id': 0, 'name': 'payload', 'type': {
                    'type': 'ROW',
                    'fields': [{'id': 0, 'name': 'age', 'type': 'INT'}],
                }},
            ],
        }))
        self.assertIn('payload', result)
        obj_fields = result['payload']
        self.assertEqual(len(obj_fields), 1)
        self.assertEqual(obj_fields[0].field_name, 'age')
        self.assertIsNotNone(obj_fields[0].schema.scalar_arrow_type)

    def test_parse_multiple_fields_multiple_cols(self):
        result = parse_shredding_schema_option(json.dumps({
            'type': 'ROW',
            'fields': [
                {'id': 0, 'name': 'col_a', 'type': {'type': 'ROW', 'fields': [
                    {'id': 0, 'name': 'x', 'type': 'BIGINT'},
                    {'id': 1, 'name': 'y', 'type': 'VARCHAR'},
                ]}},
                {'id': 1, 'name': 'col_b', 'type': {'type': 'ROW', 'fields': [
                    {'id': 0, 'name': 'flag', 'type': 'BOOLEAN'},
                ]}},
            ],
        }))
        self.assertIn('col_a', result)
        self.assertIn('col_b', result)
        self.assertEqual(len(result['col_a']), 2)
        self.assertEqual(result['col_a'][0].field_name, 'x')
        self.assertEqual(result['col_a'][1].field_name, 'y')
        self.assertEqual(result['col_b'][0].field_name, 'flag')

    def test_parse_invalid_top_level_type_raises(self):
        with self.assertRaises(ValueError):
            parse_shredding_schema_option(json.dumps({'type': 'ARRAY', 'element': 'INT'}))

    # -----------------------------------------------------------------------
    # shredding_schema_to_arrow_type
    # -----------------------------------------------------------------------

    def test_arrow_type_structure_single_scalar(self):
        obj_fields = self._obj_fields_for('col', [('score', 'BIGINT')])
        arrow_type = shredding_schema_to_arrow_type(obj_fields)

        self.assertTrue(pa.types.is_struct(arrow_type))
        names = {arrow_type.field(i).name for i in range(arrow_type.num_fields)}
        self.assertIn('metadata', names)
        self.assertIn('value', names)
        self.assertIn('typed_value', names)

        tv = arrow_type.field('typed_value').type
        self.assertTrue(pa.types.is_struct(tv))
        self.assertEqual(tv.num_fields, 1)
        self.assertEqual(tv.field(0).name, 'score')

        sub = tv.field('score').type
        self.assertTrue(pa.types.is_struct(sub))
        sub_names = {sub.field(i).name for i in range(sub.num_fields)}
        self.assertIn('value', sub_names)
        self.assertIn('typed_value', sub_names)

    # -----------------------------------------------------------------------
    # decompose_variant
    # -----------------------------------------------------------------------

    def test_decompose_scalar_field_extracted(self):
        """A shredded scalar field ends up in typed_value; non-shredded fields go to overflow."""
        obj_fields = self._obj_fields_for('col', [('age', 'BIGINT')])
        gv = GenericVariant.from_python({'age': 25, 'name': 'alice'})
        result = decompose_variant(gv, obj_fields)

        self.assertIn('metadata', result)
        self.assertIn('value', result)
        self.assertIn('typed_value', result)

        tv = result['typed_value']
        self.assertIn('age', tv)
        self.assertIsNone(tv['age']['value'])
        self.assertEqual(tv['age']['typed_value'], 25)

        # 'name' not in schema → must appear in overflow
        overflow = result['value']
        if overflow:
            overflow_gv = GenericVariant(overflow, result['metadata'])
            self.assertIn('name', overflow_gv.to_python())

    def test_decompose_absent_field_is_null(self):
        """A shredded field absent from the variant yields {value: None, typed_value: None}."""
        obj_fields = self._obj_fields_for('col', [('missing_field', 'BIGINT')])
        gv = GenericVariant.from_python({'other': 99})
        result = decompose_variant(gv, obj_fields)
        tv = result['typed_value']
        self.assertIsNone(tv['missing_field']['value'])
        self.assertIsNone(tv['missing_field']['typed_value'])

    def test_decompose_non_object_variant_all_overflow(self):
        """A non-object variant goes fully into overflow; typed_value is null (not a struct).

        Java's ShreddingUtils.rebuild requires typed_value to be null (not a non-null struct
        with all-null sub-fields) when the variant is not an object, so that the overflow
        binary can be consumed directly without expecting it to be an OBJECT variant.
        """
        obj_fields = self._obj_fields_for('col', [('x', 'BIGINT')])
        gv = GenericVariant.from_python(42)
        result = decompose_variant(gv, obj_fields)
        self.assertIsNotNone(result['value'])
        self.assertIsNone(result['typed_value'])

    def test_decompose_multiple_typed_fields(self):
        """Multiple shredded fields are all correctly split from the variant."""
        obj_fields = self._obj_fields_for('col', [('score', 'BIGINT'), ('tag', 'VARCHAR')])
        gv = GenericVariant.from_python({'score': 100, 'tag': 'gold', 'extra': True})
        result = decompose_variant(gv, obj_fields)

        tv = result['typed_value']
        self.assertEqual(tv['score']['typed_value'], 100)
        self.assertEqual(tv['tag']['typed_value'], 'gold')
        # 'extra' must be in overflow
        overflow = result['value']
        if overflow:
            overflow_py = GenericVariant(overflow, result['metadata']).to_python()
            self.assertIn('extra', overflow_py)


# ===========================================================================
# 8. Full-chain via Paimon API
#    Write and read using CatalogFactory / Schema / write_arrow / to_arrow —
#    the same code path used by real applications.
# ===========================================================================

class TestVariantPaimonTable(unittest.TestCase):
    """End-to-end tests that use the real Paimon Python API.

    Each test creates its own table (unique name) within a shared in-process
    warehouse so that tests are fully independent.
    """

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp()
        warehouse = os.path.join(cls.tmpdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def _write_and_read(self, table, pa_data: pa.Table) -> pa.Table:
        """Write pa_data to the table and read all rows back."""
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(pa_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        return read_builder.new_read().to_arrow(splits)

    def _pa_schema(self):
        return pa.schema([
            pa.field('id', pa.int64()),
            pa.field('payload', _variant_arrow_type()),
        ])

    def test_plain_variant_write_and_read(self):
        """Plain VARIANT: GenericVariant → write_arrow → to_arrow → GenericVariant."""
        schema = Schema.from_pyarrow_schema(self._pa_schema())
        self.catalog.create_table('default.plain_variant', schema, False)
        table = self.catalog.get_table('default.plain_variant')

        gvs = [
            GenericVariant.from_python({'age': 30, 'city': 'Beijing'}),
            GenericVariant.from_python({'score': 99, 'active': True}),
            GenericVariant.from_json('[1, 2, 3]'),
        ]
        data = pa.table(
            {'id': [1, 2, 3], 'payload': GenericVariant.to_arrow_array(gvs)},
            schema=self._pa_schema(),
        )
        result = self._write_and_read(table, data)

        self.assertEqual(result.num_rows, 3)
        payload_col = result.column('payload')

        gv0 = GenericVariant.from_arrow_struct(payload_col[0].as_py())
        self.assertEqual(gv0.to_python(), {'age': 30, 'city': 'Beijing'})

        gv1 = GenericVariant.from_arrow_struct(payload_col[1].as_py())
        self.assertEqual(gv1.to_python(), {'score': 99, 'active': True})

        gv2 = GenericVariant.from_arrow_struct(payload_col[2].as_py())
        self.assertEqual(gv2.to_python(), [1, 2, 3])

    def test_plain_variant_null_row(self):
        """SQL-NULL VARIANT rows are stored and retrieved as None."""
        schema = Schema.from_pyarrow_schema(self._pa_schema())
        self.catalog.create_table('default.plain_variant_null', schema, False)
        table = self.catalog.get_table('default.plain_variant_null')

        gvs = [GenericVariant.from_python({'x': 1}), None, GenericVariant.from_python({'x': 3})]
        data = pa.table(
            {'id': [1, 2, 3], 'payload': GenericVariant.to_arrow_array(gvs)},
            schema=self._pa_schema(),
        )
        result = self._write_and_read(table, data)

        payload_col = result.column('payload')
        self.assertIsNotNone(payload_col[0].as_py())
        self.assertIsNone(payload_col[1].as_py())
        self.assertIsNotNone(payload_col[2].as_py())

    def test_shredded_variant_write_and_read(self):
        """Shredded VARIANT: writer shreds automatically, reader assembles transparently."""
        shredding_json = _schema_json('payload', [('age', 'BIGINT'), ('city', 'VARCHAR')])
        schema = Schema.from_pyarrow_schema(
            self._pa_schema(),
            options={'variant.shreddingSchema': shredding_json},
        )
        self.catalog.create_table('default.shredded_variant', schema, False)
        table = self.catalog.get_table('default.shredded_variant')

        gvs = [
            GenericVariant.from_python({'age': 28, 'city': 'Beijing'}),
            GenericVariant.from_python({'age': 35, 'city': 'Shanghai'}),
        ]
        data = pa.table(
            {'id': [1, 2], 'payload': GenericVariant.to_arrow_array(gvs)},
            schema=self._pa_schema(),
        )
        result = self._write_and_read(table, data)

        self.assertEqual(result.num_rows, 2)
        payload_col = result.column('payload')

        py0 = GenericVariant.from_arrow_struct(payload_col[0].as_py()).to_python()
        self.assertEqual(py0['age'], 28)
        self.assertEqual(py0['city'], 'Beijing')

        py1 = GenericVariant.from_arrow_struct(payload_col[1].as_py()).to_python()
        self.assertEqual(py1['age'], 35)
        self.assertEqual(py1['city'], 'Shanghai')

    def test_shredded_variant_overflow_preserved(self):
        """Fields outside the shredding schema survive in overflow bytes end-to-end."""
        shredding_json = _schema_json('payload', [('age', 'BIGINT')])
        schema = Schema.from_pyarrow_schema(
            self._pa_schema(),
            options={'variant.shreddingSchema': shredding_json},
        )
        self.catalog.create_table('default.shredded_overflow', schema, False)
        table = self.catalog.get_table('default.shredded_overflow')

        # 'city' and 'active' are NOT in the shredding schema → stored as overflow
        gv = GenericVariant.from_python({'age': 30, 'city': 'Beijing', 'active': True})
        data = pa.table(
            {'id': [1], 'payload': GenericVariant.to_arrow_array([gv])},
            schema=self._pa_schema(),
        )
        result = self._write_and_read(table, data)

        py = GenericVariant.from_arrow_struct(result.column('payload')[0].as_py()).to_python()
        self.assertEqual(py['age'], 30)
        self.assertEqual(py['city'], 'Beijing')
        self.assertEqual(py['active'], True)

    def test_shredded_variant_null_row(self):
        """SQL-NULL VARIANT rows survive the shred → assemble cycle as None."""
        shredding_json = _schema_json('payload', [('x', 'BIGINT')])
        schema = Schema.from_pyarrow_schema(
            self._pa_schema(),
            options={'variant.shreddingSchema': shredding_json},
        )
        self.catalog.create_table('default.shredded_null', schema, False)
        table = self.catalog.get_table('default.shredded_null')

        gvs = [GenericVariant.from_python({'x': 7}), None]
        data = pa.table(
            {'id': [1, 2], 'payload': GenericVariant.to_arrow_array(gvs)},
            schema=self._pa_schema(),
        )
        result = self._write_and_read(table, data)

        payload_col = result.column('payload')
        gv = GenericVariant.from_arrow_struct(payload_col[0].as_py())
        self.assertEqual(gv.to_python(), {'x': 7})
        self.assertIsNone(payload_col[1].as_py())


if __name__ == '__main__':
    unittest.main()
