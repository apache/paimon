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

import unittest
from parameterized import parameterized
import pyarrow as pa

from pypaimon.schema.data_types import (DataField, AtomicType, ArrayType, MultisetType, MapType,
                                        RowType, VectorType, PyarrowFieldParser,
                                        is_blob_file_type)


class DataTypesTest(unittest.TestCase):
    def test_large_string_schema_preserves_field_contract(self):
        from pypaimon.schema.schema import Schema

        arrow_schema = pa.schema([
            pa.field('text', pa.large_string(), nullable=False,
                     metadata={b'description': b'task label'}),
            pa.field('nested', pa.struct([
                pa.field('labels', pa.list_(pa.large_string())),
                pa.field('mapping', pa.map_(pa.large_string(), pa.large_string())),
            ])),
        ])
        schema = Schema.from_pyarrow_schema(arrow_schema)
        restored = PyarrowFieldParser.from_paimon_schema(schema.fields)
        self.assertEqual(restored, pa.schema([
            pa.field('text', pa.string(), nullable=False,
                     metadata={b'description': b'task label'}),
            pa.field('nested', pa.struct([
                pa.field('labels', pa.list_(pa.string())),
                pa.field('mapping', pa.map_(pa.string(), pa.string())),
            ])),
        ]))
        self.assertEqual(schema.fields[0].description, 'task label')

    def test_atomic_type(self):
        self.assertEqual(str(AtomicType("BLOB")), "BLOB")
        self.assertEqual(str(AtomicType("TINYINT", nullable=False)), "TINYINT NOT NULL")
        self.assertEqual(str(AtomicType("BIGINT", nullable=False)), "BIGINT NOT NULL")
        self.assertEqual(str(AtomicType("BOOLEAN", nullable=False)), "BOOLEAN NOT NULL")
        self.assertEqual(str(AtomicType("DOUBLE")), "DOUBLE")
        self.assertEqual(str(AtomicType("STRING")), "STRING")
        self.assertEqual(str(AtomicType("BINARY(12)")), "BINARY(12)")
        self.assertEqual(str(AtomicType("DECIMAL(10, 6)")), "DECIMAL(10, 6)")
        self.assertEqual(str(AtomicType("BYTES")), "BYTES")
        self.assertEqual(str(AtomicType("DATE")), "DATE")
        self.assertEqual(str(AtomicType("TIME(0)")), "TIME(0)")
        self.assertEqual(str(AtomicType("TIMESTAMP(0)")), "TIMESTAMP(0)")
        self.assertEqual(str(AtomicType("SMALLINT", nullable=False)),
                         str(AtomicType.from_dict(AtomicType("SMALLINT", nullable=False).to_dict())))
        self.assertEqual(str(AtomicType("INT")),
                         str(AtomicType.from_dict(AtomicType("INT").to_dict())))

    def test_parameterized_atomic_type_not_null_roundtrip(self):
        # ``to_dict`` appends " NOT NULL" to the type string; the parser must
        # strip it back into ``nullable`` instead of keeping it inside
        # ``AtomicType.type``. Parameterized types take the paren branch where
        # this used to be missed, so a re-serialize doubled the suffix and
        # ``from_paimon_type`` blew up with "... NOT NULL NOT NULL".
        for type_str in ("DECIMAL(12, 2)", "VARCHAR(10)", "CHAR(5)",
                         "TIMESTAMP(3)", "TIME(0)", "BINARY(12)"):
            original = AtomicType(type_str, nullable=False)
            parsed = AtomicType.from_dict(original.to_dict())
            self.assertEqual(parsed.type, type_str, type_str)
            self.assertFalse(parsed.nullable, type_str)
            self.assertEqual(parsed, original, type_str)
            # Round-trips stably and stays materializable as a PyArrow type.
            self.assertEqual(parsed.to_dict(), original.to_dict(), type_str)
            PyarrowFieldParser.from_paimon_type(parsed)

    @parameterized.expand([
        (ArrayType, AtomicType("TIMESTAMP(6)"), "ARRAY<TIMESTAMP(6)>", "ARRAY<ARRAY<TIMESTAMP(6)>>"),
        (MultisetType, AtomicType("TIMESTAMP(6)"), "MULTISET<TIMESTAMP(6)>", "MULTISET<MULTISET<TIMESTAMP(6)>>")
    ])
    def test_complex_types(self, data_type_class, element_type, expected1, expected2):
        self.assertEqual(str(data_type_class(True, element_type)), expected1)
        self.assertEqual(str(data_type_class(True, data_type_class(True, element_type))), expected2)
        self.assertEqual(str(data_type_class(False, element_type)), expected1 + " NOT NULL")
        self.assertEqual(str(data_type_class(False, element_type)),
                         str(data_type_class.from_dict(data_type_class(False, element_type).to_dict())))
        self.assertEqual(str(data_type_class(True, element_type)),
                         str(data_type_class.from_dict(data_type_class(True, element_type).to_dict())))

    def test_array_element_nullability_roundtrip(self):
        for element_nullable in (True, False):
            paimon_type = ArrayType(
                True,
                AtomicType("BLOB", nullable=element_nullable),
            )

            arrow_type = PyarrowFieldParser.from_paimon_type(paimon_type)

            self.assertEqual(arrow_type.value_field.nullable, element_nullable)
            self.assertEqual(
                PyarrowFieldParser.to_paimon_type(arrow_type, nullable=True),
                paimon_type,
            )

    @parameterized.expand([
        (nullable, element_nullable)
        for nullable in (True, False)
        for element_nullable in (True, False)
    ])
    def test_multiset_json_uses_canonical_type(self, nullable, element_nullable):
        element = "INT" + ("" if element_nullable else " NOT NULL")
        data_type = MultisetType(nullable, AtomicType("INT", element_nullable))
        self.assertEqual(data_type.to_dict(), {
            "type": "MULTISET" + ("" if nullable else " NOT NULL"),
            "element": element, "nullable": nullable,
        })
        self.assertEqual(MultisetType.from_dict(data_type.to_dict()), data_type)
        self.assertEqual(str(data_type), "MULTISET<{}>{}".format(
            element, "" if nullable else " NOT NULL"))

    @parameterized.expand([(True,), (False,)])
    def test_legacy_multiset_json(self, nullable):
        legacy = {
            "type": "MULTISET<INT>" + ("" if nullable else " NOT NULL"),
            "element": "INT", "nullable": nullable,
        }
        expected = MultisetType(nullable, AtomicType("INT"))
        self.assertEqual(MultisetType.from_dict(legacy), expected)
        legacy.pop("nullable")
        self.assertEqual(MultisetType.from_dict(legacy), expected)

    def test_map_type(self):
        self.assertEqual(str(MapType(True, AtomicType("STRING"), AtomicType("TIMESTAMP(6)"))),
                         "MAP<STRING, TIMESTAMP(6)>")

    @parameterized.expand([
        (nullable, key_nullable, value_nullable)
        for nullable in (True, False)
        for key_nullable in (True, False)
        for value_nullable in (True, False)
    ])
    def test_map_json_uses_canonical_type(self, nullable, key_nullable, value_nullable):
        key = "STRING" + ("" if key_nullable else " NOT NULL")
        value = "INT" + ("" if value_nullable else " NOT NULL")
        data_type = MapType(nullable, AtomicType("STRING", key_nullable),
                            AtomicType("INT", value_nullable))
        self.assertEqual(data_type.to_dict(), {
            "type": "MAP" + ("" if nullable else " NOT NULL"),
            "key": key, "value": value, "nullable": nullable,
        })
        self.assertEqual(str(data_type), "MAP<{}, {}>{}".format(
            key, value, "" if nullable else " NOT NULL"))

    @parameterized.expand([
        ("MAP<STRING NOT NULL, INT NOT NULL>", {}, True),
        ("MAP<STRING NOT NULL, INT NOT NULL>", {"nullable": True}, True),
        ("MAP<STRING NOT NULL, INT NOT NULL>", {"nullable": False}, False),
        ("MAP<STRING NOT NULL, INT NOT NULL> NOT NULL", {}, False),
        ("MAP<STRING NOT NULL, INT NOT NULL> NOT NULL", {"nullable": None}, False),
        ("MAP<STRING NOT NULL, INT NOT NULL> NOT NULL", {"nullable": True}, True),
    ])
    def test_legacy_map_json_nullability(self, type_name, attributes, nullable):
        legacy = dict({"type": type_name, "key": "STRING NOT NULL", "value": "INT NOT NULL"},
                      **attributes)
        self.assertEqual(MapType.from_dict(legacy), MapType(
            nullable, AtomicType("STRING", False), AtomicType("INT", False)))

    def test_map_nullability_dict_roundtrip(self):
        for map_nullable in (True, False):
            for value_nullable in (True, False):
                original = MapType(
                    map_nullable,
                    AtomicType("STRING", nullable=False),
                    AtomicType("INT", nullable=value_nullable),
                )

                self.assertEqual(MapType.from_dict(original.to_dict()), original)

        legacy = MapType(
            True,
            AtomicType("STRING", nullable=False),
            AtomicType("INT"),
        ).to_dict()
        legacy.pop("nullable")
        self.assertTrue(MapType.from_dict(legacy).nullable)

    def test_map_blob_value_nullability_roundtrip(self):
        for value_nullable in (True, False):
            paimon_type = MapType(
                True,
                AtomicType("STRING", nullable=False),
                AtomicType("BLOB", nullable=value_nullable),
            )

            arrow_type = PyarrowFieldParser.from_paimon_type(paimon_type)

            self.assertFalse(arrow_type.key_field.nullable)
            self.assertEqual(arrow_type.item_field.nullable, value_nullable)
            self.assertEqual(
                PyarrowFieldParser.to_paimon_type(arrow_type, nullable=True),
                paimon_type,
            )
            self.assertTrue(is_blob_file_type(paimon_type))

    def test_vector_type(self):
        vector_type = VectorType(True, AtomicType("FLOAT"), 3)
        self.assertEqual(str(vector_type), "VECTOR<FLOAT, 3>")
        self.assertEqual(
            vector_type.to_dict(),
            {
                "type": "VECTOR",
                "element": "FLOAT",
                "length": 3,
                "nullable": True
            }
        )
        self.assertEqual(vector_type, VectorType.from_dict(vector_type.to_dict()))
        self.assertEqual(hash(vector_type), hash(VectorType(True, AtomicType("FLOAT"), 3)))

        not_null_vector = VectorType(False, AtomicType("FLOAT", nullable=False), 3)
        self.assertEqual(str(not_null_vector), "VECTOR<FLOAT NOT NULL, 3> NOT NULL")
        self.assertEqual(not_null_vector, VectorType.from_dict(not_null_vector.to_dict()))

        with self.assertRaises(ValueError):
            VectorType(True, AtomicType("FLOAT"), 0)
        with self.assertRaises(ValueError):
            VectorType(True, AtomicType("STRING"), 3)
        with self.assertRaises(ValueError):
            VectorType(True, ArrayType(True, AtomicType("INT")), 3)

    def test_row_type(self):
        self.assertEqual(str(RowType(True, [DataField(0, "a", AtomicType("STRING"), "Someone's desc."),
                                            DataField(1, "b", AtomicType("TIMESTAMP(6)"),)])),
                         "ROW<a: STRING COMMENT Someone's desc., b: TIMESTAMP(6)>")
        row_data = RowType(True, [DataField(0, "a", AtomicType("STRING"), "Someone's desc."),
                                  DataField(1, "b", AtomicType("TIMESTAMP(6)"),)])
        self.assertEqual(str(row_data),
                         str(RowType.from_dict(row_data.to_dict())))

    def test_struct_from_paimon_to_pyarrow(self):
        paimon_row = RowType(
            nullable=True,
            fields=[
                DataField(0, "field1", AtomicType("INT")),
                DataField(1, "field2", AtomicType("STRING")),
                DataField(2, "field3", AtomicType("DOUBLE"))
            ]
        )
        pa_struct = PyarrowFieldParser.from_paimon_type(paimon_row)

        self.assertTrue(pa.types.is_struct(pa_struct))
        self.assertEqual(len(pa_struct), 3)
        self.assertEqual(pa_struct[0].name, "field1")
        self.assertEqual(pa_struct[1].name, "field2")
        self.assertEqual(pa_struct[2].name, "field3")
        self.assertTrue(pa.types.is_int32(pa_struct[0].type))
        self.assertTrue(pa.types.is_string(pa_struct[1].type))
        self.assertTrue(pa.types.is_float64(pa_struct[2].type))

    def test_struct_from_pyarrow_to_paimon(self):
        pa_struct = pa.struct([
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
            pa.field("score", pa.float64())
        ])
        paimon_row = PyarrowFieldParser.to_paimon_type(pa_struct, nullable=True)
        
        self.assertIsInstance(paimon_row, RowType)
        self.assertTrue(paimon_row.nullable)
        self.assertEqual(len(paimon_row.fields), 3)
        self.assertEqual(paimon_row.fields[0].name, "name")
        self.assertEqual(paimon_row.fields[1].name, "age")
        self.assertEqual(paimon_row.fields[2].name, "score")
        self.assertEqual(paimon_row.fields[0].type.type, "STRING")
        self.assertEqual(paimon_row.fields[1].type.type, "INT")
        self.assertEqual(paimon_row.fields[2].type.type, "DOUBLE")

    def test_nested_field_roundtrip(self):
        nested_field = RowType(
            nullable=True,
            fields=[
                DataField(0, "inner_field1", AtomicType("STRING")),
                DataField(1, "inner_field2", AtomicType("INT"))
            ]
        )
        paimon_row = RowType(
            nullable=True,
            fields=[
                DataField(0, "outer_field1", AtomicType("BIGINT")),
                DataField(1, "nested", nested_field)
            ]
        )
        pa_struct = PyarrowFieldParser.from_paimon_type(paimon_row)

        converted_paimon_row = PyarrowFieldParser.to_paimon_type(pa_struct, nullable=True)
        self.assertIsInstance(converted_paimon_row, RowType)
        self.assertEqual(len(converted_paimon_row.fields), 2)
        self.assertEqual(converted_paimon_row.fields[0].name, "outer_field1")
        self.assertEqual(converted_paimon_row.fields[1].name, "nested")
        
        converted_nested_field = converted_paimon_row.fields[1].type
        self.assertIsInstance(converted_nested_field, RowType)
        self.assertEqual(len(converted_nested_field.fields), 2)
        self.assertEqual(converted_nested_field.fields[0].name, "inner_field1")
        self.assertEqual(converted_nested_field.fields[1].name, "inner_field2")

    def test_vector_pyarrow_roundtrip(self):
        paimon_vector = VectorType(True, AtomicType("FLOAT"), 3)
        pa_type = PyarrowFieldParser.from_paimon_type(paimon_vector)

        self.assertTrue(pa.types.is_fixed_size_list(pa_type))
        self.assertEqual(pa_type.list_size, 3)
        self.assertTrue(pa.types.is_float32(pa_type.value_type))

        converted_paimon_vector = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
        self.assertEqual(converted_paimon_vector, paimon_vector)

        avro_type = PyarrowFieldParser.to_avro_type(pa_type, "embedding")
        self.assertEqual(avro_type, {"type": "array", "items": "float"})

    def test_time_type(self):
        pa_type = PyarrowFieldParser.from_paimon_type(AtomicType("TIME"))
        self.assertEqual(pa_type, pa.time32('ms'))

        pa_type_with_precision = PyarrowFieldParser.from_paimon_type(AtomicType("TIME(3)"))
        self.assertEqual(pa_type_with_precision, pa.time32('ms'))

        paimon_type = PyarrowFieldParser.to_paimon_type(pa.time32('ms'), nullable=True)
        self.assertEqual(paimon_type.type, "TIME(0)")

    def test_avro_timestamp_seconds_maps_and_roundtrips(self):
        import datetime
        import os
        import tempfile

        import fastavro

        from pypaimon.filesystem.local_file_io import LocalFileIO

        # TIMESTAMP(0) -> pyarrow 's'; Avro's coarsest timestamp is millis, which
        # holds seconds losslessly and matches Java AvroSchemaConverter (precision<=3).
        self.assertEqual(
            PyarrowFieldParser.to_avro_type(pa.timestamp('s'), 'ts', 'r'),
            {"type": "long", "logicalType": "timestamp-millis"})
        self.assertEqual(
            PyarrowFieldParser.to_avro_type(pa.timestamp('s', tz='UTC'), 'ts', 'r'),
            {"type": "long", "logicalType": "local-timestamp-millis"})
        # nanos stays rejected: Avro has no nanos logical type (matches Java precision>6).
        with self.assertRaises(ValueError):
            PyarrowFieldParser.to_avro_type(pa.timestamp('ns'), 'ts', 'r')

        # A second-granularity value round-trips unchanged (no seconds/millis mixup).
        ts = datetime.datetime(2024, 1, 2, 3, 4, 5)
        table = pa.table({"ts": pa.array([ts], pa.timestamp('s'))})
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "data.avro")
            LocalFileIO().write_avro(path, table)
            with open(path, 'rb') as f:
                rows = list(fastavro.reader(f))
        self.assertEqual(rows[0]["ts"].replace(tzinfo=None), ts)
