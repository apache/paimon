"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import unittest
from parameterized import parameterized
import pyarrow as pa

from pypaimon.schema.data_types import (DataField, AtomicType, ArrayType, MultisetType, MapType,
                                        RowType, PyarrowFieldParser)


class DataTypesTest(unittest.TestCase):
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

    def test_map_type(self):
        self.assertEqual(str(MapType(True, AtomicType("STRING"), AtomicType("TIMESTAMP(6)"))),
                         "MAP<STRING, TIMESTAMP(6)>")

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

    def test_timestamp_without_timezone_millis_to_avro(self):
        """Test PyArrow timestamp[ms] without timezone converts to local-timestamp-millis."""
        field_type = pa.timestamp('ms')
        avro_type = PyarrowFieldParser.to_avro_type(field_type, "ts_field")

        self.assertEqual(avro_type["type"], "long")
        self.assertEqual(avro_type["logicalType"], "local-timestamp-millis")

    def test_timestamp_without_timezone_micros_to_avro(self):
        """Test PyArrow timestamp[us] without timezone converts to local-timestamp-micros."""
        field_type = pa.timestamp('us')
        avro_type = PyarrowFieldParser.to_avro_type(field_type, "ts_field")

        self.assertEqual(avro_type["type"], "long")
        self.assertEqual(avro_type["logicalType"], "local-timestamp-micros")

    def test_timestamp_with_timezone_micros_to_avro(self):
        """Test PyArrow timestamp[us] with timezone converts to timestamp-micros."""
        field_type = pa.timestamp('us', tz='UTC')
        avro_type = PyarrowFieldParser.to_avro_type(field_type, "ts_field")

        self.assertEqual(avro_type["type"], "long")
        self.assertEqual(avro_type["logicalType"], "timestamp-micros")

    def test_timestamp_with_non_utc_timezone_to_avro(self):
        """Test PyArrow timestamp with non-UTC timezone converts to timestamp-millis."""
        field_type = pa.timestamp('ms', tz='Asia/Shanghai')
        avro_type = PyarrowFieldParser.to_avro_type(field_type, "ts_field")

        self.assertEqual(avro_type["type"], "long")
        self.assertEqual(avro_type["logicalType"], "timestamp-millis")

    def test_timestamp_unsupported_unit_raises_error(self):
        """Test PyArrow timestamp with unsupported unit raises ValueError."""
        field_type = pa.timestamp('ns')

        with self.assertRaises(ValueError) as context:
            PyarrowFieldParser.to_avro_type(field_type, "ts_field")

        self.assertIn("ns", str(context.exception))

    def test_timestamp_in_schema_to_avro(self):
        """Test timestamp field in PyArrow schema converts correctly to Avro schema."""
        pa_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("local_ts", pa.timestamp('ms')),
            pa.field("utc_ts", pa.timestamp('us', tz='UTC'))
        ])

        avro_schema = PyarrowFieldParser.to_avro_schema(pa_schema, name="test_record")

        self.assertEqual(avro_schema["type"], "record")
        self.assertEqual(avro_schema["name"], "test_record")
        self.assertEqual(len(avro_schema["fields"]), 3)

        local_ts_field = avro_schema["fields"][1]
        self.assertEqual(local_ts_field["name"], "local_ts")
        local_ts_type = local_ts_field["type"][1]
        self.assertEqual(local_ts_type["logicalType"], "local-timestamp-millis")

        utc_ts_field = avro_schema["fields"][2]
        self.assertEqual(utc_ts_field["name"], "utc_ts")
        utc_ts_type = utc_ts_field["type"][1]
        self.assertEqual(utc_ts_type["logicalType"], "timestamp-micros")
