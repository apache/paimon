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

from pypaimon.schema.data_types import DataField, AtomicType, ArrayType, MultisetType, MapType, RowType


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
