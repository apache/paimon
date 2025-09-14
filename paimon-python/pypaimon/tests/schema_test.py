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

import pyarrow

from pypaimon import Schema
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, PyarrowFieldParser)
from pypaimon.schema.table_schema import TableSchema


class SchemaTestCase(unittest.TestCase):
    def test_types(self):
        data_fields = [
            DataField(0, "f0", AtomicType('TINYINT'), 'desc'),
            DataField(1, "f1", AtomicType('SMALLINT'), 'desc'),
            DataField(2, "f2", AtomicType('INT'), 'desc'),
            DataField(3, "f3", AtomicType('BIGINT'), 'desc'),
            DataField(4, "f4", AtomicType('FLOAT'), 'desc'),
            DataField(5, "f5", AtomicType('DOUBLE'), 'desc'),
            DataField(6, "f6", AtomicType('BOOLEAN'), 'desc'),
            DataField(7, "f7", AtomicType('STRING'), 'desc'),
            DataField(8, "f8", AtomicType('BINARY(12)'), 'desc'),
            DataField(9, "f9", AtomicType('DECIMAL(10, 6)'), 'desc'),
            DataField(10, "f10", AtomicType('BYTES'), 'desc'),
            DataField(11, "f11", AtomicType('DATE'), 'desc'),
            DataField(12, "f12", AtomicType('TIME(0)'), 'desc'),
            DataField(13, "f13", AtomicType('TIME(3)'), 'desc'),
            DataField(14, "f14", AtomicType('TIME(6)'), 'desc'),
            DataField(15, "f15", AtomicType('TIME(9)'), 'desc'),
            DataField(16, "f16", AtomicType('TIMESTAMP(0)'), 'desc'),
            DataField(17, "f17", AtomicType('TIMESTAMP(3)'), 'desc'),
            DataField(18, "f18", AtomicType('TIMESTAMP(6)'), 'desc'),
            DataField(19, "f19", AtomicType('TIMESTAMP(9)'), 'desc'),
            DataField(20, "arr", ArrayType(True, AtomicType('INT')), 'desc arr1'),
            DataField(21, "map1",
                      MapType(False, AtomicType('INT', False),
                              MapType(False, AtomicType('INT', False), AtomicType('INT', False))),
                      'desc map1'),
        ]
        table_schema = TableSchema(TableSchema.CURRENT_VERSION, len(data_fields), data_fields,
                                   max(field.id for field in data_fields),
                                   [], [], {}, "")
        pa_fields = []
        for field in table_schema.fields:
            pa_field = PyarrowFieldParser.from_paimon_field(field)
            pa_fields.append(pa_field)
        schema = Schema.from_pyarrow_schema(
            pa_schema=pyarrow.schema(pa_fields),
            partition_keys=table_schema.partition_keys,
            primary_keys=table_schema.primary_keys,
            options=table_schema.options,
            comment=table_schema.comment
        )
        table_schema2 = TableSchema.from_schema(len(data_fields), schema)
        l1 = []
        for field in table_schema.fields:
            l1.append(field.to_dict())
        l2 = []
        for field in table_schema2.fields:
            l2.append(field.to_dict())
        self.assertEqual(l1, l2)
