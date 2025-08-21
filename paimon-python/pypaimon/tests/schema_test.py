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

from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType, PyarrowFieldParser)
from pypaimon.schema.schema import Schema
from pypaimon.schema.table_schema import TableSchema


class SchemaTestCase(unittest.TestCase):
    def test_types(self):
        data_fields = [
            DataField(0, "name", AtomicType('INT'), 'desc  name'),
            DataField(1, "arr", ArrayType(True, AtomicType('INT')), 'desc arr1'),
            DataField(2, "map1",
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
