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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest.mock import Mock

from pypaimon.read.read_builder import ReadBuilder
from pypaimon.read.read_type import OutputProjection
from pypaimon.schema.data_types import AtomicType, DataField


class NamedVariantProjectionTest(unittest.TestCase):

    def test_string_literals_work_on_supported_python_versions(self):
        table = Mock()
        table.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('VARIANT')),
        ]
        table.options.row_tracking_enabled.return_value = False

        builder = ReadBuilder(table).with_projection({
            'identifier': 'id',
            'x': "try_variant_get(payload, '$.x', 'float')",
            'y': 'variant_get("payload", "$.y", "float")',
        })

        self.assertEqual([f.name for f in builder.read_type()], ['id', 'payload'])
        self.assertEqual(
            [field.description for field in builder.read_type()[1].type.fields],
            ['__VARIANT_METADATA$.x;false;UTC',
             '__VARIANT_METADATA$.y;true;UTC'])
        self.assertEqual(builder._output_projection, OutputProjection([
            ('identifier', ['id']),
            ('x', ['payload', '0']),
            ('y', ['payload', '1']),
        ], True))


if __name__ == '__main__':
    unittest.main()
