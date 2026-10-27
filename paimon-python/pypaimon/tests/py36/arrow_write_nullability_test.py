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

"""Write validation also supports the Arrow 6 API used with Python 3.6."""

import unittest

import pyarrow as pa

from pypaimon.schema.arrow_schema import normalize_arrow_write_nullability


class ArrowWriteNullabilityTest(unittest.TestCase):

    def test_map_null_parents_slices_and_required_values(self):
        values = pa.array([None, None, 7, None], type=pa.int32())
        original = pa.MapArray.from_arrays(
            pa.array([0, 1, 2, 3, 4], type=pa.int32()),
            pa.array(['outside', 'hidden', 'visible', 'outside']), values)
        validity = pa.array([True, False, True, True]).buffers()[1]
        array = pa.Array.from_buffers(
            original.type, len(original), [validity, original.buffers()[1]],
            children=[pa.StructArray.from_arrays(
                [original.keys, original.items],
                fields=[original.type.key_field, original.type.item_field])])
        declared = pa.map_(pa.string(), pa.field('value', pa.int32(), nullable=False))
        # Arrow 6's MAP type equality ignores item nullability. A required
        # sibling field makes the ROW retargeting explicit on every version.
        row_type = pa.struct([
            pa.field('attrs', declared), pa.field('tag', pa.int32(), nullable=False)])
        target = pa.schema([('nested', row_type)])

        def source_data(factory, offset, length):
            nested = pa.StructArray.from_arrays([
                array.slice(offset, length), pa.array([1] * length, type=pa.int32())
            ], names=['attrs', 'tag'])
            return factory.from_arrays([nested], names=['nested'])

        for factory in (pa.Table, pa.RecordBatch):
            for offset, length in ((1, 2), (1, 1), (4, 0)):
                source = source_data(factory, offset, length)
                result = normalize_arrow_write_nullability(source, target)
                self.assertEqual(result.to_pydict(), source.to_pydict())
                self.assertEqual(result.schema.field('nested').type, row_type)
                self.assertFalse(result.schema.field('nested').type[0].type.item_field.nullable)
                result.validate(full=True)
            source = source_data(factory, 2, 2)
            with self.assertRaisesRegex(ValueError, 'nested.attrs.value.*NULL'):
                normalize_arrow_write_nullability(source, target)
