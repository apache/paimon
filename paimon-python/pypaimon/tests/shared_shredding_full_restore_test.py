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

from datetime import time
import unittest

import pyarrow as pa

from pypaimon.data.map_shared_shredding import assemble_shared_shredding_map


class SharedShreddingFullRestoreTest(unittest.TestCase):
    def test_entry_order_duplicates_unknown_ids_and_slices(self):
        for value_type, value in [
            (pa.int64(), 9007199254740993),
            (pa.list_(pa.int64()), [None, 1]),
            (pa.struct([("x", pa.int64())]), {"x": 1}),
            (pa.map_(pa.string(), pa.int64()), [("x", None)]),
        ]:
            for mapping_type in [pa.list_(pa.int32()), pa.large_list(pa.int32())]:
                with self.subTest(value_type=value_type, mapping_type=mapping_type):
                    mapping = pa.array([[0, 0], [-1, 99], None, [-1, -1], [1, 0]], type=mapping_type)
                    values = pa.array([value] * 5, type=value_type)
                    overflow = pa.array(
                        [[(0, value), (99, value), (-1, None)], [(1, None)], [(0, value)], None, []],
                        type=pa.map_(pa.int32(), value_type),
                    )
                    column = pa.StructArray.from_arrays(
                        [mapping, values, pa.nulls(5, type=value_type), overflow],
                        names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
                        mask=pa.array([False, False, True, False, False]),
                    )
                    expected = [
                        [("a", value), ("a", None), ("a", value), ("neg", None)],
                        [("b", None)],
                        None,
                        [],
                        [("b", value), ("a", None)],
                    ]
                    column = pa.concat_arrays([column] * 9)
                    expected = expected * 9
                    for start, size in [(0, 45), (1, 44), (3, 31), (3, 32), (3, 33), (3, 1), (2, 0)]:
                        result = assemble_shared_shredding_map(
                            column.slice(start, size), pa.map_(pa.string(), value_type), {0: "a", 1: "b", -1: "neg"}, 2
                        )
                        end = start + size
                        self.assertEqual(expected[start:end], result.to_pylist())
                        self.assertEqual(pa.map_(pa.string(), value_type), result.type)

    def test_mapping_validation_and_missing_columns(self):
        for mapping, message in [([0], "length"), ([None, 0], "contain null"), (None, "length"), ([0, 1], "Missing")]:
            column = pa.StructArray.from_arrays(
                [pa.array([[-1, -1]] * 32 + [mapping], type=pa.list_(pa.int32()))], names=["__field_mapping"]
            )
            with self.subTest(mapping=mapping):
                with self.assertRaisesRegex(ValueError, message):
                    assemble_shared_shredding_map(column, pa.map_(pa.string(), pa.int64()), {0: "a", 1: "b"}, 2)
        column = pa.StructArray.from_arrays(
            [pa.array([[-1, 99]], type=pa.list_(pa.int32()))], names=["__field_mapping"]
        )
        self.assertEqual(
            [[]], assemble_shared_shredding_map(column, pa.map_(pa.string(), pa.int64()), {0: "a"}, 2).to_pylist()
        )

    def test_orc_time_and_no_physical_columns(self):
        column = pa.StructArray.from_arrays(
            [
                pa.array([[], []], type=pa.list_(pa.int32())),
                pa.array([[(0, 1234)], []], type=pa.map_(pa.int32(), pa.int32())),
            ],
            names=["__field_mapping", "__overflow"],
        )
        result = assemble_shared_shredding_map(column, pa.map_(pa.string(), pa.time32("ms")), {0: "a"}, 0)
        self.assertEqual([[("a", time(0, 0, 1, 234000))], []], result.to_pylist())

    def test_large_integer_ids_are_not_rounded(self):
        field_id = 2**63 + 1
        column = pa.StructArray.from_arrays(
            [pa.array([[field_id]] * 34, type=pa.large_list(pa.uint64())), pa.array(range(34), type=pa.int64())],
            names=["__field_mapping", "__col_0"],
        )
        result = assemble_shared_shredding_map(
            column.slice(1, 32), pa.map_(pa.string(), pa.int64()), {field_id: "a"}, 1
        )
        self.assertEqual([[("a", row)] for row in range(1, 33)], result.to_pylist())
