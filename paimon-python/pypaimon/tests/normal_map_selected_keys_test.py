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

from datetime import date, datetime, time, timezone
from decimal import Decimal
import unittest

import pyarrow as pa

from pypaimon.data.map_shared_shredding import assemble_normal_map_selected_keys


class NormalMapSelectedKeysTest(unittest.TestCase):
    def test_first_match_nulls_and_slices(self):
        cases = [
            (pa.bool_(), True),
            (pa.int8(), -128),
            (pa.int16(), 32767),
            (pa.int32(), -2147483648),
            (pa.int64(), 9007199254740993),
            (pa.float32(), float("inf")),
            (pa.float64(), float("-inf")),
            (pa.decimal128(28, 6), Decimal("12345678901234567890.123456")),
            (pa.date32(), date(1960, 1, 1)),
            (pa.time32("ms"), time(12, 34, 56, 123000)),
            (pa.timestamp("us"), datetime(1960, 1, 1, 0, 0, 0, 123456)),
            (pa.timestamp("us", "UTC"), datetime(1960, 1, 1, tzinfo=timezone.utc)),
            (pa.binary(), b"\x00\xff"),
            (pa.string(), "中文"),
            (pa.list_(pa.int64()), [None, 9007199254740993]),
            (pa.struct([("x", pa.int64())]), {"x": None}),
            (pa.map_(pa.string(), pa.int64()), [("x", 1), ("x", None)]),
        ]
        for key_type in [pa.string(), pa.large_string()]:
            for value_type, value in cases:
                with self.subTest(key_type=key_type, value_type=value_type):
                    rows = [
                        [("a", value)],
                        [("a", None), ("a", value)],
                        [("b", value), ("a", value), ("a", None)],
                        [],
                        None,
                    ]
                    column = pa.array(rows, type=pa.map_(key_type, value_type))
                    expected = [
                        {"a": value, "missing": None},
                        {"a": None, "missing": None},
                        {"a": value, "missing": None},
                        {"a": None, "missing": None},
                        None,
                    ]
                    for offset, size in [(0, 5), (1, 4), (2, 0), (2, 1)]:
                        actual = assemble_normal_map_selected_keys(
                            column.slice(offset, size), ["a", "missing"], value_type
                        )
                        end = offset + size
                        self.assertEqual(expected[offset:end], actual.to_pylist())
                        self.assertEqual(value_type, actual.type[0].type)

    def test_null_map_hides_physical_entries(self):
        column = pa.MapArray.from_arrays(
            pa.array([0, None, 2, 2], type=pa.int32()), pa.array(["a", "a"]), pa.array([10, 20])
        )
        result = assemble_normal_map_selected_keys(column, ["a"], pa.int64())
        self.assertEqual([{"a": 10}, None, {"a": None}], result.to_pylist())
        self.assertEqual([10, None, None], result.field(0).to_pylist())

    def test_restores_orc_time_after_selection(self):
        column = pa.array([[("a", 1234)], [], None], type=pa.map_(pa.string(), pa.int32()))
        actual = assemble_normal_map_selected_keys(column, ["a"], pa.time32("ms"))
        self.assertEqual([{"a": time(0, 0, 1, 234000)}, {"a": None}, None], actual.to_pylist())

    def test_non_string_keys_keep_comparison_semantics(self):
        for key_type, key in [(pa.binary(), b"a"), (pa.int32(), 1)]:
            column = pa.array([[(key, 10)]], type=pa.map_(key_type, pa.int64()))
            result = assemble_normal_map_selected_keys(column, ["a"], pa.int64())
            self.assertEqual([{"a": None}], result.to_pylist())

    def test_extension_values_use_take_fallback(self):
        class IntExtension(pa.ExtensionType):
            def __init__(self):
                super().__init__(pa.int64(), "paimon.test.map-int")

            def __arrow_ext_serialize__(self):
                return b""

            @classmethod
            def __arrow_ext_deserialize__(cls, storage_type, serialized):
                return cls()

        value_type = IntExtension()
        values = pa.ExtensionArray.from_storage(value_type, pa.array([10, None, 20]))
        column = pa.MapArray.from_arrays(
            pa.array([0, 1, 3], type=pa.int32()), pa.array(["a", "a", "a"]), values
        )
        result = assemble_normal_map_selected_keys(column, ["a"], value_type)
        self.assertEqual(value_type, result.field(0).type)
        self.assertEqual([10, None], result.field(0).storage.to_pylist())
