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

"""Tests that cast_row_to_string reproduces the Java cast-to-string rules."""

import struct
import unittest
from datetime import date, datetime, time
from decimal import Decimal

from pypaimon.casting.row_to_string import (cast_row_to_string,
                                            cast_value_to_string)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow


def _row(*pairs):
    fields = [DataField(i, "f" + str(i), AtomicType(t))
              for i, (t, _) in enumerate(pairs)]
    return GenericRow([v for _, v in pairs], fields)


def _cast(type_name, value):
    return cast_value_to_string(value, AtomicType(type_name))


def _f32(value):
    # a FLOAT is widened to float64 on the way in, as the reader does
    widened = struct.unpack("<f", struct.pack("<f", value))[0]
    return _cast("FLOAT", widened)


class RowToStringTest(unittest.TestCase):

    def test_none_row(self):
        self.assertIsNone(cast_row_to_string(None))

    def test_empty_row_of_unpartitioned_table(self):
        self.assertEqual("{}", cast_row_to_string(_row()))

    def test_fields_are_comma_separated(self):
        self.assertEqual("{1, a}", cast_row_to_string(_row(("INT", 1),
                                                           ("STRING", "a"))))

    def test_null_field_is_a_literal(self):
        self.assertEqual("{null, a}", cast_row_to_string(_row(("INT", None),
                                                              ("STRING", "a"))))

    def test_boolean_is_lower_case(self):
        self.assertEqual("true", _cast("BOOLEAN", True))
        self.assertEqual("false", _cast("BOOLEAN", False))

    def test_integers(self):
        self.assertEqual("-7", _cast("TINYINT", -7))
        self.assertEqual("42", _cast("INT", 42))
        self.assertEqual("9223372036854775807", _cast("BIGINT",
                                                      9223372036854775807))

    def test_decimal_keeps_its_scale(self):
        self.assertEqual("1.50", _cast("DECIMAL(10, 2)", Decimal("1.50")))

    def test_double_decimal_range(self):
        self.assertEqual("1.5", _cast("DOUBLE", 1.5))
        self.assertEqual("1.0", _cast("DOUBLE", 1.0))
        self.assertEqual("0.001", _cast("DOUBLE", 0.001))
        self.assertEqual("123456.0", _cast("DOUBLE", 123456.0))
        self.assertEqual("9999999.0", _cast("DOUBLE", 9999999.0))
        self.assertEqual("-1.5", _cast("DOUBLE", -1.5))

    def test_double_scientific_matches_java(self):
        # Java Double.toString: upper case E, no + sign, no zero padding
        self.assertEqual("1.0E7", _cast("DOUBLE", 1e7))
        self.assertEqual("1.0E-5", _cast("DOUBLE", 1e-5))
        self.assertEqual("1.0E-4", _cast("DOUBLE", 1e-4))
        self.assertEqual("1.0E20", _cast("DOUBLE", 1e20))
        self.assertEqual("1.0E-20", _cast("DOUBLE", 1e-20))
        self.assertEqual("1.23456789E300", _cast("DOUBLE", 1.23456789e300))
        self.assertEqual("1.23456789012345E14",
                         _cast("DOUBLE", 123456789012345.0))

    def test_double_signed_zero(self):
        self.assertEqual("0.0", _cast("DOUBLE", 0.0))
        self.assertEqual("-0.0", _cast("DOUBLE", -0.0))

    def test_double_non_finite_matches_java(self):
        self.assertEqual("NaN", _cast("DOUBLE", float("nan")))
        self.assertEqual("Infinity", _cast("DOUBLE", float("inf")))
        self.assertEqual("-Infinity", _cast("DOUBLE", float("-inf")))

    def test_float_prints_the_shortest_float32_form(self):
        # a float32 0.1 widened to float64 is 0.10000000149011612
        self.assertEqual("0.1", _f32(0.1))
        self.assertEqual("1.0", _f32(1.0))
        self.assertEqual("12345.678", _f32(12345.678))

    def test_float_scientific_matches_java(self):
        self.assertEqual("1.0E7", _f32(1e7))
        self.assertEqual("1.0E-5", _f32(1e-5))
        self.assertEqual("1.0E-4", _f32(1e-4))
        self.assertEqual("3.4E38", _f32(3.4e38))
        self.assertEqual("0.001", _f32(1e-3))
        self.assertEqual("9999999.0", _f32(9999999.0))
        self.assertEqual("-1.5", _f32(-1.5))

    def test_float_signed_zero_and_non_finite(self):
        self.assertEqual("0.0", _f32(0.0))
        self.assertEqual("-0.0", _f32(-0.0))
        self.assertEqual("NaN", _f32(float("nan")))
        self.assertEqual("Infinity", _f32(float("inf")))

    def test_binary_is_decoded_as_utf8(self):
        self.assertEqual("ab", _cast("BYTES", b"ab"))

    def test_date(self):
        self.assertEqual("2024-01-02", _cast("DATE", date(2024, 1, 2)))

    def test_timestamp_separator_is_a_space(self):
        value = datetime(2024, 1, 2, 3, 4, 5, 123456)
        self.assertEqual("2024-01-02 03:04:05.123456",
                         _cast("TIMESTAMP(6)", value))

    def test_timestamp_fraction_is_kept_up_to_precision(self):
        value = datetime(2024, 1, 2, 3, 4, 5, 0)
        self.assertEqual("2024-01-02 03:04:05", _cast("TIMESTAMP(0)", value))
        self.assertEqual("2024-01-02 03:04:05.000", _cast("TIMESTAMP(3)", value))
        self.assertEqual("2024-01-02 03:04:05.000000",
                         _cast("TIMESTAMP(6)", value))

    def test_timestamp_trailing_zeros_are_stripped_down_to_precision(self):
        value = datetime(2024, 1, 2, 3, 4, 5, 120000)
        self.assertEqual("2024-01-02 03:04:05.12", _cast("TIMESTAMP(0)", value))
        self.assertEqual("2024-01-02 03:04:05.120", _cast("TIMESTAMP(3)", value))

    def test_time(self):
        self.assertEqual("03:04:05", _cast("TIME", time(3, 4, 5)))
        self.assertEqual("03:04:05", _cast("TIME(0)", time(3, 4, 5)))
        self.assertEqual("03:04:05.123",
                         _cast("TIME(3)", time(3, 4, 5, 123000)))

    def test_time_keeps_one_fraction_digit_at_least(self):
        self.assertEqual("03:04:05.0", _cast("TIME(3)", time(3, 4, 5)))
        self.assertEqual("03:04:05.5", _cast("TIME(3)", time(3, 4, 5, 500000)))

    def test_unknown_type_falls_back_to_str(self):
        self.assertEqual("x", _cast("SOMETHING_NEW", "x"))


if __name__ == "__main__":
    unittest.main()
