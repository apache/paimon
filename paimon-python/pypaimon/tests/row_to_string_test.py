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

    def test_decimal_never_uses_scientific_notation(self):
        # Java Decimal.toString is BigDecimal.toPlainString; str(Decimal)
        # would give "0E-9" and "-1E-9" here
        self.assertEqual("0.000000000",
                         _cast("DECIMAL(20, 9)", Decimal("0E-9")))
        self.assertEqual("-0.000000001",
                         _cast("DECIMAL(20, 9)", Decimal("-1E-9")))
        self.assertEqual("0.000000000000000000",
                         _cast("DECIMAL(38, 18)", Decimal("0E-18")))

    def test_row_with_a_float_field_is_not_rendered(self):
        self.assertIsNone(cast_row_to_string(_row(("INT", 1),
                                                  ("FLOAT", 1.5))))
        self.assertIsNone(cast_row_to_string(_row(("DOUBLE", 1.5))))
        self.assertIsNone(cast_row_to_string(_row(("REAL", 1.5))))
        self.assertIsNone(cast_row_to_string(_row(("FLOAT NOT NULL", 1.5))))

    def test_float_field_stops_the_row_even_when_its_value_is_null(self):
        # the check is on the declared type, so all rows of a table agree
        self.assertIsNone(cast_row_to_string(_row(("INT", 1),
                                                  ("DOUBLE", None))))

    def test_row_with_a_local_zoned_timestamp_is_not_rendered(self):
        # Java formats these in TimeZone.getDefault(), so the same manifest
        # reads differently depending on where the query runs
        value = datetime(2024, 1, 2, 3, 4, 5)
        self.assertIsNone(cast_row_to_string(_row(("TIMESTAMP_LTZ(3)", value))))
        self.assertIsNone(cast_row_to_string(
            _row(("TIMESTAMP(3) WITH LOCAL TIME ZONE", value))))

    def test_unrenderable_values_are_rejected(self):
        self.assertRaises(ValueError, _cast, "FLOAT", 1.5)
        self.assertRaises(ValueError, _cast, "DOUBLE", 1.5)
        self.assertRaises(ValueError, _cast, "TIMESTAMP_LTZ(3)",
                          datetime(2024, 1, 2, 3, 4, 5))
        self.assertRaises(ValueError, _cast, "BYTES", b"ab")

    def test_row_with_a_binary_field_is_not_rendered(self):
        # malformed UTF-8 yields a different replacement char count than the
        # JDK decoder gives, so binary is not rendered at all
        self.assertIsNone(cast_row_to_string(_row(("BYTES", b"ab"))))
        self.assertIsNone(cast_row_to_string(_row(("BINARY(2)", b"ab"))))
        self.assertIsNone(cast_row_to_string(_row(("VARBINARY(10)", b"ab"))))

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

    def test_time_keeps_a_zero_that_a_truncated_tail_sits_behind(self):
        # 101 ms at precision 2 is ".10" in Java, not ".1": it stops only
        # once the remaining fraction is exactly zero
        self.assertEqual("03:04:05.10", _cast("TIME(2)", time(3, 4, 5, 101000)))
        self.assertEqual("03:04:05.00", _cast("TIME(2)", time(3, 4, 5, 1000)))
        self.assertEqual("03:04:05.0", _cast("TIME(2)", time(3, 4, 5)))
        self.assertEqual("03:04:05.01", _cast("TIME(2)", time(3, 4, 5, 10000)))

    def test_unknown_type_falls_back_to_str(self):
        self.assertEqual("x", _cast("SOMETHING_NEW", "x"))


if __name__ == "__main__":
    unittest.main()
