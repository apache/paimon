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

import unittest
from decimal import Decimal as BigDecimal

from pypaimon.data.decimal import Decimal


class DecimalTest(unittest.TestCase):

    def test_from_big_decimal(self):
        d = Decimal.from_big_decimal(BigDecimal("12.34"), precision=10, scale=2)

        self.assertIsNotNone(d)
        self.assertEqual(d.precision, 10)
        self.assertEqual(d.scale, 2)
        self.assertEqual(d.to_big_decimal(), BigDecimal("12.34"))
        self.assertEqual(d.to_unscaled_long(), 1234)

    def test_from_big_decimal_overflow(self):
        d = Decimal.from_big_decimal(BigDecimal("12345678901.23"), precision=10, scale=2)
        self.assertIsNone(d)

    def test_from_unscaled_long(self):
        d = Decimal.from_unscaled_long(1234, precision=10, scale=2)

        self.assertEqual(d.precision, 10)
        self.assertEqual(d.scale, 2)
        self.assertEqual(d.to_big_decimal(), BigDecimal("12.34"))

    def test_from_unscaled_bytes(self):
        d = Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)

        d2 = Decimal.from_unscaled_bytes(
            d.to_unscaled_bytes(),
            precision=10,
            scale=2,
        )

        self.assertEqual(d2, d)

    def test_zero(self):
        d = Decimal.zero(precision=10, scale=2)
        self.assertEqual(d.to_big_decimal(), BigDecimal("0.00"))

    def test_copy(self):
        d = Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)
        copy = d.copy()

        self.assertEqual(copy, d)
        self.assertIsNot(copy, d)

    def test_compare(self):
        d1 = Decimal.from_big_decimal(BigDecimal("1.23"), 10, 2)
        d2 = Decimal.from_big_decimal(BigDecimal("2.34"), 10, 2)

        self.assertLess(d1.compare_to(d2), 0)
        self.assertGreater(d2.compare_to(d1), 0)
        self.assertEqual(d1.compare_to(d1.copy()), 0)

    def test_hash(self):
        d1 = Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)
        d2 = Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)

        self.assertEqual(hash(d1), hash(d2))

    def test_to_big_decimal(self):
        d = Decimal.from_unscaled_long(1234, 10, 2)
        self.assertEqual(d.to_big_decimal(), BigDecimal("12.34"))

    def test_to_unscaled_long(self):
        d = Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)
        self.assertEqual(d.to_unscaled_long(), 1234)

    def test_to_unscaled_bytes(self):
        d = Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)

        self.assertEqual(
            int.from_bytes(d.to_unscaled_bytes(), "big", signed=True),
            1234,
        )

    def test_to_string(self):
        self.assertEqual(
            str(Decimal.from_big_decimal(BigDecimal("12.34"), 10, 2)),
            "12.34",
        )

        self.assertEqual(
            str(Decimal.from_big_decimal(BigDecimal("0.0000000000000000001"), 39, 19)),
            "0.0000000000000000001",
        )

    def test_is_compact(self):
        self.assertTrue(Decimal.is_compact_precision(18))
        self.assertFalse(Decimal.is_compact_precision(19))

        self.assertTrue(Decimal.from_big_decimal(BigDecimal("1"), 18, 0).is_compact())
        self.assertFalse(Decimal.from_big_decimal(BigDecimal("1"), 19, 0).is_compact())

    def test_extract_decimal_precision_scale(self):
        self.assertEqual(Decimal.extract_decimal_precision_scale("DECIMAL"), (10, 0))
        self.assertEqual(Decimal.extract_decimal_precision_scale("DECIMAL(20)"), (20, 0))
        self.assertEqual(Decimal.extract_decimal_precision_scale("DECIMAL(20,5)"), (20, 5))
        self.assertEqual(Decimal.extract_decimal_precision_scale("NUMERIC(18,2)"), (18, 2))
        self.assertEqual(Decimal.extract_decimal_precision_scale("DEC(8,3)"), (8, 3))

    def test_extract_decimal_precision_scale_invalid(self):
        with self.assertRaises(ValueError):
            Decimal.extract_decimal_precision_scale("INT")
