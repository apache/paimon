################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Unit tests for the built-in :class:`FieldAggregator` subclasses.

Drives each aggregator directly to pin down the value semantics
(reset behaviour, null handling, type validation) without going
through the merge function or the read pipeline. End-to-end coverage
on real PK tables lives in ``test_aggregation_e2e.py``.
"""

import datetime
import unittest
from decimal import Decimal

from pypaimon.read.reader.aggregate import create_field_aggregator
from pypaimon.read.reader.aggregate.aggregators import (
    FieldBoolAndAgg,
    FieldBoolOrAgg,
    FieldFirstNonNullValueAgg,
    FieldFirstValueAgg,
    FieldLastNonNullValueAgg,
    FieldLastValueAgg,
    FieldMaxAgg,
    FieldMinAgg,
    FieldPrimaryKeyAgg,
    FieldSumAgg,
)
from pypaimon.schema.data_types import AtomicType


def _make(identifier, sql_type):
    """Build an aggregator through the public registry path so we also
    exercise the registered factory (including its type validation).
    """
    return create_field_aggregator(
        AtomicType(sql_type), "field0", identifier, options=None
    )


class FieldPrimaryKeyAggTest(unittest.TestCase):

    def test_returns_input_field(self):
        agg = _make("primary_key", "BIGINT")
        self.assertIsInstance(agg, FieldPrimaryKeyAgg)
        self.assertEqual(agg.agg(None, 5), 5)
        self.assertEqual(agg.agg(99, 5), 5)
        self.assertIsNone(agg.agg(5, None))


class FieldLastValueAggTest(unittest.TestCase):

    def test_last_value_wins_including_null(self):
        agg = _make("last_value", "VARCHAR")
        self.assertIsInstance(agg, FieldLastValueAgg)
        self.assertEqual(agg.agg(None, "a"), "a")
        self.assertEqual(agg.agg("a", "b"), "b")
        # Crucially: a later null replaces the accumulator (unlike
        # last_non_null_value).
        self.assertIsNone(agg.agg("a", None))


class FieldLastNonNullValueAggTest(unittest.TestCase):

    def test_null_inputs_are_absorbed(self):
        agg = _make("last_non_null_value", "INT")
        self.assertIsInstance(agg, FieldLastNonNullValueAgg)
        self.assertEqual(agg.agg(None, 1), 1)
        self.assertEqual(agg.agg(1, 2), 2)
        self.assertEqual(agg.agg(2, None), 2)
        self.assertIsNone(agg.agg(None, None))


class FieldFirstValueAggTest(unittest.TestCase):

    def test_first_value_locks_after_first_add(self):
        agg = _make("first_value", "VARCHAR")
        self.assertIsInstance(agg, FieldFirstValueAgg)
        # First add returns input, even if input is None.
        self.assertIsNone(agg.agg(None, None))
        # Subsequent adds preserve the accumulator (None) regardless of input.
        self.assertIsNone(agg.agg(None, "later"))

    def test_reset_re_arms_first_value(self):
        agg = _make("first_value", "INT")
        self.assertEqual(agg.agg(None, 5), 5)
        self.assertEqual(agg.agg(5, 9), 5)  # locked
        agg.reset()
        # After reset the next add is treated as the first again.
        self.assertEqual(agg.agg(None, 42), 42)


class FieldFirstNonNullValueAggTest(unittest.TestCase):

    def test_first_non_null_skips_nulls(self):
        agg = _make("first_non_null_value", "INT")
        self.assertIsInstance(agg, FieldFirstNonNullValueAgg)
        # Initial null does not lock — accumulator stays None.
        self.assertIsNone(agg.agg(None, None))
        # First non-null locks.
        self.assertEqual(agg.agg(None, 7), 7)
        # Subsequent values do not replace the locked first.
        self.assertEqual(agg.agg(7, 99), 7)
        self.assertEqual(agg.agg(7, None), 7)

    def test_reset_re_arms_first_non_null(self):
        agg = _make("first_non_null_value", "INT")
        self.assertEqual(agg.agg(None, 1), 1)
        self.assertEqual(agg.agg(1, 2), 1)
        agg.reset()
        self.assertEqual(agg.agg(None, 9), 9)


class FieldSumAggTest(unittest.TestCase):

    def test_int_sum(self):
        agg = _make("sum", "BIGINT")
        self.assertIsInstance(agg, FieldSumAgg)
        self.assertEqual(agg.agg(None, 5), 5)
        self.assertEqual(agg.agg(5, 7), 12)

    def test_float_sum(self):
        agg = _make("sum", "DOUBLE")
        self.assertAlmostEqual(agg.agg(1.5, 2.25), 3.75)

    def test_decimal_sum(self):
        agg = _make("sum", "DECIMAL(10,2)")
        result = agg.agg(Decimal("1.23"), Decimal("4.56"))
        self.assertEqual(result, Decimal("5.79"))

    def test_null_inputs_return_non_null_operand(self):
        agg = _make("sum", "INT")
        self.assertEqual(agg.agg(None, 5), 5)
        self.assertEqual(agg.agg(5, None), 5)
        self.assertIsNone(agg.agg(None, None))

    def test_non_numeric_type_rejected_at_construction(self):
        with self.assertRaises(ValueError) as ctx:
            _make("sum", "VARCHAR")
        self.assertIn("numeric", str(ctx.exception))


class FieldMaxAggTest(unittest.TestCase):

    def test_numeric_max(self):
        agg = _make("max", "INT")
        self.assertIsInstance(agg, FieldMaxAgg)
        self.assertEqual(agg.agg(3, 7), 7)
        self.assertEqual(agg.agg(7, 3), 7)
        self.assertEqual(agg.agg(5, 5), 5)

    def test_string_max(self):
        agg = _make("max", "VARCHAR")
        self.assertEqual(agg.agg("apple", "banana"), "banana")
        self.assertEqual(agg.agg("banana", "apple"), "banana")

    def test_date_max(self):
        agg = _make("max", "DATE")
        d1 = datetime.date(2020, 1, 1)
        d2 = datetime.date(2025, 6, 15)
        self.assertEqual(agg.agg(d1, d2), d2)
        self.assertEqual(agg.agg(d2, d1), d2)

    def test_null_inputs_return_non_null_operand(self):
        agg = _make("max", "INT")
        self.assertEqual(agg.agg(None, 5), 5)
        self.assertEqual(agg.agg(5, None), 5)
        self.assertIsNone(agg.agg(None, None))


class FieldMinAggTest(unittest.TestCase):

    def test_numeric_min(self):
        agg = _make("min", "INT")
        self.assertIsInstance(agg, FieldMinAgg)
        self.assertEqual(agg.agg(3, 7), 3)
        self.assertEqual(agg.agg(7, 3), 3)
        self.assertEqual(agg.agg(5, 5), 5)

    def test_string_min(self):
        agg = _make("min", "VARCHAR")
        self.assertEqual(agg.agg("apple", "banana"), "apple")

    def test_null_inputs_return_non_null_operand(self):
        agg = _make("min", "INT")
        self.assertEqual(agg.agg(None, 5), 5)
        self.assertEqual(agg.agg(5, None), 5)
        self.assertIsNone(agg.agg(None, None))


class FieldBoolOrAggTest(unittest.TestCase):

    def test_truth_table(self):
        agg = _make("bool_or", "BOOLEAN")
        self.assertIsInstance(agg, FieldBoolOrAgg)
        self.assertTrue(agg.agg(True, True))
        self.assertTrue(agg.agg(True, False))
        self.assertTrue(agg.agg(False, True))
        self.assertFalse(agg.agg(False, False))

    def test_null_inputs_return_non_null_operand(self):
        agg = _make("bool_or", "BOOLEAN")
        self.assertTrue(agg.agg(None, True))
        self.assertFalse(agg.agg(False, None))
        self.assertIsNone(agg.agg(None, None))

    def test_non_boolean_type_rejected_at_construction(self):
        with self.assertRaises(ValueError) as ctx:
            _make("bool_or", "INT")
        self.assertIn("BOOLEAN", str(ctx.exception))


class FieldBoolAndAggTest(unittest.TestCase):

    def test_truth_table(self):
        agg = _make("bool_and", "BOOLEAN")
        self.assertIsInstance(agg, FieldBoolAndAgg)
        self.assertTrue(agg.agg(True, True))
        self.assertFalse(agg.agg(True, False))
        self.assertFalse(agg.agg(False, True))
        self.assertFalse(agg.agg(False, False))

    def test_null_inputs_return_non_null_operand(self):
        agg = _make("bool_and", "BOOLEAN")
        self.assertTrue(agg.agg(None, True))
        self.assertFalse(agg.agg(False, None))
        self.assertIsNone(agg.agg(None, None))

    def test_non_boolean_type_rejected_at_construction(self):
        with self.assertRaises(ValueError) as ctx:
            _make("bool_and", "VARCHAR")
        self.assertIn("BOOLEAN", str(ctx.exception))


class RegistrationTest(unittest.TestCase):
    """Sanity check that all 10 expected aggregators (the primary-key
    placeholder plus 9 value aggregators) are registered when the
    package is imported. Guards against future refactors silently
    dropping a registration.
    """

    EXPECTED = frozenset([
        "primary_key",
        "last_value", "last_non_null_value",
        "first_value", "first_non_null_value",
        "sum", "max", "min",
        "bool_or", "bool_and",
    ])

    def test_all_expected_aggregators_registered(self):
        from pypaimon.read.reader.aggregate import _FACTORIES
        registered = set(_FACTORIES.keys())
        missing = self.EXPECTED - registered
        self.assertEqual(missing, set(),
                         "Missing built-in aggregators: {}".format(missing))


if __name__ == '__main__':
    unittest.main()
