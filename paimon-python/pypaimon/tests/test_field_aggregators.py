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
from functools import reduce
from typing import List

from pypaimon.common.options import CoreOptions, Options
from pypaimon.data import Timestamp
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
    FieldListaggAgg,
    FieldNestedUpdateAgg,
)
from pypaimon.schema.data_types import AtomicType, DataField, RowType, ArrayType
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import InternalRow


def _make(identifier, sql_type, options: CoreOptions = None):
    """Build an aggregator through the public registry path so we also
    exercise the registered factory (including its type validation).
    """
    if options is None:
        options = CoreOptions(Options.from_none())

    return create_field_aggregator(
        AtomicType(sql_type), "field0", identifier, options=options
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


class FieldListaggAggTest(unittest.TestCase):

    def test_default_delimiter(self):
        agg = _make("listagg", "STRING")
        self.assertIsInstance(agg, FieldListaggAgg)

        self.assertEqual(
            agg.agg("user1", "user2"),
            "user1,user2",
        )

    def test_default_delimiter_distinct(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.distinct': True}))
        )

        result = reduce(
            agg.agg,
            [
                "user1",
                "user2",
                "user1",
                "user3",
            ],
        )

        self.assertEqual(result, "user1,user2,user3")

    def test_custom_delimiter_empty_strings(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({
                'fields.field0.list-agg-delimiter': ';',
                'fields.field0.distinct': True
            })),
        )

        result = reduce(
            agg.agg,
            [
                "",
                "",
            ],
        )

        self.assertEqual(result, "")

    def test_default_delimiter_distinct_multi_user(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.distinct': True})),
        )

        result = reduce(
            agg.agg,
            [
                "user1",
                "user2",
                "user1,user3",
            ],
        )

        self.assertEqual(result, "user1,user2,user3")

    def test_default_delimiter_distinct_empty_left(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.distinct': True})),
        )

        result = reduce(
            agg.agg,
            [
                "",
                "user2",
                "user1,user3",
            ],
        )

        self.assertEqual(result, "user2,user1,user3")

    def test_custom_delimiter_distinct_multi_kv(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({
                'fields.field0.list-agg-delimiter': ';',
                'fields.field0.distinct': True
            }))
        )

        result = reduce(
            agg.agg,
            [
                "k1=v1;k2=v2",
                "k1=v1;k3=v3",
                "",
            ],
        )

        self.assertEqual(result, "k1=v1;k2=v2;k3=v3")

    def test_custom_delimiter_whitespace(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({
                'fields.field0.list-agg-delimiter': ' ',
                'fields.field0.distinct': True
            })),
        )

        result = reduce(
            agg.agg,
            [
                "k1=v1 k2=v2",
                " k1=v1  k3=v3",
                " ",
            ],
        )

        self.assertEqual(result, "k1=v1 k2=v2 k3=v3")

    def test_default_delimiter_distinct_multi_duplicate_kv(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.distinct': True}))
        )

        result = reduce(
            agg.agg,
            [
                "k1=v1,k2=v2",
                "k1=v1,k2=v3",
                "",
            ],
        )

        self.assertEqual(result, "k1=v1,k2=v2,k2=v3")

    def test_custom_delimiter(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.list-agg-delimiter': '-'})),
        )

        self.assertEqual(
            agg.agg("user1", "user2"),
            "user1-user2",
        )

    def test_distinct_should_not_match_substring(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.distinct': True})),
        )

        result = agg.agg(
            "abc,def,asd",
            "ab,xy",
        )

        self.assertEqual(
            result,
            "abc,def,asd,ab,xy",
        )

    def test_distinct_substring_custom_delimiter(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({
                'fields.field0.list-agg-delimiter': ';',
                'fields.field0.distinct': True
            })),
        )

        result = agg.agg(
            "abc;def;asd",
            "ab;xy;def",
        )

        self.assertEqual(
            result,
            "abc;def;asd;ab;xy",
        )

    def test_ignore_blank_values(self):
        agg = _make("listagg", "STRING")

        result = reduce(
            agg.agg,
            [
                "user1",
                "",
                " ",
                "   ",
                "\t",
                "\n",
                "\r",
                "\r\n",
                " \t\n ",
                " \t\n\r\n \u3000 ",
                "user2",
                "\u3000",
                "\u2000",
            ],
        )

        self.assertEqual(
            result,
            "user1,user2",
        )

    def test_distinct_ignore_blank_values(self):
        agg = _make(
            "listagg",
            "STRING",
            CoreOptions(Options({'fields.field0.distinct': True})),
        )

        result = reduce(
            agg.agg,
            [
                "user1",
                "user2",
                "user1",
                "user3",
                "",
                " ",
                "   ",
                "\t",
                "\n",
                "\r",
                "\r\n",
                " \t\n ",
                " \t\n\r\n \u3000 ",
                "user2",
                "user3",
                "\u3000",
                "\u2000",
            ],
        )

        self.assertEqual(
            result,
            "user1,user2,user3",
        )

    def test_first_non_blank_value_without_leading_delimiter(self):
        agg = _make("listagg", "STRING")

        acc = None
        acc = agg.agg(acc, " ")
        acc = agg.agg(acc, "first line")

        self.assertEqual(
            acc,
            "first line",
        )


class FieldNestedUpdateAggTest(unittest.TestCase):
    IDENTIFIER = "nested_update"

    DEFAULT_FIELDS = [
        DataField(0, "k0", AtomicType("INT")),
        DataField(1, "k1", AtomicType("INT")),
        DataField(2, "v", AtomicType("STRING")),
    ]

    SEQUENCE_FIELDS = [
        DataField(0, "k0", AtomicType("INT")),
        DataField(1, "k1", AtomicType("INT")),
        DataField(2, "v", AtomicType("STRING")),
        DataField(3, "seq", AtomicType("INT")),
    ]

    def _make_data_type(self, fields: List[DataField] = None):
        if fields is None:
            fields = self.DEFAULT_FIELDS
        return ArrayType(
            True,
            RowType(True, fields)
        )

    def _make(self, data_type, options: CoreOptions = None):
        """Build an aggregator through the public registry path so we also
        exercise the registered factory (including its type validation).
        """
        if options is None:
            options = CoreOptions(Options.from_none())

        return create_field_aggregator(
            data_type, "field0", self.IDENTIFIER, options=options
        )

    def row(self, *values, fields: List[DataField] = None):
        if fields is None:
            fields = self.DEFAULT_FIELDS
        return GenericRow(list(values), fields)

    def test_field_nested_update(self):
        agg = self._make(
            self._make_data_type(),
            CoreOptions(Options(
                {
                    'fields.field0.nested-key': 'k0,k1'
                }
            ))
        )
        self.assertIsInstance(agg, FieldNestedUpdateAgg)

        accumulator = None

        current: InternalRow = self.row(0, 0, "A")
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [self.row(0, 0, "A")])

        current = self.row(0, 1, "B")
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 0, "A"),
            self.row(0, 1, "B"),
        ])

        current = self.row(0, 1, "b")
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 0, "A"),
            self.row(0, 1, "b"),
        ])

        accumulator = agg.retract(accumulator, [self.row(0, 1, "b")])
        self.assertCountEqual(accumulator, [self.row(0, 0, "A")])

    def test_field_nested_append(self):
        agg = self._make(self._make_data_type())

        accumulator = None

        current = self.row(0, 1, "B")
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B")])

        current = self.row(0, 1, "b")
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B"),
            self.row(0, 1, "b"),
        ])

        accumulator = agg.retract(accumulator, [self.row(0, 1, "b")])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B")])

    def test_field_nested_update_with_sequence_field_prerequisite(self):
        # nested-sequence-field without nested-key should fail
        with self.assertRaisesRegex(
                ValueError,
                "Option 'fields.<field-name>.nested-sequence-field' requires "
                "'fields.<field-name>.nested-key' to be configured.",
        ):
            self._make(
                self._make_data_type(fields=self.SEQUENCE_FIELDS),
                CoreOptions(
                    Options(
                        {
                            "fields.field0.nested-sequence-field": "seq"
                        }
                    )
                )
            )

        seq_agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                    }
                )
            )
        )

        self.assertIsInstance(seq_agg, FieldNestedUpdateAgg)

        accumulator = None

        accumulator = seq_agg.agg(accumulator, [self.row(0, 1, "A", 1)])
        accumulator = seq_agg.agg(accumulator, [self.row(0, 1, "B", 2)])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B", 2)])

        # older sequence value should be ignored
        accumulator = seq_agg.agg(accumulator, [self.row(0, 1, "b_Late", 1)])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B", 2)])

    def test_field_nested_update_with_nested_key_null_strategy_prerequisite(self):
        # nested-key-null-strategy requires nested-key
        with self.assertRaisesRegex(
                ValueError,
                "Option 'fields.<field-name>.nested-key-null-strategy' requires "
                "'fields.<field-name>.nested-key' to be configured.",
        ):
            self._make(
                self._make_data_type(fields=self.SEQUENCE_FIELDS),
                CoreOptions(
                    Options(
                        {
                            "fields.field0.nested-key-null-strategy": "merge"
                        }
                    )
                )
            )

        # merge strategy
        merge_agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "merge",
                    }
                )
            )
        )

        merge_accumulator = None

        merge_accumulator = merge_agg.agg(merge_accumulator, [self.row(0, None, "A", 1)])
        self.assertCountEqual(merge_accumulator, [self.row(0, None, "A", 1)])

        # ignore strategy
        ignore_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "ignore",
                    }
                )
            )
        )

        ignore_accumulator = None

        ignore_accumulator = ignore_agg.agg(ignore_accumulator, [self.row(0, 1, "A", 1)])
        ignore_accumulator = ignore_agg.agg(ignore_accumulator, [self.row(0, None, "B", 2)])
        self.assertCountEqual(ignore_accumulator, [self.row(0, 1, "A", 1)])

        # error strategy
        error_agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "error",
                    }
                )
            )
        )

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            error_agg.agg(None, [self.row(0, None, "B", 2)])

    def test_field_nested_append_with_count_limit(self):
        agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.count-limit": "2"
                    }
                )
            )
        )

        accumulator = None

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B")])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B")])

        accumulator = agg.agg(accumulator, [self.row(0, 1, "b")])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B"),
            self.row(0, 1, "b"),
        ])

        # count limit = 2
        # third element should be dropped
        accumulator = agg.agg(accumulator, [self.row(0, 1, "C")])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B"),
            self.row(0, 1, "b"),
        ])

    def test_field_nested_append_with_count_limit_on_first_input_array(self):
        agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.count-limit": "2"
                    }
                )
            )
        )

        accumulator = agg.agg(
            None,
            [
                self.row(0, 1, "B"),
                None,
                self.row(0, 1, "b"),
                self.row(0, 1, "C"),
            ],
        )

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B"),
            self.row(0, 1, "b"),
        ])

    def test_field_nested_update_with_count_limit_updates_existing_key_at_limit_without_sequence(self):
        agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.count-limit": "2"
                    }
                )
            )
        )

        accumulator = None

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B")])
        accumulator = agg.agg(accumulator, [self.row(1, 2, "C")])

        # update existing key when count limit reached
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated")])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated"),
            self.row(1, 2, "C"),
        ])

        # new key exceeds limit, should be ignored
        accumulator = agg.agg(accumulator, [self.row(2, 3, "D")])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated"),
            self.row(1, 2, "C"),
        ])

    def test_field_nested_update_with_count_limit_on_first_input_array_without_sequence(self):
        agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.count-limit": "2",
                    }
                )
            )
        )

        accumulator = agg.agg(
            None,
            [
                self.row(0, 1, "B"),
                self.row(1, 2, "C"),
                self.row(2, 3, "D"),
                self.row(0, 1, "B_updated"),
            ],
        )

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated"),
            self.row(1, 2, "C"),
        ])

    def test_field_nested_update_with_sequence_field(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                    }
                )
            )
        )

        accumulator = None

        current = self.row(0, 0, "A", 1)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [current])

        current = self.row(0, 1, "B", 2)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 0, "A", 1),
            self.row(0, 1, "B", 2),
        ])

        current = self.row(0, 1, "b", 3)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 0, "A", 1),
            self.row(0, 1, "b", 3),
        ])

        # lower sequence should be ignored
        current = self.row(0, 1, "B_late", 2)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 0, "A", 1),
            self.row(0, 1, "b", 3),
        ])

        accumulator = agg.retract(accumulator, [self.row(0, 1, "b", 3)])
        self.assertCountEqual(accumulator, [self.row(0, 0, "A", 1), ])

    def test_field_nested_update_agg_with_multiple_sequence_fields(self):
        fields = self.DEFAULT_FIELDS + [
            DataField(3, "seq", AtomicType("INT")),
            DataField(4, "ts", AtomicType("TIMESTAMP(3)"))
        ]
        agg = self._make(
            self._make_data_type(fields=fields),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq,ts",
                    }
                )
            )
        )

        accumulator = None

        ts1 = Timestamp.from_epoch_millis(1000)
        ts2 = Timestamp.from_epoch_millis(2000)
        ts3 = Timestamp.from_epoch_millis(3000)

        accumulator = agg.agg(accumulator, [self.row(1, 0, "A", 1, ts2)])
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 2, ts1)])
        self.assertCountEqual(accumulator, [
            self.row(1, 0, "A", 1, ts2),
            self.row(0, 1, "B", 2, ts1),
        ])

        accumulator = agg.agg(accumulator, [self.row(1, 1, "C", 1, ts2)])
        self.assertCountEqual(accumulator, [
            self.row(1, 0, "A", 1, ts2),
            self.row(0, 1, "B", 2, ts1),
            self.row(1, 1, "C", 1, ts2),
        ])

        # smaller second sequence should be ignored
        accumulator = agg.agg(accumulator, [self.row(1, 0, "A_late_updated_by_ts", 1, ts1)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(1, 0, "A", 1, ts2),
                self.row(0, 1, "B", 2, ts1),
                self.row(1, 1, "C", 1, ts2),
            ]
        )

        # same seq, larger ts should update
        accumulator = agg.agg(accumulator, [self.row(1, 0, "A_updated_by_ts", 1, ts3)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(1, 0, "A_updated_by_ts", 1, ts3),
                self.row(0, 1, "B", 2, ts1),
                self.row(1, 1, "C", 1, ts2),
            ]
        )

        # smaller first sequence ignored even with larger ts
        accumulator = agg.agg(accumulator, [self.row(0, 1, "b_ignored", 1, ts3)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(1, 0, "A_updated_by_ts", 1, ts3),
                self.row(0, 1, "B", 2, ts1),
                self.row(1, 1, "C", 1, ts2),
            ]
        )

        # same seq, larger ts
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated_by_ts", 2, ts2)])

        self.assertCountEqual(
            accumulator,
            [
                self.row(1, 0, "A_updated_by_ts", 1, ts3),
                self.row(0, 1, "B_updated_by_ts", 2, ts2),
                self.row(1, 1, "C", 1, ts2),
            ]
        )

        # larger first sequence wins
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated_by_seq", 3, ts1)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(1, 0, "A_updated_by_ts", 1, ts3),
                self.row(0, 1, "B_updated_by_seq", 3, ts1),
                self.row(1, 1, "C", 1, ts2),
            ]
        )

        accumulator = agg.retract(accumulator, [self.row(0, 1, "B_updated_by_seq", 3, ts1)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(1, 0, "A_updated_by_ts", 1, ts3),
                self.row(1, 1, "C", 1, ts2),
            ]
        )

    def test_field_nested_update_agg_with_count_limit_with_sequence_field_without_nested_key(self):
        with self.assertRaisesRegex(
                ValueError,
                "Option 'fields.<field-name>.nested-sequence-field' requires "
                "'fields.<field-name>.nested-key' to be configured.",
        ):
            self._make(
                self._make_data_type(fields=self.SEQUENCE_FIELDS),
                CoreOptions(
                    Options(
                        {
                            "fields.field0.nested-sequence-field": "seq",
                            "fields.field0.count-limit": "2",
                        }
                    )
                )
            )

        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.count-limit": "2",
                    }
                )
            )
        )

        accumulator = None
        accumulator = agg.agg(accumulator, [self.row(0, 1, "A", 1)])
        accumulator = agg.agg(accumulator, [self.row(0, 2, "B", 2)])
        accumulator = agg.agg(accumulator, [self.row(0, 3, "C", 3)])
        accumulator = agg.agg(accumulator, [self.row(0, 1, "A_Update", 4)])
        accumulator = agg.agg(accumulator, [self.row(0, 2, "B_Late", 1)])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "A_Update", 4),
            self.row(0, 2, "B", 2),
        ])

    def test_field_nested_update_agg_with_count_limit_with_sequence_field(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.count-limit": "2",
                    }
                )
            )
        )

        accumulator = None

        current = self.row(0, 1, "B", 1)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B", 1)])

        current = self.row(0, 1, "B_updated", 2)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [self.row(0, 1, "B_updated", 2)])

        current = self.row(1, 2, "C", 3)
        accumulator = agg.agg(accumulator, [current])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 2),
            self.row(1, 2, "C", 3),
        ])

        current = self.row(0, 3, "D", 4)
        accumulator = agg.agg(accumulator, [current])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 2),
            self.row(1, 2, "C", 3),
        ])

    def test_field_nested_update_agg_with_count_limit_updates_existing_key_at_limit(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.count-limit": "2",
                    }
                )
            )
        )

        accumulator = None

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 1)])
        accumulator = agg.agg(accumulator, [self.row(1, 2, "C", 3)])

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated", 4)])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(1, 2, "C", 3),
        ])

        accumulator = agg.agg(accumulator, [self.row(2, 3, "D", 5)])
        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(1, 2, "C", 3),
        ])

    def test_field_nested_update_agg_with_count_limit_on_first_input_array_with_sequence(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.count-limit": "2",
                    }
                )
            )
        )

        accumulator = agg.agg(
            None,
            [
                self.row(0, 1, "B", 1),
                self.row(1, 2, "C", 3),
                self.row(2, 3, "D", 5),
                self.row(0, 1, "B_updated", 4),
            ],
        )

        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 1, "B_updated", 4),
                self.row(1, 2, "C", 3),
            ],
        )

    def test_field_nested_update_agg_when_nested_key_null_use_merge_strategy(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "merge",
                    }
                )
            )
        )

        current = self.row(0, None, "C", 3)
        accumulator = agg.agg(None, [current])
        self.assertCountEqual(accumulator, [current])

        current = self.row(None, None, "D", 4)
        accumulator = agg.agg(None, [current])
        self.assertCountEqual(accumulator, [current])

        accumulator = agg.agg(None, [self.row(0, 0, "A", 1)])
        self.assertCountEqual(accumulator, [self.row(0, 0, "A", 1)])

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 2)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 0, "A", 1),
                self.row(0, 1, "B", 2),
            ],
        )

        accumulator = agg.agg(accumulator, [self.row(0, None, "C", 3)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 0, "A", 1),
                self.row(0, 1, "B", 2),
                self.row(0, None, "C", 3),
            ],
        )

        accumulator = agg.agg(accumulator, [self.row(None, None, "D", 4)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 0, "A", 1),
                self.row(0, 1, "B", 2),
                self.row(0, None, "C", 3),
                self.row(None, None, "D", 4),
            ],
        )

    def test_field_nested_update_agg_when_nested_key_null_use_ignore_strategy(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "ignore",
                    }
                )
            )
        )

        accumulator = agg.agg(None, [self.row(0, None, "C", 3)])
        self.assertCountEqual(accumulator, [])

        accumulator = agg.agg(None, [self.row(None, None, "D", 4)])
        self.assertCountEqual(accumulator, [])

        accumulator = agg.agg(None, [self.row(0, 0, "A", 1)])
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 2)])

        accumulator = agg.agg(accumulator, [self.row(0, None, "C", 3)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 0, "A", 1),
                self.row(0, 1, "B", 2),
            ],
        )

        accumulator = agg.agg(accumulator, [self.row(None, None, "D", 4)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 0, "A", 1),
                self.row(0, 1, "B", 2),
            ],
        )

    def test_field_nested_update_agg_when_nested_key_null_use_throw_error_strategy(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "error",
                    }
                )
            )
        )

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            agg.agg(None, [self.row(0, None, "C", 3)])

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            agg.agg(None, [self.row(None, None, "D", 4)])

        accumulator = agg.agg(None, [self.row(0, 0, "A", 1)])
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 2)])
        self.assertCountEqual(
            accumulator,
            [
                self.row(0, 0, "A", 1),
                self.row(0, 1, "B", 2),
            ],
        )

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            agg.agg(accumulator, [self.row(0, None, "C", 3)])

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            agg.agg(accumulator, [self.row(None, None, "D", 4)])

    def test_field_nested_update_with_count_limit_when_nested_key_null_use_merge_strategy(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.nested-key-null-strategy": "merge",
                        "fields.field0.count-limit": "3",
                    }
                )
            )
        )

        accumulator = None

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 1)])
        accumulator = agg.agg(accumulator, [self.row(None, 2, "NULL_2", 2)])
        accumulator = agg.agg(accumulator, [self.row(None, None, "NULL_NULL", 3)])
        accumulator = agg.agg(accumulator, [self.row(1, 2, "C", 5)])

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated", 4)])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(None, 2, "NULL_2", 2),
            self.row(None, None, "NULL_NULL", 3),
        ])

    def test_field_nested_update_with_count_limit_when_nested_key_null_use_ignore_strategy(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.nested-key-null-strategy": "ignore",
                        "fields.field0.count-limit": "3",
                    }
                )
            )
        )

        accumulator = None

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 1)])
        accumulator = agg.agg(accumulator, [self.row(None, 2, "NULL_2", 2)])
        accumulator = agg.agg(accumulator, [self.row(None, None, "NULL_NULL", 3)])
        accumulator = agg.agg(accumulator, [self.row(1, 2, "C", 3)])

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated", 4)])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(1, 2, "C", 3),
        ])

        accumulator = agg.agg(accumulator, [self.row(2, 3, "D", 5)])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(1, 2, "C", 3),
            self.row(2, 3, "D", 5),
        ])

    def test_field_nested_update_with_count_limit_when_nested_key_null_use_throw_error_strategy(self):
        agg = self._make(
            self._make_data_type(fields=self.SEQUENCE_FIELDS),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-sequence-field": "seq",
                        "fields.field0.nested-key-null-strategy": "error",
                        "fields.field0.count-limit": "3",
                    }
                )
            )
        )

        accumulator = None

        accumulator = agg.agg(accumulator, [self.row(0, 1, "B", 1)])

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            agg.agg(accumulator, [self.row(None, 2, "NULL_2", 2)])

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            agg.agg(accumulator, [self.row(None, None, "NULL_NULL", 3)])

        accumulator = agg.agg(accumulator, [self.row(1, 2, "C", 3)])
        accumulator = agg.agg(accumulator, [self.row(0, 1, "B_updated", 4)])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(1, 2, "C", 3),
        ])

        accumulator = agg.agg(accumulator, [self.row(2, 3, "D", 5)])

        self.assertCountEqual(accumulator, [
            self.row(0, 1, "B_updated", 4),
            self.row(1, 2, "C", 3),
            self.row(2, 3, "D", 5),
        ])

    def test_field_nested_update_retract_applies_nested_key_null_strategy_to_accumulator(self):
        merge_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                    }
                )
            )
        )

        accumulator = None
        accumulator = merge_agg.agg(accumulator, [self.row(0, None, "A")])
        accumulator = merge_agg.agg(accumulator, [self.row(1, 0, "B")])
        accumulator = merge_agg.agg(accumulator, [self.row(1, 1, "C")])

        ignore_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "IGNORE",
                    }
                )
            )
        )

        result = ignore_agg.retract(accumulator, [self.row(1, 0, "B")])
        self.assertCountEqual(result, [self.row(1, 1, "C")])

        error_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "ERROR",
                    }
                )
            )
        )

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            error_agg.retract(accumulator, [self.row(1, 0, "B")])

    def test_field_nested_update_retract_applies_nested_key_null_strategy_to_retract_input(self):
        merge_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                    }
                )
            )
        )

        accumulator = None
        accumulator = merge_agg.agg(accumulator, [self.row(0, 0, "A")])
        accumulator = merge_agg.agg(accumulator, [self.row(1, 1, "B")])

        ignore_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "IGNORE",
                    }
                )
            )
        )

        result = ignore_agg.retract(accumulator, [self.row(0, None, "X")])

        self.assertCountEqual(result, [
            self.row(0, 0, "A"),
            self.row(1, 1, "B"),
        ])

        error_agg = self._make(
            self._make_data_type(),
            CoreOptions(
                Options(
                    {
                        "fields.field0.nested-key": "k0,k1",
                        "fields.field0.nested-key-null-strategy": "ERROR",
                    }
                )
            )
        )

        with self.assertRaisesRegex(
                ValueError,
                "Nested key contains null values. Primary key fields must not be null.",
        ):
            error_agg.retract(accumulator, [self.row(0, None, "X")])


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
