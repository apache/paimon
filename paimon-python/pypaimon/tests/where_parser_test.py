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

import datetime
import decimal
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.where_parser import parse_where_clause, _tokenize, _cast_literal
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField


class WhereParserTokenizeTest(unittest.TestCase):
    """Tests for the tokenizer."""

    def test_simple_comparison(self):
        tokens = _tokenize("age > 18")
        self.assertEqual(tokens, ['age', '>', '18'])

    def test_string_literal(self):
        tokens = _tokenize("name = 'Alice'")
        self.assertEqual(tokens, ['name', '=', "'Alice'"])

    def test_double_quoted_string(self):
        tokens = _tokenize('city = "Beijing"')
        self.assertEqual(tokens, ['city', '=', '"Beijing"'])

    def test_multi_char_operators(self):
        tokens = _tokenize("age >= 18 AND age <= 30")
        self.assertEqual(tokens, ['age', '>=', '18', 'AND', 'age', '<=', '30'])

    def test_not_equal_operators(self):
        tokens = _tokenize("status != 'active'")
        self.assertEqual(tokens, ['status', '!=', "'active'"])

        tokens = _tokenize("status <> 'active'")
        self.assertEqual(tokens, ['status', '<>', "'active'"])

    def test_in_list(self):
        tokens = _tokenize("id IN (1, 2, 3)")
        self.assertEqual(tokens, ['id', 'IN', '(', '1', ',', '2', ',', '3', ')'])

    def test_parenthesized_group(self):
        tokens = _tokenize("(age > 18 OR name = 'Bob')")
        self.assertEqual(tokens, ['(', 'age', '>', '18', 'OR', 'name', '=', "'Bob'", ')'])

    def test_between(self):
        tokens = _tokenize("score BETWEEN 60 AND 100")
        self.assertEqual(tokens, ['score', 'BETWEEN', '60', 'AND', '100'])

    def test_is_null(self):
        tokens = _tokenize("deleted_at IS NULL")
        self.assertEqual(tokens, ['deleted_at', 'IS', 'NULL'])

    def test_is_not_null(self):
        tokens = _tokenize("name IS NOT NULL")
        self.assertEqual(tokens, ['name', 'IS', 'NOT', 'NULL'])


class WhereParserCastLiteralTest(unittest.TestCase):
    """Tests for literal type casting."""

    def test_cast_int(self):
        self.assertEqual(_cast_literal('42', 'INT'), 42)
        self.assertEqual(_cast_literal('42', 'INTEGER'), 42)
        self.assertEqual(_cast_literal('42', 'BIGINT'), 42)
        self.assertEqual(_cast_literal('42', 'TINYINT'), 42)
        self.assertEqual(_cast_literal('42', 'SMALLINT'), 42)

    def test_cast_float(self):
        self.assertAlmostEqual(_cast_literal('3.14', 'FLOAT'), 3.14)
        self.assertAlmostEqual(_cast_literal('3.14', 'DOUBLE'), 3.14)

    def test_cast_decimal(self):
        # A DECIMAL literal must stay exact (decimal.Decimal); a float would never
        # compare equal to the Decimal a DECIMAL column reads back as.
        value = _cast_literal('99.99', 'DECIMAL(10,2)')
        self.assertIsInstance(value, decimal.Decimal)
        self.assertEqual(value, decimal.Decimal('99.99'))

    def test_cast_decimal_high_precision_rescales(self):
        # A DECIMAL(38, 2) literal in integer form exceeds the default decimal
        # context (precision 28); without a column-wide context the rescale would
        # be dropped and the literal would bind at scale 0.
        value = _cast_literal('123456789012345678901234567890123456',
                              'DECIMAL(38, 2)')
        self.assertEqual(value.as_tuple().exponent, -2)
        self.assertEqual(
            value, decimal.Decimal('123456789012345678901234567890123456.00'))

    def test_cast_date(self):
        value = _cast_literal('2024-01-01', 'DATE')
        self.assertEqual(value, datetime.date(2024, 1, 1))

    def test_cast_time(self):
        value = _cast_literal('12:30:00', 'TIME(0)')
        self.assertEqual(value, datetime.time(12, 30, 0))

    def test_cast_time_with_fraction(self):
        value = _cast_literal('12:30:00.5', 'TIME(3)')
        self.assertEqual(value, datetime.time(12, 30, 0, 500000))

    def test_cast_time_rejects_offset(self):
        # A Paimon TIME has no time zone; an offset-bearing literal must not
        # silently drop the offset and match the wall-clock value.
        with self.assertRaises(ValueError):
            _cast_literal('12:30:00+01:00', 'TIME(0)')

    def test_cast_time_rejects_malformed(self):
        with self.assertRaises(ValueError):
            _cast_literal('25:00:00', 'TIME(0)')

    def test_cast_boolean(self):
        self.assertTrue(_cast_literal('true', 'BOOLEAN'))
        self.assertTrue(_cast_literal('True', 'BOOLEAN'))
        self.assertTrue(_cast_literal('1', 'BOOLEAN'))
        self.assertFalse(_cast_literal('false', 'BOOLEAN'))
        self.assertFalse(_cast_literal('0', 'BOOLEAN'))

    def test_cast_string(self):
        self.assertEqual(_cast_literal('hello', 'STRING'), 'hello')
        self.assertEqual(_cast_literal('hello', 'VARCHAR(100)'), 'hello')


class WhereParserParseTest(unittest.TestCase):
    """Tests for the full WHERE clause parser."""

    @classmethod
    def setUpClass(cls):
        cls.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'name', AtomicType('STRING')),
            DataField(2, 'age', AtomicType('INT')),
            DataField(3, 'score', AtomicType('DOUBLE')),
            DataField(4, 'city', AtomicType('STRING')),
            DataField(5, 'active', AtomicType('BOOLEAN')),
        ]

    def test_empty_string(self):
        result = parse_where_clause('', self.fields)
        self.assertIsNone(result)

    def test_whitespace_only(self):
        result = parse_where_clause('   ', self.fields)
        self.assertIsNone(result)

    def test_equal_int(self):
        predicate = parse_where_clause("id = 1", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'equal')
        self.assertEqual(predicate.field, 'id')
        self.assertEqual(predicate.literals, [1])

    def test_equal_string(self):
        predicate = parse_where_clause("name = 'Alice'", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'equal')
        self.assertEqual(predicate.field, 'name')
        self.assertEqual(predicate.literals, ['Alice'])

    def test_not_equal(self):
        predicate = parse_where_clause("id != 5", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'notEqual')
        self.assertEqual(predicate.field, 'id')
        self.assertEqual(predicate.literals, [5])

    def test_not_equal_diamond(self):
        predicate = parse_where_clause("id <> 5", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'notEqual')
        self.assertEqual(predicate.literals, [5])

    def test_less_than(self):
        predicate = parse_where_clause("age < 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'lessThan')
        self.assertEqual(predicate.field, 'age')
        self.assertEqual(predicate.literals, [30])

    def test_less_or_equal(self):
        predicate = parse_where_clause("age <= 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'lessOrEqual')
        self.assertEqual(predicate.literals, [30])

    def test_greater_than(self):
        predicate = parse_where_clause("age > 18", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'greaterThan')
        self.assertEqual(predicate.field, 'age')
        self.assertEqual(predicate.literals, [18])

    def test_greater_or_equal(self):
        predicate = parse_where_clause("age >= 18", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'greaterOrEqual')
        self.assertEqual(predicate.literals, [18])

    def test_is_null(self):
        predicate = parse_where_clause("city IS NULL", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'isNull')
        self.assertEqual(predicate.field, 'city')

    def test_is_not_null(self):
        predicate = parse_where_clause("name IS NOT NULL", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'isNotNull')
        self.assertEqual(predicate.field, 'name')

    def test_in_int_list(self):
        predicate = parse_where_clause("id IN (1, 2, 3)", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'in')
        self.assertEqual(predicate.field, 'id')
        self.assertEqual(predicate.literals, [1, 2, 3])

    def test_in_string_list(self):
        predicate = parse_where_clause("city IN ('Beijing', 'Shanghai')", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'in')
        self.assertEqual(predicate.field, 'city')
        self.assertEqual(predicate.literals, ['Beijing', 'Shanghai'])

    def test_not_in(self):
        predicate = parse_where_clause("id NOT IN (4, 5)", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'notIn')
        self.assertEqual(predicate.field, 'id')
        self.assertEqual(predicate.literals, [4, 5])

    def test_between(self):
        predicate = parse_where_clause("age BETWEEN 20 AND 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'between')
        self.assertEqual(predicate.field, 'age')
        self.assertEqual(predicate.literals, [20, 30])

    def test_between_float(self):
        predicate = parse_where_clause("score BETWEEN 60.0 AND 100.0", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'between')
        self.assertEqual(predicate.field, 'score')
        self.assertAlmostEqual(predicate.literals[0], 60.0)
        self.assertAlmostEqual(predicate.literals[1], 100.0)

    def test_not_between(self):
        predicate = parse_where_clause("age NOT BETWEEN 20 AND 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'notBetween')
        self.assertEqual(predicate.field, 'age')
        self.assertEqual(predicate.literals, [20, 30])

    def test_not_between_float(self):
        predicate = parse_where_clause("score NOT BETWEEN 60.0 AND 100.0", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'notBetween')
        self.assertEqual(predicate.field, 'score')
        self.assertAlmostEqual(predicate.literals[0], 60.0)
        self.assertAlmostEqual(predicate.literals[1], 100.0)

    def test_not_between_case_insensitive(self):
        predicate = parse_where_clause("age not between 20 and 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'notBetween')
        self.assertEqual(predicate.literals, [20, 30])

    def test_not_between_with_and_connector(self):
        predicate = parse_where_clause(
            "age NOT BETWEEN 20 AND 30 AND name = 'Alice'", self.fields
        )
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'and')
        self.assertEqual(predicate.literals[0].method, 'notBetween')
        self.assertEqual(predicate.literals[0].field, 'age')
        self.assertEqual(predicate.literals[0].literals, [20, 30])
        self.assertEqual(predicate.literals[1].method, 'equal')
        self.assertEqual(predicate.literals[1].field, 'name')

    def test_error_not_between_missing_and(self):
        with self.assertRaises(ValueError):
            parse_where_clause("age NOT BETWEEN 20 30", self.fields)

    def test_like(self):
        predicate = parse_where_clause("name LIKE 'A%'", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'like')
        self.assertEqual(predicate.field, 'name')
        self.assertEqual(predicate.literals, ['A%'])

    def test_and_connector(self):
        predicate = parse_where_clause("age > 18 AND name = 'Alice'", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'and')
        self.assertEqual(len(predicate.literals), 2)
        self.assertEqual(predicate.literals[0].method, 'greaterThan')
        self.assertEqual(predicate.literals[0].field, 'age')
        self.assertEqual(predicate.literals[1].method, 'equal')
        self.assertEqual(predicate.literals[1].field, 'name')

    def test_or_connector(self):
        predicate = parse_where_clause("age < 20 OR age > 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'or')
        self.assertEqual(len(predicate.literals), 2)
        self.assertEqual(predicate.literals[0].method, 'lessThan')
        self.assertEqual(predicate.literals[1].method, 'greaterThan')

    def test_and_has_higher_precedence_than_or(self):
        # "a = 1 OR b = 2 AND c = 3" should be parsed as "a = 1 OR (b = 2 AND c = 3)"
        predicate = parse_where_clause("id = 1 OR age = 25 AND name = 'Alice'", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'or')
        self.assertEqual(len(predicate.literals), 2)
        self.assertEqual(predicate.literals[0].method, 'equal')
        self.assertEqual(predicate.literals[0].field, 'id')
        self.assertEqual(predicate.literals[1].method, 'and')
        self.assertEqual(len(predicate.literals[1].literals), 2)

    def test_parenthesized_group(self):
        predicate = parse_where_clause("(id = 1 OR id = 2) AND age > 18", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'and')
        self.assertEqual(len(predicate.literals), 2)
        self.assertEqual(predicate.literals[0].method, 'or')
        self.assertEqual(predicate.literals[1].method, 'greaterThan')

    def test_nested_parentheses(self):
        predicate = parse_where_clause(
            "(age > 18 AND (name = 'Alice' OR name = 'Bob'))", self.fields
        )
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'and')
        self.assertEqual(predicate.literals[0].method, 'greaterThan')
        self.assertEqual(predicate.literals[1].method, 'or')

    def test_multiple_and_conditions(self):
        predicate = parse_where_clause(
            "id > 0 AND age >= 20 AND name IS NOT NULL", self.fields
        )
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'and')
        self.assertEqual(len(predicate.literals), 3)

    def test_complex_expression(self):
        predicate = parse_where_clause(
            "age > 18 OR (name = 'Bob' AND city IN ('Beijing', 'Shanghai'))",
            self.fields
        )
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'or')
        self.assertEqual(predicate.literals[0].method, 'greaterThan')
        and_part = predicate.literals[1]
        self.assertEqual(and_part.method, 'and')
        self.assertEqual(and_part.literals[0].method, 'equal')
        self.assertEqual(and_part.literals[1].method, 'in')

    def test_case_insensitive_keywords(self):
        predicate = parse_where_clause("age between 20 and 30", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'between')

        predicate = parse_where_clause("name is null", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'isNull')

        predicate = parse_where_clause("name is not null", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'isNotNull')

        predicate = parse_where_clause("id in (1, 2)", self.fields)
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate.method, 'in')

    def test_type_casting_int_field(self):
        predicate = parse_where_clause("age = 25", self.fields)
        self.assertIsInstance(predicate.literals[0], int)

    def test_type_casting_double_field(self):
        predicate = parse_where_clause("score > 3.14", self.fields)
        self.assertIsInstance(predicate.literals[0], float)

    def test_type_casting_string_field(self):
        predicate = parse_where_clause("name = 'Alice'", self.fields)
        self.assertIsInstance(predicate.literals[0], str)

    def test_error_missing_closing_paren(self):
        with self.assertRaises(ValueError):
            parse_where_clause("(age > 18", self.fields)

    def test_error_unexpected_end(self):
        with self.assertRaises(ValueError):
            parse_where_clause("age", self.fields)

    def test_error_missing_in_paren(self):
        with self.assertRaises(ValueError):
            parse_where_clause("id IN 1, 2, 3", self.fields)

    def test_error_between_missing_and(self):
        with self.assertRaises(ValueError):
            parse_where_clause("age BETWEEN 20 30", self.fields)

    def test_error_unexpected_trailing_tokens(self):
        with self.assertRaises(ValueError):
            parse_where_clause("age > 18 extra_token", self.fields)

    def test_error_is_without_null(self):
        with self.assertRaises(ValueError):
            parse_where_clause("age IS SOMETHING", self.fields)

    def test_error_unknown_field(self):
        with self.assertRaises(ValueError) as context:
            parse_where_clause("nonexistent = 1", self.fields)
        self.assertIn("Unknown field", str(context.exception))
        self.assertIn("nonexistent", str(context.exception))

    def test_error_non_atomic_type_field(self):
        fields_with_array = self.fields + [
            DataField(6, 'tags', ArrayType(True, AtomicType('STRING'))),
        ]
        with self.assertRaises(ValueError) as context:
            parse_where_clause("tags = 'foo'", fields_with_array)
        self.assertIn("non-atomic type", str(context.exception))
        self.assertIn("tags", str(context.exception))


class WhereParserScanTest(unittest.TestCase):
    """End-to-end: a WHERE clause on DECIMAL/DATE/TIME columns returns the row."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(cls.tempdir, 'warehouse')})
        cls.catalog.create_database('default', False)
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('price', pa.decimal128(10, 2)),
            ('big', pa.decimal128(38, 2)),
            ('d', pa.date32()),
            ('t', pa.time32('ms')),
        ])
        cls.catalog.create_table(
            'default.where_literal_types', Schema.from_pyarrow_schema(pa_schema), False)
        cls.table = cls.catalog.get_table('default.where_literal_types')
        data = pa.table({
            'id': pa.array([1, 2], pa.int32()),
            'price': pa.array(
                [decimal.Decimal('99.99'), decimal.Decimal('50.00')], pa.decimal128(10, 2)),
            'big': pa.array(
                [decimal.Decimal('123456789012345678901234567890123456.00'),
                 decimal.Decimal('1.00')], pa.decimal128(38, 2)),
            'd': pa.array(
                [datetime.date(2024, 1, 1), datetime.date(2020, 1, 1)], pa.date32()),
            't': pa.array(
                [datetime.time(12, 30, 0), datetime.time(1, 0, 0)], pa.time32('ms')),
        })
        wb = cls.table.new_batch_write_builder()
        writer = wb.new_write()
        commit = wb.new_commit()
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _scan_ids(self, where):
        predicate = parse_where_clause(where, self.table.table_schema.fields)
        read_builder = self.table.new_read_builder().with_filter(predicate)
        splits = read_builder.new_scan().plan().splits()
        return read_builder.new_read().to_arrow(splits).column('id').to_pylist()

    def test_decimal_equality_returns_matching_row(self):
        # DECIMAL(10,2) reads back as Decimal; the old float literal dropped the row.
        self.assertEqual(self._scan_ids("price = 99.99"), [1])

    def test_decimal_literal_rescaled_to_column_scale(self):
        # The pushed-down filter binds the literal's own scale, so an integer or
        # trailing-zero literal must be rescaled to the column scale to match.
        self.assertEqual(self._scan_ids("price = 50"), [2])
        self.assertEqual(self._scan_ids("price = 99.990"), [1])

    def test_decimal_literal_finer_than_column_matches_nothing(self):
        # A literal too precise for the column (99.999 on DECIMAL(10,2)) cannot equal
        # any stored value; it must return no rows rather than round into a match.
        self.assertEqual(self._scan_ids("price = 99.999"), [])

    def test_high_precision_decimal_integer_literal_matches(self):
        # A DECIMAL(38, 2) integer-form literal exceeds the default decimal
        # context; it must still rescale and match the stored scale-2 value.
        self.assertEqual(
            self._scan_ids("big = 123456789012345678901234567890123456"), [1])

    def test_malformed_decimal_raises_value_error(self):
        # parse_where_clause documents ValueError; a bad decimal must not leak
        # decimal.InvalidOperation past the CLI's `except ValueError`.
        with self.assertRaises(ValueError):
            parse_where_clause("price = abc", self.table.table_schema.fields)

    def test_date_equality_returns_matching_row(self):
        # DATE literal left as str raised in the arrow comparison kernel.
        self.assertEqual(self._scan_ids("d = '2024-01-01'"), [1])

    def test_time_equality_returns_matching_row(self):
        self.assertEqual(self._scan_ids("t = '12:30:00'"), [1])


if __name__ == '__main__':
    unittest.main()
