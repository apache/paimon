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

import unittest

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import AtomicType, DataField


def _single_int_builder() -> PredicateBuilder:
    return PredicateBuilder([DataField(0, 'f0', AtomicType('INT'))])


def _two_int_builder() -> PredicateBuilder:
    return PredicateBuilder([
        DataField(0, 'f0', AtomicType('INT')),
        DataField(1, 'f1', AtomicType('INT')),
    ])


class PredicateNegateTest(unittest.TestCase):

    def test_equal_negate(self):
        builder = _single_int_builder()
        predicate = builder.equal('f0', 5)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.not_equal('f0', 5))

    def test_not_equal_negate(self):
        builder = _single_int_builder()
        predicate = builder.not_equal('f0', 5)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.equal('f0', 5))

    def test_less_than_negate(self):
        builder = _single_int_builder()
        predicate = builder.less_than('f0', 5)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.greater_or_equal('f0', 5))

    def test_less_or_equal_negate(self):
        builder = _single_int_builder()
        predicate = builder.less_or_equal('f0', 5)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.greater_than('f0', 5))

    def test_greater_than_negate(self):
        builder = _single_int_builder()
        predicate = builder.greater_than('f0', 5)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.less_or_equal('f0', 5))

    def test_greater_or_equal_negate(self):
        builder = _single_int_builder()
        predicate = builder.greater_or_equal('f0', 5)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.less_than('f0', 5))

    def test_is_null_negate(self):
        builder = _single_int_builder()
        predicate = builder.is_null('f0')
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.is_not_null('f0'))

    def test_is_not_null_negate(self):
        builder = _single_int_builder()
        predicate = builder.is_not_null('f0')
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.is_null('f0'))

    def test_in_negate(self):
        builder = _single_int_builder()
        predicate = builder.is_in('f0', [1, 3])
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.is_not_in('f0', [1, 3]))

    def test_not_in_negate(self):
        builder = _single_int_builder()
        predicate = builder.is_not_in('f0', [1, 3])
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.is_in('f0', [1, 3]))

    def test_between_negate(self):
        builder = _single_int_builder()
        predicate = builder.between('f0', 1, 10)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.not_between('f0', 1, 10))

    def test_not_between_negate(self):
        builder = _single_int_builder()
        predicate = builder.not_between('f0', 1, 10)
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, builder.between('f0', 1, 10))

    def test_always_true_negate(self):
        predicate = PredicateBuilder.always_true()
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, PredicateBuilder.always_false())

    def test_always_false_negate(self):
        predicate = PredicateBuilder.always_false()
        negated = predicate.negate()
        self.assertIsNotNone(negated)
        self.assertEqual(negated, PredicateBuilder.always_true())

    def test_starts_with_negate_returns_none(self):
        builder = PredicateBuilder([DataField(0, 'f0', AtomicType('STRING'))])
        predicate = builder.startswith('f0', 'abc')
        self.assertIsNone(predicate.negate())

    def test_ends_with_negate_returns_none(self):
        builder = PredicateBuilder([DataField(0, 'f0', AtomicType('STRING'))])
        predicate = builder.endswith('f0', 'abc')
        self.assertIsNone(predicate.negate())

    def test_contains_negate_returns_none(self):
        builder = PredicateBuilder([DataField(0, 'f0', AtomicType('STRING'))])
        predicate = builder.contains('f0', 'abc')
        self.assertIsNone(predicate.negate())

    def test_like_negate_returns_none(self):
        builder = PredicateBuilder([DataField(0, 'f0', AtomicType('STRING'))])
        predicate = builder.like('f0', '%abc%')
        self.assertIsNone(predicate.negate())

    def test_and_negate(self):
        builder = _two_int_builder()
        predicate = PredicateBuilder.and_predicates([
            builder.equal('f0', 3),
            builder.equal('f1', 5),
        ])
        negated = predicate.negate()
        expected = PredicateBuilder.or_predicates([
            builder.not_equal('f0', 3),
            builder.not_equal('f1', 5),
        ])
        self.assertIsNotNone(negated)
        self.assertEqual(negated, expected)

    def test_or_negate(self):
        builder = _two_int_builder()
        predicate = PredicateBuilder.or_predicates([
            builder.equal('f0', 3),
            builder.equal('f1', 5),
        ])
        negated = predicate.negate()
        expected = PredicateBuilder.and_predicates([
            builder.not_equal('f0', 3),
            builder.not_equal('f1', 5),
        ])
        self.assertIsNotNone(negated)
        self.assertEqual(negated, expected)

    def test_and_with_non_negatable_child_returns_none(self):
        """If any child cannot be negated, the whole AND negate returns None."""
        builder = PredicateBuilder([DataField(0, 'f0', AtomicType('STRING'))])
        predicate = PredicateBuilder.and_predicates([
            builder.equal('f0', 'a'),
            builder.startswith('f0', 'abc'),
        ])
        self.assertIsNone(predicate.negate())

    def test_or_with_non_negatable_child_returns_none(self):
        """If any child cannot be negated, the whole OR negate returns None."""
        builder = PredicateBuilder([DataField(0, 'f0', AtomicType('STRING'))])
        predicate = PredicateBuilder.or_predicates([
            builder.equal('f0', 'a'),
            builder.like('f0', '%abc%'),
        ])
        self.assertIsNone(predicate.negate())

    def test_double_negate_equal(self):
        builder = _single_int_builder()
        predicate = builder.equal('f0', 5)
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)

    def test_double_negate_less_than(self):
        builder = _single_int_builder()
        predicate = builder.less_than('f0', 5)
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)

    def test_double_negate_in(self):
        builder = _single_int_builder()
        predicate = builder.is_in('f0', [1, 2, 3])
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)

    def test_double_negate_between(self):
        builder = _single_int_builder()
        predicate = builder.between('f0', 1, 10)
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)

    def test_double_negate_is_null(self):
        builder = _single_int_builder()
        predicate = builder.is_null('f0')
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)

    def test_double_negate_always_true(self):
        predicate = PredicateBuilder.always_true()
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)

    def test_double_negate_and(self):
        builder = _two_int_builder()
        predicate = PredicateBuilder.and_predicates([
            builder.equal('f0', 3),
            builder.equal('f1', 5),
        ])
        double_negated = predicate.negate().negate()
        self.assertEqual(double_negated, predicate)


if __name__ == '__main__':
    unittest.main()
