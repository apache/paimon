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
from concurrent.futures import ThreadPoolExecutor

from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.schema.data_types import DataField, IntType
from pypaimon.utils.range import Range


class StubGlobalIndexReader(GlobalIndexReader):
    """A test reader that returns a fixed result for equal predicates."""

    def __init__(self, result):
        self._result = result

    def visit_equal(self, field_ref, literal):
        return self._result

    def close(self):
        pass


def _make_fields():
    return [
        DataField(0, "a", IntType()),
        DataField(1, "b", IntType()),
        DataField(2, "c", IntType()),
    ]


def _result_of(*row_ids):
    return GlobalIndexResult.from_range(Range(min(row_ids), max(row_ids)))


class GlobalIndexEvaluatorTest(unittest.TestCase):

    def test_single_field_sequential(self):
        fields = _make_fields()
        expected = GlobalIndexResult.from_range(Range(1, 3))

        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(expected)],
        )

        predicate = Predicate(method='equal', index=0, field='a', literals=[42])
        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        self.assertEqual(result.results().cardinality(), 3)
        evaluator.close()

    def test_and_parallel_multiple_fields(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 5))
        result_b = GlobalIndexResult.from_range(Range(3, 7))

        field_results = {0: result_a, 1: result_b}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(field_results[field.id])],
            executor,
        )

        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(method='equal', index=0, field='a', literals=[42]),
                Predicate(method='equal', index=1, field='b', literals=[99]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # intersection of [1,5] and [3,7] -> [3,5]
        self.assertEqual(bm.cardinality(), 3)
        for v in [3, 4, 5]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_or_parallel_multiple_fields(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 2))
        result_b = GlobalIndexResult.from_range(Range(10, 11))

        field_results = {0: result_a, 1: result_b}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(field_results[field.id])],
            executor,
        )

        predicate = Predicate(
            method='or', index=None, field=None,
            literals=[
                Predicate(method='equal', index=0, field='a', literals=[42]),
                Predicate(method='equal', index=1, field='b', literals=[99]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # union of [1,2] and [10,11] -> {1,2,10,11}
        self.assertEqual(bm.cardinality(), 4)
        for v in [1, 2, 10, 11]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_or_returns_none_when_child_unsupported(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 2))

        def readers_fn(field):
            if field.id == 0:
                return [StubGlobalIndexReader(result_a)]
            return []

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(fields, readers_fn, executor)

        predicate = Predicate(
            method='or', index=None, field=None,
            literals=[
                Predicate(method='equal', index=0, field='a', literals=[42]),
                Predicate(method='equal', index=1, field='b', literals=[99]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNone(result)
        evaluator.close()
        executor.shutdown(wait=False)

    def test_and_with_disjoint_results(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 3))
        result_b = GlobalIndexResult.from_range(Range(10, 12))

        field_results = {0: result_a, 1: result_b}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(field_results[field.id])],
            executor,
        )

        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(method='equal', index=0, field='a', literals=[42]),
                Predicate(method='equal', index=1, field='b', literals=[99]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        self.assertTrue(result.is_empty())
        evaluator.close()
        executor.shutdown(wait=False)

    def test_no_executor_falls_back_to_sequential(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 5))
        result_b = GlobalIndexResult.from_range(Range(3, 7))

        field_results = {0: result_a, 1: result_b}
        call_count = [0]

        def readers_fn(field):
            call_count[0] += 1
            return [StubGlobalIndexReader(field_results[field.id])]

        evaluator = GlobalIndexEvaluator(fields, readers_fn)

        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(method='equal', index=0, field='a', literals=[42]),
                Predicate(method='equal', index=1, field='b', literals=[99]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        self.assertEqual(call_count[0], 2)
        evaluator.close()

    def test_nested_and_does_not_deadlock_with_small_pool(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 5))
        result_b = GlobalIndexResult.from_range(Range(3, 7))
        result_c = GlobalIndexResult.from_range(Range(4, 5))

        field_results = {0: result_a, 1: result_b, 2: result_c}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(field_results[field.id])],
            executor,
        )

        # Nested binary tree: and(and(a, b), c)
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(
                    method='and', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[1]),
                        Predicate(method='equal', index=1, field='b', literals=[2]),
                    ],
                ),
                Predicate(method='equal', index=2, field='c', literals=[3]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # intersection of [1,5], [3,7], [4,5] -> [4,5]
        self.assertEqual(bm.cardinality(), 2)
        for v in [4, 5]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_nested_or_does_not_deadlock_with_small_pool(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 2))
        result_b = GlobalIndexResult.from_range(Range(3, 4))
        result_c = GlobalIndexResult.from_range(Range(5, 6))

        field_results = {0: result_a, 1: result_b, 2: result_c}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(field_results[field.id])],
            executor,
        )

        # Nested binary tree: or(or(a, b), c)
        predicate = Predicate(
            method='or', index=None, field=None,
            literals=[
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[1]),
                        Predicate(method='equal', index=1, field='b', literals=[2]),
                    ],
                ),
                Predicate(method='equal', index=2, field='c', literals=[3]),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # union of [1,2], [3,4], [5,6] -> {1,2,3,4,5,6}
        self.assertEqual(bm.cardinality(), 6)
        for v in [1, 2, 3, 4, 5, 6]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_null_predicate(self):
        fields = _make_fields()
        evaluator = GlobalIndexEvaluator(fields, lambda field: [])

        result = evaluator.evaluate(None)

        self.assertIsNone(result)
        evaluator.close()


if __name__ == '__main__':
    unittest.main()
