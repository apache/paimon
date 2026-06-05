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

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.schema.data_types import DataField, AtomicType
from pypaimon.utils.range import Range


class StubGlobalIndexReader(GlobalIndexReader):
    """A test reader that returns a fixed result for equal predicates."""

    def __init__(self, result):
        self._result = result

    def visit_equal(self, field_ref, literal):
        return _completed_future(self._result)

    def close(self):
        pass


class AsyncStubReader(GlobalIndexReader):
    """A test reader that dispatches to an executor (simulating real async readers)."""

    def __init__(self, result, executor):
        self._result = result
        self._executor = executor

    def visit_equal(self, field_ref, literal):
        return self._executor.submit(lambda: self._result)

    def close(self):
        pass


def _make_fields():
    return [
        DataField(0, "a", AtomicType("INT")),
        DataField(1, "b", AtomicType("INT")),
        DataField(2, "c", AtomicType("INT")),
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
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
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
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
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

        evaluator = GlobalIndexEvaluator(fields, readers_fn)

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

    def test_and_with_disjoint_results(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 3))
        result_b = GlobalIndexResult.from_range(Range(10, 12))

        field_results = {0: result_a, 1: result_b}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
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
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
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
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
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

    def test_mixed_nested_does_not_deadlock_with_small_pool(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 5))
        result_b = GlobalIndexResult.from_range(Range(3, 7))
        result_c = GlobalIndexResult.from_range(Range(1, 3))

        field_results = {0: result_a, 1: result_b, 2: result_c}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
        )

        # AND(OR(a, b), OR(a, c)) — mixed nesting
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[1]),
                        Predicate(method='equal', index=1, field='b', literals=[2]),
                    ],
                ),
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[3]),
                        Predicate(method='equal', index=2, field='c', literals=[4]),
                    ],
                ),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # OR(a, b) = union([1,5], [3,7]) = [1,7]
        # OR(a, c) = union([1,5], [1,3]) = [1,5]
        # AND = intersection = [1,5]
        self.assertEqual(bm.cardinality(), 5)
        for v in [1, 2, 3, 4, 5]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_deep_mixed_nested_does_not_deadlock_with_small_pool(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 5))
        result_b = GlobalIndexResult.from_range(Range(2, 6))
        result_c = GlobalIndexResult.from_range(Range(3, 7))

        field_results = {0: result_a, 1: result_b, 2: result_c}

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [AsyncStubReader(field_results[field.id], executor)],
        )

        # AND(OR(AND(a, b), c), OR(AND(a, c), b)) — deep mixed nesting
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(
                    method='or', index=None, field=None,
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
                ),
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(
                            method='and', index=None, field=None,
                            literals=[
                                Predicate(method='equal', index=0, field='a', literals=[4]),
                                Predicate(method='equal', index=2, field='c', literals=[5]),
                            ],
                        ),
                        Predicate(method='equal', index=1, field='b', literals=[6]),
                    ],
                ),
            ],
        )

        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # OR(AND(a,b), c): AND(a,b)=intersect([1,5],[2,6])=[2,5], c=[3,7] => union=[2,7]
        # OR(AND(a,c), b): AND(a,c)=intersect([1,5],[3,7])=[3,5], b=[2,6] => union=[2,6]
        # top AND: intersect([2,7],[2,6]) = [2,6]
        self.assertEqual(bm.cardinality(), 5)
        for v in [2, 3, 4, 5, 6]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_same_field_predicates_accessed_concurrently(self):
        fields = _make_fields()

        concurrency = [0]
        max_concurrency = [0]
        lock = threading.Lock()

        executor = ThreadPoolExecutor(max_workers=4)

        class ConcurrencyDetectingReader(GlobalIndexReader):
            def __init__(self, result):
                self._result = result

            def visit_equal(self, field_ref, literal):
                def _work():
                    with lock:
                        concurrency[0] += 1
                        max_concurrency[0] = max(max_concurrency[0], concurrency[0])
                    time.sleep(0.05)
                    with lock:
                        concurrency[0] -= 1
                    return self._result
                return executor.submit(_work)

            def close(self):
                pass

        result_a = GlobalIndexResult.from_range(Range(1, 5))
        reader = ConcurrencyDetectingReader(result_a)

        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [reader],
        )

        # AND(a=1, a=2, a=3) — evaluator dispatches concurrently, readers own their thread-safety
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(method='equal', index=0, field='a', literals=[1]),
                Predicate(method='equal', index=0, field='a', literals=[2]),
                Predicate(method='equal', index=0, field='a', literals=[3]),
            ],
        )

        evaluator.evaluate(predicate)

        self.assertGreater(max_concurrency[0], 1)
        evaluator.close()
        executor.shutdown(wait=False)

    def test_mixed_nested_same_field_accessed_concurrently(self):
        fields = _make_fields()

        concurrency_a = [0]
        max_concurrency_a = [0]
        lock = threading.Lock()

        executor = ThreadPoolExecutor(max_workers=4)

        class ConcurrencyDetectingReader(GlobalIndexReader):
            def __init__(self, result):
                self._result = result

            def visit_equal(self, field_ref, literal):
                def _work():
                    with lock:
                        concurrency_a[0] += 1
                        max_concurrency_a[0] = max(max_concurrency_a[0], concurrency_a[0])
                    time.sleep(0.05)
                    with lock:
                        concurrency_a[0] -= 1
                    return self._result
                return executor.submit(_work)

            def close(self):
                pass

        result_all = GlobalIndexResult.from_range(Range(1, 5))
        detecting_reader = ConcurrencyDetectingReader(result_all)
        normal_reader = StubGlobalIndexReader(result_all)

        def readers_fn(field):
            if field.id == 0:
                return [detecting_reader]
            return [normal_reader]

        evaluator = GlobalIndexEvaluator(fields, readers_fn)

        # AND(OR(a=1, b=2), OR(a=3, c=4)) — field a in both OR subtrees
        # evaluator dispatches concurrently, readers own their thread-safety
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[1]),
                        Predicate(method='equal', index=1, field='b', literals=[2]),
                    ],
                ),
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[3]),
                        Predicate(method='equal', index=2, field='c', literals=[4]),
                    ],
                ),
            ],
        )

        evaluator.evaluate(predicate)

        self.assertGreater(max_concurrency_a[0], 1)
        evaluator.close()
        executor.shutdown(wait=False)

    def test_internally_locked_reader_serializes_access(self):
        fields = _make_fields()

        concurrency = [0]
        max_concurrency = [0]
        count_lock = threading.Lock()

        executor = ThreadPoolExecutor(max_workers=4)

        class InternallyLockedReader(GlobalIndexReader):
            def __init__(self):
                self._lock = threading.Lock()

            def visit_equal(self, field_ref, literal):
                def _work():
                    with self._lock:
                        with count_lock:
                            concurrency[0] += 1
                            max_concurrency[0] = max(max_concurrency[0], concurrency[0])
                        time.sleep(0.05)
                        with count_lock:
                            concurrency[0] -= 1
                        return GlobalIndexResult.from_range(Range(1, 3))
                return executor.submit(_work)

            def close(self):
                pass

        locked_reader = InternallyLockedReader()

        def readers_fn(field):
            if field.id == 0:
                return [locked_reader]
            return [StubGlobalIndexReader(GlobalIndexResult.from_range(Range(1, 5)))]

        evaluator = GlobalIndexEvaluator(fields, readers_fn)

        # AND(OR(a=1, b=2), OR(a=3, c=4)) — field a in both OR subtrees
        # reader with internal lock serializes access
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[1]),
                        Predicate(method='equal', index=1, field='b', literals=[2]),
                    ],
                ),
                Predicate(
                    method='or', index=None, field=None,
                    literals=[
                        Predicate(method='equal', index=0, field='a', literals=[3]),
                        Predicate(method='equal', index=2, field='c', literals=[4]),
                    ],
                ),
            ],
        )

        evaluator.evaluate(predicate)

        self.assertEqual(max_concurrency[0], 1)
        evaluator.close()
        executor.shutdown(wait=False)

    def test_multiple_readers_per_field_combined_with_and(self):
        fields = _make_fields()
        reader_result1 = GlobalIndexResult.from_range(Range(1, 5))
        reader_result2 = GlobalIndexResult.from_range(Range(3, 7))

        executor = ThreadPoolExecutor(max_workers=2)
        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [
                AsyncStubReader(reader_result1, executor),
                AsyncStubReader(reader_result2, executor),
            ],
        )

        predicate = Predicate(method='equal', index=0, field='a', literals=[42])
        result = evaluator.evaluate(predicate)

        self.assertIsNotNone(result)
        bm = result.results()
        # Multiple readers for same field are combined with AND (intersection)
        self.assertEqual(bm.cardinality(), 3)
        for v in [3, 4, 5]:
            self.assertTrue(bm.contains(v))
        evaluator.close()
        executor.shutdown(wait=False)

    def test_non_field_leaf_predicate_does_not_throw(self):
        fields = _make_fields()
        result_a = GlobalIndexResult.from_range(Range(1, 3))

        evaluator = GlobalIndexEvaluator(
            fields,
            lambda field: [StubGlobalIndexReader(result_a)],
        )

        # AND(non-field leaf, a=1) — non-field leaf has field=None
        predicate = Predicate(
            method='and', index=None, field=None,
            literals=[
                Predicate(method='alwaysTrue', index=None, field=None, literals=[]),
                Predicate(method='equal', index=0, field='a', literals=[42]),
            ],
        )

        result = evaluator.evaluate(predicate)

        # alwaysTrue has no reader support, returns None — AND ignores it
        self.assertIsNotNone(result)
        self.assertEqual(result.results().cardinality(), 3)
        evaluator.close()

    def test_null_predicate(self):
        fields = _make_fields()
        evaluator = GlobalIndexEvaluator(fields, lambda field: [])

        result = evaluator.evaluate(None)

        self.assertIsNone(result)
        evaluator.close()


if __name__ == '__main__':
    unittest.main()
