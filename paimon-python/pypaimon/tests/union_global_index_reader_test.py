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
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import Mock

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_reader import (
    FieldRef,
    GlobalIndexReader,
    _completed_future,
)
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.union_global_index_reader import UnionGlobalIndexReader
from pypaimon.utils.range import Range


class _EqualReader(GlobalIndexReader):

    def __init__(self, result):
        self._result = result

    def visit_equal(self, field_ref, literal):
        return _completed_future(self._result)

    def close(self):
        pass


class _VectorReader(GlobalIndexReader):

    def __init__(self, result_future):
        self._result_future = result_future

    def visit_vector_search(self, vector_search):
        return self._result_future

    def close(self):
        pass


class UnionGlobalIndexReaderTest(unittest.TestCase):

    def test_scalar_unsupported_is_propagated(self):
        supported = GlobalIndexResult.from_range(Range(1, 2))
        reader = UnionGlobalIndexReader([_EqualReader(supported), _EqualReader(None)])

        result = reader.visit_equal(FieldRef(0, "f0", "INT"), 1).result()

        self.assertIsNone(result)

    def test_all_scalar_visitors_propagate_unsupported(self):
        field_ref = FieldRef(0, "f0", "INT")
        cases = [
            ("visit_not_equal", (field_ref, 1)),
            ("visit_less_than", (field_ref, 1)),
            ("visit_less_or_equal", (field_ref, 1)),
            ("visit_greater_than", (field_ref, 1)),
            ("visit_greater_or_equal", (field_ref, 1)),
            ("visit_is_null", (field_ref,)),
            ("visit_is_not_null", (field_ref,)),
            ("visit_in", (field_ref, [1, 2])),
            ("visit_not_in", (field_ref, [1, 2])),
            ("visit_starts_with", (field_ref, "1")),
            ("visit_ends_with", (field_ref, "1")),
            ("visit_contains", (field_ref, "1")),
            ("visit_like", (field_ref, "1%")),
            ("visit_between", (field_ref, 1, 2)),
            ("visit_not_between", (field_ref, 1, 2)),
        ]

        for method_name, args in cases:
            with self.subTest(method=method_name):
                supported_reader = Mock(spec=GlobalIndexReader)
                unsupported_reader = Mock(spec=GlobalIndexReader)
                supported_result = GlobalIndexResult.from_range(Range(1, 2))
                getattr(supported_reader, method_name).return_value = _completed_future(
                    supported_result)
                getattr(unsupported_reader, method_name).return_value = _completed_future(None)
                reader = UnionGlobalIndexReader([supported_reader, unsupported_reader])

                result = getattr(reader, method_name)(*args).result()

                self.assertIsNone(result)

    def test_batch_vector_search_returns_before_children_complete(self):
        child_result = Future()
        reader = UnionGlobalIndexReader([_VectorReader(child_result)])
        search = BatchVectorSearch(vectors=[[1.0]], limit=1, field_name="f0")
        executor = ThreadPoolExecutor(max_workers=1)
        invocation = executor.submit(reader.visit_batch_vector_search, search)

        try:
            result_future = invocation.result(timeout=1)
            self.assertFalse(result_future.done())
        finally:
            child_result.set_result(None)
            executor.shutdown()

        self.assertEqual([None], result_future.result(timeout=1))

    def test_batch_vector_search_propagates_failure_asynchronously(self):
        child_result = Future()
        reader = UnionGlobalIndexReader([_VectorReader(child_result)])
        search = BatchVectorSearch(vectors=[[1.0]], limit=1, field_name="f0")

        result_future = reader.visit_batch_vector_search(search)
        child_result.set_exception(RuntimeError("async failure"))

        with self.assertRaisesRegex(RuntimeError, "async failure"):
            result_future.result(timeout=1)

    def test_close_propagates_failure_after_closing_all_readers(self):
        failing_reader = Mock(spec=GlobalIndexReader)
        failing_reader.close.side_effect = OSError("close failed")
        remaining_reader = Mock(spec=GlobalIndexReader)
        reader = UnionGlobalIndexReader([failing_reader, remaining_reader])

        with self.assertRaisesRegex(OSError, "close failed"):
            reader.close()

        remaining_reader.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
