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

"""Global index evaluator for filtering data using global indexes."""

import threading
from collections import deque
from concurrent.futures import Future
from typing import Callable, Collection, Dict, List, Optional

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.common.predicate import Predicate
from pypaimon.schema.data_types import DataField


class GlobalIndexEvaluator:
    """Predicate evaluator for filtering data using global indexes.

    Reader visit methods return Future internally — the evaluator no longer
    dispatches to an executor.
    """

    def __init__(
        self,
        fields: List[DataField],
        readers_function: Callable[[DataField], Collection[GlobalIndexReader]],
    ):
        self._fields = fields
        self._field_by_name = {f.name: f for f in fields}
        self._readers_function = readers_function
        self._index_readers_cache: Dict[int, Collection[GlobalIndexReader]] = {}

    def evaluate(
        self,
        predicate: Optional[Predicate]
    ) -> Optional[GlobalIndexResult]:
        if predicate is None:
            return None
        future = self._visit_async(predicate)
        return future.result()

    def _visit_async(self, predicate) -> Future:
        if isinstance(predicate, Predicate) and predicate.method in ('and', 'or'):
            return self._visit_compound_async(predicate)
        return self._visit_leaf_async(predicate)

    def _visit_leaf_async(self, predicate: Predicate) -> Future:
        field = self._field_by_name.get(predicate.field)
        if field is None:
            f = Future()
            f.set_result(None)
            return f

        field_id = field.id
        readers = self._index_readers_cache.get(field_id)
        if readers is None:
            readers = self._readers_function(field)
            self._index_readers_cache[field_id] = readers

        field_ref = FieldRef(predicate.index, predicate.field, str(field.type))

        reader_futures = []
        for reader in readers:
            reader_futures.append(
                self._visit_function(reader, predicate, field_ref)
            )

        all_done = Future()
        if not reader_futures:
            all_done.set_result(None)
            return all_done

        remaining = [len(reader_futures)]
        count_lock = threading.Lock()

        def on_done(_):
            with count_lock:
                remaining[0] -= 1
                if remaining[0] == 0:
                    try:
                        all_done.set_result(
                            self._combine_reader_results(reader_futures)
                        )
                    except Exception as e:
                        all_done.set_exception(e)

        for rf in reader_futures:
            rf.add_done_callback(on_done)

        return all_done

    def _combine_reader_results(
        self, reader_futures: List[Future]
    ) -> Optional[GlobalIndexResult]:
        compound_result: Optional[GlobalIndexResult] = None
        for f in reader_futures:
            child_result = f.result()
            if child_result is None:
                continue
            if compound_result is not None:
                compound_result = compound_result.and_(child_result)
            else:
                compound_result = child_result
            if compound_result.is_empty():
                return compound_result
        return compound_result

    def _visit_compound_async(self, predicate: Predicate) -> Future:
        children = self._flatten_children(predicate.method, predicate.literals)
        child_futures = [self._visit_async(child) for child in children]

        all_done = Future()
        if not child_futures:
            all_done.set_result(None)
            return all_done

        remaining = [len(child_futures)]
        lock = threading.Lock()

        def on_done(_):
            with lock:
                remaining[0] -= 1
                if remaining[0] == 0:
                    try:
                        results = [f.result() for f in child_futures]
                        all_done.set_result(
                            self._combine_results(results, predicate.method)
                        )
                    except Exception as e:
                        all_done.set_exception(e)

        for cf in child_futures:
            cf.add_done_callback(on_done)

        return all_done

    def _combine_results(
        self, results: List[Optional[GlobalIndexResult]], method: str
    ) -> Optional[GlobalIndexResult]:
        if method == 'or':
            compound_result = GlobalIndexResult.create_empty()
            for child_result in results:
                if child_result is None:
                    return None
                compound_result = compound_result.or_(child_result)
            return compound_result
        else:
            compound_result: Optional[GlobalIndexResult] = None
            for child_result in results:
                if child_result is not None:
                    if compound_result is not None:
                        compound_result = compound_result.and_(child_result)
                    else:
                        compound_result = child_result
                if compound_result is not None and compound_result.is_empty():
                    return compound_result
            return compound_result

    def _flatten_children(self, method: str, children) -> list:
        result = []
        stack = deque(children)
        while stack:
            child = stack.popleft()
            if isinstance(child, Predicate) and child.method == method:
                for grandchild in reversed(child.literals):
                    stack.appendleft(grandchild)
            else:
                result.append(child)
        return result

    def _visit_function(
        self,
        reader: GlobalIndexReader,
        predicate: Predicate,
        field_ref: FieldRef
    ) -> 'Future[Optional[GlobalIndexResult]]':
        method = predicate.method
        literals = predicate.literals

        if method == 'equal':
            return reader.visit_equal(field_ref, literals[0])
        elif method == 'notEqual':
            return reader.visit_not_equal(field_ref, literals[0])
        elif method == 'lessThan':
            return reader.visit_less_than(field_ref, literals[0])
        elif method == 'lessOrEqual':
            return reader.visit_less_or_equal(field_ref, literals[0])
        elif method == 'greaterThan':
            return reader.visit_greater_than(field_ref, literals[0])
        elif method == 'greaterOrEqual':
            return reader.visit_greater_or_equal(field_ref, literals[0])
        elif method == 'isNull':
            return reader.visit_is_null(field_ref)
        elif method == 'isNotNull':
            return reader.visit_is_not_null(field_ref)
        elif method == 'in':
            return reader.visit_in(field_ref, literals)
        elif method == 'notIn':
            return reader.visit_not_in(field_ref, literals)
        elif method == 'startsWith':
            return reader.visit_starts_with(field_ref, literals[0])
        elif method == 'endsWith':
            return reader.visit_ends_with(field_ref, literals[0])
        elif method == 'contains':
            return reader.visit_contains(field_ref, literals[0])
        elif method == 'like':
            return reader.visit_like(field_ref, literals[0])
        elif method == 'between':
            return reader.visit_between(field_ref, literals[0], literals[1])

        from pypaimon.globalindex.global_index_reader import _completed_future
        return _completed_future(None)

    def close(self) -> None:
        for readers in self._index_readers_cache.values():
            for reader in readers:
                try:
                    reader.close()
                except Exception:
                    pass
        self._index_readers_cache.clear()

    def __enter__(self) -> 'GlobalIndexEvaluator':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
