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

"""A GlobalIndexReader that unions results from multiple underlying readers.

Each visit_* call dispatches to every underlying reader (which returns Future
internally) and combines the results via OR once all futures complete.
"""

import threading
from concurrent.futures import Future
from typing import Callable, List, Optional

from pypaimon.globalindex.global_index_reader import FieldRef, GlobalIndexReader, _completed_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult


class UnionGlobalIndexReader(GlobalIndexReader):

    def __init__(self, readers: List[GlobalIndexReader]):
        self._readers = readers

    def _union_futures(self, visitor: Callable[[GlobalIndexReader], 'Future[Optional[GlobalIndexResult]]']
                       ) -> 'Future[Optional[GlobalIndexResult]]':
        futures = [visitor(reader) for reader in self._readers]

        if not futures:
            return _completed_future(None)

        all_done = Future()
        remaining = [len(futures)]
        lock = threading.Lock()

        def on_done(_):
            with lock:
                remaining[0] -= 1
                if remaining[0] == 0:
                    try:
                        result: Optional[GlobalIndexResult] = None
                        for f in futures:
                            current = f.result()
                            if current is None:
                                continue
                            if result is None:
                                result = current
                            else:
                                result = result.or_(current)
                        all_done.set_result(result)
                    except Exception as e:
                        all_done.set_exception(e)

        for f in futures:
            f.add_done_callback(on_done)

        return all_done

    # ---- vector / full-text search ----------------------------------------

    def visit_vector_search(self, vector_search) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_vector_search(vector_search))

    def visit_full_text_search(self, full_text_search) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_full_text_search(full_text_search))

    # ---- scalar predicates (every reader sees the visit) ------------------

    def visit_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_equal(field_ref, literal))

    def visit_not_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_not_equal(field_ref, literal))

    def visit_less_than(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_less_than(field_ref, literal))

    def visit_less_or_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_less_or_equal(field_ref, literal))

    def visit_greater_than(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_greater_than(field_ref, literal))

    def visit_greater_or_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_greater_or_equal(field_ref, literal))

    def visit_is_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_is_null(field_ref))

    def visit_is_not_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_is_not_null(field_ref))

    def visit_in(self, field_ref: FieldRef, literals) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_in(field_ref, literals))

    def visit_not_in(self, field_ref: FieldRef, literals) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_not_in(field_ref, literals))

    def visit_starts_with(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_starts_with(field_ref, literal))

    def visit_ends_with(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_ends_with(field_ref, literal))

    def visit_contains(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_contains(field_ref, literal))

    def visit_like(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_like(field_ref, literal))

    def visit_between(self, field_ref: FieldRef, from_v, to_v) -> 'Future[Optional[GlobalIndexResult]]':
        return self._union_futures(lambda r: r.visit_between(field_ref, from_v, to_v))

    def close(self) -> None:
        for reader in self._readers:
            try:
                reader.close()
            except Exception:
                pass
