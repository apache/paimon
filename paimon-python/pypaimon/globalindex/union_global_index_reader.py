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

"""A GlobalIndexReader that unions results from multiple underlying readers.

Each visit_* call is dispatched to every underlying reader and the results are
OR-combined into a single ``GlobalIndexResult``. Readers returning ``None``
("cannot answer") are skipped; an empty bitmap DOES contribute to the union
and is NOT a short-circuit signal.
"""

from typing import Callable, List, Optional

from pypaimon.globalindex.global_index_reader import FieldRef, GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult


class UnionGlobalIndexReader(GlobalIndexReader):

    def __init__(self, readers: List[GlobalIndexReader]):
        self._readers = readers

    def _union(self, visitor: Callable[[GlobalIndexReader], Optional[GlobalIndexResult]]
               ) -> Optional[GlobalIndexResult]:
        result: Optional[GlobalIndexResult] = None
        for reader in self._readers:
            current = visitor(reader)
            if current is None:
                continue
            if result is None:
                result = current
            else:
                result = result.or_(current)
        return result

    # ---- vector / full-text search ----------------------------------------

    def visit_vector_search(self, vector_search) -> Optional[GlobalIndexResult]:
        from pypaimon.globalindex.vector_search_result import (
            ScoredGlobalIndexResult,
        )
        result: Optional[ScoredGlobalIndexResult] = None
        for reader in self._readers:
            current = reader.visit_vector_search(vector_search)
            if current is None:
                continue
            result = current if result is None else result.or_(current)
        return result

    def visit_full_text_search(self, full_text_search) -> Optional[GlobalIndexResult]:
        return self._union(lambda r: r.visit_full_text_search(full_text_search))

    # ---- scalar predicates (every reader sees the visit) ------------------

    def visit_equal(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_equal(field_ref, literal))

    def visit_not_equal(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_not_equal(field_ref, literal))

    def visit_less_than(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_less_than(field_ref, literal))

    def visit_less_or_equal(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_less_or_equal(field_ref, literal))

    def visit_greater_than(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_greater_than(field_ref, literal))

    def visit_greater_or_equal(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_greater_or_equal(field_ref, literal))

    def visit_is_null(self, field_ref: FieldRef):
        return self._union(lambda r: r.visit_is_null(field_ref))

    def visit_is_not_null(self, field_ref: FieldRef):
        return self._union(lambda r: r.visit_is_not_null(field_ref))

    def visit_in(self, field_ref: FieldRef, literals):
        return self._union(lambda r: r.visit_in(field_ref, literals))

    def visit_not_in(self, field_ref: FieldRef, literals):
        return self._union(lambda r: r.visit_not_in(field_ref, literals))

    def visit_starts_with(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_starts_with(field_ref, literal))

    def visit_ends_with(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_ends_with(field_ref, literal))

    def visit_contains(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_contains(field_ref, literal))

    def visit_like(self, field_ref: FieldRef, literal):
        return self._union(lambda r: r.visit_like(field_ref, literal))

    def visit_between(self, field_ref: FieldRef, from_v, to_v):
        return self._union(lambda r: r.visit_between(field_ref, from_v, to_v))

    def close(self) -> None:
        for reader in self._readers:
            try:
                reader.close()
            except Exception:
                pass
