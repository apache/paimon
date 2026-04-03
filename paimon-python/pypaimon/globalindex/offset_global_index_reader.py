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

"""A GlobalIndexReader that wraps another reader and applies an offset to all row IDs."""

from typing import List, Optional

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef
from pypaimon.globalindex.global_index_result import GlobalIndexResult


class OffsetGlobalIndexReader(GlobalIndexReader):
    """
    A GlobalIndexReader that wraps another reader and applies an offset
    to all row IDs in the results.
    """

    def __init__(self, wrapped: GlobalIndexReader, offset: int, to: int):
        self._wrapped = wrapped
        self._offset = offset
        self._to = to

    def visit_vector_search(self, vector_search) -> Optional[GlobalIndexResult]:
        result = self._wrapped.visit_vector_search(
            vector_search.offset_range(self._offset, self._to))
        if result is not None:
            return result.offset(self._offset)
        return None

    def visit_full_text_search(self, full_text_search) -> Optional[GlobalIndexResult]:
        result = self._wrapped.visit_full_text_search(full_text_search)
        if result is not None:
            return result.offset(self._offset)
        return None

    def visit_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_equal(field_ref, literal))

    def visit_not_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_not_equal(field_ref, literal))

    def visit_less_than(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_less_than(field_ref, literal))

    def visit_less_or_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_less_or_equal(field_ref, literal))

    def visit_greater_than(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_greater_than(field_ref, literal))

    def visit_greater_or_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_greater_or_equal(field_ref, literal))

    def visit_is_null(self, field_ref: FieldRef) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_is_null(field_ref))

    def visit_is_not_null(self, field_ref: FieldRef) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_is_not_null(field_ref))

    def visit_in(self, field_ref: FieldRef, literals: List[object]) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_in(field_ref, literals))

    def visit_not_in(self, field_ref: FieldRef, literals: List[object]) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_not_in(field_ref, literals))

    def visit_starts_with(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_starts_with(field_ref, literal))

    def visit_ends_with(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_ends_with(field_ref, literal))

    def visit_contains(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_contains(field_ref, literal))

    def visit_like(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_like(field_ref, literal))

    def visit_between(self, field_ref: FieldRef, min_v: object, max_v: object) -> Optional[GlobalIndexResult]:
        return self._apply_offset(self._wrapped.visit_between(field_ref, min_v, max_v))

    def _apply_offset(self, result: Optional[GlobalIndexResult]) -> Optional[GlobalIndexResult]:
        if result is not None:
            return result.offset(self._offset)
        return None

    def close(self) -> None:
        self._wrapped.close()
