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

"""Global index reader interface."""

from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pypaimon.globalindex.global_index_result import GlobalIndexResult
    from pypaimon.globalindex.vector_search import VectorSearch


class FieldRef:
    """Reference to a field in the schema."""

    def __init__(self, index: int, name: str, data_type: str):
        self.index = index
        self.name = name
        self.data_type = data_type


class GlobalIndexReader(ABC):
    """
    Index reader for global index, returns GlobalIndexResult.
    
    This is the base interface for all global index readers.
    """

    def visit_vector_search(self, vector_search: 'VectorSearch') -> Optional['GlobalIndexResult']:
        """Visit a vector search query."""
        raise NotImplementedError("Vector search not supported by this reader")

    def visit_equal(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit an equality predicate."""
        return None

    def visit_not_equal(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a not-equal predicate."""
        return None

    def visit_less_than(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a less-than predicate."""
        return None

    def visit_less_or_equal(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a less-or-equal predicate."""
        return None

    def visit_greater_than(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a greater-than predicate."""
        return None

    def visit_greater_or_equal(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a greater-or-equal predicate."""
        return None

    def visit_is_null(self, field_ref: FieldRef) -> Optional['GlobalIndexResult']:
        """Visit an is-null predicate."""
        return None

    def visit_is_not_null(self, field_ref: FieldRef) -> Optional['GlobalIndexResult']:
        """Visit an is-not-null predicate."""
        return None

    def visit_in(self, field_ref: FieldRef, literals: List[object]) -> Optional['GlobalIndexResult']:
        """Visit an in predicate."""
        return None

    def visit_not_in(self, field_ref: FieldRef, literals: List[object]) -> Optional['GlobalIndexResult']:
        """Visit a not-in predicate."""
        return None

    def visit_starts_with(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a starts-with predicate."""
        return None

    def visit_ends_with(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit an ends-with predicate."""
        return None

    def visit_contains(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a contains predicate."""
        return None

    def visit_like(self, field_ref: FieldRef, literal: object) -> Optional['GlobalIndexResult']:
        """Visit a like predicate."""
        return None

    def visit_between(self, field_ref: FieldRef, min_v: object, max_v: object) -> Optional['GlobalIndexResult']:
        """Visit a between predicate."""
        return None

    @abstractmethod
    def close(self) -> None:
        """Close the reader and release resources."""
        pass
