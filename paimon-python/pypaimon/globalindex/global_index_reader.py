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

"""Global index reader interface."""

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import List, Optional


class FieldRef:
    """Reference to a field in the schema."""

    def __init__(self, index: int, name: str, data_type: str):
        self.index = index
        self.name = name
        self.data_type = data_type


def _completed_future(value):
    """Create a Future that is already completed with the given value."""
    f = Future()
    f.set_result(value)
    return f


def _map_future(source, transform):
    """Create a new Future whose result is transform(source.result())."""
    result = Future()

    def on_done(f):
        try:
            result.set_result(transform(f.result()))
        except Exception as e:
            result.set_exception(e)

    source.add_done_callback(on_done)
    return result


class GlobalIndexReader(ABC):
    """Index reader for global index. All visit methods return Future[Optional[GlobalIndexResult]]."""

    def visit_vector_search(self, vector_search: 'VectorSearch') -> 'Future[Optional[GlobalIndexResult]]':
        raise NotImplementedError("Vector search not supported by this reader")

    def visit_full_text_search(self, full_text_search: 'FullTextSearch') -> 'Future[Optional[GlobalIndexResult]]':
        raise NotImplementedError("Full-text search not supported by this reader")

    def visit_equal(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_not_equal(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_less_than(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_less_or_equal(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_greater_than(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_greater_or_equal(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_is_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_is_not_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_in(self, field_ref: FieldRef, literals: List[object]) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_not_in(self, field_ref: FieldRef, literals: List[object]) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_starts_with(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_ends_with(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_contains(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_like(self, field_ref: FieldRef, literal: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    def visit_between(self, field_ref: FieldRef, min_v: object, max_v: object) -> 'Future[Optional[GlobalIndexResult]]':
        return _completed_future(None)

    @abstractmethod
    def close(self) -> None:
        pass
