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

from abc import ABC, abstractmethod
from typing import Callable, List, Optional

from pypaimon.utils.roaring_bitmap import RoaringBitmap64
from pypaimon.utils.range import Range


class GlobalIndexResult(ABC):
    """Global index result represents row ids as a compressed bitmap."""

    @abstractmethod
    def results(self) -> RoaringBitmap64:
        """Returns the bitmap representing row ids."""
        pass

    def offset(self, start_offset: int) -> 'GlobalIndexResult':
        """Returns a new result with row IDs offset by the given amount."""
        if start_offset == 0:
            return self
        bitmap = self.results()
        offset_bitmap = RoaringBitmap64()
        for row_id in bitmap:
            offset_bitmap.add(row_id + start_offset)
        return LazyGlobalIndexResult(lambda: offset_bitmap)

    def and_(self, other: 'GlobalIndexResult') -> 'GlobalIndexResult':
        """Returns the intersection of this result and the other result."""
        return LazyGlobalIndexResult(
            lambda: RoaringBitmap64.and_(self.results(), other.results())
        )

    def or_(self, other: 'GlobalIndexResult') -> 'GlobalIndexResult':
        """Returns the union of this result and the other result."""
        return LazyGlobalIndexResult(
            lambda: RoaringBitmap64.or_(self.results(), other.results())
        )

    def is_empty(self) -> bool:
        """Check if the result is empty."""
        return self.results().is_empty()

    @staticmethod
    def create_empty() -> 'GlobalIndexResult':
        """Returns an empty GlobalIndexResult."""
        return LazyGlobalIndexResult(lambda: RoaringBitmap64())

    @staticmethod
    def create(supplier: Callable[[], RoaringBitmap64]) -> 'GlobalIndexResult':
        """Returns a new GlobalIndexResult from supplier."""
        return LazyGlobalIndexResult(supplier)

    @staticmethod
    def from_range(range_: Range) -> 'GlobalIndexResult':
        """Returns a new GlobalIndexResult from Range."""
        def create_bitmap():
            result = RoaringBitmap64()
            result.add_range(range_.from_, range_.to)
            return result
        return LazyGlobalIndexResult(create_bitmap)

    @staticmethod
    def from_ranges(ranges: List[Range]) -> 'GlobalIndexResult':
        """Returns a new GlobalIndexResult from multiple Ranges."""
        def create_bitmap():
            result = RoaringBitmap64()
            for r in ranges:
                result.add_range(r.from_, r.to)
            return result
        return LazyGlobalIndexResult(create_bitmap)


class LazyGlobalIndexResult(GlobalIndexResult):
    """Lazy implementation of GlobalIndexResult that delays computation."""

    def __init__(self, supplier: Callable[[], RoaringBitmap64]):
        self._supplier = supplier
        self._cached: Optional[RoaringBitmap64] = None

    def results(self) -> RoaringBitmap64:
        if self._cached is None:
            self._cached = self._supplier()
        return self._cached
