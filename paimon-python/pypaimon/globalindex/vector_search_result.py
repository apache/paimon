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

"""Vector search global index result."""

from abc import abstractmethod
from typing import Callable, Dict, Optional

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


# Type alias for score getter function
ScoreGetter = Callable[[int], Optional[float]]


class ScoredGlobalIndexResult(GlobalIndexResult):
    """
    Vector search global index result for vector index.
    
    This extends GlobalIndexResult with score information for each row ID.
    """

    @abstractmethod
    def score_getter(self) -> ScoreGetter:
        """Returns a function to get the score for a given row ID."""
        pass

    def offset(self, offset: int) -> 'ScoredGlobalIndexResult':
        """Returns a new result with row IDs offset by the given amount."""
        if offset == 0:
            return self
        
        bitmap = self.results()
        this_score_getter = self.score_getter()
        
        offset_bitmap = RoaringBitmap64()
        for row_id in bitmap:
            offset_bitmap.add(row_id + offset)
        
        return SimpleScoredGlobalIndexResult(
            offset_bitmap,
            lambda row_id: this_score_getter(row_id - offset)
        )

    def or_(self, other: GlobalIndexResult) -> GlobalIndexResult:
        """Returns the union of this result and the other result."""
        if not isinstance(other, ScoredGlobalIndexResult):
            return super().or_(other)
        
        this_row_ids = self.results()
        this_score_getter = self.score_getter()
        
        other_row_ids = other.results()
        other_score_getter = other.score_getter()
        
        result_or = RoaringBitmap64.or_(this_row_ids, other_row_ids)
        
        def combined_score_getter(row_id: int) -> Optional[float]:
            if row_id in this_row_ids:
                return this_score_getter(row_id)
            return other_score_getter(row_id)
        
        return SimpleScoredGlobalIndexResult(result_or, combined_score_getter)

    @staticmethod
    def create(
        supplier: Callable[[], RoaringBitmap64],
        score_getter: ScoreGetter
    ) -> 'ScoredGlobalIndexResult':
        """Creates a new VectorSearchGlobalIndexResult from supplier."""
        return LazyScoredGlobalIndexResult(supplier, score_getter)


class SimpleScoredGlobalIndexResult(ScoredGlobalIndexResult):
    """Simple implementation of VectorSearchGlobalIndexResult."""

    def __init__(self, bitmap: RoaringBitmap64, score_getter_fn: ScoreGetter):
        self._bitmap = bitmap
        self._score_getter_fn = score_getter_fn

    def results(self) -> RoaringBitmap64:
        return self._bitmap

    def score_getter(self) -> ScoreGetter:
        return self._score_getter_fn


class LazyScoredGlobalIndexResult(ScoredGlobalIndexResult):
    """Lazy implementation of VectorSearchGlobalIndexResult."""

    def __init__(self, supplier: Callable[[], RoaringBitmap64], score_getter_fn: ScoreGetter):
        self._supplier = supplier
        self._score_getter_fn = score_getter_fn
        self._cached: Optional[RoaringBitmap64] = None

    def results(self) -> RoaringBitmap64:
        if self._cached is None:
            self._cached = self._supplier()
        return self._cached

    def score_getter(self) -> ScoreGetter:
        return self._score_getter_fn


class DictBasedScoredIndexResult(ScoredGlobalIndexResult):
    """Vector search result backed by a dictionary of row_id -> score."""

    def __init__(self, id_to_scores: Dict[int, float]):
        self._id_to_scores = id_to_scores
        self._bitmap: Optional[RoaringBitmap64] = None

    def results(self) -> RoaringBitmap64:
        if self._bitmap is None:
            self._bitmap = RoaringBitmap64()
            for row_id in self._id_to_scores.keys():
                self._bitmap.add(row_id)
        return self._bitmap

    def score_getter(self) -> ScoreGetter:
        return lambda row_id: self._id_to_scores.get(row_id)
