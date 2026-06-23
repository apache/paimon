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

"""Full-text read to read index files."""

from abc import ABC, abstractmethod
from concurrent.futures import wait
from typing import Dict, List

from pypaimon.globalindex.full_text_query import (
    BooleanQuery,
    BoostQuery,
    FullTextQuery,
    MatchQuery,
    MultiMatchQuery,
    PhraseQuery,
)
from pypaimon.globalindex.full_text_search import FullTextSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search_result import (
    DictBasedScoredIndexResult,
    ScoredGlobalIndexResult,
)
from pypaimon.table.source.full_text_search_split import FullTextSearchSplit
from pypaimon.table.source.full_text_scan import FullTextScanPlan
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class FullTextRead(ABC):
    """Full-text read to read index files."""

    def read_plan(self, plan: FullTextScanPlan) -> GlobalIndexResult:
        return self.read(plan.splits())

    @abstractmethod
    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        pass


class FullTextReadImpl(FullTextRead):
    """Implementation for FullTextRead."""

    def __init__(
        self,
        table: 'FileStoreTable',
        limit: int,
        text_column,
        query: FullTextQuery
    ):
        self._table = table
        self._limit = limit
        self._text_columns = text_column if isinstance(text_column, list) else [text_column]
        self._query = query

    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        if not splits:
            return GlobalIndexResult.create_empty()

        splits_by_column: Dict[str, List[FullTextSearchSplit]] = {}
        for split in splits:
            splits_by_column.setdefault(split.column_name, []).append(split)
        return self._eval_query(self._query, splits_by_column).top_k(self._limit)

    def _eval_query(
            self,
            query: FullTextQuery,
            splits_by_column: Dict[str, List[FullTextSearchSplit]]
    ) -> ScoredGlobalIndexResult:
        if isinstance(query, MatchQuery):
            return self._eval_column_query(query, query.column, splits_by_column)
        if isinstance(query, PhraseQuery):
            return self._eval_column_query(query, query.column, splits_by_column)
        if isinstance(query, MultiMatchQuery):
            results = []
            for column, boost in zip(query.columns, query.boosts):
                match = MatchQuery(
                    query.query, column, boost=boost, operator=query.operator)
                results.append(
                    self._eval_column_query(match, column, splits_by_column))
            return _or(results).top_k(self._limit)
        if isinstance(query, BoostQuery):
            positive = self._eval_query(query.positive, splits_by_column)
            negative = self._eval_query(query.negative, splits_by_column)
            return _boost(positive, negative, query.negative_boost)
        if isinstance(query, BooleanQuery):
            result = None
            for child in query.must():
                child_result = self._eval_query(child, splits_by_column)
                result = child_result if result is None else _and(result, child_result)

            should_results = []
            for child in query.should():
                should_results.append(self._eval_query(child, splits_by_column))
            if should_results:
                should_result = _or(should_results)
                result = should_result if result is None else _and_with_bonus(
                    result, should_result)

            if result is None:
                return ScoredGlobalIndexResult.create_empty()
            for child in query.must_not():
                result = _and_not(result, self._eval_query(child, splits_by_column))
            return result
        raise ValueError("Unsupported full-text query type: %s" % type(query).__name__)

    def _eval_column_query(
            self,
            query: FullTextQuery,
            column: str,
            splits_by_column: Dict[str, List[FullTextSearchSplit]]
    ) -> ScoredGlobalIndexResult:
        splits = splits_by_column.get(column, [])
        if not splits:
            return ScoredGlobalIndexResult.create_empty()
        futures = [
            self._eval(
                split.row_range_start, split.row_range_end,
                split.full_text_index_files,
                query,
            )
            for split in splits
        ]

        wait(futures)

        merged_scores = {}
        for future in futures:
            split_result = future.result()
            if split_result is not None:
                score_getter = split_result.score_getter()
                for row_id in split_result.results():
                    if row_id not in merged_scores:
                        merged_scores[row_id] = score_getter(row_id)

        return DictBasedScoredIndexResult(merged_scores).top_k(self._limit)

    def _eval(self, row_range_start, row_range_end, full_text_index_files, query):
        index_io_meta_list = []
        for index_file in full_text_index_files:
            meta = index_file.global_index_meta
            assert meta is not None
            index_io_meta_list.append(
                GlobalIndexIOMeta(
                    file_name=index_file.file_name,
                    file_size=index_file.file_size,
                    metadata=meta.index_meta,
                    external_path=index_file.external_path,
                )
            )

        index_type = full_text_index_files[0].index_type
        index_path = self._table.path_factory().global_index_path_factory().index_path()
        file_io = self._table.file_io

        reader = _create_full_text_reader(
            index_type, file_io, index_path,
            index_io_meta_list
        )

        full_text_search = FullTextSearch(
            query=query,
            limit=_candidate_limit(row_range_start, row_range_end),
        )

        offset_reader = OffsetGlobalIndexReader(reader, row_range_start, row_range_end)
        future = offset_reader.visit_full_text_search(full_text_search)
        future.add_done_callback(lambda _: reader.close())
        return future


def _and(left: ScoredGlobalIndexResult, right: ScoredGlobalIndexResult):
    bitmap = RoaringBitmap64.and_(left.results(), right.results())
    left_score = left.score_getter()
    right_score = right.score_getter()
    return ScoredGlobalIndexResult.create(
        bitmap,
        lambda row_id: (left_score(row_id) or 0.0) + (right_score(row_id) or 0.0),
    )


def _or(results: List[ScoredGlobalIndexResult]):
    scores = {}
    for result in results:
        score_getter = result.score_getter()
        for row_id in result.results():
            scores[row_id] = scores.get(row_id, 0.0) + (score_getter(row_id) or 0.0)
    return DictBasedScoredIndexResult(scores)


def _and_with_bonus(base: ScoredGlobalIndexResult, bonus: ScoredGlobalIndexResult):
    bitmap = base.results()
    base_score = base.score_getter()
    bonus_score = bonus.score_getter()
    bonus_rows = bonus.results()
    return ScoredGlobalIndexResult.create(
        bitmap,
        lambda row_id: (
            (base_score(row_id) or 0.0)
            + ((bonus_score(row_id) or 0.0) if bonus_rows.contains(row_id) else 0.0)
        ),
    )


def _and_not(left: ScoredGlobalIndexResult, right: ScoredGlobalIndexResult):
    bitmap = RoaringBitmap64.remove_all(left.results(), right.results())
    return ScoredGlobalIndexResult.create(bitmap, left.score_getter())


def _boost(
        positive: ScoredGlobalIndexResult,
        negative: ScoredGlobalIndexResult,
        negative_boost: float):
    positive_score = positive.score_getter()
    negative_rows = negative.results()
    return ScoredGlobalIndexResult.create(
        positive.results(),
        lambda row_id: (
            (positive_score(row_id) or 0.0)
            * (negative_boost if negative_rows.contains(row_id) else 1.0)
        ),
    )


def _candidate_limit(row_range_start: int, row_range_end: int) -> int:
    return row_range_end - row_range_start + 1


def _create_full_text_reader(index_type, file_io, index_path, index_io_meta_list):
    """Create a global index reader for full-text search."""
    from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
        TANTIVY_FULLTEXT_IDENTIFIER,
    )
    if index_type == TANTIVY_FULLTEXT_IDENTIFIER:
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )
        return TantivyFullTextGlobalIndexReader(file_io, index_path, index_io_meta_list)
    raise ValueError(f"Unsupported full-text index type: '{index_type}'")
