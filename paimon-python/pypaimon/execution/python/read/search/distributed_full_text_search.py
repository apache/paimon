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

"""Python full-text tasks preserve corpus statistics and plan-ordered merging."""

from contextlib import closing

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.python.read.search.full_text_read import (
    DataEvolutionFullTextRead, _index_candidate_limit, _index_type, _raw_row_ranges, _split_search_splits,
)
from pypaimon.execution.python.read.search import global_index_live_row_filter
from pypaimon.execution.python.read.search.distributed_vector_search import _index_tasks, _scores
from pypaimon.table.source.search_execution import SearchTask
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class DistributedFullTextRead(DataEvolutionFullTextRead):
    def __init__(self, reader, executor):
        super().__init__(reader._table, reader._limit, reader._text_columns, reader._query,
                         reader._partition_filter, reader._filter)
        self._executor = executor

    def _worker_reader(self):
        reader = DataEvolutionFullTextRead(
            self._table.copy({"global-index.enabled": "false"}), self._limit, self._text_columns, self._query,
            self._partition_filter, self._filter)
        reader._snapshot = self._snapshot
        return reader

    def read(self, splits):
        # Keep bitmap construction and candidate data reads off the driver.
        index_splits, raw_splits = _split_search_splits(splits)
        indexed = self._eval_column_query({self._text_columns[0].name: index_splits}, None)
        raw = self._read_raw_search(_raw_row_ranges(raw_splits), _index_type(index_splits))
        return indexed.or_(raw).top_k(self._limit)

    def _eval_column_query(self, splits_by_column, live_rows):
        items = splits_by_column.get(self._text_columns[0].name, [])
        if not items:
            return DictBasedScoredIndexResult({})
        merged = {}
        reader = self._worker_reader()
        reader._limit = _index_candidate_limit(items, self._limit)
        work = _index_tasks(_search_index, reader, items, self._filter, _prepare_scalar_filter)
        with closing(self._executor.map(work, ordered=True)) as tasks:
            for _, scores in tasks:
                # Duplicate row IDs keep the first planned shard's score.
                for row_id, score in scores.items():
                    merged.setdefault(row_id, score)
        return DictBasedScoredIndexResult(merged).top_k(self._limit)

    def _read_raw_search(self, ranges, index_type, include_row_ids=None):
        if not ranges:
            return DictBasedScoredIndexResult({})
        include = None if include_row_ids is None else include_row_ids.serialize()
        # Splitting this corpus would change document frequencies and BM25.
        # Keep the existing local corpus intact, but read/build/search on a worker.
        work = [SearchTask(_search_raw, self._worker_reader(), (ranges, index_type, include))]
        with closing(self._executor.map(work)) as tasks:
            for _, scores in tasks:
                return DictBasedScoredIndexResult(scores)


def _search_index(reader, split, scalar_filter=None):
    live = global_index_live_row_filter.live_rows(
        reader._table, reader._partition_filter, reader._snapshot,
        row_ranges=[Range(split.row_range_start, split.row_range_end)])
    if scalar_filter is None:
        include = reader._matched_indexed_rows([split], live)
    else:
        bitmap, exact = scalar_filter
        result = None if bitmap is None else GlobalIndexResult.create(
            RoaringBitmap64.deserialize(bitmap), is_exact=exact)
        include = reader._matched_indexed_rows([split], live, result)
    if include is not None and include.is_empty():
        return {}
    return _scores(reader._eval(split.row_range_start, split.row_range_end,
                                split.full_text_index_files, include).result())


def _prepare_scalar_filter(reader, split):
    result = reader._scalar_index_result(split.scalar_index_files, reader._snapshot)
    return (None, False) if result is None else (result.results().serialize(), result.is_exact())


def _search_raw(reader, item):
    ranges, index_type, include = item
    include = None if include is None else RoaringBitmap64.deserialize(include)
    if reader._filter is not None:
        live = global_index_live_row_filter.live_rows(
            reader._table, reader._partition_filter, reader._snapshot, row_ranges=ranges)
        candidates = GlobalIndexResult.from_ranges(ranges).results()
        if live is not None:
            candidates = RoaringBitmap64.and_(candidates, live)
        if include is not None:
            candidates = RoaringBitmap64.and_(candidates, include)
        include = reader._matching_rows(candidates)
    return _scores(reader._read_raw_search(ranges, index_type, include))
