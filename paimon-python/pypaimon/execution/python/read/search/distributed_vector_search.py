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

"""Python search tasks and global result coordination for distributed executors."""

from collections import Counter
from contextlib import closing
import math

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.python.read.search.vector_search_read import (
    BatchVectorSearchReadImpl, DataEvolutionVectorRead,
    _filtered_raw_row_ranges, _offer_score, _scored_result, _split_search_splits,
)
from pypaimon.execution.python.read.search import global_index_live_row_filter
from pypaimon.table.source.search_execution import SearchTask
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class DistributedVectorSearchRead(DataEvolutionVectorRead):
    """Replace split execution while retaining local merge/refinement semantics."""

    def __init__(self, reader, executor):
        super().__init__(
            reader._table, reader._limit, reader._vector_column, reader._query_vector,
            reader._filter, reader._partition_filter, reader._options)
        self._executor = executor

    def _prepare_search_splits(self, splits, snapshot):
        # Scalar evaluation and candidate verification belong to the workers.
        return _split_search_splits(splits)

    def _search_index_splits(self, splits, query, search_limit, snapshot, batch=False):
        if not splits:
            return
        # Send predicates and metadata, leaving each worker to build and consume
        # its own filter. Batch queries reuse the same filter for every vector.
        context = (self._table, self._vector_column, query, search_limit, self._options,
                   self._filter, self._partition_filter, snapshot)
        work = _index_tasks(_search_index_split, context, splits, self._filter)
        with closing(self._executor.map(work, ordered=True)) as tasks:
            for _, (metric, scores) in tasks:
                if metric is not None:
                    self._set_index_metric(metric)
                # Keep plan order, including duplicate-row precedence, regardless
                # of worker completion order. Do not refine or truncate per worker.
                yield DictBasedScoredIndexResult(scores)
                del scores

    def _read_raw_search(self, raw_row_ranges, pre_filter, query_vector,
                         index_type=None, include_filter=True,
                         score_candidates=None, snapshot=None):
        ranges = _filtered_raw_row_ranges(raw_row_ranges, pre_filter)
        if score_candidates is not None:
            # Refine only the globally selected candidates. Intersect before
            # planning so workers neither read nor score other rows.
            ranges = _filtered_raw_row_ranges(ranges, score_candidates.to_range_list())
        if not ranges:
            return DictBasedScoredIndexResult({})
        table_read, splits = self._plan_raw_read(ranges, include_filter, snapshot)
        context = (table_read, self._vector_column, query_vector,
                   self._limit, self._search_metric(index_type))
        heap = []
        # Keep each planned data split intact: its files may be vertical column
        # groups that must be read together to reconstruct a logical row.
        work = [SearchTask(_search_raw_split, context, split) for split in splits]
        with closing(self._executor.map(work)) as tasks:
            for _, scores in tasks:
                for row_id, score in scores.items():
                    _offer_score(heap, self._limit, row_id, score)
        return _scored_result(heap)


class DistributedBatchVectorSearchRead(BatchVectorSearchReadImpl):

    def __init__(self, reader, executor):
        super().__init__(reader._table, reader._limit, reader._vector_column, reader._query_vectors,
                         reader._filter, reader._partition_filter, reader._options)
        self._executor = executor

    def _prepare_search_splits(self, splits, snapshot):
        # Scalar evaluation and candidate verification belong to the workers.
        return _split_search_splits(splits)

    def _search_index_splits(self, splits, query, search_limit, snapshot, batch=False):
        if not splits:
            return
        # Send predicates and metadata, leaving each worker to build and consume
        # its own filter. Batch queries reuse the same filter for every vector.
        context = (self._table, self._vector_column, query, search_limit, self._options,
                   self._filter, self._partition_filter, snapshot)
        work = _index_tasks(_search_batch_index_split, context, splits, self._filter)
        with closing(self._executor.map(work, ordered=True)) as tasks:
            for _, (metric, scores) in tasks:
                if metric is not None:
                    self._set_index_metric(metric)
                # Retain plan order for duplicate IDs and global per-query selection.
                yield [DictBasedScoredIndexResult(values) for values in scores]
                del scores

    def _read_raw_batch_search(self, raw_row_ranges, pre_filter, index_type=None, snapshot=None):
        heaps = [[] for _ in self._query_vectors]
        ranges = _filtered_raw_row_ranges(raw_row_ranges, pre_filter)
        if not ranges:
            return [_scored_result(heap) for heap in heaps]
        table_read, splits = self._plan_raw_read(ranges, True, snapshot)
        context = (table_read, self._vector_column, self._query_vectors,
                   self._limit, self._search_metric(index_type))
        work = [SearchTask(_search_batch_raw_split, context, split) for split in splits]
        with closing(self._executor.map(work)) as tasks:
            for _, results in tasks:
                for heap, scores in zip(heaps, results):
                    for row_id, score in scores.items():
                        _offer_score(heap, self._limit, row_id, score)
        return [_scored_result(heap) for heap in heaps]

    def _stream_rerank_candidates(self, candidates, union_candidates, query_vectors,
                                  index_type, snapshot):
        # Candidates have already been selected globally, separately per query.
        queries_by_row = {}
        for query_index, result in enumerate(candidates):
            for row_id in result.results():
                queries_by_row.setdefault(row_id, []).append(query_index)
        table_read, splits = self._plan_raw_read(
            union_candidates.to_range_list(), include_filter=False, snapshot=snapshot)
        context = (table_read, self._vector_column, query_vectors, self._limit,
                   self._search_metric(index_type), queries_by_row)
        heaps = [[] for _ in query_vectors]
        work = [SearchTask(_search_batch_refine_split, context, split) for split in splits]
        with closing(self._executor.map(work)) as tasks:
            for _, results in tasks:
                for heap, scores in zip(heaps, results):
                    for row_id, score in scores.items():
                        _offer_score(heap, self._limit, row_id, score)
        return [_scored_result(heap) for heap in heaps]


def _search_index_split(context, split, scalar_filter=None):
    table, column, query, limit, options, filter_, partition_filter, snapshot = context
    # Explicit shard indexes are evaluated below. Candidate verification must
    # not rediscover and scan table-wide scalar indexes in every worker.
    table = table.copy({"global-index.enabled": "false"})
    reader = DataEvolutionVectorRead(
        table, limit, column, query, filter_, partition_filter, options)
    include = _worker_pre_filter(reader, split, snapshot, scalar_filter)
    result = reader._eval(
        split.row_range_start, split.row_range_end,
        split.vector_index_files, query, limit, include).result()
    # The reader closes its native resources before returning a completed result.
    # Return metric even for zero hits, so inconsistent indexes cannot be hidden
    # by a scalar filter and raw fallback uses the persisted index metric.
    return reader._index_metric, _scores(result)


def _worker_pre_filter(reader, split, snapshot, scalar_filter):
    live_rows = global_index_live_row_filter.live_rows(
        reader._table, reader._partition_filter, snapshot,
        row_ranges=[Range(split.row_range_start, split.row_range_end)])
    if scalar_filter is None:
        matched = reader._scalar_matched_rows([split], snapshot)
    else:
        bitmap, exact = scalar_filter
        result = None if bitmap is None else GlobalIndexResult.create(
            RoaringBitmap64.deserialize(bitmap), is_exact=exact)
        matched = reader._refine_scalar_result([split], result, snapshot)
    filters = reader._combine_pre_filters([split], live_rows, matched)
    return filters[0] if filters else None


def _prepare_scalar_filter(context, split):
    table, column, query, limit, options, filter_, partition_filter, snapshot = context
    reader = DataEvolutionVectorRead(table, limit, column, query, filter_, partition_filter, options)
    result = reader._scalar_index_result([split], snapshot)
    return (None, False) if result is None else (result.results().serialize(), result.is_exact())


def _index_tasks(operation, context, splits, predicate, scalar_operation=None):
    """Share identical scalar work while each shard verifies its own candidates."""
    scalar_operation = scalar_operation or _prepare_scalar_filter

    def key(split):
        return tuple(sorted(file.file_name for file in split.scalar_index_files))

    counts = Counter(key(split) for split in splits) if predicate is not None else {}
    shared = {}
    tasks = []
    for split in splits:
        files = key(split) if counts else None
        dependencies = ()
        if files and counts.get(files, 0) > 1:
            if files not in shared:
                shared[files] = SearchTask(scalar_operation, context, split)
            dependencies = (shared[files],)
        tasks.append(SearchTask(operation, context, split, dependencies))
    return tasks


def _search_raw_split(context, split):
    from pypaimon.read.read_support import _ClosableArrowBatchReader

    table_read, column, query, limit, metric = context
    scorer = DataEvolutionVectorRead(table_read.table, limit, column, query)
    reader, batches = table_read._new_arrow_batch_reader([split])
    with _ClosableArrowBatchReader(reader, batches) as batch_reader:
        return _scores(scorer._score_raw_batches(batch_reader, query, metric, reject_nan=True))


def _scores(result):
    if result is None:
        return {}
    getter = result.score_getter()
    scores = {row_id: getter(row_id) for row_id in result.results()}
    if any(math.isnan(score) for score in scores.values()):
        raise ValueError("Ray vector search cannot rank NaN scores.")
    return scores


def _search_batch_index_split(context, split, scalar_filter=None):
    table, column, queries, limit, options, filter_, partition_filter, snapshot = context
    # Explicit shard indexes are evaluated below. Candidate verification must
    # not rediscover and scan table-wide scalar indexes in every worker.
    table = table.copy({"global-index.enabled": "false"})
    scorer = BatchVectorSearchReadImpl(
        table, limit, column, queries, filter_, partition_filter, options)
    include = _worker_pre_filter(scorer, split, snapshot, scalar_filter)
    reader, offset_reader = scorer._open_offset_reader(
        split.vector_index_files, split.row_range_start, split.row_range_end)
    try:
        # Bound native query-matrix scratch to at most 64 queries / 4 MiB
        # (or one vector), keeping the shard open across all query blocks.
        block_size = max(1, min(64, (1 << 20) // max(1, len(queries[0]))))
        results = []
        for start in range(0, len(queries), block_size):
            search = BatchVectorSearch(queries[start:start + block_size], limit, column.name,
                                       include_row_ids=include, options=options)
            results.extend(_batch_scores(result) for result in offset_reader.visit_batch_vector_search(search).result())
        return scorer._index_metric, results
    finally:
        reader.close()


def _search_batch_raw_split(context, split):
    from pypaimon.read.read_support import _ClosableArrowBatchReader

    table_read, column, queries, limit, metric = context
    scorer = BatchVectorSearchReadImpl(table_read.table, limit, column, queries)
    reader, batches = table_read._new_arrow_batch_reader([split])
    with _ClosableArrowBatchReader(reader, batches) as batch_reader:
        return [_batch_scores(result) for result in scorer._score_raw_batch_queries(batch_reader, metric)]


def _search_batch_refine_split(context, split):
    table_read, column, queries, limit, metric, queries_by_row = context
    scorer = BatchVectorSearchReadImpl(table_read.table, limit, column, queries)
    # Read the candidate union once, scoring each row only for its own queries.
    return [_batch_scores(result) for result in scorer._score_refine_splits(
        table_read, [split], queries_by_row, queries, metric)]


def _batch_scores(result):
    if result is None:
        return {}
    getter = result.score_getter()
    return {row_id: getter(row_id) for row_id in result.results()}
