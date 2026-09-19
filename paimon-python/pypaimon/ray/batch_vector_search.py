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

"""Ray execution of snapshot-pinned batch vector queries."""

from contextlib import closing

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.ray.vector_search import _execution_options, _map_tasks, _require_ray
from pypaimon.table.source.vector_search_read import (
    BatchVectorSearchReadImpl, _filtered_raw_row_ranges, _offer_score, _scored_result,
)
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def _execute_batch_vector_search(builder, *, concurrency=None, ray_remote_args=None):
    concurrency, remote_args = _execution_options(concurrency, ray_remote_args)
    reader = builder.new_batch_vector_search_read()
    if (type(reader) is not BatchVectorSearchReadImpl
            or not reader._table.options.data_evolution_enabled()):
        raise ValueError("Ray vector search supports only data-evolution tables.")
    _require_ray()
    plan = builder.new_vector_search_scan().scan()
    return _RayBatchVectorSearchRead(reader, concurrency, remote_args).read_batch_plan(plan)


class _RayBatchVectorSearchRead(BatchVectorSearchReadImpl):

    def __init__(self, reader, concurrency, remote_args):
        super().__init__(reader._table, reader._limit, reader._vector_column, reader._query_vectors,
                         reader._filter, reader._partition_filter, reader._options)
        self._concurrency = concurrency
        self._remote_args = remote_args

    def _search_index_splits(self, splits, query, search_limit, pre_filters, batch=False):
        items = [(split, None if not pre_filters or pre_filters[i] is None
                  else pre_filters[i].serialize()) for i, split in enumerate(splits)]
        context = (self._table, self._vector_column, query, search_limit, self._options)
        results = [None] * len(splits)
        with closing(_map_tasks(
                _search_batch_index_split, context, items, self._concurrency, self._remote_args)) as tasks:
            for ordinal, (metric, scores) in tasks:
                if metric is not None:
                    self._set_index_metric(metric)
                # Retain plan order for duplicate IDs and global per-query selection.
                results[ordinal] = [DictBasedScoredIndexResult(values) for values in scores]
        return results

    def _read_raw_batch_search(self, raw_row_ranges, pre_filter, index_type=None, snapshot=None):
        heaps = [[] for _ in self._query_vectors]
        ranges = _filtered_raw_row_ranges(raw_row_ranges, pre_filter)
        if not ranges:
            return [_scored_result(heap) for heap in heaps]
        table_read, splits = self._plan_raw_read(ranges, True, snapshot)
        context = (table_read, self._vector_column, self._query_vectors,
                   self._limit, self._search_metric(index_type))
        with closing(_map_tasks(
                _search_batch_raw_split, context, splits, self._concurrency, self._remote_args)) as tasks:
            for _, results in tasks:
                for heap, scores in zip(heaps, results):
                    for row_id, score in scores.items():
                        _offer_score(heap, self._limit, row_id, score)
        return [_scored_result(heap) for heap in heaps]


def _search_batch_index_split(context, item):
    table, column, queries, limit, options = context
    split, include_bytes = item
    scorer = BatchVectorSearchReadImpl(table, limit, column, queries, options=options)
    include = None if include_bytes is None else RoaringBitmap64.deserialize(include_bytes)
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
            results.extend(_scores(result) for result in offset_reader.visit_batch_vector_search(search).result())
        return scorer._index_metric, results
    finally:
        reader.close()


def _search_batch_raw_split(context, split):
    from pypaimon.read.table_read import _ClosableArrowBatchReader

    table_read, column, queries, limit, metric = context
    scorer = BatchVectorSearchReadImpl(table_read.table, limit, column, queries)
    reader, batches = table_read._new_arrow_batch_reader([split])
    with _ClosableArrowBatchReader(reader, batches) as batch_reader:
        return [_scores(result) for result in scorer._score_raw_batch_queries(batch_reader, metric)]


def _scores(result):
    if result is None:
        return {}
    getter = result.score_getter()
    return {row_id: getter(row_id) for row_id in result.results()}
