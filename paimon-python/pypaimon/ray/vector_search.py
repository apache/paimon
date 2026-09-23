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

"""Ray execution of snapshot-pinned, single-vector queries."""

from contextlib import closing
import math

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.vector_search_read import (
    DataEvolutionVectorRead,
    _filtered_raw_row_ranges,
    _offer_score,
    _scored_result,
)
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def _execute_vector_search(builder, *, concurrency=None, ray_remote_args=None):
    """Execute the builder of an already snapshot-pinned ``VectorQuery``."""
    concurrency, remote_args = _execution_options(concurrency, ray_remote_args)
    reader = builder.new_vector_search_read()
    if (type(reader) is not DataEvolutionVectorRead
            or not reader._table.options.data_evolution_enabled()):
        raise ValueError("Ray vector search supports only data-evolution tables.")
    if not all(math.isfinite(float(value)) for value in reader._query_vector):
        raise ValueError("Ray vector search requires a finite query vector.")

    _require_ray()
    plan = builder.new_vector_search_scan().scan()
    return _RayVectorSearchRead(reader, concurrency, remote_args).read_plan(plan)


def _execution_options(concurrency, ray_remote_args):
    concurrency = 4 if concurrency is None else concurrency
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError("concurrency must be a positive integer.")
    remote_args = dict(ray_remote_args or {})
    if "num_returns" in remote_args:
        raise ValueError("Vector search manages num_returns; omit it from ray_remote_args.")
    return concurrency, remote_args


def _require_ray():
    try:
        import ray  # noqa: F401
    except ModuleNotFoundError as error:
        if error.name != "ray":
            raise
        raise ImportError("Ray vector search requires pypaimon[ray].") from error


class _RayVectorSearchRead(DataEvolutionVectorRead):
    """Replace split execution while retaining local merge/refinement semantics."""

    def __init__(self, reader, concurrency, remote_args):
        super().__init__(
            reader._table, reader._limit, reader._vector_column, reader._query_vector,
            reader._filter, reader._partition_filter, reader._options)
        self._concurrency = concurrency
        self._remote_args = remote_args

    def _search_index_splits(self, splits, query, search_limit, pre_filters, batch=False):
        # Filters are planned once on the driver, at the query snapshot. Each
        # worker receives only its own include-row bitmap, not a table-wide set.
        items = [(split, None if not pre_filters or pre_filters[i] is None
                  else pre_filters[i].serialize()) for i, split in enumerate(splits)]
        context = (self._table, self._vector_column, query, search_limit, self._options)
        with closing(_map_tasks(
                _search_index_split, context, items, self._concurrency, self._remote_args, True)) as tasks:
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
        with closing(_map_tasks(
                _search_raw_split, context, splits, self._concurrency, self._remote_args)) as tasks:
            for _, scores in tasks:
                for row_id, score in scores.items():
                    _offer_score(heap, self._limit, row_id, score)
        return _scored_result(heap)


def _search_index_split(context, item):
    table, column, query, limit, options = context
    split, include_bytes = item
    reader = DataEvolutionVectorRead(table, limit, column, query, options=options)
    include = None if include_bytes is None else RoaringBitmap64.deserialize(include_bytes)
    result = reader._eval(
        split.row_range_start, split.row_range_end,
        split.vector_index_files, query, limit, include).result()
    # The reader closes its native resources before returning a completed result.
    # Return metric even for zero hits, so inconsistent indexes cannot be hidden
    # by a scalar filter and raw fallback uses the persisted index metric.
    return reader._index_metric, _scores(result)


def _search_raw_split(context, split):
    from pypaimon.read.table_read import _ClosableArrowBatchReader

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


def _map_tasks(worker, context, items, concurrency, remote_args, ordered=False):
    """Bound in-flight work and, optionally, completed results awaiting plan order."""
    import ray

    if not items:
        return
    # Share only serializable planning state, never open readers or closures.
    context_ref = ray.put(context)
    remote = ray.remote(worker).options(**remote_args)
    remaining = iter(enumerate(items))
    pending = {}
    buffered = {}
    next_ordinal = 0
    try:
        while True:
            while len(pending) + len(buffered) < concurrency:
                item = next(remaining, None)
                if item is None:
                    break
                ordinal, split = item
                pending[remote.remote(context_ref, split)] = ordinal
            if not pending:
                break
            ready, _ = ray.wait(list(pending), num_returns=1)
            ref = ready[0]
            result = ray.get(ref)
            ordinal = pending.pop(ref)
            if ordered:
                buffered[ordinal] = result
                del result
                while next_ordinal in buffered:
                    yield next_ordinal, buffered.pop(next_ordinal)
                    next_ordinal += 1
            else:
                yield ordinal, result
                del result
    finally:
        for ref in pending:
            try:
                ray.cancel(ref)
            except Exception:
                # Preserve the search failure if the cluster is already gone.
                pass
