# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Bucket task execution with the local PK reader's global ranking semantics."""

from pypaimon.execution.python.read.search.distributed_search import closing_task_results
from pypaimon.execution.python.read.search.global_index_live_row_filter import table_at_snapshot
from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead, _top_k
from pypaimon.execution.python.read.search.primary_key_batch_vector_read import BatchPrimaryKeyVectorRead
from pypaimon.table.source.search_execution import SearchTask
from pypaimon.table.source.search_types import PrimaryKeyVectorScanPlan


class DistributedPrimaryKeyVectorRead(PrimaryKeyVectorRead):

    def __init__(self, reader, executor):
        super().__init__(reader._table, reader._limit, reader._vector_column, reader._query_vector,
                         reader._filter, reader._partition_filter, reader._options)
        self._executor = executor

    def _context(self, plan):
        # Workers consume only the supplied bucket metadata, never a fresh scan.
        return (table_at_snapshot(self._table, plan.snapshot()), self._limit,
                self._vector_column, self._query_vector, self._options)

    def _search_indexed_candidates(self, plan, indexed_limit):
        context = self._context(plan)
        work = [SearchTask(_search_index_bucket, context,
                           (plan.snapshot_id, split, indexed_limit))
                for split in plan.splits() if split.payloads]
        if not work:
            return []

        def candidates():
            with closing_task_results(self._executor.map(work)) as tasks:
                for _, (metric, positions) in tasks:
                    # Even an empty shard must participate in metric validation.
                    if metric is not None:
                        self._set_index_metric(metric)
                    yield from positions

        # Retain the ANN budget until global refinement, not just each bucket's K.
        return _top_k(candidates(), indexed_limit)

    def _search_exact_candidates(self, plan):
        context = self._context(plan)
        work = [SearchTask(_search_exact_bucket, context,
                           (plan.snapshot_id, split, self._index_metric))
                for split in plan.splits() if split.uncovered_data_files]
        if not work:
            return []

        def candidates():
            with closing_task_results(self._executor.map(work)) as tasks:
                for _, positions in tasks:
                    yield from positions

        return _top_k(candidates(), self._limit)


def _search_index_bucket(context, item):
    reader = _new_reader(context)
    snapshot_id, split, indexed_limit = item
    plan = PrimaryKeyVectorScanPlan(snapshot_id, [split])
    positions = reader._search_indexed_candidates(plan, indexed_limit)
    return reader._index_metric, positions


def _search_exact_bucket(context, item):
    reader = _new_reader(context)
    snapshot_id, split, metric = item
    if metric is not None:
        reader._set_index_metric(metric)
    return reader._search_exact_candidates(PrimaryKeyVectorScanPlan(snapshot_id, [split]))


def _new_reader(context):
    table, limit, column, query, options = context
    return PrimaryKeyVectorRead(table, limit, column, query, options=options)


class DistributedBatchPrimaryKeyVectorRead(BatchPrimaryKeyVectorRead):

    def __init__(self, reader, executor):
        super().__init__(reader._table, reader._limit, reader._vector_column, reader._query_vectors,
                         reader._filter, reader._partition_filter, reader._options)
        self._executor = executor

    def _search_indexed_candidates(self, plan, indexed_limit):
        return self._search_buckets(plan, _search_batch_index_bucket, indexed_limit, indexed=True)

    def _search_exact_candidates(self, plan):
        return self._search_buckets(plan, _search_batch_exact_bucket, self._limit, indexed=False)

    def _search_buckets(self, plan, operation, limit, indexed):
        from itertools import chain

        context = (table_at_snapshot(self._table, plan.snapshot()), self._limit,
                   self._vector_column, self._query_vectors, self._options)
        work = [SearchTask(operation, context, (
            plan.snapshot_id, split, limit if indexed else self._index_metric))
            for split in plan.splits() if (split.payloads if indexed else split.uncovered_data_files)]
        candidates = [[] for _ in self._query_vectors]
        if not work:
            return candidates
        with closing_task_results(self._executor.map(work)) as tasks:
            for _, (metric, results) in tasks:
                if metric is not None:
                    self._set_index_metric(metric)
                if len(results) != len(candidates):
                    raise ValueError("PK bucket task returned the wrong query count.")
                for i, positions in enumerate(results):
                    candidates[i] = _top_k(chain(candidates[i], positions), limit)
        return candidates


def _search_batch_index_bucket(context, item):
    reader = _new_batch_reader(context)
    snapshot_id, split, indexed_limit = item
    positions = reader._search_indexed_candidates(PrimaryKeyVectorScanPlan(snapshot_id, [split]), indexed_limit)
    return reader._index_metric, positions


def _search_batch_exact_bucket(context, item):
    reader = _new_batch_reader(context)
    snapshot_id, split, metric = item
    if metric is not None:
        reader._set_index_metric(metric)
    positions = reader._search_exact_candidates(PrimaryKeyVectorScanPlan(snapshot_id, [split]))
    return reader._index_metric, positions


def _new_batch_reader(context):
    table, limit, column, queries, options = context
    return BatchPrimaryKeyVectorRead(table, limit, column, queries, options=options)
