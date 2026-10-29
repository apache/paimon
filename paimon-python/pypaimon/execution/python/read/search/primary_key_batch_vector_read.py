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

"""Batch PK searches sharing index readers and physical vector reads."""

import math
from itertools import chain

import pyarrow as pa

from pypaimon.common.options.core_options import GlobalIndexSearchMode
from pypaimon.execution.python.read.search.primary_key_vector_read import (
    PrimaryKeyVectorRead, _deleted_positions, _include_row_ids, _localized_candidates, _top_k,
)
from pypaimon.execution.python.read.search.vector_search_read import (
    _iter_arrow_batch_scores, _iter_arrow_scores,
)
from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition, _partition_bytes, _position_key,
)
from pypaimon.table.source.search_types import (
    BatchVectorSearchRead, PrimaryKeyVectorScanPlan, PrimaryKeyVectorSearchSplit,
)


class BatchPrimaryKeyVectorRead(PrimaryKeyVectorRead, BatchVectorSearchRead):

    def __init__(self, table, limit, vector_column, query_vectors, filter_=None,
                 partition_filter=None, options=None):
        queries = [list(query) for query in query_vectors]
        if not queries or not queries[0] or any(len(query) != len(queries[0]) for query in queries):
            raise ValueError("PK batch search requires nonempty query vectors of the same dimension.")
        if not all(math.isfinite(float(value)) for query in queries for value in query):
            raise ValueError("PK batch search requires finite query vectors.")
        super().__init__(table, limit, vector_column, queries[0], filter_, partition_filter, options)
        self._query_vectors = queries

    def _new_distributed_reader(self, executor):
        if type(self) is not BatchPrimaryKeyVectorRead:
            raise ValueError("Custom vector readers must define their own distributed execution.")
        from pypaimon.execution.python.read.search.distributed_search import validate_query_vectors
        from pypaimon.execution.python.read.search.distributed_primary_key_vector_search import (
            DistributedBatchPrimaryKeyVectorRead,
        )
        validate_query_vectors(self._query_vectors)
        return DistributedBatchPrimaryKeyVectorRead(self, executor)

    def read_batch_plan(self, plan, executor=None):
        if executor is not None:
            return self._with_executor(executor).read_batch_plan(plan)
        if not isinstance(plan, PrimaryKeyVectorScanPlan):
            raise ValueError("Primary-key vector read requires a PrimaryKeyVectorScanPlan.")
        self._index_metric = None
        index_type = self._table.options.primary_key_vector_index_type(self._vector_column.name)
        indexed_limit = self._indexed_search_limit(index_type)
        if indexed_limit > 2147483647:
            raise ValueError("Vector search limit overflow: limit=%d" % self._limit)
        indexed = self._search_indexed_candidates(plan, indexed_limit)
        if self._configured_refine_factor(index_type) > 0:
            indexed = self._rerank_indexed_batch(plan, indexed, index_type)
        exact = ([[] for _ in self._query_vectors]
                 if self._table.options.vector_index_search_mode() == GlobalIndexSearchMode.FAST
                 else self._search_exact_candidates(plan))
        sources = [split.data_split for split in plan.splits()]
        return [PrimaryKeyScoredResult(plan.snapshot_id, sources, _top_k(chain(ann, raw), self._limit))
                for ann, raw in zip(indexed, exact)]

    def _read_batch(self, splits, snapshot):
        if splits and snapshot is None:
            raise ValueError("PK batch search requires a captured plan; use read_batch_plan().")
        return self.read_batch_plan(PrimaryKeyVectorScanPlan(
            0 if snapshot is None else snapshot.id, splits, snapshot))

    def _search_indexed_candidates(self, plan, indexed_limit):
        candidates = [[] for _ in self._query_vectors]
        deleted = _deleted_positions(self._table, plan)
        # Keep each payload open across bounded query blocks.
        block_size = max(1, min(64, (1 << 20) // len(self._query_vectors[0])))
        for split in plan.splits():
            for payload in split.payloads:
                source = PrimaryKeyIndexSourceMeta.from_index_file(payload)
                include = _include_row_ids(split, source.source_files, deleted)
                count = sum(file.row_count for file in source.source_files)
                reader, offset = self._open_offset_reader([payload], 0, count - 1)
                try:
                    for start in range(0, len(self._query_vectors), block_size):
                        queries = self._query_vectors[start:start + block_size]
                        search = BatchVectorSearch(queries, indexed_limit, self._vector_column.name,
                                                   include_row_ids=include, options=self._options)
                        results = offset.visit_batch_vector_search(search).result()
                        if len(results) != len(queries):
                            raise ValueError("PK index task returned the wrong query count.")
                        for i, result in enumerate(results, start):
                            candidates[i] = _top_k(chain(candidates[i], _localized_candidates(
                                result, split, source, deleted)), indexed_limit)
                finally:
                    reader.close()
        return candidates

    def _search_exact_candidates(self, plan):
        candidates = [[] for _ in self._query_vectors]
        metric = self._search_metric(self._table.options.primary_key_vector_index_type(self._vector_column.name))
        for split, data_file, positions, vectors in self._raw_batches(plan):
            partition = _partition_bytes(split.data_split.partition)
            for start, query_index, scores in _iter_arrow_batch_scores(vectors, self._query_vectors, metric):
                rows = (PrimaryKeySearchPosition(
                    partition, split.data_split.bucket, data_file.file_name, position, score)
                    for position, score in zip(positions[start:start + len(scores)], scores) if score is not None)
                candidates[query_index] = _top_k(chain(candidates[query_index], rows), self._limit)
        return candidates

    def _rerank_indexed_batch(self, plan, candidates, index_type):
        selected = [{_position_key(position): position for position in group} for group in candidates]
        if any(len(group) != len(positions) for group, positions in zip(selected, candidates)):
            raise ValueError("Duplicate primary-key vector candidate.")
        union = {key: position for group in selected for key, position in group.items()}
        sources = [split.data_split for split in plan.splits()]
        result = PrimaryKeyScoredResult(plan.snapshot_id, sources, list(union.values()))
        # Reuse the physical reader on the candidate union; each query scores only
        # its own globally selected ANN candidates, never another query's rows.
        splits = [PrimaryKeyVectorSearchSplit(
            split.data_split(), (), (split.files[0].file_name,),
            {split.files[0].file_name: tuple(split.row_ranges())}) for split in result.splits]
        candidate_plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, splits, plan.snapshot())
        metric = self._search_metric(index_type)
        reranked = [[] for _ in self._query_vectors]
        for split, data_file, positions, vectors in self._raw_batches(candidate_plan):
            keys = [(_partition_bytes(split.data_split.partition), split.data_split.bucket,
                     data_file.file_name, position) for position in positions]
            for i, query in enumerate(self._query_vectors):
                offsets = [offset for offset, key in enumerate(keys) if key in selected[i]]
                if not offsets:
                    continue
                rows = vectors.take(pa.array(offsets, type=pa.int64()))
                for offset, score in zip(offsets, _iter_arrow_scores(rows, query, metric)):
                    if score is None:
                        raise ValueError("Primary-key vector candidate contains a null vector.")
                    reranked[i].append(selected[i].pop(keys[offset]).with_score(score))
        if any(selected):
            raise ValueError("Failed to read all primary-key vector candidates for reranking.")
        return [_top_k(group, self._limit) for group in reranked]
