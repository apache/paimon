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

"""Vector search read to read index files."""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, wait

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.special_fields import SpecialFields
from pypaimon.table.source.vector_search_split import (
    IndexVectorSearchSplit,
    RawVectorSearchSplit,
)
from pypaimon.table.source import global_index_live_row_filter
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class VectorSearchRead(ABC):
    """Vector search read to read index files."""

    def read_plan(self, plan):
        # type: (VectorSearchScanPlan) -> GlobalIndexResult
        return self._read(plan.splits(), plan.snapshot())

    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        return self._read(splits, None)

    @abstractmethod
    def _read(self, splits, snapshot):
        pass


class BatchVectorSearchRead(ABC):
    """Batch vector search read to read index files."""

    def read_batch_plan(self, plan):
        # type: (VectorSearchScanPlan) -> List[GlobalIndexResult]
        return self._read_batch(plan.splits(), plan.snapshot())

    def read_batch(self, splits):
        # type: (List[VectorSearchSplit]) -> List[GlobalIndexResult]
        return self._read_batch(splits, None)

    @abstractmethod
    def _read_batch(self, splits, snapshot):
        pass


class AbstractVectorSearchReadImpl:
    """Base implementation for vector search reads."""

    def __init__(
        self,
        table,
        limit,
        vector_column,
        filter_=None,
        partition_filter=None,
        options=None,
    ):
        self._table = table
        self._limit = limit
        self._vector_column = vector_column
        self._filter = filter_
        self._partition_filter = partition_filter
        self._options = dict(options or {})
        self._index_metric = None

    def _search_metric(self, index_type=None):
        if self._index_metric is not None:
            return self._index_metric
        return _raw_search_metric(
            self._table, self._vector_column, self._options, index_type)

    def _record_index_metric(self, reader, index_type):
        """Keep one persisted metric for indexed scores, raw search and refinement."""
        metric_getter = getattr(reader, "vector_metric", None)
        if metric_getter is None:
            return
        metric = _normalize_metric(metric_getter())
        requested = _configured_vector_metric(
            self._options, self._vector_column, index_type)
        if requested is not None and requested != metric:
            raise ValueError(
                "Query vector metric '%s' does not match index metric '%s' for column '%s'."
                % (requested, metric, self._vector_column.name))
        if self._index_metric is not None and self._index_metric != metric:
            raise ValueError(
                "Cannot merge vector indexes with different metrics '%s' and '%s' for column '%s'."
                % (self._index_metric, metric, self._vector_column.name))
        self._index_metric = metric

    def _pre_filters(self, splits, snapshot=None):
        # type: (list) -> List[RoaringBitmap64]
        """Evaluate live-row/scalar filters and return one bitmap per index split."""
        if not splits:
            return []

        live_rows = global_index_live_row_filter.live_rows(
            self._table, self._partition_filter, snapshot)
        matched_rows = self._scalar_matched_rows(splits, snapshot)
        if live_rows is None and matched_rows is None:
            return []

        include_row_ids = []
        has_filter = False
        for split in splits:
            split_range = Range(split.row_range_start, split.row_range_end)
            include = _bitmap_of_range(split_range)
            if live_rows is not None:
                include = RoaringBitmap64.and_(include, live_rows)
            if matched_rows is not None:
                include = RoaringBitmap64.and_(include, matched_rows)

            if include.cardinality() == split_range.count():
                include_row_ids.append(None)
            else:
                include_row_ids.append(include)
                has_filter = True
        return include_row_ids if has_filter else []

    def _scalar_matched_rows(self, splits, snapshot=None):
        """Evaluate scalar indexes and return matching global row ids."""
        if self._filter is None:
            return None

        # Collect scalar index files across splits, deduplicated by file name.
        seen = set()
        scalar_files = []
        for split in splits:
            for index_file in split.scalar_index_files:
                if index_file.file_name in seen:
                    continue
                seen.add(index_file.file_name)
                scalar_files.append(index_file)

        if not scalar_files:
            return RoaringBitmap64()

        from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
        scanner = DataEvolutionGlobalIndexScanner.create(
            self._table,
            index_files=scalar_files,
            partition_filter=self._partition_filter,
            snapshot=snapshot,
        )
        if scanner is None:
            return RoaringBitmap64()
        try:
            result = scanner.scan(self._filter)
            if result is None:
                return RoaringBitmap64()
            return result.results()
        finally:
            scanner.close()

    def _pre_filter(self, splits, snapshot=None):
        # Backwards-compatible helper used by older tests/callers.
        pre_filters = self._pre_filters(splits, snapshot)
        if not pre_filters:
            return None
        merged = RoaringBitmap64()
        for split, bitmap in zip(splits, pre_filters):
            if bitmap is None:
                merged.add_range(split.row_range_start, split.row_range_end)
            else:
                merged = RoaringBitmap64.or_(merged, bitmap)
        return merged

    def _raw_pre_filter(self, splits, snapshot=None):
        if self._filter is None:
            return None
        raw_row_ranges = _raw_row_ranges(splits)
        if not raw_row_ranges:
            return None

        seen = set()
        scalar_files = []
        for split in splits:
            for index_file in split.scalar_index_files:
                if index_file.file_name in seen:
                    continue
                seen.add(index_file.file_name)
                scalar_files.append(index_file)
        if not scalar_files:
            return None

        from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
        scanner = DataEvolutionGlobalIndexScanner.create(
            self._table,
            index_files=scalar_files,
            partition_filter=self._partition_filter,
            snapshot=snapshot,
        )
        if scanner is None:
            return None
        try:
            evaluation = scanner.scan_with_coverage(self._filter)
            if evaluation is None:
                return None
            include_ranges = evaluation.result.results().to_range_list()
            include_ranges.extend(
                scanner.unindexed_ranges(
                    self._filter,
                    search_mode=self._table.options.scalar_index_search_mode(),
                    contributing_field_ids=(
                        evaluation.contributing_field_ids),
                ))
            return Range.and_(
                raw_row_ranges,
                Range.sort_and_merge_overlap(include_ranges, True),
            )
        finally:
            scanner.close()

    def _open_offset_reader(self, vector_index_files, row_range_start, row_range_end):
        """Open a vector index reader for the split, wrapped with the row-id offset.

        The caller must close the returned reader once its future completes.
        """
        index_io_meta_list = []
        for index_file in vector_index_files:
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

        reader = _create_vector_reader(
            vector_index_files[0].index_type,
            self._table.file_io,
            self._table.path_factory().global_index_path_factory().index_path(),
            index_io_meta_list,
            self._table.table_schema.options,
        )
        try:
            self._record_index_metric(reader, vector_index_files[0].index_type)
            return reader, OffsetGlobalIndexReader(reader, row_range_start, row_range_end)
        except Exception:
            reader.close()
            raise

    def _eval(self, row_range_start, row_range_end, vector_index_files,
              query_vector, search_limit, include_row_ids):
        from pypaimon.globalindex.global_index_reader import _completed_future

        if not vector_index_files:
            return _completed_future(None)

        vector_search = VectorSearch(
            vector=query_vector,
            limit=search_limit,
            field_name=self._vector_column.name,
            options=self._options,
        )
        if include_row_ids is not None:
            vector_search = vector_search.with_include_row_ids(include_row_ids)

        reader, offset_reader = self._open_offset_reader(
            vector_index_files, row_range_start, row_range_end)
        try:
            future = offset_reader.visit_vector_search(vector_search)
        except BaseException:
            reader.close()
            raise
        future.add_done_callback(lambda _: reader.close())
        return future

    def _read_raw_search(self, raw_row_ranges, pre_filter, query_vector,
                         index_type=None, include_filter=True,
                         score_candidates=None, snapshot=None):
        raw_row_ranges = _filtered_raw_row_ranges(raw_row_ranges, pre_filter)
        if not raw_row_ranges:
            return DictBasedScoredIndexResult({})

        table = self._read_raw_arrow(raw_row_ranges, include_filter, snapshot)
        if table is None or table.num_rows == 0:
            return DictBasedScoredIndexResult({})

        top_k_heap = []
        metric = self._search_metric(index_type)
        block_size = _score_block_size(query_vector)
        for start in range(0, table.num_rows, block_size):
            block = table.slice(start, block_size)
            row_ids = block.column(SpecialFields.ROW_ID.name).to_pylist()
            vectors = block.column(self._vector_column.name)
            if score_candidates is not None:
                positions = [i for i, row_id in enumerate(row_ids)
                             if row_id in score_candidates]
                if not positions:
                    continue
                vectors = vectors.take(positions)
                row_ids = [row_ids[i] for i in positions]
            for row_id, score in zip(
                row_ids, _iter_arrow_scores(vectors, query_vector, metric)
            ):
                if score is not None:
                    _offer_score(top_k_heap, self._limit, row_id, score)
        return _scored_result(top_k_heap)

    def _read_raw_vectors(self, candidates, include_filter=True, snapshot=None):
        return self._read_raw_candidate_vectors(
            candidates.to_range_list(), candidates, include_filter, snapshot)

    def _read_raw_candidate_vectors(self, raw_row_ranges, candidates,
                                    include_filter=True, snapshot=None):
        raw_row_ranges = _filtered_raw_row_ranges(raw_row_ranges, None)
        if not raw_row_ranges:
            return {}

        table = self._read_raw_arrow(raw_row_ranges, include_filter, snapshot)
        if table is None or table.num_rows == 0:
            return {}

        row_ids = table.column(SpecialFields.ROW_ID.name).to_pylist()
        vectors = table.column(self._vector_column.name).to_pylist()
        raw_vectors = {}
        for row_id, stored in zip(row_ids, vectors):
            if candidates is not None and row_id not in candidates:
                continue
            if stored is None:
                continue
            raw_vectors[row_id] = _to_vector_list(stored)
        return raw_vectors

    def _read_raw_arrow(self, raw_row_ranges, include_filter, snapshot=None):
        reader, splits = self._plan_raw_read(raw_row_ranges, include_filter, snapshot)
        return reader.to_arrow(splits)

    def _plan_raw_read(self, raw_row_ranges, include_filter, snapshot=None):
        read_table = global_index_live_row_filter.table_at_snapshot(
            self._table, snapshot)
        read_builder = read_table.new_read_builder()
        if self._partition_filter is not None:
            read_builder = read_builder.with_partition_filter(
                self._partition_filter)
        if include_filter and self._filter is not None:
            read_builder = read_builder.with_filter(self._filter)
        read_builder = read_builder.with_projection(
            self._raw_search_projection(include_filter))
        plan = read_builder.new_scan().with_row_ranges(raw_row_ranges).plan()
        return read_builder.new_read(), plan.splits()

    def _score_raw_vectors(self, candidates, raw_vectors, query_vector, metric, top_k):
        top_k_heap = []
        row_ids, vectors = [], []

        def offer_block():
            for row_id, score in zip(row_ids, _score_rows(vectors, query_vector, metric)):
                _offer_score(top_k_heap, top_k, row_id, score)

        block_size = _score_block_size(query_vector)
        for row_id in candidates:
            stored_vector = raw_vectors.get(row_id)
            if stored_vector is None:
                continue
            row_ids.append(row_id)
            vectors.append(stored_vector)
            if len(vectors) == block_size:
                offer_block()
                row_ids, vectors = [], []
        if vectors:
            offer_block()
        return _scored_result(top_k_heap)

    def _read_raw_refine_search(self, candidates, query_vector, index_type=None,
                                snapshot=None):
        return self._read_raw_candidate_search(
            candidates.to_range_list(),
            candidates,
            query_vector,
            index_type,
            include_filter=False,
            snapshot=snapshot,
        )

    def _read_raw_candidate_search(self, raw_row_ranges, candidates, query_vector,
                                   index_type=None, include_filter=False,
                                   snapshot=None):
        return self._read_raw_search(
            raw_row_ranges,
            None,
            query_vector,
            index_type,
            include_filter=include_filter,
            score_candidates=candidates,
            snapshot=snapshot,
        )

    def _raw_search_projection(self, include_filter):
        projection = [self._vector_column.name]
        if include_filter and self._filter is not None:
            filter_fields = _predicate_field_names(self._filter)
            for field in self._table.fields:
                if field.name in filter_fields and field.name not in projection:
                    projection.append(field.name)
        if SpecialFields.ROW_ID.name not in projection:
            projection.append(SpecialFields.ROW_ID.name)
        return projection

    def _eval_batch(self, row_range_start, row_range_end, vector_index_files,
                    query_vectors, search_limit, include_row_ids):
        from pypaimon.globalindex.global_index_reader import _completed_future

        if not vector_index_files:
            return _completed_future([None] * len(query_vectors))

        batch_vector_search = BatchVectorSearch(
            vectors=query_vectors,
            limit=search_limit,
            field_name=self._vector_column.name,
            options=self._options,
        )
        if include_row_ids is not None:
            batch_vector_search = batch_vector_search.with_include_row_ids(include_row_ids)

        reader, offset_reader = self._open_offset_reader(
            vector_index_files, row_range_start, row_range_end)
        try:
            future = offset_reader.visit_batch_vector_search(batch_vector_search)
        except BaseException:
            reader.close()
            raise
        future.add_done_callback(lambda _: reader.close())
        return future

    def _indexed_search_limit(self, index_type):
        refine_factor = self._configured_refine_factor(index_type)
        if refine_factor == 0:
            return self._limit
        return self._limit * refine_factor

    def _maybe_rerank_indexed_result(self, result, index_type, query_vector,
                                     snapshot=None):
        if (self._configured_refine_factor(index_type) == 0 or
                result.results().is_empty()):
            return result
        candidates = result.top_k(self._indexed_search_limit(index_type))
        return self._read_raw_refine_search(
            candidates.results(),
            query_vector,
            index_type,
            snapshot,
        )

    def _maybe_rerank_indexed_results(self, results, index_type, query_vectors,
                                      snapshot=None):
        if self._configured_refine_factor(index_type) == 0:
            return results

        search_limit = self._indexed_search_limit(index_type)
        candidates = [result.top_k(search_limit) for result in results]
        union_candidates = RoaringBitmap64()
        for result in candidates:
            union_candidates = RoaringBitmap64.or_(
                union_candidates, result.results())
        if union_candidates.is_empty():
            return candidates

        return self._stream_rerank_candidates(
            candidates, union_candidates, query_vectors, index_type, snapshot)

    def _stream_rerank_candidates(self, candidates, union_candidates, query_vectors,
                                  index_type, snapshot):
        # Retain only candidate membership, not all candidate vectors as Python lists.
        queries_by_row = {}
        for query_index, result in enumerate(candidates):
            for row_id in result.results():
                queries_by_row.setdefault(row_id, []).append(query_index)
        table_read, splits = self._plan_raw_read(
            union_candidates.to_range_list(), include_filter=False, snapshot=snapshot)
        metric = self._search_metric(index_type)
        workers = min(len(splits), table_read._resolve_parallelism(None, len(splits)))
        if workers <= 1:
            return self._score_refine_splits(
                table_read, splits, queries_by_row, query_vectors, metric)

        # Preserve split read parallelism with one streaming reader per worker.
        # Candidate membership is shared; vectors are released after each batch.
        heaps = [[] for _ in query_vectors]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(
                self._score_refine_splits, table_read, splits[i::workers],
                queries_by_row, query_vectors, metric) for i in range(workers)]
            for future in futures:
                for heap, result in zip(heaps, future.result()):
                    score_getter = result.score_getter()
                    for row_id in result.results():
                        _offer_score(heap, self._limit, row_id, score_getter(row_id))
        return [_scored_result(heap) for heap in heaps]

    def _score_refine_splits(self, table_read, splits, queries_by_row, query_vectors, metric):
        from pypaimon.read.table_read import _ClosableArrowBatchReader

        heaps = [[] for _ in query_vectors]
        reader, batches = table_read._new_arrow_batch_reader(splits)
        with _ClosableArrowBatchReader(reader, batches) as batch_reader:
            for batch in batch_reader:
                row_ids = batch.column(SpecialFields.ROW_ID.name).to_pylist()
                vectors = batch.column(self._vector_column.name)
                positions = {}
                for position, row_id in enumerate(row_ids):
                    for query_index in queries_by_row.get(row_id, ()):
                        positions.setdefault(query_index, []).append(position)
                for query_index, selected in positions.items():
                    query = query_vectors[query_index]
                    # Bound Arrow take allocations as well as the scoring scratch matrix.
                    block_size = _score_block_size(query)
                    for start in range(0, len(selected), block_size):
                        block = selected[start:start + block_size]
                        scores = _iter_arrow_scores(vectors.take(block), query, metric)
                        for position, score in zip(block, scores):
                            if score is not None:
                                _offer_score(heaps[query_index], self._limit,
                                             row_ids[position], score)
                del batch, row_ids, vectors
        return [_scored_result(heap) for heap in heaps]

    def _configured_refine_factor(self, index_type):
        value = _configured_refine_factor(
            self._options, self._vector_column.name, index_type)
        if value is None:
            value = _configured_refine_factor(
                _table_options_map(self._table), self._vector_column.name, index_type)
        if value is None:
            return 0
        try:
            factor = int(value)
        except ValueError as e:
            raise ValueError(
                "Invalid vector refine factor: %s. Must be an integer." % value
            ) from e
        if factor <= 0:
            raise ValueError("Vector refine factor must be positive, got: %s" % value)
        return factor


class DataEvolutionVectorRead(AbstractVectorSearchReadImpl, VectorSearchRead):
    """Implementation for VectorSearchRead."""

    def __init__(self, table, limit, vector_column, query_vector, filter_=None,
                 partition_filter=None, options=None):
        super().__init__(table, limit, vector_column,
                         filter_=filter_,
                         partition_filter=partition_filter,
                         options=options)
        self._query_vector = query_vector

    def _read(self, splits, snapshot):
        self._index_metric = None
        index_splits, raw_splits = _split_search_splits(splits)
        if not index_splits and not raw_splits:
            return GlobalIndexResult.create_empty()

        indexed = (
            DictBasedScoredIndexResult({})
            if not index_splits
            else self._read_indexed(index_splits, self._query_vector, snapshot)
        )
        raw_result = self._read_raw_search(
            _raw_row_ranges(raw_splits),
            self._raw_pre_filter(raw_splits, snapshot),
            self._query_vector,
            _raw_search_index_type(raw_splits),
            snapshot=snapshot,
        )
        return indexed.or_(raw_result).top_k(self._limit)

    def _read_indexed(self, splits, query_vector, snapshot):
        index_type = _vector_index_type(splits)
        search_limit = self._indexed_search_limit(index_type)
        pre_filters = self._pre_filters(splits, snapshot)
        futures = [
            self._eval(
                split.row_range_start, split.row_range_end,
                split.vector_index_files,
                query_vector,
                search_limit,
                None if not pre_filters else pre_filters[i]
            )
            for i, split in enumerate(splits)
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

        indexed = DictBasedScoredIndexResult(merged_scores).top_k(search_limit)
        return self._maybe_rerank_indexed_result(
            indexed, index_type, query_vector, snapshot)


class BatchVectorSearchReadImpl(AbstractVectorSearchReadImpl,
                                BatchVectorSearchRead):
    """Batch vector search read; result ``i`` corresponds to query vector ``i``."""

    def __init__(self, table, limit, vector_column, query_vectors,
                 filter_=None, partition_filter=None, options=None):
        super().__init__(table, limit, vector_column,
                         filter_=filter_,
                         partition_filter=partition_filter,
                         options=options)
        self._query_vectors = list(query_vectors)

    def _read_batch(self, splits, snapshot):
        self._index_metric = None
        n = len(self._query_vectors)
        index_splits, raw_splits = _split_search_splits(splits)
        if not index_splits and not raw_splits:
            return [GlobalIndexResult.create_empty() for _ in range(n)]

        # One native batch call per INDEX split (all query vectors at once),
        # passing that split's pre-filter. Each future returns n per-query results.
        index_type = _vector_index_type(index_splits)
        search_limit = self._indexed_search_limit(index_type)
        pre_filters = self._pre_filters(index_splits, snapshot)
        futures = [
            self._eval_batch(
                split.row_range_start, split.row_range_end,
                split.vector_index_files, self._query_vectors,
                search_limit,
                None if not pre_filters else pre_filters[i],
            )
            for i, split in enumerate(index_splits)
        ]

        wait(futures)

        # Merge each query vector's indexed results across index splits.
        merged_scores = [{} for _ in range(n)]
        for future in futures:
            split_results = future.result()
            for i in range(n):
                split_result = split_results[i]
                if split_result is None:
                    continue
                score_getter = split_result.score_getter()
                for row_id in split_result.results():
                    if row_id not in merged_scores[i]:
                        merged_scores[i][row_id] = score_getter(row_id)

        indexed_results = [
            DictBasedScoredIndexResult(merged_scores[i]).top_k(search_limit)
            for i in range(n)
        ]
        indexed_results = self._maybe_rerank_indexed_results(
            indexed_results, index_type, self._query_vectors, snapshot)

        # Each query: merge indexed results with the raw (brute-force) fallback.
        raw_pre_filter = self._raw_pre_filter(raw_splits, snapshot)
        raw_ranges = _raw_row_ranges(raw_splits)
        raw_index_type = _raw_search_index_type(raw_splits)
        raw_results = self._read_raw_batch_search(
            raw_ranges, raw_pre_filter, raw_index_type, snapshot)
        return [
            indexed.or_(raw).top_k(self._limit)
            for indexed, raw in zip(indexed_results, raw_results)
        ]

    def _read_raw_batch_search(self, raw_row_ranges, pre_filter,
                               index_type=None, snapshot=None):
        """Scan raw rows once, keeping a separate top-k heap for each query."""
        heaps = [[] for _ in self._query_vectors]
        raw_row_ranges = _filtered_raw_row_ranges(raw_row_ranges, pre_filter)
        if not raw_row_ranges or not heaps:
            return [_scored_result(heap) for heap in heaps]

        table_read, splits = self._plan_raw_read(raw_row_ranges, True, snapshot)
        metric = self._search_metric(index_type)
        workers = min(len(splits), table_read._resolve_parallelism(None, len(splits)))
        if workers <= 1:
            return self._score_raw_splits(table_read, splits, metric)

        # Keep only one streaming reader and Q top-k heaps per worker, even
        # when the plan contains many splits. Each split is scanned once.
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(
                self._score_raw_splits, table_read, splits[i::workers], metric)
                for i in range(workers)]
            for future in futures:
                for heap, result in zip(heaps, future.result()):
                    score_getter = result.score_getter()
                    for row_id in result.results():
                        _offer_score(heap, self._limit, row_id, score_getter(row_id))
        return [_scored_result(heap) for heap in heaps]

    def _score_raw_splits(self, table_read, splits, metric):
        from pypaimon.read.table_read import _ClosableArrowBatchReader

        heaps = [[] for _ in self._query_vectors]
        reader, batches = table_read._new_arrow_batch_reader(splits)
        # Close the underlying iterator as well if scoring fails mid-batch.
        with _ClosableArrowBatchReader(reader, batches) as batch_reader:
            for batch in batch_reader:
                row_ids = batch.column(SpecialFields.ROW_ID.name).to_pylist()
                vectors = batch.column(self._vector_column.name)
                for start, query_index, scores in _iter_arrow_batch_scores(
                    vectors, self._query_vectors, metric
                ):
                    heap = heaps[query_index]
                    block_row_ids = row_ids[start:start + len(scores)]
                    for row_id, score in zip(block_row_ids, scores):
                        if score is not None:
                            _offer_score(heap, self._limit, row_id, score)
                del batch, row_ids, vectors
        return [_scored_result(heap) for heap in heaps]


def _create_vector_reader(index_type, file_io, index_path, index_io_meta_list, options=None):
    """Create a global index reader for vector search."""
    from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
        LUMINA_IDENTIFIERS,
        LuminaVectorGlobalIndexReader,
    )
    from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
        VINDEX_IDENTIFIERS,
        VindexVectorGlobalIndexReader,
    )
    if index_type in LUMINA_IDENTIFIERS:
        return LuminaVectorGlobalIndexReader(
            file_io, index_path, index_io_meta_list, options
        )
    if index_type in VINDEX_IDENTIFIERS:
        return VindexVectorGlobalIndexReader(
            file_io, index_path, index_io_meta_list, options
        )
    raise ValueError("Unsupported vector index type: '%s'" % index_type)


def _split_search_splits(splits):
    index_splits = []
    raw_splits = []
    for split in splits:
        if isinstance(split, IndexVectorSearchSplit):
            index_splits.append(split)
        elif isinstance(split, RawVectorSearchSplit):
            raw_splits.append(split)
    return index_splits, raw_splits


def _raw_row_ranges(raw_splits):
    ranges = []
    for split in raw_splits:
        ranges.extend(split.row_ranges)
    return Range.sort_and_merge_overlap(ranges, True)


def _filtered_raw_row_ranges(raw_row_ranges, pre_filter):
    raw_row_ranges = Range.sort_and_merge_overlap(raw_row_ranges, True)
    if pre_filter is None:
        return raw_row_ranges
    return Range.and_(
        raw_row_ranges,
        Range.sort_and_merge_overlap(pre_filter, True),
    )


def _raw_search_index_type(raw_splits):
    for split in raw_splits:
        if split.index_type is not None:
            return split.index_type
    return None


def _predicate_field_names(predicate):
    if predicate is None:
        return set()
    if predicate.method in ("and", "or"):
        names = set()
        for child in predicate.literals or []:
            names.update(_predicate_field_names(child))
        return names
    return {predicate.field} if predicate.field is not None else set()


def _vector_index_type(index_splits):
    for split in index_splits:
        if split.vector_index_files:
            return split.vector_index_files[0].index_type
    return None


def _empty_bitmaps(size):
    return [RoaringBitmap64() for _ in range(size)]


def _bitmap_of_range(row_range):
    bitmap = RoaringBitmap64()
    bitmap.add_range(row_range.from_, row_range.to)
    return bitmap


def _to_vector_list(value):
    if hasattr(value, "to_list"):
        return value.to_list()
    if hasattr(value, "as_py"):
        value = value.as_py()
    return list(value)


def _offer_score(heap, top_k, row_id, score):
    if top_k <= 0:
        return
    import heapq

    entry = (score, -row_id, row_id)
    if len(heap) < top_k:
        heapq.heappush(heap, entry)
    elif entry[:2] > heap[0][:2]:
        heapq.heapreplace(heap, entry)


def _scored_result(heap):
    return DictBasedScoredIndexResult({row_id: score for score, _, row_id in heap})


def _check_vector_dimension(query_vector, stored_vector):
    if len(stored_vector) != len(query_vector):
        raise ValueError(
            "Query vector dimension mismatch: expected %d, got %d"
            % (len(stored_vector), len(query_vector)))


def _configured_refine_factor(options, vector_column_name, index_type):
    prefixes = []
    field_prefix = "fields.%s." % vector_column_name
    _add_refine_prefixes(prefixes, field_prefix, index_type)
    _add_refine_prefixes(prefixes, "", index_type)

    for prefix in prefixes:
        for suffix in (
            "refine_factor",
            "refine-factor",
            "rerank_factor",
            "rerank-factor",
        ):
            value = options.get(prefix + suffix)
            if value is not None:
                return str(value).strip()
    return None


def _add_refine_prefixes(prefixes, base, index_type):
    if index_type:
        prefixes.append(base + index_type + ".")
        normalized = _normalize_index_type(index_type)
        if normalized != index_type:
            prefixes.append(base + normalized + ".")
        if normalized.startswith("ivf"):
            prefixes.append(base + "ivf.")
    prefixes.append(base)


def _normalize_index_type(index_type):
    return str(index_type).lower().replace("-", "_")


def _table_options_map(table):
    table_options = getattr(getattr(table, "options", None), "options", None)
    return table_options.to_map() if table_options is not None else {}


def _configured_vector_metric(options, vector_column, index_type=None):
    field_prefix = "fields.%s." % vector_column.name
    index_prefix = "%s." % index_type if index_type else None
    keys = [field_prefix + "pk-vector.distance.metric",
            field_prefix + "distance.metric", field_prefix + "metric"]
    if index_prefix is not None:
        keys.extend([index_prefix + "distance.metric", index_prefix + "metric"])
    keys.extend(["test.vector.metric", "lumina.distance.metric", "distance.metric", "metric"])
    for key in keys:
        if key in options:
            return _normalize_metric(options[key])
    return None


def _raw_search_metric(table, vector_column, options, index_type=None):
    from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import VINDEX_IDENTIFIERS

    table_map = _table_options_map(table)
    for source in (options, table_map):
        configured = _configured_vector_metric(source, vector_column, index_type)
        if configured is not None:
            return configured

    # Before an index exists, use its writer's default, not another column's metric.
    if index_type in VINDEX_IDENTIFIERS:
        return "inner_product"

    inferred = None
    for key, value in list(options.items()) + list(table_map.items()):
        if key.endswith(".distance.metric") or key.endswith(".metric"):
            metric = _normalize_metric(value)
            if metric in ("l2", "cosine", "inner_product"):
                if inferred is not None and inferred != metric:
                    return "l2"
                inferred = metric
    return inferred or "l2"


def _normalize_metric(metric):
    return str(metric).lower().replace("-", "_")


def _score_block_size(query):
    # Target 8 MiB per float64 matrix, allowing at least one vector.
    return max(1, min(1024, (1 << 20) // max(1, len(query))))


def _iter_arrow_scores(vectors, query, metric):
    block_size = _score_block_size(query)
    for start in range(0, len(vectors), block_size):
        block = vectors.slice(start, block_size)
        matrix = _arrow_score_matrix(block, len(query))
        scores = None if matrix is None else _compute_scores(query, matrix, metric)
        if scores is None:
            for vector in block.to_pylist():
                if vector is None:
                    yield None
                else:
                    vector = _to_vector_list(vector)
                    _check_vector_dimension(query, vector)
                    yield _compute_score(query, vector, metric)
        else:
            yield from scores


def _iter_arrow_batch_scores(vectors, queries, metric):
    """Yield vectorized scores for one Arrow batch without expanding its rows.

    The float64 row matrix is materialized once per bounded block and reused for
    every query. Only the scratch matrix for the active query is retained, so
    memory does not grow with the number of queries.
    """
    import numpy as np

    if not queries:
        return
    block_size = min(_score_block_size(query) for query in queries)
    dimension = len(queries[0])
    fast_queries = metric in ("l2", "cosine", "inner_product")
    if fast_queries:
        for query in queries:
            try:
                query_array = np.asarray(query, dtype=np.float64)
            except (ValueError, TypeError, OverflowError):
                fast_queries = False
                break
            if (query_array.ndim != 1 or len(query_array) != dimension or
                    not dimension or not np.isfinite(query_array).all()):
                fast_queries = False
                break

    for start in range(0, len(vectors), block_size):
        block = vectors.slice(start, block_size)
        matrix = _arrow_score_matrix(block, dimension) if fast_queries else None
        if matrix is not None and not np.isfinite(matrix).all():
            matrix = None
        if matrix is None:
            rows = block.to_pylist()
            for query_index, query in enumerate(queries):
                yield start, query_index, _score_rows(rows, query, metric)
            continue

        last_query = len(queries) - 1
        for query_index, query in enumerate(queries):
            # Scoring mutates its owned matrix. Reuse the original for the last
            # query and keep only one scratch copy for every preceding query.
            working = matrix if query_index == last_query else matrix.copy()
            scores = _compute_scores(query, working, metric)
            assert scores is not None
            yield start, query_index, scores


def _arrow_score_matrix(vectors, dimension):
    """Return one regular float32 Arrow vector block as owned float64 rows."""
    import numpy as np
    import pyarrow as pa

    if isinstance(vectors, pa.ChunkedArray):
        vectors = vectors.combine_chunks()
    dtype = vectors.type
    if not ((pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or
             pa.types.is_fixed_size_list(dtype)) and not vectors.null_count):
        return None
    flat = vectors.flatten()
    if not pa.types.is_float32(flat.type) or flat.null_count:
        return None
    if pa.types.is_fixed_size_list(dtype):
        regular = dtype.list_size == dimension
    else:
        offsets = vectors.offsets.to_numpy(zero_copy_only=False)
        regular = bool(np.all(np.diff(offsets) == dimension))
    if not regular or not dimension:
        return None
    return flat.to_numpy(zero_copy_only=False).astype(np.float64).reshape(
        len(vectors), dimension)


def _score_rows(vectors, query, metric):
    import numpy as np

    scores = None
    if vectors and all(vector is not None for vector in vectors):
        try:
            matrix = np.array(vectors, dtype=np.float64)
        except (ValueError, TypeError, OverflowError):
            matrix = None
        if matrix is not None:
            scores = _compute_scores(query, matrix, metric)
    if scores is not None:
        return scores
    result = []
    for vector in vectors:
        if vector is None:
            result.append(None)
        else:
            vector = _to_vector_list(vector)
            _check_vector_dimension(query, vector)
            result.append(_compute_score(query, vector, metric))
    return result


def _compute_scores(query, matrix, metric):
    """Score an owned float64 matrix while preserving scalar reduction semantics.

    L2/cosine use left-to-right accumulation, as in the scalar loops. Inner
    product uses Python's sum on precomputed products, retaining its behavior
    across Python versions (including compensated summation on Python 3.12+).
    Returning None selects the scalar fallback.
    """
    import numpy as np

    try:
        query = np.asarray(query, dtype=np.float64)
    except (ValueError, TypeError, OverflowError):
        return None
    if (matrix.ndim != 2 or query.ndim != 1 or not len(query) or
            matrix.shape[1] != len(query) or not np.isfinite(matrix).all() or
            not np.isfinite(query).all()):
        return None
    if metric not in ("l2", "cosine", "inner_product"):
        return None
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        if metric == "l2":
            np.subtract(query, matrix, out=matrix)
            np.square(matrix, out=matrix)
            np.add.accumulate(matrix, axis=1, out=matrix)
            return (1.0 / (1.0 + matrix[:, -1])).tolist()
        if metric == "cosine":
            norms = np.square(matrix)
            np.add.accumulate(norms, axis=1, out=norms)
            stored_norms = norms[:, -1].copy()
            del norms
            query_norm = 0.0
            for value in query:
                value = float(value)
                query_norm += value * value
        np.multiply(matrix, query, out=matrix)
        if metric == "inner_product":
            return [sum(row) for row in matrix.tolist()]
        matrix[:, 0] += 0.0  # Match a scalar accumulator initialized to +0.0.
        np.add.accumulate(matrix, axis=1, out=matrix)
        denominators = [query_norm ** 0.5 * float(norm) ** 0.5 for norm in stored_norms]
        return [0.0 if denominator == 0 else float(dot) / denominator
                for dot, denominator in zip(matrix[:, -1], denominators)]


def _compute_score(query, stored, metric):
    if metric == "l2":
        sum_sq = 0.0
        for q, s in zip(query, stored):
            diff = float(q) - float(s)
            sum_sq += diff * diff
        return 1.0 / (1.0 + sum_sq)
    if metric == "cosine":
        dot = 0.0
        norm_a = 0.0
        norm_b = 0.0
        for q, s in zip(query, stored):
            q = float(q)
            s = float(s)
            dot += q * s
            norm_a += q * q
            norm_b += s * s
        denominator = (norm_a ** 0.5) * (norm_b ** 0.5)
        return 0.0 if denominator == 0 else dot / denominator
    if metric == "inner_product":
        return sum(float(q) * float(s) for q, s in zip(query, stored))
    raise ValueError("Unknown vector search metric: %s" % metric)
