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
from concurrent.futures import wait

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.vector_search_split import (
    IndexVectorSearchSplit,
    RawVectorSearchSplit,
)
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class VectorSearchRead(ABC):
    """Vector search read to read index files."""

    def read_plan(self, plan):
        # type: (VectorSearchScanPlan) -> GlobalIndexResult
        return self.read(plan.splits())

    @abstractmethod
    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        pass


class BatchVectorSearchRead(ABC):
    """Batch vector search read to read index files."""

    def read_batch_plan(self, plan):
        # type: (VectorSearchScanPlan) -> List[GlobalIndexResult]
        return self.read_batch(plan.splits())

    @abstractmethod
    def read_batch(self, splits):
        # type: (List[VectorSearchSplit]) -> List[GlobalIndexResult]
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

    def _pre_filters(self, splits):
        # type: (list) -> List[RoaringBitmap64]
        """Evaluate scalar indexes and return one include bitmap per index split."""
        if self._filter is None:
            return []

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
            return _empty_bitmaps(len(splits))

        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner
        scanner = GlobalIndexScanner.create(
            self._table,
            index_files=scalar_files,
            partition_filter=self._partition_filter,
        )
        if scanner is None:
            return _empty_bitmaps(len(splits))
        try:
            result = scanner.scan(self._filter)
            if result is None:
                return _empty_bitmaps(len(splits))
            matched_rows = result.results()
        finally:
            scanner.close()

        include_row_ids = []
        for split in splits:
            split_rows = _bitmap_of_range(
                Range(split.row_range_start, split.row_range_end))
            include_row_ids.append(RoaringBitmap64.and_(matched_rows, split_rows))
        return include_row_ids

    def _pre_filter(self, splits):
        # Backwards-compatible helper used by older tests/callers.
        pre_filters = self._pre_filters(splits)
        if not pre_filters:
            return None
        merged = RoaringBitmap64()
        for bitmap in pre_filters:
            merged = RoaringBitmap64.or_(merged, bitmap)
        return merged

    def _raw_pre_filter(self, splits):
        if self._filter is None:
            return None
        raw_rows = _bitmap_of_ranges(_raw_row_ranges(splits))
        if raw_rows.is_empty():
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

        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner
        scanner = GlobalIndexScanner.create(
            self._table,
            index_files=scalar_files,
            partition_filter=self._partition_filter,
        )
        if scanner is None:
            return None
        try:
            result = scanner.scan(self._filter)
            if result is None:
                return None
            include = result.results()
            include = RoaringBitmap64.or_(
                include,
                scanner.unindexed_rows(self._filter).results())
            return RoaringBitmap64.and_(include, raw_rows)
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
        return reader, OffsetGlobalIndexReader(reader, row_range_start, row_range_end)

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
        future = offset_reader.visit_vector_search(vector_search)
        future.add_done_callback(lambda _: reader.close())
        return future

    def _read_raw_search(self, raw_row_ranges, pre_filter, query_vector, index_type=None):
        raw_row_ranges = Range.sort_and_merge_overlap(raw_row_ranges, True)
        if pre_filter is not None:
            raw_rows = RoaringBitmap64.and_(_bitmap_of_ranges(raw_row_ranges), pre_filter)
            return self._read_raw_bitmap_search(raw_rows, query_vector, index_type)
        if not raw_row_ranges:
            return DictBasedScoredIndexResult({})

        read_builder = self._table.new_read_builder()
        if self._partition_filter is not None:
            read_builder = read_builder.with_partition_filter(
                self._partition_filter)
        if self._filter is not None:
            read_builder = read_builder.with_filter(self._filter)
        from pypaimon.table.special_fields import SpecialFields
        projection = [f.name for f in self._table.fields]
        if SpecialFields.ROW_ID.name not in projection:
            projection.append(SpecialFields.ROW_ID.name)
        read_builder = read_builder.with_projection(projection)
        plan = read_builder.new_scan().with_global_index_result(
            GlobalIndexResult.from_ranges(raw_row_ranges)).plan()
        table = read_builder.new_read().to_arrow(plan.splits())
        if table is None or table.num_rows == 0:
            return DictBasedScoredIndexResult({})

        row_ids = table.column(SpecialFields.ROW_ID.name).to_pylist()
        vectors = table.column(self._vector_column.name).to_pylist()
        metric = _raw_search_metric(
            self._table, self._vector_column, self._options, index_type)
        scores = {}
        for row_id, stored in zip(row_ids, vectors):
            if stored is None:
                continue
            stored_vector = _to_vector_list(stored)
            if len(stored_vector) != len(query_vector):
                raise ValueError(
                    "Query vector dimension mismatch: expected %d, got %d"
                    % (len(stored_vector), len(query_vector)))
            scores[row_id] = _compute_score(query_vector, stored_vector, metric)
        return DictBasedScoredIndexResult(scores).top_k(self._limit)

    def _read_raw_bitmap_search(self, raw_rows, query_vector, index_type=None):
        if raw_rows.is_empty():
            return DictBasedScoredIndexResult({})

        read_builder = self._table.new_read_builder()
        if self._partition_filter is not None:
            read_builder = read_builder.with_partition_filter(
                self._partition_filter)
        if self._filter is not None:
            read_builder = read_builder.with_filter(self._filter)
        from pypaimon.table.special_fields import SpecialFields
        projection = [f.name for f in self._table.fields]
        if SpecialFields.ROW_ID.name not in projection:
            projection.append(SpecialFields.ROW_ID.name)
        read_builder = read_builder.with_projection(projection)
        plan = read_builder.new_scan().with_global_index_result(
            GlobalIndexResult.create(raw_rows)).plan()
        table = read_builder.new_read().to_arrow(plan.splits())
        if table is None or table.num_rows == 0:
            return DictBasedScoredIndexResult({})

        row_ids = table.column(SpecialFields.ROW_ID.name).to_pylist()
        vectors = table.column(self._vector_column.name).to_pylist()
        metric = _raw_search_metric(
            self._table, self._vector_column, self._options, index_type)
        scores = {}
        for row_id, stored in zip(row_ids, vectors):
            if stored is None:
                continue
            stored_vector = _to_vector_list(stored)
            if len(stored_vector) != len(query_vector):
                raise ValueError(
                    "Query vector dimension mismatch: expected %d, got %d"
                    % (len(stored_vector), len(query_vector)))
            scores[row_id] = _compute_score(query_vector, stored_vector, metric)
        return DictBasedScoredIndexResult(scores).top_k(self._limit)

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
        future = offset_reader.visit_batch_vector_search(batch_vector_search)
        future.add_done_callback(lambda _: reader.close())
        return future

    def _indexed_search_limit(self, index_type):
        refine_factor = self._configured_refine_factor(index_type)
        if refine_factor == 0:
            return self._limit
        return self._limit * refine_factor

    def _maybe_rerank_indexed_result(self, result, index_type, query_vector):
        if (self._configured_refine_factor(index_type) == 0 or
                result.results().is_empty()):
            return result
        candidates = result.top_k(self._indexed_search_limit(index_type))
        return self._read_raw_bitmap_search(
            candidates.results(), query_vector, index_type)

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


class VectorSearchReadImpl(AbstractVectorSearchReadImpl, VectorSearchRead):
    """Implementation for VectorSearchRead."""

    def __init__(self, table, limit, vector_column, query_vector, filter_=None,
                 partition_filter=None, options=None):
        super().__init__(table, limit, vector_column,
                         filter_=filter_,
                         partition_filter=partition_filter,
                         options=options)
        self._query_vector = query_vector

    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        index_splits, raw_splits = _split_search_splits(splits)
        if not index_splits and not raw_splits:
            return GlobalIndexResult.create_empty()

        indexed = (
            DictBasedScoredIndexResult({})
            if not index_splits
            else self._read_indexed(index_splits, self._query_vector)
        )
        raw_result = self._read_raw_search(
            _raw_row_ranges(raw_splits),
            self._raw_pre_filter(raw_splits),
            self._query_vector,
            _raw_search_index_type(raw_splits),
        )
        return indexed.or_(raw_result).top_k(self._limit)

    def _read_indexed(self, splits, query_vector):
        index_type = _vector_index_type(splits)
        search_limit = self._indexed_search_limit(index_type)
        pre_filters = self._pre_filters(splits)
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
        return self._maybe_rerank_indexed_result(indexed, index_type, query_vector)


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

    def read_batch(self, splits):
        # type: (List[VectorSearchSplit]) -> List[GlobalIndexResult]
        n = len(self._query_vectors)
        index_splits, raw_splits = _split_search_splits(splits)
        if not index_splits and not raw_splits:
            return [GlobalIndexResult.create_empty() for _ in range(n)]

        # One native batch call per INDEX split (all query vectors at once),
        # passing that split's pre-filter. Each future returns n per-query results.
        index_type = _vector_index_type(index_splits)
        search_limit = self._indexed_search_limit(index_type)
        pre_filters = self._pre_filters(index_splits)
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

        # Each query: merge indexed results with the raw (brute-force) fallback.
        raw_pre_filter = self._raw_pre_filter(raw_splits)
        raw_ranges = _raw_row_ranges(raw_splits)
        raw_index_type = _raw_search_index_type(raw_splits)
        results = []
        for i in range(n):
            indexed = DictBasedScoredIndexResult(merged_scores[i]).top_k(search_limit)
            indexed = self._maybe_rerank_indexed_result(
                indexed, index_type, self._query_vectors[i])
            raw = self._read_raw_search(
                raw_ranges, raw_pre_filter, self._query_vectors[i], raw_index_type)
            results.append(indexed.or_(raw).top_k(self._limit))
        return results


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


def _raw_search_index_type(raw_splits):
    for split in raw_splits:
        if split.index_type is not None:
            return split.index_type
    return None


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


def _bitmap_of_ranges(ranges):
    bitmap = RoaringBitmap64()
    for row_range in ranges:
        bitmap.add_range(row_range.from_, row_range.to)
    return bitmap


def _to_vector_list(value):
    if hasattr(value, "to_list"):
        return value.to_list()
    if hasattr(value, "as_py"):
        value = value.as_py()
    return list(value)


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


def _raw_search_metric(table, vector_column, options, index_type=None):
    candidates = []
    field_prefix = "fields.%s." % vector_column.name
    index_prefix = "%s." % index_type if index_type else None
    for key in [
        field_prefix + "distance.metric",
        field_prefix + "metric",
        *(([
            index_prefix + "distance.metric",
            index_prefix + "metric",
        ]) if index_prefix is not None else []),
        "test.vector.metric",
        "lumina.distance.metric",
        "distance.metric",
        "metric",
    ]:
        if key in options:
            candidates.append(options[key])
    table_map = _table_options_map(table)
    for key in [
        field_prefix + "distance.metric",
        field_prefix + "metric",
        *(([
            index_prefix + "distance.metric",
            index_prefix + "metric",
        ]) if index_prefix is not None else []),
        "test.vector.metric",
        "lumina.distance.metric",
        "distance.metric",
        "metric",
    ]:
        if key in table_map:
            candidates.append(table_map[key])
    if candidates:
        return _normalize_metric(candidates[0])

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
