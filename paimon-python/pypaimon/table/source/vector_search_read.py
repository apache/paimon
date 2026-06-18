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

import math
from abc import ABC, abstractmethod
from concurrent.futures import wait

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.utils.range import Range


GLOBAL_INDEX_FAST_SEARCH = "global-index.fast-search"


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

    GLOBAL_INDEX_FAST_SEARCH = GLOBAL_INDEX_FAST_SEARCH

    def __init__(self, table, limit, vector_column, filter_=None,
                 options=None, partition_filter=None):
        self._table = table
        self._limit = limit
        self._vector_column = vector_column
        self._filter = filter_
        self._partition_filter = partition_filter
        self._options = dict(options or {})
        self._vector_metric = None

    def _search_one(self, query_vector, splits, pre_filter):
        # type: (list, list, Optional[RoaringBitmap64]) -> GlobalIndexResult
        """Search one query vector across all splits and merge per-split results."""
        futures = [
            self._eval(
                split.row_range_start, split.row_range_end,
                split.vector_index_files, query_vector, pre_filter
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

        return DictBasedScoredIndexResult(merged_scores)

    def _pre_filter(self, splits):
        # type: (list) -> Optional[RoaringBitmap64]
        """Evaluate the scalar filter against scalar global indexes to produce a row-id bitmap."""
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
            return None

        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner
        scanner = GlobalIndexScanner.create(self._table, index_files=scalar_files)
        if scanner is None:
            return None
        try:
            result = scanner.scan(self._filter)
            if result is None:
                return None
            return result.results()
        finally:
            scanner.close()

    def _eval(self, row_range_start, row_range_end, vector_index_files,
              query_vector, include_row_ids):
        from pypaimon.globalindex.global_index_reader import _completed_future

        if not vector_index_files:
            return _completed_future(None)
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

        index_type = vector_index_files[0].index_type
        index_path = self._table.path_factory().global_index_path_factory().index_path()
        file_io = self._table.file_io
        options = self._table.table_schema.options

        vector_search = VectorSearch(
            vector=query_vector,
            limit=self._limit,
            field_name=self._vector_column.name,
            options=self._index_options(),
        )
        if include_row_ids is not None:
            vector_search = vector_search.with_include_row_ids(include_row_ids)

        reader = _create_vector_reader(
            index_type, file_io, index_path,
            index_io_meta_list, options
        )
        if self._slow_search_enabled() and self._vector_metric is None:
            self._vector_metric = reader.vector_metric()
        offset_reader = OffsetGlobalIndexReader(reader, row_range_start, row_range_end)
        future = offset_reader.visit_vector_search(vector_search)
        future.add_done_callback(lambda _: reader.close())
        return future

    def _slow_search_enabled(self):
        return not self._fast_search()

    def _fast_search(self):
        return str(
            self._table_option(GLOBAL_INDEX_FAST_SEARCH, "true")
        ).lower() == "true"

    def _with_slow_search(self, result, splits, query_vector):
        if not self._slow_search_enabled():
            return result.top_k(self._limit)

        raw_result = self._read_slow_search(splits, query_vector)
        return result.or_(raw_result).top_k(self._limit)

    def _read_slow_search(self, splits, query_vector):
        from pypaimon.table.special_fields import SpecialFields

        read_type = self._read_type_with_row_id()
        range_discovery_builder = self._new_raw_read_builder(
            read_type, include_filter=False)
        all_data_plan = range_discovery_builder.new_scan().plan()
        non_indexed_ranges = self._non_indexed_ranges(all_data_plan, splits)
        if not non_indexed_ranges:
            return DictBasedScoredIndexResult({})

        raw_splits = self._wrap_splits_with_row_ranges(
            all_data_plan.splits(), non_indexed_ranges)
        if not raw_splits:
            return DictBasedScoredIndexResult({})

        read_builder = self._new_raw_read_builder(read_type, include_filter=True)
        arrow_table = read_builder.new_read().to_arrow(raw_splits)
        if arrow_table is None or arrow_table.num_rows == 0:
            return DictBasedScoredIndexResult({})

        row_id_name = SpecialFields.ROW_ID.name
        vector_name = self._vector_column.name
        if row_id_name not in arrow_table.column_names:
            raise ValueError(
                "Vector slow search requires row tracking column %s."
                % row_id_name)
        if vector_name not in arrow_table.column_names:
            raise ValueError(
                "Vector slow search read type does not contain vector column %s."
                % vector_name)

        metric = self._slow_search_metric()
        query = self._normalize_vector(query_vector)
        scores = {}
        row_ids = arrow_table.column(row_id_name).to_pylist()
        vectors = arrow_table.column(vector_name).to_pylist()
        for row_id, stored in zip(row_ids, vectors):
            if row_id is None or stored is None:
                continue
            row_id = int(row_id)
            if not self._contains_row_id(non_indexed_ranges, row_id):
                continue
            stored_vector = self._normalize_vector(stored)
            if len(stored_vector) != len(query):
                raise ValueError(
                    "Query vector dimension mismatch: expected %d, got %d"
                    % (len(stored_vector), len(query)))
            scores[row_id] = self._compute_score(query, stored_vector, metric)

        return DictBasedScoredIndexResult(scores).top_k(self._limit)

    def _read_type_with_row_id(self):
        from pypaimon.table.special_fields import SpecialFields

        fields = list(self._table.fields)
        if any(f.name == SpecialFields.ROW_ID.name for f in fields):
            return fields
        return SpecialFields.row_type_with_row_id(fields)

    def _new_raw_read_builder(self, read_type, include_filter):
        read_builder = self._table.new_read_builder().with_read_type(read_type)
        if self._partition_filter is not None:
            read_builder = read_builder.with_partition_filter(self._partition_filter)
        if include_filter and self._filter is not None:
            read_builder = read_builder.with_filter(self._filter)
        return read_builder

    def _non_indexed_ranges(self, all_data_plan, splits):
        data_ranges = []
        for split in all_data_plan.splits():
            data_ranges.extend(self._split_row_ranges(split))

        indexed_ranges = [
            Range(split.row_range_start, split.row_range_end)
            for split in splits
        ]
        indexed_ranges = Range.sort_and_merge_overlap(indexed_ranges, True)

        ranges = []
        for data_range in Range.sort_and_merge_overlap(data_ranges, True):
            ranges.extend(data_range.exclude(indexed_ranges))
        return Range.sort_and_merge_overlap(ranges, True)

    def _split_row_ranges(self, split):
        from pypaimon.globalindex.indexed_split import IndexedSplit

        if isinstance(split, IndexedSplit):
            return list(split.row_ranges())

        ranges = []
        for data_file in getattr(split, "files", []) or []:
            row_range = self._file_row_range(data_file)
            if row_range is not None:
                ranges.append(row_range)
        return ranges

    def _file_row_range(self, data_file):
        try:
            row_range = data_file.row_id_range()
        except Exception:
            row_range = None
        if row_range is not None:
            return row_range

        first_row_id = getattr(data_file, "first_row_id", None)
        row_count = getattr(data_file, "row_count", None)
        if first_row_id is None or row_count is None or row_count <= 0:
            return None
        return Range(int(first_row_id), int(first_row_id) + int(row_count) - 1)

    def _wrap_splits_with_row_ranges(self, splits, row_ranges):
        from pypaimon.globalindex.indexed_split import IndexedSplit

        indexed_splits = []
        for split in splits:
            if isinstance(split, IndexedSplit):
                data_split = split.data_split()
                available_ranges = list(split.row_ranges())
            else:
                data_split = split
                available_ranges = self._split_row_ranges(split)

            expected = Range.and_(available_ranges, row_ranges)
            if expected:
                indexed_splits.append(IndexedSplit(data_split, expected))
        return indexed_splits

    def _contains_row_id(self, ranges, row_id):
        for row_range in ranges:
            if row_range.contains(row_id):
                return True
        return False

    def _index_options(self):
        return dict(self._options)

    def _slow_search_metric(self):
        metric = self._vector_metric
        if metric is None:
            return "l2"
        return str(metric).lower().replace("-", "_")

    def _table_option(self, key, default=None):
        options = self._table_schema_options()
        if isinstance(options, dict):
            return options.get(key, default)
        if hasattr(options, "get"):
            try:
                value = options.get(key)
                return default if value is None else value
            except TypeError:
                pass
        return default

    def _table_schema_options(self):
        table_schema = getattr(self._table, "table_schema", None)
        return getattr(table_schema, "options", {}) or {}

    @staticmethod
    def _normalize_vector(vector):
        if hasattr(vector, "as_py"):
            vector = vector.as_py()
        if hasattr(vector, "tolist"):
            vector = vector.tolist()
        return [float(v) for v in vector]

    @staticmethod
    def _compute_score(query, stored, metric):
        if metric == "l2":
            sum_sq = 0.0
            for query_value, stored_value in zip(query, stored):
                diff = query_value - stored_value
                sum_sq += diff * diff
            return 1.0 / (1.0 + sum_sq)
        if metric == "cosine":
            dot = 0.0
            norm_a = 0.0
            norm_b = 0.0
            for query_value, stored_value in zip(query, stored):
                dot += query_value * stored_value
                norm_a += query_value * query_value
                norm_b += stored_value * stored_value
            denominator = math.sqrt(norm_a) * math.sqrt(norm_b)
            return 0.0 if denominator == 0.0 else dot / denominator
        if metric == "inner_product":
            dot = 0.0
            for query_value, stored_value in zip(query, stored):
                dot += query_value * stored_value
            return dot
        raise ValueError("Unknown vector search metric: %s" % metric)


class VectorSearchReadImpl(AbstractVectorSearchReadImpl, VectorSearchRead):
    """Implementation for VectorSearchRead."""

    def __init__(self, table, limit, vector_column, query_vector, filter_=None,
                 options=None, partition_filter=None):
        super().__init__(table, limit, vector_column,
                         filter_=filter_, options=options,
                         partition_filter=partition_filter)
        self._query_vector = query_vector

    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        if not splits and self._fast_search():
            return GlobalIndexResult.create_empty()

        result = (
            DictBasedScoredIndexResult({})
            if not splits
            else self._search_one(self._query_vector, splits, self._pre_filter(splits))
        )
        return self._with_slow_search(result, splits, self._query_vector)


class BatchVectorSearchReadImpl(AbstractVectorSearchReadImpl,
                                BatchVectorSearchRead):
    """Batch vector search read; result ``i`` corresponds to query vector ``i``."""

    def __init__(self, table, limit, vector_column, query_vectors,
                 filter_=None, options=None, partition_filter=None):
        super().__init__(table, limit, vector_column,
                         filter_=filter_, options=options,
                         partition_filter=partition_filter)
        self._query_vectors = list(query_vectors)

    def read_batch(self, splits):
        # type: (List[VectorSearchSplit]) -> List[GlobalIndexResult]
        n = len(self._query_vectors)
        if not splits and self._fast_search():
            return [GlobalIndexResult.create_empty() for _ in range(n)]

        results = []
        pre_filter = self._pre_filter(splits) if splits else None
        for vector in self._query_vectors:
            result = (
                DictBasedScoredIndexResult({})
                if not splits
                else self._search_one(vector, splits, pre_filter)
            )
            results.append(self._with_slow_search(result, splits, vector))
        return results


def _create_vector_reader(index_type, file_io, index_path, index_io_meta_list, options=None):
    """Create a global index reader for vector search."""
    from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
        LUMINA_IDENTIFIERS,
        LuminaVectorGlobalIndexReader,
    )
    if index_type in LUMINA_IDENTIFIERS:
        return LuminaVectorGlobalIndexReader(
            file_io, index_path, index_io_meta_list, options
        )
    raise ValueError("Unsupported vector index type: '%s'" % index_type)
