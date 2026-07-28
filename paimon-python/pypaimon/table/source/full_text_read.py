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
from io import BytesIO
from typing import Dict, List

from pypaimon.globalindex.full_text_search import FullTextSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search_result import (
    DictBasedScoredIndexResult,
    ScoredGlobalIndexResult,
)
from pypaimon.table.source import global_index_live_row_filter
from pypaimon.table.source.full_text_search_split import (
    FullTextSearchSplit,
    IndexFullTextSearchSplit,
    RawFullTextSearchSplit,
)
from pypaimon.table.source.full_text_scan import FullTextScanPlan
from pypaimon.utils.range import Range


class FullTextRead(ABC):
    """Full-text read to read index files."""

    def read_plan(self, plan: FullTextScanPlan) -> GlobalIndexResult:
        return self.read(plan.splits())

    @abstractmethod
    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        pass


class DataEvolutionFullTextRead(FullTextRead):
    """Implementation for FullTextRead."""

    def __init__(
        self,
        table: 'FileStoreTable',
        limit: int,
        text_column,
        query: str,
        partition_filter=None,
    ):
        self._table = table
        self._limit = limit
        self._text_columns = text_column if isinstance(text_column, list) else [text_column]
        if len(self._text_columns) != 1:
            raise ValueError(
                "Full-text search expects exactly one text column, got: %s"
                % self._text_columns)
        self._query = query
        self._partition_filter = partition_filter

    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        index_splits, raw_splits = _split_search_splits(splits)
        if not index_splits and not raw_splits:
            return GlobalIndexResult.create_empty()

        splits_by_column: Dict[str, List[IndexFullTextSearchSplit]] = {}
        for split in index_splits:
            splits_by_column.setdefault(split.column_name, []).append(split)
        live_rows = global_index_live_row_filter.live_rows(
            self._table, self._partition_filter)
        indexed_result = self._eval_column_query(splits_by_column, live_rows)
        raw_result = self._read_raw_search(
            _raw_row_ranges(raw_splits), _index_type(index_splits))
        return indexed_result.or_(raw_result).top_k(self._limit)

    def _eval_column_query(
            self,
            splits_by_column: Dict[str, List[IndexFullTextSearchSplit]],
            live_rows
    ) -> ScoredGlobalIndexResult:
        column = self._text_columns[0].name
        splits = splits_by_column.get(column, [])
        if not splits:
            return ScoredGlobalIndexResult.create_empty()
        futures = []
        for split in splits:
            include_row_ids = global_index_live_row_filter.for_range(
                live_rows, split.row_range_start, split.row_range_end)
            if include_row_ids is not None and include_row_ids.is_empty():
                continue
            futures.append(self._eval(
                split.row_range_start, split.row_range_end,
                split.full_text_index_files,
                include_row_ids,
            ))
        if not futures:
            return ScoredGlobalIndexResult.create_empty()

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

    def _eval(self, row_range_start, row_range_end, full_text_index_files,
              include_row_ids):
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
            field_name=self._text_columns[0].name,
            query=self._query,
            limit=_candidate_limit(row_range_start, row_range_end),
        )
        if include_row_ids is not None:
            full_text_search = full_text_search.with_include_row_ids(include_row_ids)

        offset_reader = OffsetGlobalIndexReader(reader, row_range_start, row_range_end)
        future = offset_reader.visit_full_text_search(full_text_search)
        future.add_done_callback(lambda _: reader.close())
        return future

    def _read_raw_search(self, raw_row_ranges, index_type):
        raw_row_ranges = Range.sort_and_merge_overlap(raw_row_ranges, True)
        if not raw_row_ranges:
            return DictBasedScoredIndexResult({})
        if index_type is not None and not _supports_full_text_search(index_type):
            raise ValueError(f"Unsupported full-text index type: '{index_type}'")

        row_range_start = raw_row_ranges[0].from_
        row_range_end = raw_row_ranges[-1].to
        table = self._read_raw_rows(raw_row_ranges)
        if table is None or table.num_rows == 0:
            return DictBasedScoredIndexResult({})

        from pypaimon.table.special_fields import SpecialFields

        row_ids = table.column(SpecialFields.ROW_ID.name).to_pylist()
        texts = table.column(self._text_columns[0].name).to_pylist()
        index_bytes = self._build_raw_index(row_ids, texts, row_range_start)
        if index_bytes is None:
            return DictBasedScoredIndexResult({})

        return _search_raw_full_text(
            index_bytes,
            row_range_start,
            self._query,
            _candidate_limit(row_range_start, row_range_end),
        ).top_k(self._limit)

    def _read_raw_rows(self, raw_row_ranges):
        read_builder = self._table.new_read_builder()
        if self._partition_filter is not None:
            read_builder = read_builder.with_partition_filter(self._partition_filter)

        from pypaimon.table.special_fields import SpecialFields

        projection = [self._text_columns[0].name, SpecialFields.ROW_ID.name]
        read_builder = read_builder.with_projection(projection)
        plan = read_builder.new_scan().with_global_index_result(
            GlobalIndexResult.from_ranges(raw_row_ranges)).plan()
        return read_builder.new_read().to_arrow(plan.splits())

    def _build_raw_index(self, row_ids, texts, row_range_start):
        try:
            from paimon_ftindex import FullTextIndexWriter
        except ImportError as e:
            raise ImportError(
                "paimon-ftindex is required to search uncovered full-text "
                "row ranges. Install paimon-ftindex==0.1.0 or "
                "pypaimon[full-text]."
            ) from e

        from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
            NativeFullTextIndexOptions,
        )

        writer = FullTextIndexWriter(
            NativeFullTextIndexOptions.from_options(
                _raw_search_options(self._table)).to_native_options())
        indexed = False
        try:
            for row_id, text in zip(row_ids, texts):
                if text is None:
                    continue
                writer.add_document(
                    int(row_id) - row_range_start,
                    _materialize_text(text),
                )
                indexed = True
            if not indexed:
                return None
            output = BytesIO()
            writer.write(output)
            return output.getvalue()
        finally:
            writer.close()


def _candidate_limit(row_range_start: int, row_range_end: int) -> int:
    size = row_range_end - row_range_start + 1
    return min(size, 2147483647)


def _create_full_text_reader(index_type, file_io, index_path, index_io_meta_list):
    """Create a global index reader for full-text search."""
    from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
        FULL_TEXT_IDENTIFIER,
    )
    if index_type == FULL_TEXT_IDENTIFIER:
        from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
            NativeFullTextGlobalIndexReader,
        )
        return NativeFullTextGlobalIndexReader(file_io, index_path, index_io_meta_list)
    raise ValueError(f"Unsupported full-text index type: '{index_type}'")


def _supports_full_text_search(index_type):
    from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
        FULL_TEXT_IDENTIFIER,
    )
    return index_type == FULL_TEXT_IDENTIFIER


def _split_search_splits(splits):
    index_splits = []
    raw_splits = []
    for split in splits:
        if isinstance(split, IndexFullTextSearchSplit):
            index_splits.append(split)
        elif isinstance(split, RawFullTextSearchSplit):
            raw_splits.append(split)
        else:
            raise ValueError(
                "Unsupported full-text search split: %s"
                % type(split).__name__)
    return index_splits, raw_splits


def _raw_row_ranges(raw_splits):
    ranges = []
    for split in raw_splits:
        ranges.extend(split.row_ranges)
    return Range.sort_and_merge_overlap(ranges, True)


def _index_type(index_splits):
    for split in index_splits:
        if split.full_text_index_files:
            return split.full_text_index_files[0].index_type
    return None


def _search_raw_full_text(index_bytes, row_range_start, query, limit):
    from paimon_ftindex import FullTextIndexReader
    from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
        PaimonFullTextInput,
    )

    reader = FullTextIndexReader(PaimonFullTextInput(BytesIO(index_bytes)))
    try:
        row_ids, scores = reader.search(query, limit=limit)
        return DictBasedScoredIndexResult(
            {
                row_range_start + row_id: score
                for row_id, score in zip(row_ids, scores)
            })
    finally:
        reader.close()


def _raw_search_options(table):
    table_options = getattr(getattr(table, "options", None), "options", None)
    if table_options is not None:
        options = dict(table_options.to_map())
    else:
        options = dict(getattr(getattr(table, "table_schema", None), "options", {}) or {})
    options["full-text.searcher-pool.max-size"] = "0"
    return options


def _materialize_text(value) -> str:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if not isinstance(value, str):
        raise ValueError("Unsupported field type: %s" % type(value).__name__)
    return value
