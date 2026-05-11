################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Full-text read to read index files."""

from abc import ABC, abstractmethod
from typing import List, Optional

from pypaimon.globalindex.full_text_search import FullTextSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
from pypaimon.table.source.full_text_search_split import FullTextSearchSplit
from pypaimon.table.source.full_text_scan import FullTextScanPlan


class FullTextRead(ABC):
    """Full-text read to read index files."""

    def read_plan(self, plan: FullTextScanPlan) -> GlobalIndexResult:
        return self.read(plan.splits())

    @abstractmethod
    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        pass


class FullTextReadImpl(FullTextRead):
    """Implementation for FullTextRead."""

    def __init__(
        self,
        table: 'FileStoreTable',
        limit: int,
        text_column: 'DataField',
        query_text: str
    ):
        self._table = table
        self._limit = limit
        self._text_column = text_column
        self._query_text = query_text

    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        if not splits:
            return GlobalIndexResult.create_empty()

        result = ScoredGlobalIndexResult.create_empty()
        for split in splits:
            split_result = self._eval(
                split.row_range_start, split.row_range_end,
                split.full_text_index_files
            )
            if split_result is not None:
                result = result.or_(split_result)

        return result.top_k(self._limit)

    def _eval(self, row_range_start, row_range_end, full_text_index_files
              ) -> Optional[ScoredGlobalIndexResult]:
        index_io_meta_list = []
        for index_file in full_text_index_files:
            meta = index_file.global_index_meta
            assert meta is not None
            index_io_meta_list.append(
                GlobalIndexIOMeta(
                    file_name=index_file.file_name,
                    file_size=index_file.file_size,
                    metadata=meta.index_meta
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
            query_text=self._query_text,
            limit=self._limit,
            field_name=self._text_column.name
        )

        try:
            offset_reader = OffsetGlobalIndexReader(reader, row_range_start, row_range_end)
            return offset_reader.visit_full_text_search(full_text_search)
        finally:
            reader.close()


def _create_full_text_reader(index_type, file_io, index_path, index_io_meta_list):
    """Create a global index reader for full-text search."""
    from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
        TANTIVY_FULLTEXT_IDENTIFIER,
    )
    if index_type == TANTIVY_FULLTEXT_IDENTIFIER:
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )
        return TantivyFullTextGlobalIndexReader(file_io, index_path, index_io_meta_list)
    raise ValueError(f"Unsupported full-text index type: '{index_type}'")
