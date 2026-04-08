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

"""Vector search read to read index files."""

from abc import ABC, abstractmethod

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult


class VectorSearchRead(ABC):
    """Vector search read to read index files."""

    def read_plan(self, plan):
        # type: (VectorSearchScanPlan) -> GlobalIndexResult
        return self.read(plan.splits())

    @abstractmethod
    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        pass


class VectorSearchReadImpl(VectorSearchRead):
    """Implementation for VectorSearchRead."""

    def __init__(self, table, limit, vector_column, query_vector):
        self._table = table
        self._limit = limit
        self._vector_column = vector_column
        self._query_vector = query_vector

    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        if not splits:
            return GlobalIndexResult.create_empty()

        result = ScoredGlobalIndexResult.create_empty()
        for split in splits:
            split_result = self._eval(
                split.row_range_start, split.row_range_end,
                split.vector_index_files
            )
            if split_result is not None:
                result = result.or_(split_result)

        return result.top_k(self._limit)

    def _eval(self, row_range_start, row_range_end, vector_index_files):
        # type: (int, int, list) -> Optional[ScoredGlobalIndexResult]
        if not vector_index_files:
            return None
        index_io_meta_list = []
        for index_file in vector_index_files:
            meta = index_file.global_index_meta
            assert meta is not None
            index_io_meta_list.append(
                GlobalIndexIOMeta(
                    file_name=index_file.file_name,
                    file_size=index_file.file_size,
                    metadata=meta.index_meta
                )
            )

        index_type = vector_index_files[0].index_type
        index_path = self._table.path_factory().global_index_path_factory().index_path()
        file_io = self._table.file_io
        options = self._table.table_schema.options

        vector_search = VectorSearch(
            vector=self._query_vector,
            limit=self._limit,
            field_name=self._vector_column.name
        )
        with _create_vector_reader(
            index_type, file_io, index_path,
            index_io_meta_list, options
        ) as reader:
            offset_reader = OffsetGlobalIndexReader(reader, row_range_start, row_range_end)
            return offset_reader.visit_vector_search(vector_search)


def _create_vector_reader(index_type, file_io, index_path, index_io_meta_list, options=None):
    """Create a global index reader for vector search."""
    from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
        LUMINA_VECTOR_ANN_IDENTIFIER,
        LuminaVectorGlobalIndexReader,
    )
    if index_type == LUMINA_VECTOR_ANN_IDENTIFIER:
        return LuminaVectorGlobalIndexReader(
            file_io, index_path, index_io_meta_list, options
        )
    raise ValueError("Unsupported vector index type: '%s'" % index_type)
