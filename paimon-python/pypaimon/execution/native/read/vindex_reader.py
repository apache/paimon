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

"""Vector index queries delegated to the Rust reader."""

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.vindex_input import VindexReaderResources, _filter_bytes


def _native_reader_type():
    from pypaimon.execution.native.runtime import native_runtime_available
    if not native_runtime_available():
        return None
    from pypaimon_rust.datafusion import VindexVectorGlobalIndexReader
    return VindexVectorGlobalIndexReader


def create_native_vindex_reader(file_io, index_path, io_metas, options=None):
    reader_type = _native_reader_type()
    if reader_type is None:
        return None
    return NativeVindexVectorGlobalIndexReader(file_io, index_path, io_metas, options, reader_type)


class NativeVindexVectorGlobalIndexReader(VindexReaderResources, GlobalIndexReader):
    """Rust owns vector validation, filtering, query options and scoring."""

    def __init__(self, file_io, index_path, io_metas, options, reader_type):
        super().__init__(file_io, index_path, io_metas, options)
        self._reader_type = reader_type

    def _create_reader(self, index_input, file_path):
        return self._reader_type(
            index_input, file_path, self._io_meta.file_size,
            {str(key): str(value) for key, value in self._options.items()})

    def visit_vector_search(self, vector_search):
        results = self._run_search(
            [vector_search.vector],
            vector_search.limit,
            vector_search.include_row_ids,
            vector_search.options,
        )
        return _completed_future(results[0])

    def visit_batch_vector_search(self, batch_vector_search):
        results = self._run_search(
            batch_vector_search.vectors,
            batch_vector_search.limit,
            batch_vector_search.include_row_ids,
            batch_vector_search.options,
        )
        return _completed_future(results)

    def _run_search(self, vectors, limit, include_row_ids, query_options):
        self._ensure_loaded()
        results = self._reader.search(
            vectors, limit, filter_bytes=_filter_bytes(include_row_ids),
            options={str(key): str(value) for key, value in (query_options or {}).items()})
        return [DictBasedScoredIndexResult(scores) if scores else None for scores in results]

    def vector_metric(self):
        self._ensure_loaded()
        return self._reader.metric()
