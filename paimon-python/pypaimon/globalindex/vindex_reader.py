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

"""Public vector index reader with lazy execution backend selection."""

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future


class VindexVectorGlobalIndexReader(GlobalIndexReader):
    """Query a vindex file through Native when available or the Python SDK."""

    def __init__(self, file_io, index_path, io_metas, options=None):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._options = dict(options or {})
        from pypaimon.execution.index_dispatch import _VindexReaderExecution
        self._execution = _VindexReaderExecution(self)

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

    def _ensure_loaded(self):
        self._execution.ensure_loaded()

    def _run_search(self, vectors, limit, include_row_ids, query_options):
        self._ensure_loaded()
        return self._execution._run_search(vectors, limit, include_row_ids, query_options)

    def vector_metric(self):
        self._ensure_loaded()
        return self._execution.vector_metric()

    @property
    def _reader(self):
        return self._execution.reader

    @property
    def _index_input(self):
        return self._execution.index_input

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        self._execution.close()
