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

import threading

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future


class VindexVectorGlobalIndexReader(GlobalIndexReader):
    """Query a vindex file through Native when available or the Python SDK."""

    def __init__(self, file_io, index_path, io_metas, options=None):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._options = dict(options or {})
        self._execution = None
        self._load_lock = threading.Lock()

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
        if self._execution is not None:
            return
        with self._load_lock:
            if self._execution is not None:
                return
            from pypaimon.execution.native.read.vindex_reader import create_native_vindex_reader
            execution = create_native_vindex_reader(
                self._file_io, self._index_path, [self._io_meta], self._options)
            if execution is None:
                from pypaimon.execution.python.globalindex.vindex.vindex_vector_global_index_reader import (
                    PythonVindexVectorGlobalIndexReader,
                )
                execution = PythonVindexVectorGlobalIndexReader(
                    self._file_io, self._index_path, [self._io_meta], self._options)
            # Native opening/query errors propagate; they cannot select another engine.
            execution._ensure_loaded()
            self._execution = execution

    def _run_search(self, vectors, limit, include_row_ids, query_options):
        self._ensure_loaded()
        return self._execution._run_search(vectors, limit, include_row_ids, query_options)

    def vector_metric(self):
        self._ensure_loaded()
        return self._execution.vector_metric()

    @property
    def _reader(self):
        return None if self._execution is None else self._execution._reader

    @property
    def _index_input(self):
        return None if self._execution is None else self._execution._index_input

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        execution, self._execution = self._execution, None
        if execution is not None:
            execution.close()
