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

"""Select index executors before opening inputs or building files and messages."""

import threading


class _VindexReaderExecution:
    """Load one low-level query backend without coupling the two readers."""

    def __init__(self, request):
        self._request = request
        self._reader = None
        self._load_lock = threading.RLock()
        self._generation = 0

    def ensure_loaded(self):
        if self._reader is not None:
            return
        with self._load_lock:
            if self._reader is not None:
                return
            request = self._request
            generation = self._generation
            from pypaimon.execution.native.read.vindex_reader import create_native_vindex_reader
            reader = create_native_vindex_reader(
                request._file_io, request._index_path, [request._io_meta], request._options)
            if reader is None:
                from pypaimon.execution.python.globalindex.vindex.vindex_vector_global_index_reader import (
                    PythonVindexVectorGlobalIndexReader,
                )
                reader = PythonVindexVectorGlobalIndexReader(
                    request._file_io, request._index_path, [request._io_meta], request._options)
            # Backend errors propagate; opening/querying is never replayed by another engine.
            # FileIO initialization can reenter the public reader's close().
            # Its candidate must then be released instead of being published.
            if generation == self._generation:
                reader._ensure_loaded()
            if generation != self._generation:
                cancelled = RuntimeError('Vector index reader closed during initialization')
                try:
                    reader.close()
                except BaseException as error:
                    raise cancelled from error
                raise cancelled
            self._reader = reader

    def _run_search(self, vectors, limit, include_row_ids, query_options):
        return self._reader._run_search(vectors, limit, include_row_ids, query_options)

    def vector_metric(self):
        return self._reader.vector_metric()

    @property
    def reader(self):
        return None if self._reader is None else self._reader._reader

    @property
    def index_input(self):
        return None if self._reader is None else self._reader._index_input

    def close(self):
        with self._load_lock:
            self._generation += 1
            reader, self._reader = self._reader, None
        if reader is not None:
            reader.close()


def build_global_index(request, partition_filter, *, execution="local", concurrency=None, ray_remote_args=None):
    if execution == "local" and request._table.options.native_write_enabled():
        from pypaimon.execution.native.write.native_index_build import build_native_global_index
        messages = build_native_global_index(request, partition_filter)
        if messages is not None:
            return messages
    from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
    return PythonGlobalIndexBuilder(request).build(
        partition_filter, execution=execution, concurrency=concurrency,
        ray_remote_args=ray_remote_args)


def build_global_index_drop(request, partition_filter):
    if request._table.options.native_write_enabled():
        from pypaimon.execution.native.write.native_index_drop import build_native_global_index_drop
        messages = build_native_global_index_drop(request, partition_filter)
        if messages is not None:
            return messages
    from pypaimon.execution.python.globalindex.drop_global_index import build_global_index_drop as execute
    return execute(request, partition_filter)
