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

"""Positional input callbacks and reader input ownership for vector indexes."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from urllib.parse import urlparse

from pypaimon.common.file_io import pread, supports_pread
from pypaimon.globalindex.vindex_options import READ_PARALLELISM_PARAMETER


class PaimonVindexInput:
    """Input adapter required by paimon_vindex.VectorIndexReader."""

    def __init__(self, stream, parallelism=1):
        if isinstance(parallelism, bool) or not isinstance(parallelism, int) or parallelism < 1:
            raise ValueError("Vector index read parallelism must be a positive integer")
        self._stream = stream
        self._parallelism = parallelism
        self._read_slots = threading.BoundedSemaphore(parallelism)
        self._supports_pread = supports_pread(stream)
        self._lock = threading.Lock()
        self._executor = None
        self._closed = False

    def pread_many(self, ranges):
        ranges = list(ranges)
        with self._lock:
            if self._closed:
                raise ValueError("Vector index input is closed")
            if not self._supports_pread:
                chunks = []
                for offset, length in ranges:
                    self._stream.seek(offset)
                    chunks.append(self._stream.read(length))
                return chunks
            if self._parallelism > 1 and len(ranges) > 1:
                if self._executor is None:
                    self._executor = ThreadPoolExecutor(
                        max_workers=self._parallelism,
                        thread_name_prefix="paimon-vindex-io")
            executor = self._executor

        if self._parallelism == 1:
            with self._read_slots:
                return [pread(self._stream, length, offset) for offset, length in ranges]
        if executor is None or len(ranges) <= 1:
            return [self._pread(offset, length) for offset, length in ranges]

        futures = []
        try:
            for offset, length in ranges:
                futures.append(executor.submit(self._pread, offset, length))
            return [future.result() for future in futures]
        finally:
            # A failed range must not leave reads using a stream the caller may close.
            wait(futures)

    def _pread(self, offset, length):
        # Native query workers may also issue single-range callbacks concurrently.
        with self._read_slots:
            return pread(self._stream, length, offset)

    def close(self):
        """Release workers; the owner remains responsible for closing the stream."""
        with self._lock:
            self._closed = True
            executor = self._executor
            self._executor = None
        if executor is not None:
            executor.shutdown(wait=True)


class VindexReaderResources:
    """Manage an index stream and its callbacks without choosing a query engine."""

    def __init__(self, file_io, index_path, io_metas, options=None):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._options = dict(options or {})
        self._stream = None
        self._index_input = None
        self._reader = None
        self._metadata = None
        self._search_params_type = None
        self._load_lock = threading.Lock()

    def _ensure_loaded(self):
        if self._reader is not None:
            return

        with self._load_lock:
            if self._reader is not None:
                return

            file_path = (self._io_meta.external_path
                         if self._io_meta.external_path
                         else os.path.join(self._index_path, self._io_meta.file_name))
            parallelism = _read_parallelism(self._options, file_path)
            stream = self._file_io.new_input_stream(file_path)
            reader = None
            index_input = None
            try:
                index_input = PaimonVindexInput(stream, parallelism)
                reader = self._create_reader(index_input, file_path)
                self._initialize_reader(reader)
                self._index_input = index_input
                self._reader = reader
                self._stream = stream
            except BaseException:
                try:
                    if reader is not None:
                        reader.close()
                finally:
                    try:
                        if index_input is not None:
                            index_input.close()
                    finally:
                        stream.close()
                raise

    def _create_reader(self, index_input, file_path):
        raise NotImplementedError

    def _initialize_reader(self, reader):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        try:
            if self._reader is not None:
                self._reader.close()
        finally:
            self._reader = None
            try:
                if self._index_input is not None:
                    self._index_input.close()
            finally:
                self._index_input = None
                if self._stream is not None:
                    self._stream.close()
                    self._stream = None


def _read_parallelism(options, file_path):
    value = options.get(READ_PARALLELISM_PARAMETER)
    if value is None:
        # Avoid thread scheduling overhead for local files, including Windows paths.
        scheme = urlparse(file_path).scheme
        return 1 if scheme in ("", "file") or len(scheme) == 1 else 4
    try:
        parallelism = int(str(value))
    except (ValueError, TypeError):
        parallelism = 0
    if parallelism < 1:
        raise ValueError("'%s' must be a positive integer, got: %s"
                         % (READ_PARALLELISM_PARAMETER, value))
    return parallelism


def _filter_bytes(include_row_ids):
    if include_row_ids is None:
        return None
    return include_row_ids.serialize()
