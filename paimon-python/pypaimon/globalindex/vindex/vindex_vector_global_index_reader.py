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

"""Vector global index reader using paimon-vindex."""

import os
import threading

import numpy as np

from pypaimon.common.file_io import pread, supports_pread
from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult

VINDEX_IDENTIFIERS = ("ivf-flat", "ivf-pq", "ivf-hnsw-flat", "ivf-hnsw-sq")

NPROBE_PARAMETER = "ivf.nprobe"
EF_SEARCH_PARAMETER = "hnsw.ef_search"
DEFAULT_NPROBE = 16
DEFAULT_EF_SEARCH = 0


class PaimonVindexInput:
    """Input adapter required by paimon_vindex.VectorIndexReader."""

    def __init__(self, stream):
        self._stream = stream
        self._supports_pread = supports_pread(stream)
        self._lock = threading.Lock()

    def pread_many(self, ranges):
        if self._supports_pread:
            return [pread(self._stream, length, offset) for offset, length in ranges]

        chunks = []
        with self._lock:
            for offset, length in ranges:
                self._stream.seek(offset)
                chunks.append(self._stream.read(length))
        return chunks


class VindexVectorGlobalIndexReader(GlobalIndexReader):
    """Vector global index reader using paimon-vindex."""

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
        self._load_lock = threading.Lock()

    def visit_vector_search(self, vector_search):
        self._ensure_loaded()

        query = np.asarray(vector_search.vector, dtype=np.float32)
        if query.ndim != 1:
            raise ValueError("Query vector must be a one-dimensional float32 array")
        expected_dim = self._metadata.dimension
        if query.shape[0] != expected_dim:
            raise ValueError(
                "Query vector dimension mismatch: expected %d, got %d"
                % (expected_dim, query.shape[0]))

        effective_k = self._effective_k(vector_search)
        if effective_k <= 0:
            return _completed_future(None)

        options = vector_search.options or {}
        nprobe = _int_parameter(options, NPROBE_PARAMETER, DEFAULT_NPROBE)
        ef_search = _int_parameter(options, EF_SEARCH_PARAMETER, DEFAULT_EF_SEARCH)
        filter_bytes = _filter_bytes(vector_search.include_row_ids)

        ids, distances = self._reader.search(
            query, effective_k, nprobe, ef_search, filter_bytes=filter_bytes)
        id_to_scores = _build_scores(ids, distances, self._metadata.metric)
        if not id_to_scores:
            return _completed_future(None)
        return _completed_future(DictBasedScoredIndexResult(id_to_scores))

    def vector_metric(self):
        self._ensure_loaded()
        return self._metadata.metric

    def _effective_k(self, vector_search):
        limit = vector_search.limit
        total_vectors = getattr(self._metadata, "total_vectors", limit)
        effective_k = min(limit, int(total_vectors))
        include_row_ids = vector_search.include_row_ids
        if include_row_ids is not None:
            cardinality = include_row_ids.cardinality()
            if cardinality == 0:
                return 0
            effective_k = min(effective_k, cardinality)
        return effective_k

    def _ensure_loaded(self):
        if self._reader is not None:
            return

        with self._load_lock:
            if self._reader is not None:
                return

            try:
                from paimon_vindex import VectorIndexReader
            except ImportError as e:
                raise ImportError(
                    "paimon-vindex is required to read vindex vector indexes. "
                    "Install paimon-vindex==0.1.0 or pypaimon[vindex].") from e

            file_path = (self._io_meta.external_path
                         if self._io_meta.external_path
                         else os.path.join(self._index_path, self._io_meta.file_name))
            stream = self._file_io.new_input_stream(file_path)
            try:
                index_input = PaimonVindexInput(stream)
                reader = VectorIndexReader(index_input)
                self._metadata = reader.metadata()
                self._index_input = index_input
                self._reader = reader
                self._stream = stream
            except Exception:
                stream.close()
                raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None


def _filter_bytes(include_row_ids):
    if include_row_ids is None:
        return None
    if include_row_ids.cardinality() == 0:
        return None
    return include_row_ids.serialize()


def _build_scores(ids, distances, metric):
    id_to_scores = {}
    for row_id, distance in zip(ids, distances):
        row_id = int(row_id)
        if row_id < 0:
            continue
        id_to_scores[row_id] = _convert_distance_to_score(float(distance), metric)
    return id_to_scores


def _convert_distance_to_score(distance, metric):
    if metric == "l2":
        return 1.0 / (1.0 + distance)
    if metric == "cosine":
        return 1.0 - distance
    if metric == "inner_product":
        return -distance
    raise ValueError("Unknown vector search metric: %s" % metric)


def _int_parameter(options, key, default_value):
    value = options.get(key)
    if value is None:
        return default_value
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(
            "Invalid value for '%s': %s. Must be an integer." % (key, value)) from e
