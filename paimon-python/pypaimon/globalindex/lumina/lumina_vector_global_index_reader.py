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

"""Vector global index reader using Lumina (via lumina-data SDK).

Each shard has exactly one Lumina index file. This reader lazy-loads the
index and performs vector similarity search using the lumina-data SDK.
"""

import os
import threading

import numpy as np

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult

LUMINA_IDENTIFIER = "lumina"
LUMINA_VECTOR_ANN_IDENTIFIER = "lumina-vector-ann"
LUMINA_IDENTIFIERS = (LUMINA_IDENTIFIER, LUMINA_VECTOR_ANN_IDENTIFIER)

MIN_SEARCH_LIST_SIZE = 16


def _ensure_search_list_size(options, top_k):
    """Set diskann.search.list_size when not explicitly configured."""
    if "diskann.search.list_size" not in options:
        list_size = max(int(top_k * 1.5), MIN_SEARCH_LIST_SIZE)
        options["diskann.search.list_size"] = str(list_size)


def _merge_options(base_options, index_options, query_options):
    options = dict(base_options)
    options.update(index_options)
    options.update(query_options or {})
    return options


def _collect_scored_result(distances, labels, base, k, index_metric):
    """Convert one query's [base, base+k) slice of distances/labels into a result."""
    from lumina_data import MetricType

    SENTINEL = 0xFFFFFFFFFFFFFFFF
    id_to_scores = {}
    for i in range(k):
        row_id = labels[base + i]
        if row_id == SENTINEL:
            continue
        id_to_scores[int(row_id)] = MetricType.convert_distance_to_score(
            float(distances[base + i]), index_metric)
    return DictBasedScoredIndexResult(id_to_scores)


class LuminaVectorGlobalIndexReader(GlobalIndexReader):
    """Vector global index reader using Lumina."""

    def __init__(self, file_io, index_path, io_metas, options=None):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._table_options = dict(options or {})
        self._options = {}
        self._searcher = None
        self._index_meta = None
        self._stream = None
        self._load_lock = threading.Lock()

    def visit_vector_search(self, vector_search):
        # Single-vector search is just the n == 1 case of the batch path.
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
        """Run one native batch search; result ``i`` maps to ``vectors[i]`` (``None`` if
        no hits). Single search is the n == 1 case, shared by both visit paths.
        """
        self._ensure_loaded()

        n = len(vectors)
        expected_dim = self._index_meta.dim
        query_flat = []
        for vector in vectors:
            flat = [float(v) for v in np.asarray(vector).tolist()]
            if len(flat) != expected_dim:
                raise ValueError(
                    "Query vector dimension mismatch: expected %d, got %d"
                    % (expected_dim, len(flat)))
            query_flat.extend(flat)

        index_metric = self._index_meta.metric
        count = self._searcher.get_count()
        effective_k = min(limit, count)
        if effective_k <= 0:
            return [None] * n

        if include_row_ids is not None:
            filter_id_list = list(include_row_ids)
            if len(filter_id_list) == 0:
                return [None] * n
            effective_k = min(effective_k, len(filter_id_list))
            search_opts = _merge_options(self._options, {}, query_options)
            search_opts["search.thread_safe_filter"] = "true"
            _ensure_search_list_size(search_opts, effective_k)
            distances, labels = self._searcher.search_with_filter_list(
                query_flat, n, effective_k, filter_id_list, search_opts)
        else:
            search_opts = _merge_options(self._options, {}, query_options)
            _ensure_search_list_size(search_opts, effective_k)
            distances, labels = self._searcher.search_list(
                query_flat, n, effective_k, search_opts)

        # Each query's results occupy a contiguous [q * k, q * k + k) slice.
        return [
            _collect_scored_result(
                distances, labels, q * effective_k, effective_k, index_metric)
            for q in range(n)
        ]

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        with self._load_lock:
            if self._searcher is not None:
                return

            from lumina_data import LuminaSearcher
            from pypaimon.globalindex.lumina.lumina_index_meta import LuminaIndexMeta
            from pypaimon.globalindex.lumina.lumina_vector_index_options import (
                strip_lumina_options,
            )

            self._index_meta = LuminaIndexMeta.deserialize(self._io_meta.metadata)
            self._options = _merge_options(
                strip_lumina_options(self._table_options),
                self._index_meta.options,
                {},
            )

            file_path = (self._io_meta.external_path
                         if self._io_meta.external_path
                         else os.path.join(self._index_path, self._io_meta.file_name))
            stream = self._file_io.new_input_stream(file_path)
            try:
                self._searcher = LuminaSearcher(self._options)
                self._searcher.open_stream(stream, self._io_meta.file_size)
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
        if self._searcher is not None:
            self._searcher.close()
            self._searcher = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None
