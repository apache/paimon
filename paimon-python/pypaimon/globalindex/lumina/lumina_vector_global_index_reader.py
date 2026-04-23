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

"""Vector global index reader using Lumina (via lumina-data SDK).

Each shard has exactly one Lumina index file. This reader lazy-loads the
index and performs vector similarity search using the lumina-data SDK.
"""

import os
import numpy as np

from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult

LUMINA_VECTOR_ANN_IDENTIFIER = "lumina-vector-ann"

MIN_SEARCH_LIST_SIZE = 16


def _ensure_search_list_size(search_options, top_k):
    """Set diskann.search.list_size when not explicitly configured."""
    if "diskann.search.list_size" not in search_options:
        list_size = max(int(top_k * 1.5), MIN_SEARCH_LIST_SIZE)
        search_options["diskann.search.list_size"] = str(list_size)


class LuminaVectorGlobalIndexReader(GlobalIndexReader):
    """Vector global index reader using Lumina."""

    def __init__(self, file_io, index_path, io_metas, options=None):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._options = options or {}
        self._searcher = None
        self._index_meta = None
        self._search_options = None
        self._stream = None

    def visit_vector_search(self, vector_search):
        self._ensure_loaded()

        from lumina_data import MetricType
        query_flat = [float(v) for v in np.asarray(vector_search.vector).tolist()]
        expected_dim = self._index_meta.dim
        if len(query_flat) != expected_dim:
            raise ValueError(
                "Query vector dimension mismatch: expected %d, got %d"
                % (expected_dim, len(query_flat)))

        limit = vector_search.limit
        index_metric = self._index_meta.metric

        count = self._searcher.get_count()
        effective_k = min(limit, count)
        if effective_k <= 0:
            return None

        include_row_ids = vector_search.include_row_ids

        if include_row_ids is not None:
            filter_id_list = list(include_row_ids)
            if len(filter_id_list) == 0:
                return None
            effective_k = min(effective_k, len(filter_id_list))
            search_opts = dict(self._search_options)
            search_opts["search.thread_safe_filter"] = "true"
            _ensure_search_list_size(search_opts, effective_k)
            distances, labels = self._searcher.search_with_filter_list(
                query_flat, 1, effective_k, filter_id_list, search_opts)
        else:
            search_opts = dict(self._search_options)
            _ensure_search_list_size(search_opts, effective_k)
            distances, labels = self._searcher.search_list(
                query_flat, 1, effective_k, search_opts)

        # Collect results with score conversion (same as Java collectResults)
        SENTINEL = 0xFFFFFFFFFFFFFFFF
        id_to_scores = {}
        for i in range(effective_k):
            row_id = labels[i]
            if row_id == SENTINEL:
                continue
            score = MetricType.convert_distance_to_score(
                float(distances[i]), index_metric)
            id_to_scores[int(row_id)] = score

        return DictBasedScoredIndexResult(id_to_scores)

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        from lumina_data import LuminaSearcher
        from pypaimon.globalindex.lumina.lumina_index_meta import LuminaIndexMeta
        from pypaimon.globalindex.lumina.lumina_vector_index_options import (
            strip_lumina_options,
        )

        self._index_meta = LuminaIndexMeta.deserialize(self._io_meta.metadata)
        # Merge paimon table options (prefix-stripped) with index metadata options;
        # index metadata takes precedence as it reflects the actual built index.
        searcher_options = strip_lumina_options(self._options)
        searcher_options.update(self._index_meta.options)
        self._search_options = searcher_options

        file_path = (self._io_meta.external_path
                     if self._io_meta.external_path
                     else os.path.join(self._index_path, self._io_meta.file_name))
        stream = self._file_io.new_input_stream(file_path)
        try:
            self._searcher = LuminaSearcher(searcher_options)
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
