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
Uses stream-based open (no temp file), mirroring the Java implementation.
"""

import os

from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult

LUMINA_VECTOR_ANN_IDENTIFIER = "lumina-vector-ann"

MIN_SEARCH_LIST_SIZE = 16


def _ensure_search_list_size(search_options, top_k):
    """Set diskann.search.list_size when not explicitly configured.

    Mirrors Java LuminaVectorGlobalIndexReader.ensureSearchListSize().
    """
    if "diskann.search.list_size" not in search_options:
        list_size = max(int(top_k * 1.5), MIN_SEARCH_LIST_SIZE)
        search_options["diskann.search.list_size"] = str(list_size)


class LuminaVectorGlobalIndexReader(GlobalIndexReader):
    """Vector global index reader using Lumina.

    Reads the Lumina index file via lumina-data SDK's LuminaSearcher and
    performs approximate nearest neighbor search.
    """

    def __init__(self, file_io, index_path, io_metas, options=None):
        # type: (object, str, List[GlobalIndexIOMeta], Optional[Dict[str, str]]) -> None
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._options = options or {}
        self._searcher = None
        self._index_meta = None
        self._stream = None

    def visit_vector_search(self, vector_search):
        # type: (...) -> Optional[ScoredGlobalIndexResult]
        self._ensure_loaded()

        import ctypes
        from lumina_data import MetricType

        query = vector_search.vector
        # Convert to ctypes array once
        if hasattr(query, 'ctypes'):
            query_arr = ctypes.cast(query.ctypes.data,
                                    ctypes.POINTER(ctypes.c_float))
        elif isinstance(query, ctypes.Array):
            query_arr = query
        else:
            if hasattr(query, 'tolist'):
                query = query.tolist()
            if not isinstance(query, list):
                query = list(query)
            query_arr = (ctypes.c_float * len(query))(*query)

        limit = vector_search.limit
        index_metric = self._index_meta.metric

        count = self._searcher.get_count()
        effective_k = min(limit, count)
        if effective_k <= 0:
            return None

        include_row_ids = vector_search.include_row_ids

        if include_row_ids is not None:
            filter_ids_list = list(include_row_ids)
            if len(filter_ids_list) == 0:
                return None
            effective_k = min(effective_k, len(filter_ids_list))
            dist_buf = (ctypes.c_float * effective_k)()
            label_buf = (ctypes.c_uint64 * effective_k)()
            # Java: searchOptions = options.toLuminaOptions(); searchOptions.putAll(indexMeta.options())
            search_opts = self._paimon_opts.to_lumina_options()
            search_opts.update(self._index_meta.options)
            search_opts["search.thread_safe_filter"] = "true"
            _ensure_search_list_size(search_opts, effective_k)
            fid_arr = (ctypes.c_uint64 * len(filter_ids_list))(*filter_ids_list)
            self._searcher.search_with_filter(
                query_arr, 1, effective_k,
                fid_arr, len(filter_ids_list),
                dist_buf, label_buf, search_opts)
        else:
            dist_buf = (ctypes.c_float * effective_k)()
            label_buf = (ctypes.c_uint64 * effective_k)()
            # Java: searchOptions = options.toLuminaOptions(); searchOptions.putAll(indexMeta.options())
            search_opts = self._paimon_opts.to_lumina_options()
            search_opts.update(self._index_meta.options)
            _ensure_search_list_size(search_opts, effective_k)
            self._searcher.search(
                query_arr, 1, effective_k,
                dist_buf, label_buf, search_opts)

        # Collect results with score conversion (same as Java collectResults)
        id_to_scores = {}
        for i in range(effective_k):
            row_id = label_buf[i]
            if row_id == 0xFFFFFFFFFFFFFFFF:
                continue
            score = MetricType.convert_distance_to_score(dist_buf[i], index_metric)
            id_to_scores[int(row_id)] = score

        return DictBasedScoredIndexResult(id_to_scores)

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        from lumina_data import LuminaSearcher
        from pypaimon.globalindex.lumina.lumina_index_meta import LuminaIndexMeta
        from pypaimon.globalindex.lumina.lumina_vector_index_options import (
            LuminaVectorIndexOptions,
        )

        # Deserialize index metadata
        self._index_meta = LuminaIndexMeta.deserialize(self._io_meta.metadata)

        # Java: searcherOptions = options.toLuminaOptions();
        #        searcherOptions.putAll(indexMeta.options());
        self._paimon_opts = LuminaVectorIndexOptions(self._options)
        searcher_options = self._paimon_opts.to_lumina_options()
        searcher_options.update(self._index_meta.options)

        file_path = os.path.join(self._index_path, self._io_meta.file_name)
        stream = self._file_io.new_input_stream(file_path)
        try:
            self._searcher = LuminaSearcher(searcher_options)
            self._searcher.open_stream(stream, self._io_meta.file_size)
            self._stream = stream
        except Exception:
            stream.close()
            raise

    # =================== unsupported =====================

    def visit_equal(self, field_ref, literal):
        return None

    def visit_not_equal(self, field_ref, literal):
        return None

    def visit_less_than(self, field_ref, literal):
        return None

    def visit_less_or_equal(self, field_ref, literal):
        return None

    def visit_greater_than(self, field_ref, literal):
        return None

    def visit_greater_or_equal(self, field_ref, literal):
        return None

    def visit_is_null(self, field_ref):
        return None

    def visit_is_not_null(self, field_ref):
        return None

    def visit_in(self, field_ref, literals):
        return None

    def visit_not_in(self, field_ref, literals):
        return None

    def visit_starts_with(self, field_ref, literal):
        return None

    def visit_ends_with(self, field_ref, literal):
        return None

    def visit_contains(self, field_ref, literal):
        return None

    def visit_like(self, field_ref, literal):
        return None

    def visit_between(self, field_ref, min_v, max_v):
        return None

    def close(self):
        if self._searcher is not None:
            self._searcher.close()
            self._searcher = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None
