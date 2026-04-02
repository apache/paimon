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
The index file is downloaded to a local temp file for the native searcher.
"""

import os
import tempfile

from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult

LUMINA_VECTOR_ANN_IDENTIFIER = "lumina-vector-ann"


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
        self._tmp_file = None

    def visit_vector_search(self, vector_search):
        # type: (...) -> Optional[ScoredGlobalIndexResult]
        self._ensure_loaded()

        import ctypes
        from lumina_data import MetricType

        query = vector_search.vector
        # Convert to ctypes array once (avoid per-call conversion)
        if hasattr(query, 'ctypes'):
            # numpy array - get pointer directly
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
        metric = self._index_meta.metric

        count = self._searcher.get_count()
        effective_k = min(limit, count)
        if effective_k <= 0:
            return None

        # Auto-set diskann.search.list_size if not configured
        search_opts = {}
        if "diskann.search.list_size" not in self._index_meta.options:
            list_size = max(int(effective_k * 1.5), 16)
            search_opts["diskann.search.list_size"] = str(list_size)

        # Pre-allocate output buffers
        dist_buf = (ctypes.c_float * effective_k)()
        label_buf = (ctypes.c_uint64 * effective_k)()

        include_row_ids = vector_search.include_row_ids
        if include_row_ids is not None:
            filter_ids_list = list(include_row_ids)
            if len(filter_ids_list) == 0:
                return None
            effective_k = min(effective_k, len(filter_ids_list))
            dist_buf = (ctypes.c_float * effective_k)()
            label_buf = (ctypes.c_uint64 * effective_k)()
            fid_arr = (ctypes.c_uint64 * len(filter_ids_list))(*filter_ids_list)
            self._searcher.search_with_filter(
                query_arr, 1, effective_k,
                fid_arr, len(filter_ids_list),
                dist_buf, label_buf,
                search_opts if search_opts else None)
        else:
            self._searcher.search(
                query_arr, 1, effective_k,
                dist_buf, label_buf,
                search_opts if search_opts else None)

        # Build scored result directly from ctypes buffers (no list conversion)
        id_to_scores = {}
        for i in range(effective_k):
            row_id = label_buf[i]
            if row_id == 0xFFFFFFFFFFFFFFFF:
                continue
            score = MetricType.convert_distance_to_score(dist_buf[i], metric)
            id_to_scores[int(row_id)] = score

        return DictBasedScoredIndexResult(id_to_scores)

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        from lumina_data import LuminaSearcher, LuminaIndexMeta
        from lumina_data.options import LuminaIndexOptions

        # Deserialize index metadata
        self._index_meta = LuminaIndexMeta.deserialize(self._io_meta.metadata)

        # Download index file to a local temp file
        file_path = os.path.join(self._index_path, self._io_meta.file_name)
        stream = self._file_io.new_input_stream(file_path)
        try:
            tmp = tempfile.NamedTemporaryFile(suffix=".lmi", delete=False)
            remaining = self._io_meta.file_size
            while remaining > 0:
                chunk_size = min(remaining, 8 * 1024 * 1024)
                data = stream.read(chunk_size)
                if not data:
                    break
                tmp.write(data)
                remaining -= len(data)
            tmp.close()
            self._tmp_file = tmp.name
        finally:
            stream.close()

        # Build searcher options from index metadata + table options
        searcher_options = dict(self._index_meta.options)
        opts = LuminaIndexOptions.from_paimon_options(self._options)
        for k, v in opts.to_native_options().items():
            searcher_options.setdefault(k, v)

        self._searcher = LuminaSearcher(searcher_options)
        self._searcher.open(self._tmp_file)

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
        if self._tmp_file is not None:
            try:
                os.unlink(self._tmp_file)
            except OSError:
                pass
            self._tmp_file = None
