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

"""Vector index queries using the paimon-vindex Python SDK."""

import numpy as np

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _completed_future
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.execution.vindex_input import VindexReaderResources, _filter_bytes
from pypaimon.globalindex.vindex_options import NPROBE_PARAMETER, L_SEARCH_PARAMETER


class PythonVindexVectorGlobalIndexReader(VindexReaderResources, GlobalIndexReader):
    """Python SDK implementation selected only when Native is unavailable."""

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

        queries = self._validate_queries(vectors)
        n = len(queries)
        effective_k = self._effective_k(limit, include_row_ids)
        if effective_k <= 0:
            return [None] * n

        options = query_options or {}
        params = _search_params(self._search_params_type, options, effective_k)
        filter_bytes = _filter_bytes(include_row_ids)

        if n == 1:
            ids, distances = self._reader.search(
                queries[0], params, filter_bytes=filter_bytes)
            return [_result_from_scores(ids, distances, self._metadata.metric)]

        if hasattr(self._reader, "search_batch"):
            batch_result = self._reader.search_batch(
                np.ascontiguousarray(queries),
                params,
                filter_bytes=filter_bytes,
            )
            return _batch_results(batch_result, n, effective_k, self._metadata.metric)

        results = []
        for query in queries:
            ids, distances = self._reader.search(
                query, params, filter_bytes=filter_bytes)
            results.append(_result_from_scores(ids, distances, self._metadata.metric))
        return results

    def _validate_queries(self, vectors):
        queries = []
        expected_dim = self._metadata.dimension
        for vector in vectors:
            query = np.asarray(vector, dtype=np.float32)
            if query.ndim != 1:
                raise ValueError("Query vector must be a one-dimensional float32 array")
            if query.shape[0] != expected_dim:
                raise ValueError(
                    "Query vector dimension mismatch: expected %d, got %d"
                    % (expected_dim, query.shape[0]))
            queries.append(query)
        return np.asarray(queries, dtype=np.float32)

    def vector_metric(self):
        self._ensure_loaded()
        return self._metadata.metric

    def _effective_k(self, limit, include_row_ids):
        total_vectors = getattr(self._metadata, "total_vectors", limit)
        effective_k = min(limit, int(total_vectors))
        if include_row_ids is not None:
            cardinality = include_row_ids.cardinality()
            if cardinality == 0:
                return 0
            effective_k = min(effective_k, cardinality)
        return effective_k

    def _create_reader(self, index_input, file_path):
        try:
            from paimon_vindex import SearchParams, VectorIndexReader
        except ImportError as error:
            raise ImportError(
                "paimon-vindex is required to read vindex vector indexes. "
                "Install paimon-vindex==0.5.0 or pypaimon[vindex].") from error
        self._search_params_type = SearchParams
        return VectorIndexReader(index_input)

    def _initialize_reader(self, reader):
        self._metadata = reader.metadata()
        reader.optimize_for_search()


def _build_scores(ids, distances, metric):
    id_to_scores = {}
    for row_id, distance in zip(ids, distances):
        row_id = int(row_id)
        if row_id < 0:
            continue
        id_to_scores[row_id] = _convert_distance_to_score(float(distance), metric)
    return id_to_scores


def _result_from_scores(ids, distances, metric):
    id_to_scores = _build_scores(ids, distances, metric)
    if not id_to_scores:
        return None
    return DictBasedScoredIndexResult(id_to_scores)


def _batch_results(batch_result, vector_count, effective_k, metric):
    if hasattr(batch_result, "ids_for_query"):
        return [
            _result_from_scores(
                batch_result.ids_for_query(i),
                batch_result.distances_for_query(i),
                metric,
            )
            for i in range(vector_count)
        ]

    ids, distances = batch_result
    return [
        _result_from_scores(
            _values_for_query(ids, i, effective_k),
            _values_for_query(distances, i, effective_k),
            metric,
        )
        for i in range(vector_count)
    ]


def _values_for_query(values, query_index, effective_k):
    array = np.asarray(values)
    if array.ndim >= 2:
        return array[query_index]
    start = query_index * effective_k
    return array[start:start + effective_k]


def _convert_distance_to_score(distance, metric):
    if metric == "l2":
        return 1.0 / (1.0 + distance)
    if metric == "cosine":
        return 1.0 - distance
    if metric == "inner_product":
        return -distance
    raise ValueError("Unknown vector search metric: %s" % metric)


def _search_params(search_params_type, options, top_k):
    nprobe = _int_parameter(options, NPROBE_PARAMETER)
    l_search = _int_parameter(options, L_SEARCH_PARAMETER)
    if nprobe is not None and l_search is not None:
        raise ValueError(
            "Cannot set both '%s' and '%s'."
            % (NPROBE_PARAMETER, L_SEARCH_PARAMETER)
        )
    if nprobe is not None:
        return search_params_type.ivf(top_k=top_k, nprobe=nprobe)
    if l_search is not None:
        return search_params_type.diskann(top_k=top_k, l_search=l_search)
    return search_params_type.automatic(top_k=top_k)


def _int_parameter(options, key):
    value = options.get(key)
    if value is None:
        return None
    try:
        text = str(value)
        digits = text[1:] if text[:1] in ('+', '-') else text
        if not digits or any(ord(char) > 0xffff or not char.isdecimal() for char in digits):
            raise ValueError('Invalid Java integer text')
        number = int(text)
        if not -(1 << 31) <= number < (1 << 31):
            raise ValueError('Java integer overflow')
        return number
    except (TypeError, ValueError) as e:
        raise ValueError(
            "Invalid value for '%s': %s. Must be an integer." % (key, value)) from e
