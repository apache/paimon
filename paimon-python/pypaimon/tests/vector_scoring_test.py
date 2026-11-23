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

import unittest
from pathlib import Path
from typing import Iterator, List, Optional, Tuple
from unittest import mock

import numpy as np
import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.execution.python.read.search.vector_scoring import _PreparedQuery, _compute_score, _compute_scores
from pypaimon.execution.python.read.search.vector_search_read import (
    DataEvolutionVectorRead,
    _iter_arrow_batch_scores,
    _iter_arrow_scores,
    _score_block_size,
    _score_rows,
)
from pypaimon.read.split import Split
from pypaimon.table.special_fields import SpecialFields
from pypaimon.tests.vector_search_filter_test import _StubTable, _field
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class VectorScoringTest(unittest.TestCase):

    def test_batch_rejects_unknown_metric_after_valid_query(self) -> None:
        vectors = pa.array([[1.0]], type=pa.list_(pa.float32(), 1))
        queries = [_PreparedQuery([1.0], metric) for metric in ("l2", "unknown")]
        with self.assertRaisesRegex(ValueError, "Unknown vector search metric: unknown"):
            list(_iter_arrow_batch_scores(vectors, queries))

    def test_scores_exactly_match_scalar_accumulation(self):
        rng = np.random.default_rng(42)
        for dimension in (1, 3, 128, 384):
            vectors = rng.standard_normal((37, dimension)).astype(np.float32)
            query = rng.standard_normal(dimension).tolist()
            for metric in ("l2", "cosine", "inner_product"):
                with self.subTest(dimension=dimension, metric=metric):
                    prepared = _PreparedQuery(query, metric)
                    expected = [_compute_score(prepared, row) for row in vectors.tolist()]
                    actual = _compute_scores(prepared, vectors.astype(np.float64))
                    self.assertEqual(expected, actual)
                    self.assertEqual(np.array(expected).tobytes(), np.array(actual).tobytes())

    def test_cancellation_zero_norms_and_close_scores(self):
        cases = [
            ([1e16, 1.0, -1e16], [[1, 1, 1], [1, 0, 1], [0, 0, 0]]),
            ([0, 0, 0], [[1, 2, 3], [0, 0, 0], [-0.0, -0.0, -0.0]]),
            ([1, 1e-8, 1e-8], [[1, 1e-8, 0], [1, 0, 1e-8], [1, 0, 0]]),
            ([1e150, 1e-150, -1e150], [[1e30, 1e-30, 1e30], [0, 1e-30, 0]]),
        ]
        for query, values in cases:
            vectors = np.array(values, dtype=np.float32)
            for metric in ("l2", "cosine", "inner_product"):
                prepared = _PreparedQuery(query, metric)
                expected = [_compute_score(prepared, row) for row in vectors.tolist()]
                self.assertEqual(expected, _compute_scores(prepared, vectors.astype(np.float64)))

    def test_arrow_slices_chunks_and_list_layouts(self):
        values = np.random.default_rng(7).standard_normal((1031, 7)).astype(np.float32).tolist()
        query = [1.0] * 7
        for dtype in (pa.list_(pa.float32()), pa.large_list(pa.float32()), pa.list_(pa.float32(), 7)):
            array = pa.array(values, type=dtype)
            sliced = array.slice(2, 1027)
            chunked = pa.chunked_array([sliced.slice(0, 511), sliced.slice(511)])
            for data in (sliced, chunked):
                for metric in ("l2", "cosine", "inner_product"):
                    prepared = _PreparedQuery(query, metric)
                    expected = [_compute_score(prepared, row) for row in values[2:1029]]
                    self.assertEqual(expected, list(_iter_arrow_scores(data, prepared)))

    def test_batch_arrow_scores_exactly_match_scalar_accumulation(self):
        rng = np.random.default_rng(17)
        values = rng.standard_normal((2051, 128)).astype(np.float32)
        vectors = pa.array(values.tolist(), type=pa.list_(pa.float32(), 128))
        queries = rng.standard_normal((5, 128)).astype(np.float32).tolist()
        for metric in ("l2", "cosine", "inner_product"):
            with self.subTest(metric=metric):
                prepared = [_PreparedQuery(q, metric) for q in queries]
                actual = [[] for unused in queries]
                for start, query_index, scores in _iter_arrow_batch_scores(vectors, prepared):
                    self.assertEqual(start, len(actual[query_index]))
                    actual[query_index].extend(scores)
                for query, scores in zip(prepared, actual):
                    expected = [
                        _compute_score(query, row)
                        for row in values.tolist()
                    ]
                    self.assertEqual(expected, scores)
                    self.assertEqual(
                        np.array(expected).tobytes(), np.array(scores).tobytes())

    def test_null_and_unsupported_data_preserve_scalar_behavior(self):
        query = _PreparedQuery([1, 2], "l2")
        for dtype in (pa.list_(pa.float32()), pa.list_(pa.float64()), pa.list_(pa.int64())):
            array = pa.array([[1, 2], None, [0, 0]], type=dtype)
            self.assertEqual([1.0, None, 1.0 / 6.0], list(_iter_arrow_scores(array, query)))
        scores = _iter_arrow_scores(pa.array([None, [1.0]], type=pa.list_(pa.float32())), query)
        self.assertIsNone(next(scores))
        with self.assertRaisesRegex(ValueError, "dimension mismatch"):
            next(scores)
        with self.assertRaisesRegex(ValueError, "dimension mismatch"):
            list(_iter_arrow_scores(pa.array([[1.0], [1.0, 2.0]]), query))
        with self.assertRaises(TypeError):
            list(_iter_arrow_scores(pa.array([[1.0, None]], type=pa.list_(pa.float32())), query))
        values = [[float("nan"), 0], [1, 0]]
        scores = list(_iter_arrow_scores(pa.array(values, type=pa.list_(pa.float32())), _PreparedQuery([1, 0], "l2")))
        self.assertTrue(np.isnan(scores[0]))
        self.assertEqual(1.0, scores[1])
        empty_row = pa.array([[]], type=pa.list_(pa.float32()))
        self.assertEqual([1.0], list(_iter_arrow_scores(empty_row, _PreparedQuery([], "l2"))))

    def test_batch_null_vectors_preserve_scores_without_scalar_fallback(self):
        rng = np.random.default_rng(37)
        values = rng.standard_normal((2053, 7)).astype(np.float32).tolist()
        for i in (0, 2, 511, 1025, 2051, 2052):
            values[i] = None
        queries = [[1.0] * 7, [0.0] * 7, [-0.5] * 7]
        for dtype in (pa.list_(pa.float32()), pa.large_list(pa.float32()), pa.list_(pa.float32(), 7)):
            sliced = pa.array(values, type=dtype).slice(2, 2050)
            chunked = pa.chunked_array([sliced.slice(0, 513), sliced.slice(513)])
            for vectors in (sliced, chunked):
                for metric in ("l2", "cosine", "inner_product"):
                    with self.subTest(dtype=dtype, chunked=isinstance(vectors, pa.ChunkedArray), metric=metric):
                        prepared = [_PreparedQuery(q, metric) for q in queries]
                        expected = [[None if row is None else _compute_score(query, row)
                                     for row in values[2:2052]] for query in prepared]
                        actual = [[] for _ in queries]
                        with mock.patch("pypaimon.execution.python.read.search.vector_search_read._compute_score",
                                        side_effect=AssertionError("valid rows must use vectorized scoring")):
                            for start, query_index, scores in _iter_arrow_batch_scores(vectors, prepared):
                                self.assertEqual(start, len(actual[query_index]))
                                actual[query_index].extend(scores)
                        self.assertEqual(expected, actual)

    def test_batch_all_null_vectors_and_invalid_children_keep_existing_behavior(self):
        queries = [[1.0, 0.0], [0.0, 0.0]]
        for dtype in (pa.list_(pa.float32()), pa.large_list(pa.float32()), pa.list_(pa.float32(), 2)):
            for metric in ("l2", "cosine", "inner_product"):
                prepared = [_PreparedQuery(q, metric) for q in queries]
                result = list(_iter_arrow_batch_scores(pa.array([None, None], type=dtype), prepared))
                self.assertEqual([(0, 0, [None, None]), (0, 1, [None, None])], result)
        prepared = [_PreparedQuery(q, "l2") for q in queries]
        for rows, exception, message in (
                ([None, [1.0]], ValueError, "dimension mismatch"),
                ([None, [1.0, None]], TypeError, "NoneType")):
            with self.subTest(rows=rows):
                vectors = pa.array(rows, type=pa.list_(pa.float32()))
                with self.assertRaisesRegex(exception, message):
                    list(_iter_arrow_batch_scores(vectors, prepared))
        vectors = pa.array([None, [float("nan"), 0.0], [1.0, 0.0]], type=pa.list_(pa.float32()))
        _, _, scores = next(_iter_arrow_batch_scores(vectors, prepared))
        self.assertIsNone(scores[0])
        self.assertTrue(np.isnan(scores[1]))
        self.assertEqual(1.0, scores[2])

    def test_batch_null_parent_ignores_invalid_child_values(self):
        vectors = pa.Array.from_buffers(
            pa.list_(pa.float32(), 2), 2, [pa.py_buffer(b"\x02")],
            children=[pa.array([float("nan"), None, 1.0, 2.0], type=pa.float32())])
        with mock.patch("pypaimon.execution.python.read.search.vector_search_read._compute_score",
                        side_effect=AssertionError("null children must not disable the fast path")):
            result = list(_iter_arrow_batch_scores(vectors, [_PreparedQuery([1.0, 2.0], "l2")]))
        self.assertEqual([(0, 0, [None, 1.0])], result)

    def test_raw_search_filters_before_scoring_and_preserves_ties(self):
        column = _field(1, "embedding", "FLOAT")
        reader = DataEvolutionVectorRead(_StubTable([column], []), 2, column, [1, 0])
        table = pa.table({SpecialFields.ROW_ID.name: [9, 3, 1, 2],
                          "embedding": pa.array([[1], [1, 0], [1, 0], None],
                                                type=pa.list_(pa.float32()))})
        candidates = RoaringBitmap64()
        for row_id in (1, 2, 3):
            candidates.add(row_id)

        def new_reader(_splits: List[Split]) -> Tuple[pa.ipc.RecordBatchReader, Iterator[pa.RecordBatch]]:
            batches = iter(table.to_batches(max_chunksize=2))
            return pa.ipc.RecordBatchReader.from_batches(table.schema, batches), batches

        table_read = mock.Mock()
        table_read._resolve_parallelism.return_value = 1
        table_read._new_arrow_batch_reader.side_effect = new_reader
        with mock.patch.object(reader, "_plan_raw_read", return_value=(table_read, [])):
            result = reader._read_raw_search([Range(0, 9)], None, [1, 0], score_candidates=candidates)
            self.assertEqual([1, 3], result.results().to_list())
            reader._limit = 1
            result = reader._read_raw_search([Range(0, 9)], None, [1, 0], score_candidates=candidates)
            self.assertEqual([1], result.results().to_list())

    def test_refinement_and_block_memory_bound(self):
        column = _field(1, "embedding", "FLOAT")
        reader = DataEvolutionVectorRead(_StubTable([column], []), 10, column, [1])
        values = np.arange(2051, dtype=np.float32) % 7
        values[-1] = 8.0
        original = values.copy()
        vectors = pa.FixedSizeListArray.from_arrays(pa.array(values), 1)
        batch = pa.record_batch([pa.array(range(len(values))), vectors], names=["_ROW_ID", "embedding"])
        candidates = range(len(values))
        result = reader._score_raw_batches([batch], [1], "inner_product", candidates)
        expected = sorted(candidates, key=lambda i: (-original[i], i))[:10]
        self.assertEqual(sorted(expected), result.results().to_list())
        np.testing.assert_array_equal(values, original)
        self.assertLessEqual(_score_block_size([0] * 4096) * 4096, 1 << 20)
        self.assertEqual([0.0], _score_rows([[0, 0]], _PreparedQuery([1, 0], "cosine")))


@pytest.mark.parametrize("batch", [False, True])
def test_query_conversion_is_shared_across_scoring_blocks(tmp_path: Path, batch: bool) -> None:
    conversions = []

    class Query(list):
        def __array__(self, dtype: Optional[np.dtype] = None, copy: Optional[bool] = None) -> np.ndarray:
            conversions.append(tuple(self))
            return np.asarray(list(self), dtype=dtype)

    # Cross the 1024-row score block boundary. Tied hits straddle that boundary.
    rows = [[1, 0]] + [[0, 1]] * 1023 + [[1, 0]]
    data = pa.table(
        {"id": range(len(rows)), "embedding": pa.array(rows, type=pa.list_(pa.float32(), 2))}
    )
    table = (
        pm.connect(options={"warehouse": str(tmp_path)})
        .create_table(
            "vectors",
            data=data,
            options={"vector.file.format": "parquet", "read.native.enabled": "false"},
        )
        .raw_table
    )
    queries = [Query([1.0, 0.0]), Query([0.0, 1.0])]
    if batch:
        builder = table.new_batch_vector_search_builder().with_query_vectors(queries)
        builder.with_vector_column("embedding").with_limit(2).with_option("metric", "cosine")
        results = builder.execute_batch_local()
    else:
        builder = table.new_vector_search_builder().with_query_vector(queries[0])
        builder.with_vector_column("embedding").with_limit(2).with_option("metric", "cosine")
        results = [builder.execute_local()]
    assert [list(result.results()) for result in results] == [[0, 1024], [1, 2]][: len(results)]
    assert all(result.score_getter()(i) == 1.0 for result in results for i in result.results())
    assert sorted(conversions) == sorted(tuple(q) for q in queries[: len(results)])
