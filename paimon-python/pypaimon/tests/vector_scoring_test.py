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
from unittest import mock

import numpy as np
import pyarrow as pa

from pypaimon.table.source.vector_search_read import (
    DataEvolutionVectorRead, _compute_score, _compute_scores, _iter_arrow_scores,
    _score_block_size, _score_rows,
)
from pypaimon.table.special_fields import SpecialFields
from pypaimon.tests.vector_search_filter_test import _StubTable, _field
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class VectorScoringTest(unittest.TestCase):

    def test_scores_exactly_match_scalar_accumulation(self):
        rng = np.random.default_rng(42)
        for dimension in (1, 3, 128, 384):
            vectors = rng.standard_normal((37, dimension)).astype(np.float32)
            query = rng.standard_normal(dimension).tolist()
            for metric in ("l2", "cosine", "inner_product"):
                with self.subTest(dimension=dimension, metric=metric):
                    expected = [_compute_score(query, row, metric) for row in vectors.tolist()]
                    actual = _compute_scores(query, vectors.astype(np.float64), metric)
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
                expected = [_compute_score(query, row, metric) for row in vectors.tolist()]
                self.assertEqual(expected, _compute_scores(query, vectors.astype(np.float64), metric))

    def test_arrow_slices_chunks_and_list_layouts(self):
        values = np.random.default_rng(7).standard_normal((1031, 7)).astype(np.float32).tolist()
        query = [1.0] * 7
        for dtype in (pa.list_(pa.float32()), pa.large_list(pa.float32()), pa.list_(pa.float32(), 7)):
            array = pa.array(values, type=dtype)
            sliced = array.slice(2, 1027)
            chunked = pa.chunked_array([sliced.slice(0, 511), sliced.slice(511)])
            for data in (sliced, chunked):
                for metric in ("l2", "cosine", "inner_product"):
                    expected = [_compute_score(query, row, metric) for row in values[2:1029]]
                    self.assertEqual(expected, list(_iter_arrow_scores(data, query, metric)))

    def test_null_and_unsupported_data_preserve_scalar_behavior(self):
        for dtype in (pa.list_(pa.float32()), pa.list_(pa.float64()), pa.list_(pa.int64())):
            array = pa.array([[1, 2], None, [0, 0]], type=dtype)
            self.assertEqual([1.0, None, 1.0 / 6.0], list(_iter_arrow_scores(array, [1, 2], "l2")))
        scores = _iter_arrow_scores(pa.array([None, [1.0]], type=pa.list_(pa.float32())), [1, 2], "l2")
        self.assertIsNone(next(scores))
        with self.assertRaisesRegex(ValueError, "dimension mismatch"):
            next(scores)
        with self.assertRaisesRegex(ValueError, "dimension mismatch"):
            list(_iter_arrow_scores(pa.array([[1.0], [1.0, 2.0]]), [1, 2], "l2"))
        with self.assertRaises(TypeError):
            list(_iter_arrow_scores(pa.array([[1.0, None]], type=pa.list_(pa.float32())), [1, 2], "l2"))
        values = [[float("nan"), 0], [1, 0]]
        scores = list(_iter_arrow_scores(pa.array(values, type=pa.list_(pa.float32())), [1, 0], "l2"))
        self.assertTrue(np.isnan(scores[0]))
        self.assertEqual(1.0, scores[1])
        self.assertEqual([1.0], list(_iter_arrow_scores(pa.array([[]], type=pa.list_(pa.float32())), [], "l2")))

    def test_raw_search_filters_before_scoring_and_preserves_ties(self):
        column = _field(1, "embedding", "FLOAT")
        reader = DataEvolutionVectorRead(_StubTable([column], []), 2, column, [1, 0])
        table = pa.table({SpecialFields.ROW_ID.name: [9, 3, 1, 2],
                          "embedding": pa.array([[1], [1, 0], [1, 0], None],
                                                type=pa.list_(pa.float32()))})
        candidates = RoaringBitmap64()
        for row_id in (1, 2, 3):
            candidates.add(row_id)
        with mock.patch.object(reader, "_read_raw_arrow", return_value=table):
            result = reader._read_raw_search([Range(0, 9)], None, [1, 0], score_candidates=candidates)
        self.assertEqual([1, 3], result.results().to_list())
        reader._limit = 1
        with mock.patch.object(reader, "_read_raw_arrow", return_value=table):
            result = reader._read_raw_search([Range(0, 9)], None, [1, 0], score_candidates=candidates)
        self.assertEqual([1], result.results().to_list())

    def test_refinement_and_block_memory_bound(self):
        column = _field(1, "embedding", "FLOAT")
        reader = DataEvolutionVectorRead(_StubTable([column], []), 10, column, [1, 0])
        values = {i: [float(i % 7), 0] for i in range(2051)}
        original = {row_id: vector[:] for row_id, vector in values.items()}
        candidates = RoaringBitmap64()
        for row_id in values:
            candidates.add(row_id)
        result = reader._score_raw_vectors(candidates, values, [1, 0], "inner_product", 10)
        expected = sorted(values, key=lambda i: (-values[i][0], i))[:10]
        self.assertEqual(sorted(expected), result.results().to_list())
        self.assertEqual(original, values)
        self.assertLessEqual(_score_block_size([0] * 4096) * 4096, 1 << 20)
        self.assertEqual([0.0], _score_rows([[0, 0]], [1, 0], "cosine"))
