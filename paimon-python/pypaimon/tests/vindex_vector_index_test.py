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

import io
import sys
import types
import unittest

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
    VindexVectorGlobalIndexReader,
    _build_scores,
)
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class _FakeMetadata:
    dimension = 2
    metric = "l2"
    total_vectors = 100


class _FakeSearchParams:

    def __init__(self, kind, top_k, width):
        self.kind = kind
        self.top_k = top_k
        self.width = width

    @classmethod
    def automatic(cls, top_k):
        return cls("automatic", top_k, 0)

    @classmethod
    def ivf(cls, top_k, nprobe):
        return cls("ivf", top_k, nprobe)

    @classmethod
    def diskann(cls, top_k, l_search):
        return cls("diskann", top_k, l_search)


class _FakeVectorIndexReader:
    instances = []

    def __init__(self, index_input):
        self.index_input = index_input
        self.search_calls = []
        self.batch_calls = []
        self.closed = False
        _FakeVectorIndexReader.instances.append(self)

    def metadata(self):
        return _FakeMetadata()

    def optimize_for_search(self):
        pass

    def search(self, query, params, filter_bytes=None):
        self.search_calls.append(
            (list(query), params, filter_bytes))
        return (
            list(range(10, 10 + params.top_k)),
            [float(i) for i in range(params.top_k)],
        )

    def search_batch(self, queries, params, filter_bytes=None):
        self.batch_calls.append(
            {
                "queries": queries,
                "params": params,
                "filter_bytes": filter_bytes,
            }
        )
        ids = []
        distances = []
        query_count = queries.shape[0]
        for query_index in range(query_count):
            base_id = (query_index + 1) * 10
            for rank in range(params.top_k):
                ids.append(base_id + rank)
                distances.append(float(rank))
        return ids, distances

    def close(self):
        self.closed = True


class _BytesFileIO:
    def new_input_stream(self, path):
        return io.BytesIO(b"fake-index")


class VindexVectorIndexTest(unittest.TestCase):

    def test_inner_product_distance_converted_to_higher_is_better_score(self):
        id_to_scores = _build_scores(
            [10, 20, 30],
            [-1.0, -0.5, -0.1],
            "inner_product")

        self.assertEqual(1.0, id_to_scores[10])
        self.assertEqual(0.5, id_to_scores[20])
        self.assertEqual(0.1, id_to_scores[30])

        top1 = DictBasedScoredIndexResult(id_to_scores).top_k(1)
        self.assertEqual([10], top1.results().to_list())

    def test_top_k_breaks_boundary_ties_by_row_id(self):
        scores = {1: 0.5, 2: 0.5, 3: 0.5, 4: 0.9}

        self.assertEqual(
            [1, 4],
            DictBasedScoredIndexResult(scores).top_k(2).results().to_list(),
        )

    def test_batch_search_uses_native_batch_api(self):
        old_module = sys.modules.get("paimon_vindex")
        sys.modules["paimon_vindex"] = types.SimpleNamespace(
            SearchParams=_FakeSearchParams,
            VectorIndexReader=_FakeVectorIndexReader)
        _FakeVectorIndexReader.instances = []

        try:
            reader = VindexVectorGlobalIndexReader(
                file_io=_BytesFileIO(),
                index_path="/tmp",
                io_metas=[GlobalIndexIOMeta(file_name="index", file_size=1)],
            )

            include_ids = RoaringBitmap64()
            include_ids.add(1)
            include_ids.add(2)

            results = reader.visit_batch_vector_search(
                BatchVectorSearch(
                    vectors=[[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]],
                    limit=3,
                    field_name="embedding",
                    options={"ivf.nprobe": "7"},
                ).with_include_row_ids(include_ids)
            ).result()

            fake_reader = _FakeVectorIndexReader.instances[0]
            self.assertEqual([], fake_reader.search_calls)
            self.assertEqual(1, len(fake_reader.batch_calls))

            call = fake_reader.batch_calls[0]
            self.assertEqual((3, 2), call["queries"].shape)
            self.assertEqual(2, call["params"].top_k)
            self.assertEqual("ivf", call["params"].kind)
            self.assertEqual(7, call["params"].width)
            self.assertIsNotNone(call["filter_bytes"])

            self.assertEqual(3, len(results))
            self.assertEqual([10, 11], results[0].results().to_list())
            self.assertEqual([20, 21], results[1].results().to_list())
            self.assertEqual([30, 31], results[2].results().to_list())
            self.assertEqual(1.0, results[0].score_getter()(10))
            self.assertEqual(0.5, results[0].score_getter()(11))

            reader.close()
            self.assertTrue(fake_reader.closed)
        finally:
            if old_module is None:
                sys.modules.pop("paimon_vindex", None)
            else:
                sys.modules["paimon_vindex"] = old_module


if __name__ == '__main__':
    unittest.main()
