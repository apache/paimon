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

import ctypes
import os
import random
import shutil
import tempfile
import unittest

import pytest

lumina_data = pytest.importorskip("lumina_data")
from lumina_data import LuminaBuilder

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.lumina.lumina_index_meta import LuminaIndexMeta
from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
    LuminaVectorGlobalIndexReader,
)
from pypaimon.globalindex.lumina.lumina_vector_index_options import (
    strip_lumina_options,
)
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def _make_vectors(n, dim, seed=42):
    random.seed(seed)
    data = [random.gauss(0, 1) for _ in range(n * dim)]
    vectors = (ctypes.c_float * (n * dim))(*data)
    ids = (ctypes.c_uint64 * n)(*range(n))
    return vectors, ids, data


class _SimpleFileIO(object):
    def new_input_stream(self, path):
        return open(path, 'rb')


class LuminaVectorIndexTest(unittest.TestCase):

    def test_build_and_read(self):
        """Build a DiskANN index and read via LuminaVectorGlobalIndexReader."""
        dim, n = 4, 100

        # Paimon table options (with lumina. prefix)
        paimon_options = {
            "lumina.index.dimension": str(dim),
            "lumina.index.type": "diskann",
            "lumina.distance.metric": "l2",
            "lumina.encoding.type": "rawf32",
            "lumina.diskann.build.ef_construction": "64",
            "lumina.diskann.build.neighbor_count": "32",
            "lumina.diskann.build.thread_count": "2",
        }

        build_options = strip_lumina_options(paimon_options)
        vectors, ids, raw = _make_vectors(n, dim, seed=777)

        tmp_dir = tempfile.mkdtemp(prefix="paimon_lumina_test_")
        file_name = "lumina-0.index"
        index_file = os.path.join(tmp_dir, file_name)

        try:
            with LuminaBuilder(build_options) as builder:
                builder.pretrain(vectors, n, dim)
                builder.insert(vectors, ids, n, dim)
                builder.dump(index_file)

            # Serialize metadata (same as Java LuminaIndexMeta)
            meta = LuminaIndexMeta(build_options)
            io_meta = GlobalIndexIOMeta(
                file_name=file_name,
                file_size=os.path.getsize(index_file),
                metadata=meta.serialize(),
            )

            # Reader receives paimon_options (with lumina. prefix)
            with LuminaVectorGlobalIndexReader(
                file_io=_SimpleFileIO(),
                index_path=tmp_dir,
                io_metas=[io_meta],
                options=paimon_options,
            ) as reader:
                vs = VectorSearch(vector=raw[:dim], limit=5, field_name="embedding")
                result = reader.visit_vector_search(vs).result()

                self.assertIsNotNone(result)
                row_ids = result.results()
                self.assertGreater(row_ids.cardinality(), 0)
                self.assertIn(0, row_ids)
                self.assertIsNotNone(result.score_getter()(0))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_filtered_search(self):
        """Test filtered vector search with include_row_ids."""
        dim, n = 4, 100

        paimon_options = {
            "lumina.index.dimension": str(dim),
            "lumina.index.type": "diskann",
            "lumina.distance.metric": "l2",
            "lumina.encoding.type": "rawf32",
            "lumina.diskann.build.ef_construction": "64",
            "lumina.diskann.build.neighbor_count": "32",
            "lumina.diskann.build.thread_count": "2",
        }

        build_options = strip_lumina_options(paimon_options)
        vectors, ids, raw = _make_vectors(n, dim, seed=99)

        tmp_dir = tempfile.mkdtemp(prefix="paimon_lumina_test_")
        file_name = "lumina-filter-0.index"
        index_file = os.path.join(tmp_dir, file_name)

        try:
            with LuminaBuilder(build_options) as builder:
                builder.pretrain(vectors, n, dim)
                builder.insert(vectors, ids, n, dim)
                builder.dump(index_file)

            meta = LuminaIndexMeta(build_options)
            io_meta = GlobalIndexIOMeta(
                file_name=file_name,
                file_size=os.path.getsize(index_file),
                metadata=meta.serialize(),
            )
            reader = LuminaVectorGlobalIndexReader(
                file_io=_SimpleFileIO(),
                index_path=tmp_dir,
                io_metas=[io_meta],
                options=paimon_options,
            )

            # Only search even IDs
            include_ids = RoaringBitmap64()
            for i in range(0, n, 2):
                include_ids.add(i)

            vs = VectorSearch(
                vector=raw[:dim], limit=3, field_name="embedding",
                include_row_ids=include_ids,
            )
            result = reader.visit_vector_search(vs).result()

            self.assertIsNotNone(result)
            for row_id in result.results():
                self.assertEqual(row_id % 2, 0)
            reader.close()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_batch_matches_single(self):
        """Native batch search must return, per query, the same result as a single search."""
        dim, n = 8, 200

        paimon_options = {
            "lumina.index.dimension": str(dim),
            "lumina.index.type": "diskann",
            "lumina.distance.metric": "l2",
            "lumina.encoding.type": "rawf32",
            "lumina.diskann.build.ef_construction": "64",
            "lumina.diskann.build.neighbor_count": "32",
            "lumina.diskann.build.thread_count": "2",
        }

        build_options = strip_lumina_options(paimon_options)
        vectors, ids, raw = _make_vectors(n, dim, seed=123)

        tmp_dir = tempfile.mkdtemp(prefix="paimon_lumina_test_")
        file_name = "lumina-batch-0.index"
        index_file = os.path.join(tmp_dir, file_name)

        def scores(result):
            if result is None:
                return {}
            getter = result.score_getter()
            return {row_id: getter(row_id) for row_id in result.results()}

        query_vectors = [raw[i * dim:(i + 1) * dim] for i in (0, 3, 7, 50, 123)]
        limit = 5

        try:
            with LuminaBuilder(build_options) as builder:
                builder.pretrain(vectors, n, dim)
                builder.insert(vectors, ids, n, dim)
                builder.dump(index_file)

            meta = LuminaIndexMeta(build_options)
            io_meta = GlobalIndexIOMeta(
                file_name=file_name,
                file_size=os.path.getsize(index_file),
                metadata=meta.serialize(),
            )

            include_ids = RoaringBitmap64()
            for i in (0, 3, 7, 50, 123, 10, 11, 12):
                include_ids.add(i)

            def assert_batch_matches_single(reader, batch_results, include_row_ids):
                for i, query_vector in enumerate(query_vectors):
                    vs = VectorSearch(
                        vector=query_vector, limit=limit, field_name="embedding")
                    if include_row_ids is not None:
                        vs = vs.with_include_row_ids(include_row_ids)
                    single = scores(reader.visit_vector_search(vs).result())
                    batch = scores(batch_results[i])
                    self.assertEqual(set(batch), set(single))
                    for row_id in batch:
                        self.assertAlmostEqual(
                            batch[row_id], single[row_id], places=5)

            with LuminaVectorGlobalIndexReader(
                file_io=_SimpleFileIO(),
                index_path=tmp_dir,
                io_metas=[io_meta],
                options=paimon_options,
            ) as reader:
                # Unfiltered: each query's batch slice must equal its single search.
                unfiltered = reader.visit_batch_vector_search(
                    BatchVectorSearch(
                        vectors=query_vectors, limit=limit, field_name="embedding")
                ).result()
                assert_batch_matches_single(reader, unfiltered, None)

                # Distinct queries must not collapse to one identical result set,
                # which would hide a wrong per-query slice.
                top_sets = {tuple(sorted(scores(r))) for r in unfiltered}
                self.assertGreater(len(top_sets), 1)

                # Filtered: a single include_row_ids bitmap shared by all queries.
                filtered = reader.visit_batch_vector_search(
                    BatchVectorSearch(
                        vectors=query_vectors, limit=limit, field_name="embedding")
                    .with_include_row_ids(include_ids)
                ).result()
                assert_batch_matches_single(reader, filtered, include_ids)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
