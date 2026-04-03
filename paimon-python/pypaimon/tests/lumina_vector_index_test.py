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

import ctypes
import os
import random
import shutil
import tempfile
import unittest

from lumina_data import LuminaBuilder
from lumina_data._native import is_available as _lumina_available

from pypaimon.globalindex.lumina.lumina_index_meta import LuminaIndexMeta

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
    LuminaVectorGlobalIndexReader,
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

    def test_build_and_read_bruteforce(self):
        dim, n = 4, 100
        vectors, ids, raw = _make_vectors(n, dim, seed=42)

        tmp_dir = tempfile.mkdtemp(prefix="paimon_lumina_test_")
        index_file = os.path.join(tmp_dir, "lumina-test-0.lmi")

        try:
            options = {
                "index.type": "bruteforce", "index.dimension": str(dim),
                "distance.metric": "l2", "encoding.type": "rawf32",
            }
            with LuminaBuilder(options) as builder:
                builder.pretrain(vectors, n, dim)
                builder.insert(vectors, ids, n, dim)
                builder.dump(index_file)

            meta = LuminaIndexMeta(options)
            io_meta = GlobalIndexIOMeta(
                file_name="lumina-test-0.lmi",
                file_size=os.path.getsize(index_file),
                metadata=meta.serialize(),
            )
            reader = LuminaVectorGlobalIndexReader(
                file_io=_SimpleFileIO(),
                index_path=tmp_dir,
                io_metas=[io_meta],
                options={"lumina.index.dimension": str(dim),
                         "lumina.distance.metric": "l2"},
            )

            vs = VectorSearch(vector=raw[:dim], limit=5, field_name="embedding")
            result = reader.visit_vector_search(vs)

            self.assertIsNotNone(result)
            id_to_scores = result._id_to_scores
            self.assertGreater(len(id_to_scores), 0)
            self.assertIn(0, id_to_scores)
            self.assertAlmostEqual(id_to_scores[0], 1.0, places=4)
            reader.close()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_build_and_read_diskann(self):
        dim, n = 8, 200
        vectors, ids, raw = _make_vectors(n, dim, seed=777)

        tmp_dir = tempfile.mkdtemp(prefix="paimon_lumina_test_")
        index_file = os.path.join(tmp_dir, "lumina-diskann-0.lmi")

        try:
            options = {
                "index.type": "diskann", "index.dimension": str(dim),
                "distance.metric": "l2", "encoding.type": "rawf32",
                "diskann.build.ef_construction": "64",
                "diskann.build.neighbor_count": "32",
                "diskann.build.thread_count": "2",
            }
            with LuminaBuilder(options) as builder:
                builder.pretrain(vectors, n, dim)
                builder.insert(vectors, ids, n, dim)
                builder.dump(index_file)

            meta = LuminaIndexMeta(options)
            io_meta = GlobalIndexIOMeta(
                file_name="lumina-diskann-0.lmi",
                file_size=os.path.getsize(index_file),
                metadata=meta.serialize(),
            )
            reader = LuminaVectorGlobalIndexReader(
                file_io=_SimpleFileIO(),
                index_path=tmp_dir,
                io_metas=[io_meta],
            )

            vs = VectorSearch(vector=raw[:dim], limit=5, field_name="embedding")
            result = reader.visit_vector_search(vs)

            self.assertIsNotNone(result)
            id_to_scores = result._id_to_scores
            self.assertGreater(len(id_to_scores), 0)
            self.assertIn(0, id_to_scores)
            reader.close()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_filtered_search(self):
        dim, n = 4, 100
        vectors, ids, raw = _make_vectors(n, dim, seed=99)

        tmp_dir = tempfile.mkdtemp(prefix="paimon_lumina_test_")
        index_file = os.path.join(tmp_dir, "lumina-filter-0.lmi")

        try:
            options = {
                "index.type": "bruteforce", "index.dimension": str(dim),
                "distance.metric": "l2", "encoding.type": "rawf32",
            }
            with LuminaBuilder(options) as builder:
                builder.pretrain(vectors, n, dim)
                builder.insert(vectors, ids, n, dim)
                builder.dump(index_file)

            meta = LuminaIndexMeta(options)
            io_meta = GlobalIndexIOMeta(
                file_name="lumina-filter-0.lmi",
                file_size=os.path.getsize(index_file),
                metadata=meta.serialize(),
            )
            reader = LuminaVectorGlobalIndexReader(
                file_io=_SimpleFileIO(),
                index_path=tmp_dir,
                io_metas=[io_meta],
            )

            include_ids = RoaringBitmap64()
            for i in range(0, n, 2):
                include_ids.add(i)

            vs = VectorSearch(
                vector=raw[:dim], limit=3, field_name="embedding",
                include_row_ids=include_ids,
            )
            result = reader.visit_vector_search(vs)

            self.assertIsNotNone(result)
            for row_id in result.results():
                self.assertEqual(row_id % 2, 0)
            reader.close()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
