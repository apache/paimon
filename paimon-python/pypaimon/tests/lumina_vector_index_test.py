#!/usr/bin/env python3
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

"""Tests for Lumina vector index integration in paimon-python.

Uses lumina-data SDK to build an index file, then reads it via
pypaimon's LuminaVectorGlobalIndexReader.
"""

import importlib
import json
import os
import shutil
import sys
import tempfile
import types
import unittest

# ---- Bootstrap: set up pypaimon as a lightweight namespace package ----
# so we can import pypaimon.globalindex.lumina without triggering
# pypaimon/__init__.py which pulls pyarrow/catalog/schema.
_pypaimon_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_project_root = os.path.dirname(_pypaimon_root)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Create lightweight pypaimon namespace (skip __init__.py)
if 'pypaimon' not in sys.modules:
    _mod = types.ModuleType('pypaimon')
    _mod.__path__ = [_pypaimon_root]
    _mod.__package__ = 'pypaimon'
    sys.modules['pypaimon'] = _mod

# Also create pypaimon.globalindex namespace to avoid its __init__.py
_gi_root = os.path.join(_pypaimon_root, 'globalindex')
if 'pypaimon.globalindex' not in sys.modules:
    _gi_mod = types.ModuleType('pypaimon.globalindex')
    _gi_mod.__path__ = [_gi_root]
    _gi_mod.__package__ = 'pypaimon.globalindex'
    sys.modules['pypaimon.globalindex'] = _gi_mod

import ctypes
from lumina_data._native import is_available as lumina_available


def make_vectors(n, dim, seed=42):
    """Generate random vectors as ctypes arrays."""
    import random
    random.seed(seed)
    data = [random.gauss(0, 1) for _ in range(n * dim)]
    vectors = (ctypes.c_float * (n * dim))(*data)
    ids = (ctypes.c_uint64 * n)(*range(n))
    return vectors, ids, data


def skip_if_no_lumina(func):
    def wrapper(*args, **kwargs):
        if not lumina_available():
            print("SKIP %s: lumina-data native library not available" % func.__name__)
            return
        return func(*args, **kwargs)
    return wrapper


# ---- Direct imports (bypassing __init__.py) ----
def _import_module(dotted_path):
    """Import a module directly without triggering parent __init__.py."""
    return importlib.import_module(dotted_path)


# Pre-load needed sub-modules with lightweight stubs for their deps
# pypaimon.utils namespace
_utils_root = os.path.join(_pypaimon_root, 'utils')
if 'pypaimon.utils' not in sys.modules:
    _u = types.ModuleType('pypaimon.utils')
    _u.__path__ = [_utils_root]
    _u.__package__ = 'pypaimon.utils'
    sys.modules['pypaimon.utils'] = _u

# pypaimon.globalindex.lumina namespace
_lumina_root = os.path.join(_gi_root, 'lumina')
if 'pypaimon.globalindex.lumina' not in sys.modules:
    _l = types.ModuleType('pypaimon.globalindex.lumina')
    _l.__path__ = [_lumina_root]
    _l.__package__ = 'pypaimon.globalindex.lumina'
    sys.modules['pypaimon.globalindex.lumina'] = _l


class SimpleFileIO(object):
    """Minimal file I/O adapter for testing."""

    def new_input_stream(self, path):
        return open(path, 'rb')


class TestLuminaVectorGlobalIndexReader(unittest.TestCase):

    @skip_if_no_lumina
    def test_build_and_read_bruteforce(self):
        """Build a bruteforce index with lumina-data, read with pypaimon reader."""
        from lumina_data import LuminaBuilder, LuminaIndexMeta

        reader_mod = _import_module(
            'pypaimon.globalindex.lumina.lumina_vector_global_index_reader')
        LuminaVectorGlobalIndexReader = reader_mod.LuminaVectorGlobalIndexReader

        meta_mod = _import_module('pypaimon.globalindex.global_index_meta')
        GlobalIndexIOMeta = meta_mod.GlobalIndexIOMeta

        vs_mod = _import_module('pypaimon.globalindex.vector_search')
        VectorSearch = vs_mod.VectorSearch

        dim, n = 4, 100
        vectors, ids, raw = make_vectors(n, dim, seed=42)

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
                file_io=SimpleFileIO(),
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
            print("PASS test_build_and_read_bruteforce: %d vectors, dim=%d" % (n, dim))

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @skip_if_no_lumina
    def test_build_and_read_diskann(self):
        """Build a DiskANN index with lumina-data, read with pypaimon reader."""
        from lumina_data import LuminaBuilder, LuminaIndexMeta

        reader_mod = _import_module(
            'pypaimon.globalindex.lumina.lumina_vector_global_index_reader')
        LuminaVectorGlobalIndexReader = reader_mod.LuminaVectorGlobalIndexReader

        meta_mod = _import_module('pypaimon.globalindex.global_index_meta')
        GlobalIndexIOMeta = meta_mod.GlobalIndexIOMeta

        vs_mod = _import_module('pypaimon.globalindex.vector_search')
        VectorSearch = vs_mod.VectorSearch

        dim, n = 8, 200
        vectors, ids, raw = make_vectors(n, dim, seed=777)

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
                file_io=SimpleFileIO(),
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
            print("PASS test_build_and_read_diskann: %d vectors, dim=%d" % (n, dim))

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @skip_if_no_lumina
    def test_filtered_search_via_reader(self):
        """Test filtered vector search through pypaimon reader."""
        from lumina_data import LuminaBuilder, LuminaIndexMeta

        reader_mod = _import_module(
            'pypaimon.globalindex.lumina.lumina_vector_global_index_reader')
        LuminaVectorGlobalIndexReader = reader_mod.LuminaVectorGlobalIndexReader

        meta_mod = _import_module('pypaimon.globalindex.global_index_meta')
        GlobalIndexIOMeta = meta_mod.GlobalIndexIOMeta

        vs_mod = _import_module('pypaimon.globalindex.vector_search')
        VectorSearch = vs_mod.VectorSearch

        try:
            from pypaimon.utils.roaring_bitmap import RoaringBitmap64
            RoaringBitmap64()
        except (ImportError, ModuleNotFoundError):
            print("SKIP test_filtered_search_via_reader: pyroaring not available")
            return

        dim, n = 4, 100
        vectors, ids, raw = make_vectors(n, dim, seed=99)

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
                file_io=SimpleFileIO(),
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
            print("PASS test_filtered_search_via_reader: %d vectors, filter=even IDs" % n)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
