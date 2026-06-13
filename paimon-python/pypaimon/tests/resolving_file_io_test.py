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

import os
import shutil
import tempfile
import unittest

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.filesystem.resolving_file_io import ResolvingFileIO


class ResolvingFileIOTest(unittest.TestCase):
    """Tests for ResolvingFileIO."""

    def test_fileio_get_returns_resolving_when_enabled(self):
        opts = Options({CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(): 'true'})
        file_io = FileIO.get("/tmp/test", opts)
        self.assertIsInstance(file_io, ResolvingFileIO)

    def test_fileio_get_returns_local_when_disabled(self):
        opts = Options({CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(): 'false'})
        file_io = FileIO.get("/tmp/test", opts)
        self.assertIsInstance(file_io, LocalFileIO)

    def test_fileio_get_returns_local_when_not_set(self):
        file_io = FileIO.get("/tmp/test", Options({}))
        self.assertIsInstance(file_io, LocalFileIO)

    def test_no_recursion(self):
        """ResolvingFileIO should not create another ResolvingFileIO internally."""
        opts = Options({CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(): 'true'})
        resolving = ResolvingFileIO(opts)
        inner = resolving._get_fileio("/tmp/test")
        self.assertNotIsInstance(inner, ResolvingFileIO)
        self.assertIsInstance(inner, LocalFileIO)

    def test_cache_hit_same_scheme_authority(self):
        resolving = ResolvingFileIO(Options({}))
        fio1 = resolving._get_fileio("/tmp/test/a.txt")
        fio2 = resolving._get_fileio("/tmp/test/b.txt")
        self.assertIs(fio1, fio2)

    def test_cache_key_uses_scheme_and_authority(self):
        resolving = ResolvingFileIO(Options({}))
        fio_local = resolving._get_fileio("/tmp/test")
        fio_file = resolving._get_fileio("file:///tmp/test2")
        self.assertIsInstance(fio_local, LocalFileIO)
        self.assertIsInstance(fio_file, LocalFileIO)

    def test_is_object_store_with_oss_warehouse(self):
        opts = Options({CatalogOptions.WAREHOUSE.key(): 'oss://bucket/warehouse'})
        resolving = ResolvingFileIO(opts)
        self.assertTrue(resolving.is_object_store())

    def test_is_object_store_with_s3_warehouse(self):
        opts = Options({CatalogOptions.WAREHOUSE.key(): 's3://bucket/warehouse'})
        resolving = ResolvingFileIO(opts)
        self.assertTrue(resolving.is_object_store())

    def test_is_object_store_with_hdfs_warehouse(self):
        opts = Options({CatalogOptions.WAREHOUSE.key(): 'hdfs://cluster/warehouse'})
        resolving = ResolvingFileIO(opts)
        self.assertFalse(resolving.is_object_store())

    def test_is_object_store_with_local_warehouse(self):
        opts = Options({CatalogOptions.WAREHOUSE.key(): 'file:///tmp/warehouse'})
        resolving = ResolvingFileIO(opts)
        self.assertFalse(resolving.is_object_store())


class ResolvingFileIOReadWriteTest(unittest.TestCase):
    """End-to-end read/write tests using ResolvingFileIO with local filesystem."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.resolving = ResolvingFileIO(Options({}))

    def tearDown(self):
        self.resolving.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_and_read(self):
        path = os.path.join(self.temp_dir, "test.txt")
        content = "hello resolving fileio"
        self.resolving.write_file(path, content)
        result = self.resolving.read_file_utf8(path)
        self.assertEqual(result, content)

    def test_exists(self):
        path = os.path.join(self.temp_dir, "test_exists.txt")
        self.assertFalse(self.resolving.exists(path))
        self.resolving.write_file(path, "data")
        self.assertTrue(self.resolving.exists(path))

    def test_delete(self):
        path = os.path.join(self.temp_dir, "test_delete.txt")
        self.resolving.write_file(path, "data")
        self.assertTrue(self.resolving.exists(path))
        self.assertTrue(self.resolving.delete(path))
        self.assertFalse(self.resolving.exists(path))

    def test_mkdirs(self):
        dir_path = os.path.join(self.temp_dir, "a", "b", "c")
        self.assertFalse(os.path.exists(dir_path))
        self.resolving.mkdirs(dir_path)
        self.assertTrue(os.path.isdir(dir_path))

    def test_rename(self):
        src = os.path.join(self.temp_dir, "src.txt")
        dst = os.path.join(self.temp_dir, "dst.txt")
        self.resolving.write_file(src, "move me")
        self.assertTrue(self.resolving.rename(src, dst))
        self.assertFalse(self.resolving.exists(src))
        self.assertEqual(self.resolving.read_file_utf8(dst), "move me")

    def test_list_status(self):
        for name in ["a.txt", "b.txt"]:
            self.resolving.write_file(os.path.join(self.temp_dir, name), name)
        entries = self.resolving.list_status(self.temp_dir)
        names = sorted([os.path.basename(e.path) for e in entries])
        self.assertEqual(names, ["a.txt", "b.txt"])

    def test_get_file_status(self):
        path = os.path.join(self.temp_dir, "status.txt")
        self.resolving.write_file(path, "12345")
        status = self.resolving.get_file_status(path)
        self.assertEqual(status.size, 5)

    def test_exists_batch(self):
        p1 = os.path.join(self.temp_dir, "batch_a.txt")
        p2 = os.path.join(self.temp_dir, "batch_b.txt")
        p3 = os.path.join(self.temp_dir, "batch_no.txt")
        self.resolving.write_file(p1, "a")
        self.resolving.write_file(p2, "b")
        result = self.resolving.exists_batch([p1, p2, p3])
        self.assertTrue(result[p1])
        self.assertTrue(result[p2])
        self.assertFalse(result[p3])

    def test_close_clears_cache(self):
        self.resolving._get_fileio("/tmp/test")
        self.assertTrue(len(self.resolving._fileio_cache) > 0)
        self.resolving.close()
        self.assertEqual(len(self.resolving._fileio_cache), 0)

    def test_cross_path_delegation(self):
        """Different local paths should use the same cached FileIO."""
        dir_a = os.path.join(self.temp_dir, "a")
        dir_b = os.path.join(self.temp_dir, "b")
        self.resolving.mkdirs(dir_a)
        self.resolving.mkdirs(dir_b)
        path_a = os.path.join(dir_a, "file.txt")
        path_b = os.path.join(dir_b, "file.txt")
        self.resolving.write_file(path_a, "content_a")
        self.resolving.write_file(path_b, "content_b")
        self.assertEqual(self.resolving.read_file_utf8(path_a), "content_a")
        self.assertEqual(self.resolving.read_file_utf8(path_b), "content_b")
        self.assertEqual(len(self.resolving._fileio_cache), 1)


if __name__ == '__main__':
    unittest.main()
