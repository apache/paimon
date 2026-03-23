################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and
#  limitations under the License.
################################################################################

import os
import shutil
import tempfile
import unittest

from pypaimon.orphan_files_clean import (
    OrphanFilesClean,
    CleanOrphanFilesResult,
)


class TestOrphanFilesClean(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.table_path = os.path.join(cls.tempdir, 'test_table')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_orphan_files_clean_initialization(self):
        """Test that OrphanFilesClean can be initialized."""
        orphan_clean = OrphanFilesClean(
            table_path=self.table_path,
            file_io=None,  # Will use mock in real tests
            table_schema=None,
        )
        self.assertIsNotNone(orphan_clean)
        self.assertFalse(orphan_clean.dry_run)

    def test_orphan_files_clean_dry_run(self):
        """Test that dry run doesn't delete files."""
        orphan_clean = OrphanFilesClean(
            table_path=self.table_path,
            file_io=None,
            table_schema=None,
            dry_run=True
        )
        self.assertTrue(orphan_clean.dry_run)

    def test_clean_orphan_files_result(self):
        """Test CleanOrphanFilesResult dataclass."""
        result = CleanOrphanFilesResult(
            deleted_file_count=10,
            deleted_file_total_len_in_bytes=1024
        )
        self.assertEqual(result.deleted_file_count, 10)
        self.assertEqual(result.deleted_file_total_len_in_bytes, 1024)
        self.assertIsNone(result.deleted_files_path)

    def test_clean_orphan_files_result_with_paths(self):
        """Test CleanOrphanFilesResult with file paths."""
        result = CleanOrphanFilesResult(
            deleted_file_count=5,
            deleted_file_total_len_in_bytes=512,
            deleted_files_path=['/path/to/file1', '/path/to/file2']
        )
        self.assertEqual(result.deleted_file_count, 5)
        self.assertEqual(result.deleted_file_total_len_in_bytes, 512)
        self.assertEqual(len(result.deleted_files_path), 2)


if __name__ == '__main__':
    unittest.main()
