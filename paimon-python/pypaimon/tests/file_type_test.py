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
#  limitations under the License.
################################################################################

"""Tests for FileType classification."""

import unittest

from pypaimon.utils.file_type import FileType


class FileTypeClassifyTest(unittest.TestCase):

    def test_snapshot(self):
        self.assertEqual(FileType.META, FileType.classify("/warehouse/db/t/snapshot/snapshot-1"))
        self.assertEqual(FileType.META, FileType.classify("snapshot-42"))

    def test_schema(self):
        self.assertEqual(FileType.META, FileType.classify("/warehouse/db/t/schema/schema-0"))

    def test_statistics(self):
        self.assertEqual(FileType.META, FileType.classify("stat-abc123"))

    def test_tag(self):
        self.assertEqual(FileType.META, FileType.classify("tag-v1.0"))

    def test_consumer(self):
        self.assertEqual(FileType.META, FileType.classify("consumer-group1"))

    def test_service(self):
        self.assertEqual(FileType.META, FileType.classify("service-abc"))

    def test_manifest(self):
        self.assertEqual(FileType.META, FileType.classify("manifest-abc123"))
        self.assertEqual(FileType.META, FileType.classify("manifest-list-abc"))
        self.assertEqual(FileType.META, FileType.classify("index-manifest-abc"))

    def test_hint_files(self):
        self.assertEqual(FileType.META, FileType.classify("EARLIEST"))
        self.assertEqual(FileType.META, FileType.classify("LATEST"))

    def test_success_files(self):
        self.assertEqual(FileType.META, FileType.classify("_SUCCESS"))
        self.assertEqual(FileType.META, FileType.classify("part-0_SUCCESS"))

    def test_changelog_meta(self):
        self.assertEqual(FileType.META, FileType.classify("/warehouse/db/t/changelog/changelog-1"))

    def test_changelog_not_in_changelog_dir(self):
        self.assertEqual(FileType.DATA, FileType.classify("/warehouse/db/t/other/changelog-1"))

    def test_global_index(self):
        self.assertEqual(FileType.GLOBAL_INDEX,
                         FileType.classify("global-index-abc-123.index"))
        self.assertEqual(FileType.GLOBAL_INDEX,
                         FileType.classify("/warehouse/db/t/global-index/global-index-uuid.index"))

    def test_file_index(self):
        self.assertEqual(FileType.FILE_INDEX,
                         FileType.classify("data-abc.orc.index"))
        self.assertEqual(FileType.FILE_INDEX,
                         FileType.classify("some-file.index"))

    def test_bucket_index(self):
        self.assertEqual(FileType.BUCKET_INDEX, FileType.classify("index-abc-0"))
        self.assertEqual(FileType.BUCKET_INDEX, FileType.classify("index-uuid-1"))

    def test_data_files(self):
        self.assertEqual(FileType.DATA, FileType.classify("data-abc.orc"))
        self.assertEqual(FileType.DATA, FileType.classify("data-abc.parquet"))
        self.assertEqual(FileType.DATA, FileType.classify("unknown-file"))

    def test_temp_file_unwrap(self):
        # .{originalName}.{UUID}.tmp -> originalName
        uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        temp_name = f".snapshot-1.{uuid}.tmp"
        self.assertEqual(FileType.META, FileType.classify(temp_name))

        temp_data = f".data-abc.orc.{uuid}.tmp"
        self.assertEqual(FileType.DATA, FileType.classify(temp_data))

    def test_temp_file_not_matching_pattern(self):
        self.assertEqual(FileType.DATA, FileType.classify(".short.tmp"))
        self.assertEqual(FileType.DATA, FileType.classify("no-leading-dot.uuid-here.tmp"))

    def test_is_index(self):
        self.assertTrue(FileType.BUCKET_INDEX.is_index())
        self.assertTrue(FileType.GLOBAL_INDEX.is_index())
        self.assertTrue(FileType.FILE_INDEX.is_index())
        self.assertFalse(FileType.META.is_index())
        self.assertFalse(FileType.DATA.is_index())


if __name__ == '__main__':
    unittest.main()
