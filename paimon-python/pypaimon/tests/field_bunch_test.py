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

from pypaimon.read.reader.field_bunch import BlobBunch
from pypaimon.utils.range import Range


class _BlobFile:
    def __init__(
            self,
            file_name: str,
            first_row_id: int,
            row_count: int,
            write_cols=None):
        self.file_name = file_name
        self.first_row_id = first_row_id
        self.row_count = row_count
        self.write_cols = write_cols or ["blob_col"]

    def row_id_range(self) -> Range:
        return Range(
            self.first_row_id,
            self.first_row_id + self.row_count - 1,
        )


class BlobBunchTest(unittest.TestCase):

    def test_finish_merges_ranges_once(self):
        bunch = BlobBunch(expected_row_count=1000)
        merge_ranges = Range.sort_and_merge_overlap

        with mock.patch.object(
                Range,
                "sort_and_merge_overlap",
                wraps=merge_ranges) as mocked_merge:
            for index in range(1000):
                bunch.add(_BlobFile(f"{index}.blob", index, 1))

            self.assertEqual(0, mocked_merge.call_count)
            bunch.finish()
            self.assertEqual(1, mocked_merge.call_count)
            self.assertEqual(1000, bunch.row_count())
            self.assertEqual(1, mocked_merge.call_count)

    def test_finish_preserves_overlapping_versions(self):
        bunch = BlobBunch(expected_row_count=20)
        files = [
            _BlobFile("old.blob", 0, 10),
            _BlobFile("new.blob", 0, 10),
            _BlobFile("tail.blob", 10, 10),
        ]
        for file in files:
            bunch.add(file)

        bunch.finish()

        self.assertEqual(files, bunch.files())
        self.assertEqual(20, bunch.row_count())

    def test_add_rejects_file_after_finish(self):
        bunch = BlobBunch(expected_row_count=1)
        bunch.add(_BlobFile("first.blob", 0, 1))
        bunch.finish()

        with self.assertRaisesRegex(ValueError, "finished blob bunch"):
            bunch.add(_BlobFile("second.blob", 1, 1))

    def test_finish_can_recover_after_validation_failure(self):
        bunch = BlobBunch(expected_row_count=10)
        bunch.add(_BlobFile("first.blob", 0, 5))
        with self.assertRaisesRegex(ValueError, "aligned with normal files"):
            bunch.finish()

        bunch.add(_BlobFile("second.blob", 5, 5))

        bunch.finish()
        self.assertEqual(10, bunch.row_count())

    def test_finish_rejects_gap_without_row_id_push_down(self):
        bunch = BlobBunch(expected_row_count=20)
        bunch.add(_BlobFile("left.blob", 0, 5))
        bunch.add(_BlobFile("right.blob", 10, 10))

        with self.assertRaisesRegex(ValueError, "contiguous row range"):
            bunch.finish()

    def test_finish_rejects_unaligned_count_without_row_id_push_down(self):
        bunch = BlobBunch(expected_row_count=20)
        bunch.add(_BlobFile("short.blob", 0, 10))

        with self.assertRaisesRegex(ValueError, "aligned with normal files"):
            bunch.finish()

    def test_finish_allows_gap_with_row_id_push_down(self):
        bunch = BlobBunch(expected_row_count=20, row_id_push_down=True)
        bunch.add(_BlobFile("left.blob", 0, 5))
        bunch.add(_BlobFile("right.blob", 10, 10))

        bunch.finish()

        self.assertEqual(15, bunch.row_count())

    def test_finish_rejects_row_count_exceeding_expected(self):
        bunch = BlobBunch(expected_row_count=10, row_id_push_down=True)
        bunch.add(_BlobFile("too-large.blob", 0, 11))

        with self.assertRaisesRegex(ValueError, "row count exceed"):
            bunch.finish()


if __name__ == "__main__":
    unittest.main()
