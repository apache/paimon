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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from types import SimpleNamespace

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.primary_key_sorted_index_result import PrimaryKeySortedIndexResult
from pypaimon.utils.range import Range


class PrimaryKeySortedIndexResultTest(unittest.TestCase):
    def test_converts_file_positions_to_indexed_split(self):
        data_file = SimpleNamespace(file_name="data.parquet", row_count=10,
                                    file_size=1, file_path=None)
        source = DataSplit([data_file], GenericRow([], []), 0, raw_convertible=True)
        file_plan = SimpleNamespace(source_split=source, file_index=0, data_file=data_file)
        evaluated = SimpleNamespace(
            file=file_plan,
            result=GlobalIndexResult.from_ranges([Range(1, 2), Range(5, 5)]),
        )

        result = PrimaryKeySortedIndexResult(
            SimpleNamespace(snapshot_id=7, files=[evaluated]))

        self.assertEqual(7, result.snapshot_id)
        self.assertEqual(1, len(result.splits))
        self.assertIsInstance(result.splits[0], IndexedSplit)
        self.assertEqual([Range(1, 2), Range(5, 5)], result.splits[0].row_ranges())

    def test_invalid_position_falls_back_to_raw_file_split(self):
        data_file = SimpleNamespace(file_name="data.parquet", row_count=2,
                                    file_size=1, file_path=None)
        source = DataSplit([data_file], GenericRow([], []), 0, raw_convertible=True)
        file_plan = SimpleNamespace(source_split=source, file_index=0, data_file=data_file)
        evaluated = SimpleNamespace(file=file_plan,
                                    result=GlobalIndexResult.from_range(Range(2, 2)))

        result = PrimaryKeySortedIndexResult(
            SimpleNamespace(snapshot_id=7, files=[evaluated]))

        self.assertIsInstance(result.splits[0], DataSplit)
        self.assertNotIsInstance(result.splits[0], IndexedSplit)


if __name__ == "__main__":
    unittest.main()
