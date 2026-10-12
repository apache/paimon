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

from unittest.mock import patch

import pyarrow as pa

import pypaimon.multimodal as pm
from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.table.source.global_index_live_row_filter import live_rows
from pypaimon.utils.range import Range


def test_scoped_live_rows_only_load_relevant_deletions(tmp_path):
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "rows", schema=pa.schema([("id", pa.int64())]), options={"file.format": "parquet"})
    table.add(pa.table({"id": [0, 1, 2]}))
    table.add(pa.table({"id": [3, 4, 5]}))
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.delete("id = 0 OR id = 3")
    with patch.object(DeletionVector, "read", wraps=DeletionVector.read) as read:
        assert list(live_rows(table.raw_table)) == [1, 2, 4, 5]
        all_reads = read.call_count
        read.reset_mock()
        # These bounds lie inside a file range and produce IndexedSplit wrappers.
        assert list(live_rows(table.raw_table, row_ranges=[Range(0, 1)])) == [1]
        assert 0 < read.call_count < all_reads
        read.reset_mock()
        assert list(live_rows(table.raw_table, row_ranges=[Range(3, 4)])) == [4]
        assert 0 < read.call_count < all_reads
        read.reset_mock()
        assert list(live_rows(table.raw_table, snapshot=snapshot, row_ranges=[Range(0, 1)])) == [0, 1]
        read.assert_not_called()
        assert list(live_rows(table.raw_table, row_ranges=[])) == []
        read.assert_not_called()
