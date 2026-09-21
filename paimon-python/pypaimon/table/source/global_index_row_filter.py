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

"""Exact row filtering for data-evolution search candidates."""

from pypaimon.read.table_read import _ClosableArrowBatchReader
from pypaimon.table.special_fields import SpecialFields
from pypaimon.table.source.global_index_live_row_filter import table_at_snapshot
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def matching_rows(table, predicate, candidates, partition_filter=None, snapshot=None):
    """Read only filter dependencies and row IDs, closing both reader and source."""
    matched = RoaringBitmap64()
    if candidates.is_empty():
        return matched
    table = table_at_snapshot(table, snapshot)
    builder = (table.new_read_builder().with_filter(predicate)
               .with_projection([SpecialFields.ROW_ID.name]))
    if partition_filter is not None:
        builder = builder.with_partition_filter(partition_filter)
    splits = builder.new_scan().with_row_ranges(candidates.to_range_list()).plan().splits()
    reader, batches = builder.new_read()._new_arrow_batch_reader(splits)
    with _ClosableArrowBatchReader(reader, batches) as batch_reader:
        for batch in batch_reader:
            for row_id in batch.column(SpecialFields.ROW_ID.name).to_pylist():
                matched.add(row_id)
    return matched
