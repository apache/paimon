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

from pypaimon.table.source.full_text_scan import FullTextScan, FullTextScanPlan
from pypaimon.table.source.primary_key_vector_scan import (
    PrimaryKeyVectorScan, PrimaryKeyVectorSearchSplit)


class PrimaryKeyFullTextSearchSplit(PrimaryKeyVectorSearchSplit):
    pass


class PrimaryKeyFullTextScan(FullTextScan):
    """Plan source-backed full-text payloads with current PK data files."""

    def __init__(self, table, definition, partition_filter=None):
        self._table = table
        self._definition = definition
        self._partition_filter = partition_filter
        self._field = next(field for field in table.fields
                           if field.id == definition.field_id)

    def scan(self):
        vector_scan = PrimaryKeyVectorScan(
            self._table, self._field,
            partition_filter=self._partition_filter,
            index_type=self._definition.index_type)
        vector_plan = vector_scan.scan()
        splits = [PrimaryKeyFullTextSearchSplit(
            split.data_split, split.payloads, split.uncovered_data_files,
            split.row_ranges_by_file)
                  for split in vector_plan.splits()]
        return PrimaryKeyFullTextScanPlan(vector_plan.snapshot_id, splits)


class PrimaryKeyFullTextScanPlan(FullTextScanPlan):
    def __init__(self, snapshot_id, splits):
        super().__init__(splits)
        self.snapshot_id = snapshot_id
