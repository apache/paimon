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

from typing import TYPE_CHECKING, List, Optional, Sequence

from pypaimon.write.commit_message import CommitMessage

if TYPE_CHECKING:
    from pypaimon.execution.python.write.table_delete import _AnchorRangesInfo


class TableDeleteByRowId:
    """Prepare row deletion messages for a table."""

    def __init__(self, table, _precomputed_anchor_ranges: Optional['_AnchorRangesInfo'] = None):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin.__init__(self, table, _precomputed_anchor_ranges)

    def delete(self, row_ids: Sequence[int]) -> List[CommitMessage]:
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin.delete(self, row_ids)

    def _snapshot_anchor_ranges(self):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._snapshot_anchor_ranges(self)

    def _validate_delete(self):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._validate_delete(self)

    @staticmethod
    def _deduplicate_row_ids(row_ids):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._deduplicate_row_ids(row_ids)

    def _load_anchor_ranges(self):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._load_anchor_ranges(self)

    def _scan_anchor_ranges(self):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._scan_anchor_ranges(self)

    def _group_delete_targets(self, row_ids, anchors):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._group_delete_targets(self, row_ids, anchors)

    def _build_commit_message(self, partition, bucket, file_positions, snapshot_id):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._build_commit_message(self, partition, bucket, file_positions, snapshot_id)

    def _read_existing_deletion_vectors(self, partition, bucket, snapshot_id):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._read_existing_deletion_vectors(self, partition, bucket, snapshot_id)

    def _write_deletion_vector_index(self, partition, bucket, deletion_vectors):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._write_deletion_vector_index(self, partition, bucket, deletion_vectors)

    def _index_file_path(self, index_file, partition, bucket):
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteMixin
        return PythonTableDeleteMixin._index_file_path(self, index_file, partition, bucket)
