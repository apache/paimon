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
        self.table = table
        from pypaimon.write.backend_dispatch import create_row_id_delete_execution
        self._execution = create_row_id_delete_execution(self, table, _precomputed_anchor_ranges)

    @classmethod
    def _from_python(cls, table, _precomputed_anchor_ranges=None):
        """Create within an operation that has already selected Python execution."""
        owner = cls.__new__(cls)
        owner.table = table
        from pypaimon.execution.python.write.table_delete import PythonTableDeleteByRowId
        owner._execution = PythonTableDeleteByRowId().initialize(table, _precomputed_anchor_ranges)
        return owner

    def delete(self, row_ids: Sequence[int]) -> List[CommitMessage]:
        return self._execution.delete(row_ids)

    def _snapshot_anchor_ranges(self):
        # Ray's metadata coordinator selects _from_python explicitly. Native
        # writers must never initialize Python's anchor planner on demand.
        return self._execution._snapshot_anchor_ranges()
