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

from abc import ABCMeta
from typing import TYPE_CHECKING, List, Optional

import pyarrow as pa

from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_identifier import validate_commit_identifier
from pypaimon.write.row_input import RowIdUpdateInputMixin

if TYPE_CHECKING:
    from pypaimon.execution.python.write.row_id_file_index import RowIdFileIndex


class TableUpdateByRowId(RowIdUpdateInputMixin, metaclass=ABCMeta):
    """Incrementally prepare updates addressed by stable row IDs."""

    FIRST_ROW_ID_COLUMN = '_FIRST_ROW_ID'

    def __init__(self, table, commit_user: str, commit_identifier: int,
                 _precomputed_files_info: Optional['RowIdFileIndex'] = None):
        self.table = table
        self.commit_user = commit_user
        self.commit_identifier = validate_commit_identifier(commit_identifier)
        from pypaimon.write.backend_dispatch import create_row_id_update_execution
        self._execution = create_row_id_update_execution(
            table, commit_user, self.commit_identifier, _precomputed_files_info)

    @classmethod
    def _from_python(cls, table, commit_user, commit_identifier, _precomputed_files_info=None):
        """Create within an operation that has already selected Python execution."""
        owner = cls.__new__(cls)
        owner.table = table
        owner.commit_user = commit_user
        owner.commit_identifier = validate_commit_identifier(commit_identifier)
        from pypaimon.write.backend_dispatch import create_python_row_id_update
        owner._execution = create_python_row_id_update(
            table, commit_user, owner.commit_identifier, _precomputed_files_info)
        return owner

    @property
    def commit_messages(self):
        """Messages accumulated by the selected row-ID execution backend."""
        return self._execution.commit_messages

    def update_columns(self, data: pa.Table, column_names: List[str]) -> List[CommitMessage]:
        """Stage column updates from an Arrow table containing ``_ROW_ID``."""
        return self._execution.update_columns(data, column_names)

    def _write_row_columns(self, update_data, column_names, blob_object_columns):
        return self._execution._write_row_columns(update_data, column_names, blob_object_columns)

    def _snapshot_files_info(self):
        # Internal Python metadata coordinators select _from_python before
        # requesting the broadcast index. Never initialize a second backend.
        return self._execution._snapshot_files_info()
