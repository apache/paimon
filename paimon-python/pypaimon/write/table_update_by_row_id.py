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

from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_input import RowIdUpdateInputMixin

if TYPE_CHECKING:
    from pypaimon.execution.python.write.row_id_file_index import RowIdFileIndex


class TableUpdateByRowId(PythonRowIdUpdateMixin, RowIdUpdateInputMixin, metaclass=ABCMeta):
    """Incrementally prepare updates addressed by stable row IDs."""

    def __init__(
            self, table, commit_user: str, commit_identifier: int,
            _precomputed_files_info: Optional["RowIdFileIndex"] = None,
    ):
        return super().__init__(table, commit_user, commit_identifier, _precomputed_files_info)

    def update_columns(self, data: pa.Table, column_names: List[str]) -> List[CommitMessage]:
        """
        Add or update columns in the table.

        Args:
            data: Input data containing row_id and columns to update
            column_names: Names of columns to update (excluding row_id)

        Returns:
            List of commit messages
        """
        return super().update_columns(data, column_names)
