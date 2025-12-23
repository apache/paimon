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
# limitations under the License.
################################################################################
from typing import List

import pyarrow as pa

from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


class TableUpdate:
    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.update_cols = None

    def with_update_type(self, update_cols: List[str]):
        for col in update_cols:
            if col not in self.table.field_names:
                raise ValueError(f"Column {col} is not in table schema.")
        if len(update_cols) == len(self.table.field_names):
            update_cols = None
        self.update_cols = update_cols
        return self

    def update_by_arrow_with_row_id(self, table: pa.Table) -> List[CommitMessage]:
        update_by_row_id = TableUpdateByRowId(self.table, self.commit_user)
        update_by_row_id.update_columns(table, self.update_cols)
        return update_by_row_id.commit_messages
