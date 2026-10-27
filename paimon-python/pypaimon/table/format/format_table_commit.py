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

from typing import List

from pypaimon.table.format.format_commit_message import FormatTableCommitMessage
from pypaimon.table.format.format_table import FormatTable


class FormatTableCommit:
    """Public commit API for the Python-only format-table implementation."""

    def __init__(self, table: FormatTable):
        from pypaimon.execution.python.write.format_table_commit import PythonFormatTableCommit

        self._execution = PythonFormatTableCommit(table)

    @property
    def table(self):
        return self._execution.table

    @table.setter
    def table(self, table):
        self._execution.table = table

    def commit(self, commit_messages: List[FormatTableCommitMessage]) -> None:
        return self._execution.commit(commit_messages)

    def abort(self, commit_messages: List[FormatTableCommitMessage]) -> None:
        return self._execution.abort(commit_messages)

    def close(self) -> None:
        return self._execution.close()
