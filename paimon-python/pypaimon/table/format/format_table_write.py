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

from typing import Dict, List, Optional

import pyarrow

from pypaimon.table.format.format_commit_message import FormatTableCommitMessage
from pypaimon.table.format.format_table import FormatTable


class FormatTableWrite:
    """Public batch writer for a format table's Python execution backend."""

    def __init__(
        self,
        table: FormatTable,
        overwrite: bool = False,
        static_partitions: Optional[Dict[str, str]] = None,
    ):
        from pypaimon.execution.python.write.format_table_write import PythonFormatTableWrite

        # Format tables have a Python-only write implementation. Select it here
        # so unsupported options still fail before an overwrite can delete data.
        self._execution = PythonFormatTableWrite(table, overwrite, static_partitions)

    @property
    def table(self):
        return self._execution.table

    @table.setter
    def table(self, table):
        self._execution.table = table

    def write_arrow(self, data: pyarrow.Table) -> None:
        from pypaimon.execution.python.write.format_table_write import _write_arrow

        return _write_arrow(self, data)

    def write_arrow_batch(self, data: pyarrow.RecordBatch) -> None:
        return self._execution.write_arrow_batch(data)

    def write_pandas(self, df) -> None:
        from pypaimon.execution.python.write.format_table_write import _write_pandas

        return _write_pandas(self, df)

    def prepare_commit(self) -> List[FormatTableCommitMessage]:
        return self._execution.prepare_commit()

    def close(self) -> None:
        return self._execution.close()
