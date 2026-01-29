# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List

import pyarrow.fs as pafs

from pypaimon.table.format.format_table import FormatTable
from pypaimon.table.format.format_table_scan import _is_data_file_name
from pypaimon.table.format.format_commit_message import FormatTableCommitMessage


def _delete_data_files_in_path(file_io, path: str) -> None:
    try:
        infos = file_io.list_status(path)
    except Exception:
        return
    for info in infos:
        if info.type == pafs.FileType.Directory:
            _delete_data_files_in_path(file_io, info.path)
        elif info.type == pafs.FileType.File:
            name = info.path.split("/")[-1] if "/" in info.path else info.path
            if _is_data_file_name(name):
                try:
                    file_io.delete(info.path, False)
                except Exception:
                    pass


class FormatTableCommit:
    """Commit for format table. Overwrite is applied in FormatTableWrite at write time."""

    def __init__(self, table: FormatTable):
        self.table = table
        self._committed = False

    def commit(self, commit_messages: List[FormatTableCommitMessage]) -> None:
        if self._committed:
            raise RuntimeError("FormatTableCommit supports only one commit.")
        self._committed = True
        return

    def abort(self, commit_messages: List[FormatTableCommitMessage]) -> None:
        for msg in commit_messages:
            for path in msg.written_paths:
                try:
                    if self.table.file_io.exists(path):
                        self.table.file_io.delete(path, False)
                except Exception:
                    pass

    def close(self) -> None:
        pass
