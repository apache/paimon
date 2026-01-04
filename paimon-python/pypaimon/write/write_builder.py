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

import uuid
from abc import ABC
from typing import Optional

from pypaimon.write.table_commit import (BatchTableCommit, StreamTableCommit,
                                         TableCommit)
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_write import (BatchTableWrite, StreamTableWrite,
                                        TableWrite)


class WriteBuilder(ABC):
    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = self._create_commit_user()
        self.static_partition = None

    def overwrite(self, static_partition: Optional[dict] = None):
        self.static_partition = static_partition if static_partition is not None else {}
        return self

    def new_write(self) -> TableWrite:
        """Returns a table write."""

    def new_update(self) -> TableUpdate:
        """Returns a table update."""

    def new_commit(self) -> TableCommit:
        """Returns a table commit."""

    def _create_commit_user(self):
        commit_user_prefix = self.table.options.commit_user_prefix()
        if commit_user_prefix is not None:
            return f"{commit_user_prefix}_{uuid.uuid4()}"
        else:
            return str(uuid.uuid4())


class BatchWriteBuilder(WriteBuilder):

    def new_write(self) -> BatchTableWrite:
        return BatchTableWrite(self.table, self.commit_user)

    def new_update(self) -> TableUpdate:
        return TableUpdate(self.table, self.commit_user)

    def new_commit(self) -> BatchTableCommit:
        commit = BatchTableCommit(self.table, self.commit_user, self.static_partition)
        return commit


class StreamWriteBuilder(WriteBuilder):

    def new_write(self) -> StreamTableWrite:
        return StreamTableWrite(self.table, self.commit_user)

    def new_update(self) -> TableUpdate:
        raise ValueError("StreamWriteBuilder.new_update() not supported.")

    def new_commit(self) -> StreamTableCommit:
        commit = StreamTableCommit(self.table, self.commit_user, self.static_partition)
        return commit
