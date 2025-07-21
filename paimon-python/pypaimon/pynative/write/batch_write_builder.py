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

from typing import Optional

from pypaimon.api import BatchTableWrite, BatchWriteBuilder, BatchTableCommit
from pypaimon.pynative.common.core_option import CoreOptions
from pypaimon.pynative.write.batch_table_commit_impl import BatchTableCommitImpl
from pypaimon.pynative.write.batch_table_write_impl import BatchTableWriteImpl


class BatchWriteBuilderImpl(BatchWriteBuilder):
    def __init__(self, table):
        from pypaimon.pynative.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = self._create_commit_user()
        self.static_partition = None

    def overwrite(self, static_partition: Optional[dict] = None) -> BatchWriteBuilder:
        self.static_partition = static_partition
        return self

    def new_write(self) -> BatchTableWrite:
        return BatchTableWriteImpl(self.table)

    def new_commit(self) -> BatchTableCommit:
        commit = BatchTableCommitImpl(self.table, self.commit_user, self.static_partition)
        return commit

    def _create_commit_user(self):
        if CoreOptions.COMMIT_USER_PREFIX in self.table.options:
            return f"{self.table.options.get(CoreOptions.COMMIT_USER_PREFIX)}_{uuid.uuid4()}"
        else:
            return str(uuid.uuid4())
