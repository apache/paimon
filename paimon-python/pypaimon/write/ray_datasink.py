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
"""
Module to reawrited a Paimon table from a Ray Dataset, by using the Ray Datasink API.
"""

from typing import Iterable
from ray.data.datasource.datasink import Datasink, WriteResult, WriteReturnType
from pypaimon.table.table import Table
from pypaimon.write.write_builder import WriteBuilder
from ray.data.block import BlockAccessor
from ray.data.block import Block
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa


class PaimonDatasink(Datasink):
    
    def __init__(self, table: Table, overwrite=False):
        self.table = table
        self.overwrite = overwrite

    def on_write_start(self, schema=None) -> None:
        """Callback for when a write job starts.

        Use this method to perform setup for write tasks. For example, creating a
        staging bucket in S3.

        Args:
            schema: Optional schema information passed by Ray Data.
        """
        self.writer_builder: WriteBuilder = self.table.new_batch_write_builder()
        if self.overwrite:
            self.writer_builder = self.writer_builder.overwrite()

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> WriteReturnType:
        """Write blocks. This is used by a single write task.

        Args:
            blocks: Generator of data blocks.
            ctx: ``TaskContext`` for the write task.

        Returns:
            Result of this write task. When the entire write operator finishes,
            All returned values will be passed as `WriteResult.write_returns`
            to `Datasink.on_write_complete`.
        """
        table_write = self.writer_builder.new_write()
        for block in blocks:
            block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()
            table_write.write_arrow(block_arrow)
        commit_messages = table_write.prepare_commit()
        table_write.close()
        return commit_messages

    def on_write_complete(self, write_result: WriteResult[WriteReturnType]):
        """Callback for when a write job completes.

        This can be used to `commit` a write output. This method must
        succeed prior to ``write_datasink()`` returning to the user. If this
        method fails, then ``on_write_failed()`` is called.

        Args:
            write_result: Aggregated result of the
            Write operator, containing write results and stats.
        """
        table_commit = self.writer_builder.new_commit()
        table_commit.commit([
            commit_message
            for commit_messages in write_result.write_returns
            for commit_message in commit_messages
        ])
        table_commit.close()
