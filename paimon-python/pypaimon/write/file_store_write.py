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

from typing import Dict, List, Tuple

import pyarrow as pa

from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter


class FileStoreWrite:
    """Base class for file store write operations."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.data_writers: Dict[Tuple, DataWriter] = {}

    def write(self, partition: Tuple, bucket: int, data: pa.RecordBatch):
        key = (partition, bucket)
        if key not in self.data_writers:
            self.data_writers[key] = self._create_data_writer(partition, bucket)
        writer = self.data_writers[key]
        writer.write(data)

    def _create_data_writer(self, partition: Tuple, bucket: int) -> DataWriter:
        if self.table.is_primary_key_table:
            return KeyValueDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
            )
        else:
            return AppendOnlyDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
            )

    def prepare_commit(self) -> List[CommitMessage]:
        commit_messages = []
        for (partition, bucket), writer in self.data_writers.items():
            committed_files = writer.prepare_commit()
            if committed_files:
                commit_message = CommitMessage(
                    partition=partition,
                    bucket=bucket,
                    new_files=committed_files
                )
                commit_messages.append(commit_message)
        return commit_messages

    def close(self):
        """Close all data writers and clean up resources."""
        for writer in self.data_writers.values():
            writer.close()
        self.data_writers.clear()
