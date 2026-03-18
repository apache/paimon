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
import random
from typing import Dict, List, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.write.writer.data_blob_writer import DataBlobWriter
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter
from pypaimon.table.bucket_mode import BucketMode


class FileStoreWrite:
    """Base class for file store write operations."""

    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.data_writers: Dict[Tuple, DataWriter] = {}
        self.max_seq_numbers: dict = {}
        self.write_cols = None
        self.commit_identifier = 0
        self.options = CoreOptions.copy(table.options)
        if self.table.bucket_mode() == BucketMode.POSTPONE_MODE:
            self.options.set(CoreOptions.DATA_FILE_PREFIX,
                             (f"{self.options.data_file_prefix()}-u-{commit_user}"
                              f"-s-{random.randint(0, 2 ** 31 - 2)}-w-"))

    def write(self, partition: Tuple, bucket: int, data: pa.RecordBatch):
        key = (partition, bucket)
        if key not in self.data_writers:
            self.data_writers[key] = self._create_data_writer(partition, bucket, self.options)
        writer = self.data_writers[key]
        writer.write(data)

    def _create_data_writer(self, partition: Tuple, bucket: int, options: CoreOptions) -> DataWriter:
        def max_seq_number():
            return self._seq_number_stats(partition).get(bucket, 1)

        # Check if table has blob columns
        if self._has_blob_columns():
            return DataBlobWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=0,
                options=options
            )
        elif self.table.is_primary_key_table:
            return KeyValueDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=max_seq_number(),
                options=options)
        else:
            seq_number = 0 if self.table.bucket_mode() == BucketMode.BUCKET_UNAWARE else max_seq_number()
            return AppendOnlyDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=seq_number,
                options=options,
                write_cols=self.write_cols
            )

    def _has_blob_columns(self) -> bool:
        """Check if the table schema contains blob columns."""
        for field in self.table.table_schema.fields:
            # Check if field type is blob
            if hasattr(field.type, 'type') and field.type.type == 'BLOB':
                return True
            # Alternative: check for specific blob type class
            elif hasattr(field.type, '__class__') and 'blob' in field.type.__class__.__name__.lower():
                return True
        return False

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        self.commit_identifier = commit_identifier
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

    def _seq_number_stats(self, partition: Tuple) -> Dict[int, int]:
        buckets = self.max_seq_numbers.get(partition)
        if buckets is None:
            buckets = self._load_seq_number_stats(partition)
            self.max_seq_numbers[partition] = buckets
        return buckets

    def _load_seq_number_stats(self, partition: Tuple) -> dict:
        read_builder = self.table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        sub_predicates = []
        for key, value in zip(self.table.partition_keys, partition):
            sub_predicates.append(predicate_builder.equal(key, value))
        partition_filter = predicate_builder.and_predicates(sub_predicates)

        scan = read_builder.with_filter(partition_filter).new_scan()
        splits = scan.plan().splits()

        max_seq_numbers = {}
        for split in splits:
            current_seq_num = max([file.max_sequence_number for file in split.files])
            existing_max = max_seq_numbers.get(split.bucket, -1)
            if current_seq_num > existing_max:
                max_seq_numbers[split.bucket] = current_seq_num
        return max_seq_numbers
