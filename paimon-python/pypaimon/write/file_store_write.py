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
from pypaimon.write.writer.data_blob_writer import DataBlobWriter
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter


class FileStoreWrite:
    """Base class for file store write operations."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.data_writers: Dict[Tuple, DataWriter] = {}
        self.max_seq_numbers = self._seq_number_stats()  # TODO: build this on-demand instead of on all
        self.write_cols = None

    def write(self, partition: Tuple, bucket: int, data: pa.RecordBatch):
        key = (partition, bucket)
        if key not in self.data_writers:
            self.data_writers[key] = self._create_data_writer(partition, bucket)
        writer = self.data_writers[key]
        writer.write(data)

    def _create_data_writer(self, partition: Tuple, bucket: int) -> DataWriter:
        # Check if table has blob columns
        if self._has_blob_columns():
            return DataBlobWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=self.max_seq_numbers.get((partition, bucket), 1),
            )
        elif self.table.is_primary_key_table:
            return KeyValueDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=self.max_seq_numbers.get((partition, bucket), 1),
            )
        else:
            return AppendOnlyDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=self.max_seq_numbers.get((partition, bucket), 1),
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

    def _seq_number_stats(self) -> dict:
        from pypaimon.manifest.manifest_file_manager import ManifestFileManager
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        snapshot_manager = SnapshotManager(self.table)
        manifest_list_manager = ManifestListManager(self.table)
        manifest_file_manager = ManifestFileManager(self.table)

        latest_snapshot = snapshot_manager.get_latest_snapshot()
        if not latest_snapshot:
            return {}
        manifest_files = manifest_list_manager.read_all(latest_snapshot)

        file_entries = []
        for manifest_file in manifest_files:
            manifest_entries = manifest_file_manager.read(manifest_file.file_name)
            for entry in manifest_entries:
                if entry.kind == 0:
                    file_entries.append(entry)

        max_seq_numbers = {}
        for entry in file_entries:
            partition_key = (tuple(entry.partition.values), entry.bucket)
            current_seq_num = entry.file.max_sequence_number
            existing_max = max_seq_numbers.get(partition_key, -1)
            if current_seq_num > existing_max:
                max_seq_numbers[partition_key] = current_seq_num
        return max_seq_numbers
