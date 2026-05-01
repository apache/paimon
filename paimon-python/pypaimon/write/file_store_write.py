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

    def disable_rolling(self):
        """Disable file rolling by setting target_file_size to max."""
        self.options.set(
            CoreOptions.TARGET_FILE_SIZE, str(2 ** 63 - 1))

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
                options=options,
                merge_function=self._build_pk_merge_function())
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

    def _build_pk_merge_function(self):
        """Build the merge function for the in-memory write buffer.

        Shares ``merge_engine_dispatch.build_merge_function`` with the
        read path so the supported engines (deduplicate, partial-update
        with no out-of-scope options) cannot drift between sides.

        For wholly unsupported engines (``aggregation`` / ``first-row``)
        the writer falls back to ``DeduplicateMergeFunction`` so the
        flushed file still maintains the LSM "PK unique within a file"
        invariant. The read path's dispatch still raises
        ``NotImplementedError``, so the user gets an explicit error
        before they observe wrong-engine data; the fallback only
        narrows the damage to "file is deduped, not aggregated"
        rather than the silent multi-row-per-PK corruption that
        existed pre-PR.

        Partial-update with out-of-scope options (sequence-group,
        per-field aggregator, ignore-delete, remove-record-on-*) does
        **not** fall back: ``partial_update_unsupported_options`` sees
        the configured keys and re-raises, so the first
        ``write_arrow`` call (where ``_create_data_writer`` first runs)
        surfaces the error. Silently degrading to dedupe there is the
        same live corruption pattern this PR exists to close.

        ``with_write_type`` (column-subset writes) on a PK table is
        also rejected here. The buffer layout
        ``_add_system_fields`` produces would carry only the subset
        on the value side, while a ``MergeFunction`` such as
        ``PartialUpdateMergeFunction`` is built against the full table
        arity -- the two sides would mismatch on
        ``KeyValue.value.get_field`` and raise ``IndexError`` at
        flush time. Refusing it explicitly avoids that obscure failure
        and keeps the supported surface narrow.

        The value-side schema must match the layout
        ``KeyValueDataWriter`` flushes -- ``_add_system_fields`` keeps
        every original user column on the value side (the primary keys
        are duplicated as ``_KEY_<pk>`` columns to the left of the
        value side). So ``value_arity`` here is ``len(table.fields)``,
        not ``len(table.fields) - len(primary_keys)``.
        """
        from pypaimon.common.merge_engine_dispatch import (
            build_merge_function, partial_update_unsupported_options)
        from pypaimon.common.options.core_options import MergeEngine
        from pypaimon.read.reader.deduplicate_merge_function import \
            DeduplicateMergeFunction

        engine = self.options.merge_engine()
        raw_options = self.options.options.to_map()

        if self.write_cols is not None:
            raise NotImplementedError(
                "with_write_type is not yet supported on primary-key "
                "tables: the writer-side merge buffer assumes the "
                "input batch carries the full table schema. Drop the "
                "with_write_type call or write the missing columns as "
                "nulls in the input batch."
            )

        # PARTIAL_UPDATE + out-of-scope option: never silently fall
        # back -- forward the read-side error verbatim so writes fail
        # before the first flush rather than corrupt the file.
        if engine == MergeEngine.PARTIAL_UPDATE \
                and partial_update_unsupported_options(raw_options):
            return build_merge_function(
                engine=engine, raw_options=raw_options,
                key_arity=len(self.table.trimmed_primary_keys),
                value_arity=len(self.table.table_schema.fields),
                value_field_nullables=[
                    f.type.nullable for f in self.table.table_schema.fields],
            )

        # Catch the dispatch's "wholly unsupported engine" raise only
        # for the engines we know are out of scope today; any other
        # NotImplementedError is a bug we want to surface, not swallow.
        if engine in (MergeEngine.AGGREGATE, MergeEngine.FIRST_ROW):
            return DeduplicateMergeFunction()

        all_value_fields = self.table.table_schema.fields
        return build_merge_function(
            engine=engine, raw_options=raw_options,
            key_arity=len(self.table.trimmed_primary_keys),
            value_arity=len(all_value_fields),
            value_field_nullables=[
                f.type.nullable for f in all_value_fields],
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
