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

import logging
import random
from typing import Dict, List, Tuple

import pyarrow as pa


logger = logging.getLogger(__name__)

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import is_blob_file_field
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import row_values_to_arrow_table
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.write.writer.dedicated_format_writer import DedicatedFormatWriter
from pypaimon.write.writer.data_vector_writer import DataVectorWriter
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter
from pypaimon.write.writer.postpone_data_writer import PostponeDataWriter
from pypaimon.table.bucket_mode import BucketMode


class FileStoreWrite:
    """Base class for file store write operations."""

    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable
        from pypaimon.common.options.core_options import MergeEngine
        from pypaimon.read.merge_engine_support import check_sequence_field_supported

        # TableWrite constructs this before the row-key extractor, whose
        # dynamic bucket index must not retain hashes for rejected writes.
        check_sequence_field_supported(table)
        if table.is_primary_key_table and table.options.merge_engine() == MergeEngine.AGGREGATE:
            from pypaimon.read.merge_engine_support import check_supported

            check_supported(table)

        self.table: FileStoreTable = table
        self.data_writers: Dict[Tuple, DataWriter] = {}
        self._runtime_total_buckets: Dict[Tuple, int] = {}
        self.max_seq_numbers: dict = {}
        self.restore_snapshot_id = None
        self.write_cols = None
        self.blob_consumer = None
        self.blob_uri_reader_factory = None
        self.commit_identifier = 0
        self.options = CoreOptions.copy(table.options)
        self.changelog_producer = self.options.changelog_producer()
        self._configure_data_file_prefix(commit_user)

    def _configure_data_file_prefix(self, commit_user):
        if self.table.bucket_mode() == BucketMode.POSTPONE_MODE:
            self.options.set(CoreOptions.DATA_FILE_PREFIX,
                             (f"{self.options.data_file_prefix()}-u-{commit_user}"
                              f"-s-{random.randint(0, 2 ** 31 - 2)}-w-"))

    def disable_rolling(self):
        """Disable size- and row-based file rolling."""
        max_value = CoreOptions.TARGET_FILE_ROW_NUM.default_value()
        self.options.set(
            CoreOptions.TARGET_FILE_SIZE, str(max_value))
        self.options.set(
            CoreOptions.TARGET_FILE_ROW_NUM, str(max_value))

    def write(
        self,
        partition: Tuple,
        bucket: int,
        data: pa.RecordBatch,
        total_buckets=None,
    ):
        self._check_runtime_bucket(partition, bucket, total_buckets)
        key = (partition, bucket)
        if key not in self.data_writers:
            self.data_writers[key] = self._create_data_writer(partition, bucket, self.options)
        writer = self.data_writers[key]
        writer.write(data)

    def write_row(
        self,
        partition: Tuple,
        bucket: int,
        row,
        values_by_name: dict,
        total_buckets=None,
    ):
        self._check_runtime_bucket(partition, bucket, total_buckets)
        key = (partition, bucket)
        if key not in self.data_writers:
            self.data_writers[key] = self._create_data_writer(partition, bucket, self.options)
        writer = self.data_writers[key]
        if hasattr(writer, 'write_row'):
            writer.write_row(row)
            return

        column_names = (
            self.write_cols
            if self.write_cols is not None
            else list(self.table.field_names)
        )
        data = row_values_to_arrow_table(
            values_by_name,
            self.table.table_schema.fields,
            column_names,
        )
        from pypaimon.write.row_kind import with_row_kind
        data = with_row_kind(self.table, data, row)
        writer.write(data.to_batches()[0])

    def roll_before_group_if_needed(self, row_count: int):
        for writer in self.data_writers.values():
            if isinstance(writer, DedicatedFormatWriter):
                writer.roll_before_group_if_needed(row_count)

    def _check_runtime_bucket(self, partition, bucket, total_buckets):
        if total_buckets is None:
            return
        if (isinstance(total_buckets, bool)
                or not isinstance(total_buckets, int)
                or total_buckets <= 0):
            raise ValueError("Total number of buckets must be positive")
        if bucket < 0 or bucket >= total_buckets:
            raise ValueError(
                "Bucket {} is out of range [0, {})".format(
                    bucket, total_buckets
                )
            )

        partition = tuple(partition)
        previous = self._runtime_total_buckets.get(partition)
        if previous is not None and previous != total_buckets:
            raise RuntimeError(
                "Try to write partition {} with a new bucket num {}, but "
                "the previous bucket num is {}.".format(
                    partition, total_buckets, previous
                )
            )
        self._runtime_total_buckets[partition] = total_buckets

    def _create_data_writer(self, partition: Tuple, bucket: int, options: CoreOptions) -> DataWriter:
        row_limit = options.target_file_row_num()
        max_value = CoreOptions.TARGET_FILE_ROW_NUM.default_value()
        if row_limit < 1:
            raise ValueError(
                "target-file-row-num should be at least 1")
        if row_limit > max_value:
            raise ValueError(
                f"target-file-row-num should be at most {max_value}")
        if row_limit != max_value:
            row_rolling_supported = (
                (self.table.options.data_evolution_enabled()
                 and not self.table.is_primary_key_table)
                or bucket == BucketMode.POSTPONE_BUCKET.value)
            if not row_rolling_supported:
                raise NotImplementedError(
                    "target-file-row-num is set on this table but pypaimon supports row-count "
                    "based file rolling only for data-evolution append and postpone tables; "
                    "unset it or write with Java/Flink/Spark.")

        def max_seq_number():
            default = -1 if self.table.is_primary_key_table else 1
            return self._seq_number_stats(partition).get(bucket, default)

        # Dedicated Blob files are an append-table layout. PK tables require
        # managed packs and references attached to their key-value Parquet files.
        if self._has_blob_columns():
            if self.table.is_primary_key_table:
                raise NotImplementedError(
                    'Primary-key Blob writes require the native writer; '
                    'enable write.native.enabled and write Arrow batches')
            return DedicatedFormatWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=0,
                options=options,
                write_cols=self.write_cols,
                blob_consumer=self.blob_consumer,
                changelog_producer=self.changelog_producer,
                blob_uri_reader_factory=self.blob_uri_reader_factory,
            )
        elif self._has_vector_columns() and options.with_vector_format():
            return DataVectorWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=0,
                options=options,
                write_cols=self.write_cols,
            )
        elif self.table.is_primary_key_table:
            writer_type = (PostponeDataWriter if bucket == BucketMode.POSTPONE_BUCKET.value
                           else KeyValueDataWriter)
            return writer_type(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=(0 if bucket == BucketMode.POSTPONE_BUCKET.value else max_seq_number()),
                options=options,
                merge_function=self._build_pk_merge_function(),
                changelog_producer=self.changelog_producer)
        else:
            seq_number = 0 if self.table.bucket_mode() == BucketMode.BUCKET_UNAWARE else max_seq_number()
            return AppendOnlyDataWriter(
                table=self.table,
                partition=partition,
                bucket=bucket,
                max_seq_number=seq_number,
                options=options,
                write_cols=self.write_cols,
                changelog_producer=self.changelog_producer
            )

    def _build_pk_merge_function(self):
        """Build the merge function for the in-memory write buffer.

        Shares ``merge_engine_dispatch.build_merge_function`` with the
        read path so the supported engines (deduplicate, first-row,
        partial-update with no out-of-scope options) cannot drift
        between sides.

        Aggregation options are validated with the read-side guard at writer
        construction. Unsupported configurations must not be committed using
        fallback merge semantics that discard input values.

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
                value_field_names=[
                    f.name for f in self.table.table_schema.fields],
            )

        if engine == MergeEngine.AGGREGATE:
            from pypaimon.read.reader.aggregation_merge_function import (
                AggregateMergeFunction, build_field_aggregators)
            fields = self.table.table_schema.fields
            return AggregateMergeFunction(
                key_arity=len(self.table.trimmed_primary_keys),
                value_arity=len(fields),
                field_aggregators=build_field_aggregators(
                    fields, self.table.primary_keys, self.options))

        all_value_fields = self.table.table_schema.fields
        return build_merge_function(
            engine=engine, raw_options=raw_options,
            key_arity=len(self.table.trimmed_primary_keys),
            value_arity=len(all_value_fields),
            value_field_nullables=[
                f.type.nullable for f in all_value_fields],
            value_field_names=[f.name for f in all_value_fields],
        )

    def _has_blob_columns(self) -> bool:
        """Check if the table schema contains blob columns."""
        return any(is_blob_file_field(field) for field in self.table.table_schema.fields)

    def _has_vector_columns(self) -> bool:
        from pypaimon.schema.data_types import VectorType
        return any(isinstance(f.type, VectorType) for f in self.table.table_schema.fields)

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        messages = self._prepare_commit_messages(commit_identifier)
        self._release_prepared_files()
        return messages

    def _prepare_commit_messages(self, commit_identifier) -> List[CommitMessage]:
        """Collect files while their parent is still preparing the complete increment."""
        self.commit_identifier = commit_identifier
        commit_messages = []
        # A BlobConsumer owns the pack bytes. prepare_commit then releases the
        # writer's copies, so commit abort must leave those ``.blob`` files alone.
        preserve_blob_files = self.blob_consumer is not None
        for (partition, bucket), writer in self.data_writers.items():
            committed_files = writer.prepare_commit()
            changelog_files = writer.prepare_changelog_commit()
            if committed_files or changelog_files:
                commit_message = CommitMessage(
                    partition=partition,
                    bucket=bucket,
                    new_files=committed_files,
                    changelog_files=changelog_files,
                    total_buckets=self._runtime_total_buckets.get(partition),
                    preserve_blob_files_on_abort=preserve_blob_files,
                )
                commit_messages.append(commit_message)
        return commit_messages

    def _release_prepared_files(self):
        # Hand off only after every partition prepared successfully. Until
        # then, close/abort must still clean up files from earlier partitions.
        for writer in self.data_writers.values():
            writer._release_prepared_files()

    def close(self):
        """Close all data writers and clean up resources."""
        for writer in self.data_writers.values():
            writer.close()
        self.data_writers.clear()
        self._runtime_total_buckets.clear()

    def abort(self):
        """Abort all data writers and clean up files produced by this write."""
        for writer in self.data_writers.values():
            try:
                writer.abort()
            except Exception as e:
                logger.warning("Failed to abort data writer.", exc_info=e)
        self.data_writers.clear()
        self._runtime_total_buckets.clear()

    def _seq_number_stats(self, partition: Tuple) -> Dict[int, int]:
        buckets = self.max_seq_numbers.get(partition)
        if buckets is None:
            buckets = self._load_seq_number_stats(partition)
            self.max_seq_numbers[partition] = buckets
        return buckets

    def _sequence_read_table(self):
        if self.restore_snapshot_id is not None:
            snapshot = None
            if self.restore_snapshot_id != 0:
                snapshot = self.table.snapshot_manager().get_snapshot_by_id(self.restore_snapshot_id)
                if snapshot is None:
                    raise ValueError("Snapshot id '{}' doesn't exist".format(self.restore_snapshot_id))
            # Replace inherited selectors and mode, including an explicit empty
            # view. plan_for_write still validates row filters and column masks.
            return self.table._copy_with_snapshot(snapshot)
        return self.table

    def _load_seq_number_stats(self, partition: Tuple) -> dict:
        read_builder = self._sequence_read_table().new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        sub_predicates = []
        for key, value in zip(self.table.partition_keys, partition):
            sub_predicates.append(predicate_builder.equal(key, value))
        partition_filter = predicate_builder.and_predicates(sub_predicates)

        scan = read_builder.with_filter(partition_filter).new_scan()
        splits = scan.plan_for_write().splits()

        max_seq_numbers = {}
        for split in splits:
            current_seq_num = max([file.max_sequence_number for file in split.files])
            existing_max = max_seq_numbers.get(split.bucket, -1)
            if current_seq_num > existing_max:
                max_seq_numbers[split.bucket] = current_seq_num
        return max_seq_numbers


class PostponeFixedBucketFileStoreWrite(FileStoreWrite):
    """File store write with runtime bucket counts for postpone tables."""

    def __init__(self, table, commit_user):
        super().__init__(table, commit_user)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        self._check_from_snapshot = snapshot.id if snapshot is not None else 0

    def _sequence_read_table(self):
        return self.table.copy({'scan.snapshot-id': str(self._check_from_snapshot)})

    def _load_seq_number_stats(self, partition):
        if self._check_from_snapshot == 0:
            return {}
        return super()._load_seq_number_stats(partition)

    def _prepare_commit_messages(self, commit_identifier):
        messages = super()._prepare_commit_messages(commit_identifier)
        for message in messages:
            message.check_from_snapshot = self._check_from_snapshot
        return messages

    def _configure_data_file_prefix(self, commit_user):
        pass
