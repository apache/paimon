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

from typing import List, Optional

import pyarrow as pa

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.schema.arrow_schema import (
    arrow_schemas_compatible, normalize_arrow_strings, normalize_arrow_write_nullability)
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.blob import BlobConsumer
from pypaimon.write.row_utils import (
    require_columns,
    row_to_named_values,
    validate_write_columns,
)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_identifier import validate_commit_identifier
from pypaimon.execution.python.write.file_store_write import FileStoreWrite

from pypaimon.write.input_adapter import _pandas_write_batch


class PythonTableWrite:
    """Bound Python writer implementing the common write execution protocol."""

    def __init__(self, writer):
        self.writer = writer

    def initialize(self, table, commit_user, static_partition: Optional[dict] = None,
                   *, restore_snapshot_id=None):
        writer = self.writer
        from pypaimon.table.file_store_table import FileStoreTable

        writer.table: FileStoreTable = table
        writer.table_pyarrow_schema = PyarrowFieldParser.from_paimon_schema(writer.table.table_schema.fields)
        writer.commit_user = commit_user
        writer.static_partition = static_partition
        writer.restore_snapshot_id = restore_snapshot_id
        writer.file_store_write = writer._create_file_store_write(commit_user)
        # Java overwrite writers ignore previous files even without an explicit
        # restore snapshot. HASH restoration is separately disabled by the row
        # key extractor; sequence restoration must also start from empty state.
        writer.file_store_write.restore_snapshot_id = (
            0 if static_partition is not None else restore_snapshot_id)
        if static_partition is not None:
            # An overwrite replaces state, not an input changelog. Java's
            # overwrite commit does not publish changelog manifests; avoid
            # writing unreferenced changelog files in the first place.
            writer.file_store_write.changelog_producer = ChangelogProducer.NONE
        writer.row_key_extractor = writer._create_row_key_extractor(static_partition)

    def _create_file_store_write(self, commit_user):
        writer = self.writer
        return FileStoreWrite(writer.table, commit_user)

    def _create_row_key_extractor(self, static_partition):
        writer = self.writer
        if writer.restore_snapshot_id is not None:
            from pypaimon.execution.python.write.row_key_extractor import DynamicBucketRowKeyExtractor
            return DynamicBucketRowKeyExtractor(
                writer.table.table_schema, table=writer.table,
                ignore_existing=static_partition is not None,
                base_snapshot_id=writer.restore_snapshot_id)
        return writer.table.create_row_key_extractor(
            ignore_existing=static_partition is not None
        )

    def write_arrow(self, data, bucket: Optional[int] = None):
        """Write Arrow data, optionally to a bucket assigned upstream."""
        writer = self.writer
        data = writer._prepare_arrow_schema(data)
        if isinstance(data, pa.RecordBatch):
            return writer.write_arrow_batch(data, bucket)
        for batch in data.to_batches():
            writer.write_arrow_batch(batch, bucket)

    def write_arrow_batch(self, data: pa.RecordBatch, bucket: Optional[int] = None):
        writer = self.writer
        if bucket is not None:
            return writer._write_precomputed_bucket(data, bucket)
        data = writer._prepare_arrow_data(data)
        if data.num_rows == 0:
            return

        groups = writer.row_key_extractor.extract_partition_bucket_groups(data)
        writer.file_store_write.validate_write_columns(
            data.schema.names, [(partition, bucket) for partition, bucket, _ in groups])
        for partition, bucket, row_indices in groups:
            if row_indices is None:
                # Every input row belongs to the same partition/bucket. Passing the
                # original batch through avoids copying large BLOB values through
                # Arrow take before the dedicated BLOB writer consumes them.
                sub_table = data
            else:
                # row_indices is an int64 array of this group's rows in
                # ascending input order (the extractor sorts grouped indices so
                # sequence-number assignment stays latest-wins correct), so the
                # span is just first..last.
                lo = row_indices[0].as_py()
                hi = row_indices[-1].as_py()
                count = len(row_indices)
                if hi - lo + 1 == count:
                    # Distinct row indices spanning exactly `count` values are
                    # contiguous, so share the original Arrow buffers instead of
                    # gathering their rows into newly allocated buffers with take.
                    sub_table = data.slice(lo, count)
                else:
                    sub_table = pa.compute.take(data, row_indices)
            writer._write_partition_bucket_batch(partition, bucket, sub_table)

    def roll_before_group_if_needed(self, row_count: int):
        """Roll current files before the next logical write group if needed."""
        writer = self.writer
        if isinstance(row_count, bool) or not isinstance(row_count, int) \
                or row_count <= 0:
            raise ValueError("Group row count must be a positive integer.")
        writer.file_store_write.roll_before_group_if_needed(row_count)

    def _write_partition_bucket_batch(self, partition, bucket, data):
        writer = self.writer
        writer.file_store_write.write(partition, bucket, data)

    def _write_precomputed_bucket(self, data, bucket):
        """Write one partition/bucket group using normal index maintenance."""
        writer = self.writer
        from pypaimon.table.bucket_mode import BucketMode
        from pypaimon.execution.python.write.row_key_extractor import DynamicBucketRowKeyExtractor

        bucket_mode = writer.table.bucket_mode()
        if bucket_mode not in (BucketMode.HASH_FIXED, BucketMode.HASH_DYNAMIC):
            raise ValueError(
                "Precomputed bucket writes are only valid for HASH_FIXED or "
                "HASH_DYNAMIC tables"
            )
        if not isinstance(writer.row_key_extractor, DynamicBucketRowKeyExtractor):
            if bucket_mode == BucketMode.HASH_DYNAMIC:
                raise RuntimeError("Dynamic bucket extractor is not configured")
        from pypaimon.index.dynamic_bucket import MAX_DYNAMIC_BUCKETS
        upper_bound = MAX_DYNAMIC_BUCKETS if bucket_mode == BucketMode.HASH_DYNAMIC else writer.table.options.bucket()
        if not 0 <= bucket < upper_bound:
            raise ValueError('Bucket id must be between 0 and {}'.format(upper_bound - 1))
        data = writer._prepare_arrow_data(data)
        if bucket_mode == BucketMode.HASH_DYNAMIC:
            partition = writer.row_key_extractor.notify_precomputed_bucket_batch(data, bucket)
        else:
            partitions = writer.row_key_extractor.extract_partitions_batch(data)
            if not partitions:
                return
            partition = tuple(partitions[0])
            if any(tuple(actual) != partition for actual in partitions):
                raise ValueError('A precomputed bucket group contained multiple partitions')
        if partition is None:
            return
        writer._write_partition_bucket_batch(partition, bucket, data)

    def write_row(self, row):
        writer = self.writer
        values_by_name = row_to_named_values(row, writer.table.table_schema.fields)
        require_columns(values_by_name, writer.table.partition_keys, "write_row")
        from pypaimon.write.row_kind import skip_write_row
        write_cols = (writer.file_store_write.write_cols
                      if set(values_by_name) == set(writer.table.field_names) else list(values_by_name))
        if skip_write_row(writer.table, values_by_name, row.get_row_kind(), write_cols):
            return
        partition, bucket = (
            writer.row_key_extractor.extract_partition_bucket_row(values_by_name)
        )
        writer._write_partition_bucket_row(
            partition, bucket, row, values_by_name
        )

    def _write_partition_bucket_row(
        self, partition, bucket, row, values_by_name
    ):
        writer = self.writer
        writer.file_store_write.write_row(
            partition, bucket, row, values_by_name
        )

    def write_pandas(self, dataframe):
        writer = self.writer
        write_cols = writer.file_store_write.write_cols
        if write_cols is not None:
            # Column-subset write (append-only ``with_write_type``): build the
            # RecordBatch against the subset schema so the input only needs the
            # written columns, mirroring the ``write_arrow`` path.
            pa_schema = writer._write_cols_pyarrow_schema(write_cols)
        else:
            pa_schema = writer.table_pyarrow_schema
        record_batch = _pandas_write_batch(
            getattr(writer, 'table', None), dataframe, pa_schema, writer.table_pyarrow_schema)
        return writer.write_arrow_batch(record_batch)

    def with_write_type(self, write_cols: List[str]):
        writer = self.writer
        write_cols = validate_write_columns(write_cols)
        for col in write_cols:
            if col not in writer.table_pyarrow_schema.names:
                raise ValueError(f"Column {col} is not in table schema.")
        full_names = writer.table_pyarrow_schema.names
        # Java preserves the declared append column order in writeCols. PK
        # writes still require the complete table layout in the Python buffer.
        full_pk_type = (writer.table.is_primary_key_table and len(write_cols) == len(full_names)
                        and set(write_cols) == set(full_names))
        if write_cols == full_names or full_pk_type:
            write_cols = None
        writer.file_store_write.write_cols = write_cols
        return self

    def with_blob_consumer(self, blob_consumer: BlobConsumer):
        writer = self.writer
        if blob_consumer is not None and not callable(blob_consumer):
            raise TypeError('blob_consumer must be callable')
        writer.file_store_write.blob_consumer = blob_consumer
        return self

    def with_blob_uri_reader_factory(self, uri_reader_factory):
        writer = self.writer
        if uri_reader_factory is not None and not callable(getattr(uri_reader_factory, 'create', None)):
            raise TypeError('URI reader factory must provide callable create')
        if writer.file_store_write._ever_written:
            raise RuntimeError(
                "with_blob_uri_reader_factory must be called before any "
                "write operation."
            )
        writer.file_store_write.blob_uri_reader_factory = uri_reader_factory
        return self

    def close(self):
        writer = self.writer
        try:
            writer.file_store_write.close()
        finally:
            writer._release_prepared_indexes()

    def abort(self):
        writer = self.writer
        try:
            writer.file_store_write.abort()
        finally:
            abort = getattr(writer.row_key_extractor, "abort", None)
            if abort is not None:
                abort()

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        commit_identifier = validate_commit_identifier(commit_identifier)
        writer = self.writer
        commit_messages = writer.file_store_write._prepare_commit_messages(commit_identifier)
        prepare_indexes = getattr(writer.row_key_extractor, "prepare_commit", None)
        index_changes = prepare_indexes() if prepare_indexes is not None else {}
        messages_by_bucket = {
            (tuple(message.partition), message.bucket): message
            for message in commit_messages
        }
        for (partition, bucket), changes in index_changes.items():
            message = messages_by_bucket.get((partition, bucket))
            if message is None:
                message = CommitMessage(
                    partition=partition,
                    bucket=bucket,
                    new_files=[],
                )
                commit_messages.append(message)
                messages_by_bucket[(partition, bucket)] = message
            message.index_adds.extend(changes.additions)
            message.index_deletes.extend(changes.deletions)
        # Hand off data, changelog and HASH indexes together. Until every
        # preparation succeeds, retain the metadata for retry and abort.
        writer.file_store_write._release_prepared_files()
        writer._release_prepared_indexes()
        return commit_messages

    def _release_prepared_indexes(self) -> None:
        writer = self.writer
        release = getattr(writer.row_key_extractor, "release_prepared", None)
        if release is not None:
            release()

    def _prepare_arrow_data(self, data):
        writer = self.writer
        data = writer._prepare_arrow_schema(data)
        # Schema-only writers can normalize input without table options.
        table = getattr(writer, 'table', None)
        if table is None:
            return data
        data = writer._normalize_arrow_write_schema(data)
        from pypaimon.write.row_kind import filter_write_batch
        write_cols = (writer.file_store_write.write_cols
                      if data.schema.names == writer.table_pyarrow_schema.names else data.schema.names)
        data = filter_write_batch(table, data, write_cols)
        if not data.num_rows:
            return data
        data = normalize_arrow_strings(data)
        target = writer.table_pyarrow_schema
        if data.schema.names != target.names:
            target = writer._write_cols_pyarrow_schema(data.schema.names)
        # Validate only surviving values. Keep the input transport metadata:
        # PyArrow otherwise rejects hidden NULL children of NULL ROW parents.
        validated = normalize_arrow_write_nullability(data, target)
        writer._validate_pyarrow_schema(validated.schema)
        return data

    def _prepare_arrow_schema(self, data):
        # Legacy schema-only callers have no row-kind options to evaluate.
        # A real writer must defer casts until ignored events have been removed.
        writer = self.writer
        if getattr(writer, 'table', None) is None:
            data = normalize_arrow_strings(data)
        writer._validate_pyarrow_schema(
            data.schema, check_nested_nullability=getattr(writer, 'table', None) is None)
        return data

    def _normalize_arrow_write_schema(self, data):
        # Compatible input batches may differ in top-level nullability or
        # metadata. Buffered writes and row-to-Arrow conversion must use the
        # same declared fields, while keeping normalized Arrow array types.
        writer = self.writer
        fields = []
        for field in data.schema:
            declared = writer.table_pyarrow_schema.field(field.name)
            fields.append(pa.field(field.name, field.type, nullable=declared.nullable,
                                   metadata=declared.metadata))
        schema = pa.schema(fields, metadata=writer.table_pyarrow_schema.metadata)
        from pypaimon.write.row_kind import write_value_schema
        schema = write_value_schema(writer.table, schema)
        if data.schema.equals(schema, check_metadata=True):
            return data
        if isinstance(data, pa.RecordBatch):
            return pa.RecordBatch.from_arrays(data.columns, schema=schema)
        return pa.Table.from_arrays(data.columns, schema=schema)

    def _validate_pyarrow_schema(self, data_schema: pa.Schema, *, check_nested_nullability=True):
        writer = self.writer
        if writer._is_compatible_pyarrow_schema(
                data_schema, writer.table_pyarrow_schema, check_nested_nullability=check_nested_nullability):
            return

        write_cols = writer.file_store_write.write_cols
        if write_cols is not None:
            write_cols_schema = writer._write_cols_pyarrow_schema(write_cols)
            if writer._is_compatible_pyarrow_schema(
                    data_schema, write_cols_schema, check_nested_nullability=check_nested_nullability):
                return

        # Existing bucket writers keep their input layout across checkpoints.
        # Their requested columns can differ from optimized physical writeCols.
        if isinstance(writer.file_store_write, FileStoreWrite):
            for columns in writer.file_store_write._write_columns_by_bucket.values():
                if columns is not None and writer._is_compatible_pyarrow_schema(
                        data_schema, writer._write_cols_pyarrow_schema(columns),
                        check_nested_nullability=check_nested_nullability):
                    return

        writer._raise_inconsistent_schema(data_schema)

    def _is_compatible_pyarrow_schema(
            self, data_schema: pa.Schema, expected_schema: pa.Schema, *, check_nested_nullability=True) -> bool:
        return arrow_schemas_compatible(
            data_schema, expected_schema, check_top_level_nullability=False,
            allow_binary_compatibility=True, check_nested_nullability=check_nested_nullability)

    def _write_cols_pyarrow_schema(self, write_cols: List[str]) -> pa.Schema:
        writer = self.writer
        table_fields = {
            field.name: field for field in writer.table_pyarrow_schema
        }
        return pa.schema([table_fields[col] for col in write_cols])

    def _raise_inconsistent_schema(self, data_schema: pa.Schema):
        writer = self.writer
        raise ValueError(f"Input schema isn't consistent with table schema and write cols. "
                         f"Input schema is: {data_schema} "
                         f"Table schema is: {writer.table_pyarrow_schema} "
                         f"Write cols is: {writer.file_store_write.write_cols}")
