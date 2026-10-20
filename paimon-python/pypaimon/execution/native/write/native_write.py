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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Optional Rust data writer behind PyPaimon's batch and stream builders."""

import pyarrow as pa

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.schema.arrow_schema import arrow_schemas_compatible
from pypaimon.schema.data_types import PyarrowFieldParser, is_blob_file_field
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.execution.native.write.native_commit import (
    create_native_write_table, from_native_commit_messages,
)
from pypaimon.write.row_utils import require_columns, row_to_named_values, row_values_to_arrow_table


def create_native_write(table, commit_user, static_partition=None, stream=False,
                        *, fixed_bucket=False, bucket_plan=None, restore_snapshot_id=None):
    """Return a native writer if the table can use the filesystem write path."""
    # DV tables flush normal level-0 files using the configured merge engine.
    # Batch visibility is controlled by deletion-vectors.merge-on-read.
    if (not native_runtime_available()
            or table.bucket_mode() not in (BucketMode.HASH_FIXED, BucketMode.HASH_DYNAMIC,
                                           BucketMode.BUCKET_UNAWARE,
                                           BucketMode.CROSS_PARTITION,
                                           BucketMode.POSTPONE_MODE)
            # The format matters only when this write emits INPUT changelogs.
            # Batch overwrites suppress them because they cannot be committed.
            or (table.options.changelog_file_format() not in (None, 'parquet')
                and table.options.changelog_producer() == ChangelogProducer.INPUT
                and (stream or static_partition is None))
            or table.options.file_format() != 'parquet'
            or table.options.video_frame_fields()):
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    if fixed_bucket:
        builder = native_table.new_postpone_fixed_bucket_write_builder()._with_commit_user(commit_user)
        if bucket_plan is not None:
            builder = builder.with_bucket_plan(bucket_plan.to_arrow(table))
        if static_partition is not None:
            builder = builder.with_overwrite(static_partition)
        return NativePostponeFixedBucketTableWrite(
            table, commit_user, static_partition, bucket_plan, builder.new_write())
    if stream:
        builder = native_table.new_stream_write_builder().with_commit_user(commit_user)
    else:
        builder = native_table.new_batch_write_builder()._with_commit_user(commit_user)
        if static_partition is not None:
            builder = builder.with_overwrite(static_partition)
    if restore_snapshot_id is not None:
        builder.with_restore_snapshot(restore_snapshot_id)
    return NativeTableWrite(table, commit_user, static_partition, stream,
                            builder.new_write(), restore_snapshot_id)


class NativeTableWrite:
    """Use Rust for Arrow batches while retaining PyPaimon's commit-message API.

    Advanced Python writer methods switch to the Python writer before the first
    native data write. Once Rust has written data, switching would split one
    logical write across two writers, so it is rejected.
    """

    def __init__(self, table, commit_user, static_partition, stream, native_writer,
                 restore_snapshot_id=None):
        self.table = table
        self.commit_user = commit_user
        self.static_partition = static_partition
        self.stream = stream
        self._native_writer = native_writer
        self._python_writer = None
        self._written = False
        self._table_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        self._schema = self._table_schema
        self._write_cols = None
        self._blob_consumer = None
        self._blob_uri_reader_factory = None
        self._restore_snapshot_id = restore_snapshot_id
        self._blob_rows = None
        self._defer_blob_input = False
        if any(is_blob_file_field(field) for field in table.table_schema.fields):
            from pypaimon.execution.native.write.native_blob_rows import NativeBlobRows
            self._blob_rows = NativeBlobRows()
            self._native_writer.with_blob_uri_reader_factory(self._blob_rows)

    def _switch_to_python(self):
        from pypaimon.execution.dispatch import switch_writer_to_python
        return switch_writer_to_python(self)

    def _new_python_writer(self):
        from pypaimon.execution.dispatch import create_python_write
        writer = create_python_write(
            self.table, self.commit_user, self.static_partition, self.stream,
            restore_snapshot_id=self._restore_snapshot_id)
        return self._configure_python_writer(writer)

    def _configure_python_writer(self, writer):
        if self._write_cols is not None:
            writer.with_write_type(self._write_cols)
        if self._blob_consumer is not None:
            writer.with_blob_consumer(self._blob_consumer)
        if self._blob_uri_reader_factory is not None:
            writer.with_blob_uri_reader_factory(self._blob_uri_reader_factory)
        return writer

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._switch_to_python(), name)

    def with_write_type(self, write_cols):
        if self._python_writer is not None:
            return self._python_writer.with_write_type(write_cols)
        names = list(write_cols)
        if self.table.is_primary_key_table:
            if len(names) == len(self._table_schema.names) and set(names) == set(self._table_schema.names):
                # Python treats a complete PK write type as the unchanged table
                # schema. It does not configure a PK column-subset writer.
                return self
            # Core's write type is an append-table operation. Keep the existing
            # Python writer contract for PK setters before any data is staged.
            return self._switch_to_python().with_write_type(names)
        self._native_writer.with_write_type(names)
        self._schema = pa.schema([self._table_schema.field(name) for name in names])
        self._write_cols = names
        return self

    def with_blob_consumer(self, blob_consumer):
        if self._python_writer is not None:
            return self._python_writer.with_blob_consumer(blob_consumer)
        if blob_consumer is None:
            self._native_writer.with_blob_consumer(None)
            self._blob_consumer = None
            return self
        if not callable(blob_consumer):
            raise TypeError('blob_consumer must be callable')
        from pypaimon.table.row.blob import BlobDescriptor

        def consume(field_name, encoded):
            descriptor = None if encoded is None else BlobDescriptor.deserialize(encoded)
            return blob_consumer(field_name, descriptor)

        self._native_writer.with_blob_consumer(consume)
        self._blob_consumer = blob_consumer
        return self

    def with_blob_uri_reader_factory(self, uri_reader_factory):
        if self._python_writer is not None:
            return self._python_writer.with_blob_uri_reader_factory(uri_reader_factory)
        if self._blob_rows is not None:
            if uri_reader_factory is not None and not callable(getattr(uri_reader_factory, 'create', None)):
                raise TypeError('URI reader factory must provide callable create')
            # Only core knows whether RowKind filtering accepted any rows or a
            # postpone writer buffered data. Validate before changing the live
            # bridge's fallback, including when clearing the application factory.
            self._native_writer.with_blob_uri_reader_factory(self._blob_rows)
            self._blob_rows.fallback = uri_reader_factory
        else:
            self._native_writer.with_blob_uri_reader_factory(uri_reader_factory)
        self._blob_uri_reader_factory = uri_reader_factory
        return self

    def write_arrow(self, data, bucket=None):
        if self._python_writer is not None:
            return self._python_writer.write_arrow(data, bucket)
        data = self._prepare_native_arrow_data(data)
        if isinstance(data, pa.RecordBatch):
            return self.write_arrow_batch(data, bucket)
        for batch in data.to_batches():
            self.write_arrow_batch(batch, bucket)

    def write_arrow_batch(self, data, bucket=None):
        if self._python_writer is not None:
            return self._python_writer.write_arrow_batch(data, bucket)
        data = self._prepare_native_arrow_data(data)
        self._write_native_batch(data, bucket)

    def _write_native_batch(self, data, bucket=None):
        if data.num_rows:
            # A failed native write may already have produced files. Never
            # retry that batch through Python after this point.
            self._written = True
        self._native_writer.write_arrow(data, bucket)

    def _prepare_native_arrow_data(self, data):
        # Core resolves row kinds and filters ignored events before inspecting
        # payload values. This facade checks only the logical Arrow schema.
        compatible = dict(check_top_level_nullability=False, allow_binary_compatibility=True,
                          check_nested_nullability=False)
        if not (arrow_schemas_compatible(data.schema, self._schema, **compatible)
                or (self._write_cols is not None
                    and arrow_schemas_compatible(data.schema, self._table_schema, **compatible))):
            raise ValueError(
                "Input schema isn't consistent with table schema and write cols. "
                f"Input schema is: {data.schema} Table schema is: {self._schema} "
                f"Write cols is: {self._write_cols}")
        # Full input can carry routing columns outside the physical write type.
        # Core routes rows first and writes only the selected columns.
        return data

    def write_pandas(self, dataframe):
        if self._python_writer is not None:
            return self._python_writer.write_pandas(dataframe)
        from pypaimon.write.input_adapter import _pandas_write_batch
        self.write_arrow_batch(_pandas_write_batch(self.table, dataframe, self._schema, self._table_schema))

    def write_ray(self, dataset, overwrite=False, concurrency=None, ray_remote_args=None,
                  hash_fixed_precluster='auto', static_partition=None):
        if self._python_writer is not None:
            return self._python_writer.write_ray(
                dataset, overwrite, concurrency, ray_remote_args, hash_fixed_precluster, static_partition)
        from pypaimon.write.input_adapter import _write_ray
        _write_ray(self, dataset, overwrite, concurrency, ray_remote_args,
                   hash_fixed_precluster, static_partition)

    def _distributed_write_options(self, *, overwrite=False):
        return {}

    def roll_before_group_if_needed(self, row_count):
        if self._python_writer is not None:
            return self._python_writer.roll_before_group_if_needed(row_count)
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
            raise ValueError('Group row count must be a positive integer.')
        # Python's group boundary only affects packed video writers. Native
        # selection excludes video, so ordinary Parquet/BLOB writes do not roll.

    def write_row(self, row):
        if self._python_writer is not None:
            return self._python_writer.write_row(row)
        values = row_to_named_values(row, self.table.table_schema.fields)
        names = self._schema.names
        require_columns(values, names, 'write_row')
        require_columns(values, self.table.partition_keys, 'write_row')
        from pypaimon.write.row_kind import skip_write_row, with_row_kind
        if skip_write_row(self.table, values, row.get_row_kind(), self._write_cols):
            return
        if self._write_cols is not None:
            routing = list(self.table.partition_keys)
            if ((self.table.bucket_mode() == BucketMode.HASH_FIXED and self.table.options.bucket() > 1)
                    or isinstance(self, NativePostponeFixedBucketTableWrite)):
                routing.extend(self.table.table_schema.bucket_keys)
            row_kind_field = self.table.options.options.to_map().get('rowkind.field')
            if row_kind_field is not None:
                routing.append(row_kind_field)
            require_columns(values, routing, 'write_row')
            if not set(routing).issubset(names):
                retained = set(names) | set(routing)
                names = self._table_schema.names
                # Only routing and written values need transport. In particular,
                # an unwritten Blob object must not be opened or encoded.
                values = {name: values.get(name) if name in retained else None for name in names}
        try:
            data = (self._blob_rows.to_batch(self.table, [values], names)
                    if self._blob_rows is not None else
                    self._prepare_native_arrow_data(row_values_to_arrow_table(
                        values, self.table.table_schema.fields, names).to_batches()[0]))
            self._write_native_batch(with_row_kind(self.table, data, row))
        finally:
            # Core externalizes Blob values during write, before buffering PK
            # records. Their live Python objects need not survive the call.
            if self._blob_rows is not None:
                self._blob_rows.release_consumed()
                if not self._defer_blob_input:
                    self._blob_rows.clear()

    def prepare_commit(self, commit_identifier=None):
        if self._python_writer is not None:
            if self.stream:
                return self._python_writer.prepare_commit(commit_identifier)
            return self._python_writer.prepare_commit()
        if self.stream:
            if commit_identifier is None:
                raise TypeError('StreamTableWrite.prepare_commit requires an identifier')
            messages = self._native_writer.prepare_commit(True, commit_identifier)
        else:
            if commit_identifier is not None:
                raise TypeError('BatchTableWrite.prepare_commit accepts no identifier')
            try:
                messages = self._native_writer.prepare_commit()
            finally:
                if self._blob_rows is not None and self._defer_blob_input:
                    self._blob_rows.clear()
        return from_native_commit_messages(self.table, messages)

    def close(self):
        if self._python_writer is not None:
            self._python_writer.close()
        elif self._native_writer is not None:
            self._native_writer.close()
            self._native_writer = None
        if self._blob_rows is not None:
            self._blob_rows.clear()

    def abort(self):
        if self._python_writer is not None:
            self._python_writer.abort()
        else:
            # Closing Rust cleans only files still owned by its writer.
            # Never delete prepared CommitMessage files: publication can
            # succeed even when its response raises an exception.
            self.close()


class NativePostponeFixedBucketTableWrite(NativeTableWrite):
    """Fixed-bucket batches retain the standard native ownership contract.

    A distributed coordinator can use Python planning before receiving data;
    workers use the same shared plan through the Rust fixed-bucket writer.
    """

    def __init__(self, table, commit_user, static_partition, bucket_plan, native_writer):
        super().__init__(table, commit_user, static_partition, False, native_writer)
        self._bucket_plan = bucket_plan
        # Automatic planning stages input until all partition sizes are known.
        self._defer_blob_input = (bucket_plan is None
                                  and table.options.postpone_default_bucket_num() is None)

    def _new_python_writer(self):
        from pypaimon.execution.dispatch import create_python_write
        return self._configure_python_writer(create_python_write(
            self.table, self.commit_user, self.static_partition,
            fixed_bucket=True, bucket_plan=self._bucket_plan))

    def _distributed_write_options(self, *, overwrite=False):
        from pypaimon.write.input_adapter import postpone_distributed_write_options
        return postpone_distributed_write_options(self.table, self._bucket_plan, overwrite)
