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

"""Writer backend selection and lifecycle, outside either execution backend."""

from typing import List

import pyarrow as pa

from pypaimon.write.row_utils import validate_write_columns
from pypaimon.write.commit_identifier import validate_commit_identifier
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage


class NativeTableWrite:
    """Keep one selected writer and allow explicit pre-mutation diagnostics."""

    def __new__(cls, *args, **kwargs):
        if cls is NativeTableWrite:
            stream = kwargs.get('stream', args[3] if len(args) > 3 else False)
            cls = NativeStreamTableWrite if stream else NativeBatchTableWrite
        return object.__new__(cls)

    def __init__(self, table, commit_user, static_partition, stream, native_writer,
                 restore_snapshot_id=None):
        from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite

        self._execution = NativeWrite(table, commit_user, static_partition, stream,
                                      native_writer, restore_snapshot_id)
        self._python_writer = None

    @classmethod
    def from_native(cls, native):
        from pypaimon.execution.native.write.native_write import (
            NativePostponeFixedBucketTableWrite as NativePostponeWrite,
        )

        if isinstance(native, NativePostponeWrite):
            selected = NativePostponeFixedBucketTableWrite
        else:
            selected = NativeStreamTableWrite if native.stream else NativeBatchTableWrite
        result = object.__new__(selected)
        result._execution = native
        result._python_writer = None
        return result

    def __getattr__(self, name):
        # Metadata or unknown-attribute inspection must never select a backend.
        execution = object.__getattribute__(self, '_execution')
        return getattr(execution, name)

    @property
    def _native_writer(self):
        return self._execution._native_writer

    @_native_writer.setter
    def _native_writer(self, writer):
        self._execution._native_writer = writer

    @property
    def _written(self):
        return self._execution._written

    def _new_python_writer(self):
        from pypaimon.write.backend_dispatch import create_python_write

        writer = create_python_write(
            self.table, self.commit_user, self.static_partition, self.stream,
            restore_snapshot_id=self._restore_snapshot_id)
        return self._configure_python_writer(writer)

    def _configure_python_writer(self, writer):
        if not self.stream:
            writer.batch_committed = self._batch_committed
        if self._write_cols is not None:
            writer.with_write_type(self._write_cols)
        if self._blob_consumer is not None:
            writer.with_blob_consumer(self._blob_consumer)
        if self._blob_uri_reader_factory is not None:
            writer.with_blob_uri_reader_factory(self._blob_uri_reader_factory)
        return writer

    def _switch_to_python(self):
        from pypaimon.write.backend_dispatch import switch_writer_to_python

        return switch_writer_to_python(self)

    @property
    def file_store_write(self):
        return self._switch_to_python().file_store_write

    @file_store_write.setter
    def file_store_write(self, writer):
        self._switch_to_python().file_store_write = writer

    @property
    def row_key_extractor(self):
        return self._switch_to_python().row_key_extractor

    @row_key_extractor.setter
    def row_key_extractor(self, extractor):
        self._switch_to_python().row_key_extractor = extractor

    @property
    def table_pyarrow_schema(self):
        return (self._python_writer.table_pyarrow_schema
                if self._python_writer is not None else self._table_schema)

    @table_pyarrow_schema.setter
    def table_pyarrow_schema(self, schema):
        self._switch_to_python().table_pyarrow_schema = schema

    @property
    def restore_snapshot_id(self):
        return (self._python_writer.restore_snapshot_id
                if self._python_writer is not None else self._restore_snapshot_id)

    @restore_snapshot_id.setter
    def restore_snapshot_id(self, snapshot_id):
        self._switch_to_python().restore_snapshot_id = snapshot_id

    @property
    def batch_committed(self):
        if self._python_writer is not None:
            return self._python_writer.batch_committed
        if self.stream:
            raise AttributeError('batch_committed')
        return self._batch_committed

    @batch_committed.setter
    def batch_committed(self, value):
        self._switch_to_python().batch_committed = value

    def with_write_type(self, write_cols):
        names = validate_write_columns(write_cols)
        if self._python_writer is not None:
            return self._python_writer.with_write_type(names)
        full_names = self._table_schema.names
        if (self.table.is_primary_key_table
                and not (len(names) == len(full_names) and set(names) == set(full_names))):
            return self._switch_to_python().with_write_type(names)
        self._execution.with_write_type(names)
        return self

    def with_blob_consumer(self, blob_consumer):
        if self._python_writer is not None:
            return self._python_writer.with_blob_consumer(blob_consumer)
        self._execution.with_blob_consumer(blob_consumer)
        return self

    def with_blob_uri_reader_factory(self, uri_reader_factory):
        if self._python_writer is not None:
            return self._python_writer.with_blob_uri_reader_factory(uri_reader_factory)
        self._execution.with_blob_uri_reader_factory(uri_reader_factory)
        return self

    def write_arrow(self, data, bucket=None):
        if self._python_writer is not None:
            return self._python_writer.write_arrow(data, bucket)
        data = self._execution._prepare_native_arrow_data(data)
        if isinstance(data, pa.RecordBatch):
            return self.write_arrow_batch(data, bucket)
        for batch in data.to_batches():
            self.write_arrow_batch(batch, bucket)

    def write_arrow_batch(self, data, bucket=None):
        execution = self._python_writer if self._python_writer is not None else self._execution
        return execution.write_arrow_batch(data, bucket)

    def write_pandas(self, dataframe):
        if self._python_writer is not None:
            return self._python_writer.write_pandas(dataframe)
        from pypaimon.write.input_adapter import _pandas_write_batch

        return self.write_arrow_batch(_pandas_write_batch(
            self.table, dataframe, self._schema, self._table_schema))

    def write_row(self, row):
        execution = self._python_writer if self._python_writer is not None else self._execution
        return execution.write_row(row)

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
        execution = self._python_writer if self._python_writer is not None else self._execution
        return execution.roll_before_group_if_needed(row_count)

    def close(self):
        if self._python_writer is not None:
            self._python_writer.close()
        return self._execution.close()

    def abort(self):
        if self._python_writer is not None:
            return self._python_writer.abort()
        return self._execution.abort()


class NativeBatchTableWrite(NativeTableWrite):
    """Batch facade with the same zero-argument prepare boundary as Python."""

    def prepare_commit(self) -> List[CommitMessage]:
        if self._python_writer is not None:
            return self._python_writer.prepare_commit()
        if self._batch_committed:
            raise RuntimeError('BatchTableWrite only supports one-time committing.')
        return self._execution.prepare_commit(BATCH_COMMIT_IDENTIFIER)


class NativeStreamTableWrite(NativeTableWrite):
    """Stream facade requiring an explicit signed commit identifier."""

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        commit_identifier = validate_commit_identifier(commit_identifier)
        if self._python_writer is not None:
            return self._python_writer.prepare_commit(commit_identifier)
        return self._execution.prepare_commit(commit_identifier)


class NativePostponeFixedBucketTableWrite(NativeBatchTableWrite):
    """Share postpone planning without coupling either backend to the other."""

    def __init__(self, table, commit_user, static_partition, bucket_plan, native_writer):
        from pypaimon.execution.native.write.native_write import (
            NativePostponeFixedBucketTableWrite as NativePostponeWrite,
        )

        self._execution = NativePostponeWrite(
            table, commit_user, static_partition, bucket_plan, native_writer)
        self._python_writer = None

    def _new_python_writer(self):
        from pypaimon.write.backend_dispatch import create_python_write

        return self._configure_python_writer(create_python_write(
            self.table, self.commit_user, self.static_partition,
            fixed_bucket=True, bucket_plan=self._bucket_plan))

    def _distributed_write_options(self, *, overwrite=False):
        from pypaimon.write.input_adapter import postpone_distributed_write_options

        return postpone_distributed_write_options(self.table, self._bucket_plan, overwrite)
