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
    """Keep one writer and configure supported capabilities before mutation."""

    def __new__(cls, *args, **kwargs):
        if cls is NativeTableWrite:
            stream = kwargs.get('stream', args[3] if len(args) > 3 else False)
            cls = NativeStreamTableWrite if stream else NativeBatchTableWrite
        return object.__new__(cls)

    def __init__(self, table, commit_user, static_partition, stream, native_writer,
                 restore_snapshot_id=None):
        from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite

        self._initialize(NativeWrite(table, commit_user, static_partition, stream,
                                     native_writer, restore_snapshot_id))

    def _initialize(self, execution):
        self._execution = execution
        self.table = execution.table
        self.commit_user = execution.commit_user
        self.static_partition = execution.static_partition
        self.stream = execution.stream
        self._python_writer = None
        self._public_writer = self

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
        result._initialize(native)
        return result

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
            restore_snapshot_id=self._execution._restore_snapshot_id, public_writer=self._public_writer)
        return self._configure_python_writer(writer)

    def _configure_python_writer(self, writer):
        if not self.stream:
            writer._batch_committed = self._execution._batch_committed
        if self._execution._write_cols is not None:
            writer.with_write_type(self._execution._write_cols)
        if self._execution._blob_consumer is not None:
            writer.with_blob_consumer(self._execution._blob_consumer)
        if self._execution._blob_uri_reader_factory is not None:
            writer.with_blob_uri_reader_factory(self._execution._blob_uri_reader_factory)
        return writer

    def _switch_to_python(self):
        from pypaimon.write.backend_dispatch import switch_writer_to_python

        return switch_writer_to_python(self)

    def with_write_type(self, write_cols):
        names = validate_write_columns(write_cols)
        if self._python_writer is not None:
            self._python_writer.with_write_type(names)
            return self
        full_names = self._execution._table_schema.names
        if (self.table.is_primary_key_table
                and not (len(names) == len(full_names) and set(names) == set(full_names))):
            self._switch_to_python().with_write_type(names)
            return self
        self._execution.with_write_type(names)
        return self

    def with_blob_consumer(self, blob_consumer):
        if self._python_writer is not None:
            self._python_writer.with_blob_consumer(blob_consumer)
            return self
        self._execution.with_blob_consumer(blob_consumer)
        return self

    def with_blob_uri_reader_factory(self, uri_reader_factory):
        if self._python_writer is not None:
            self._python_writer.with_blob_uri_reader_factory(uri_reader_factory)
            return self
        self._execution.with_blob_uri_reader_factory(uri_reader_factory)
        return self

    def write_arrow(self, data, bucket=None):
        if self._python_writer is not None:
            return self._python_writer.write_arrow(data, bucket)
        data = self._execution._prepare_native_arrow_data(data)
        if isinstance(data, pa.RecordBatch):
            return self._public_writer.write_arrow_batch(data, bucket)
        for batch in data.to_batches():
            self._public_writer.write_arrow_batch(batch, bucket)

    def write_arrow_batch(self, data, bucket=None):
        execution = self._python_writer if self._python_writer is not None else self._execution
        return execution.write_arrow_batch(data, bucket)

    def write_pandas(self, dataframe):
        if self._python_writer is not None:
            return self._python_writer.write_pandas(dataframe)
        from pypaimon.write.input_adapter import _pandas_write_batch

        return self._public_writer.write_arrow_batch(_pandas_write_batch(
            self.table, dataframe, self._execution._schema, self._execution._table_schema))

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
        if self._execution._batch_committed:
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

        self._initialize(NativePostponeWrite(
            table, commit_user, static_partition, bucket_plan, native_writer))

    def _new_python_writer(self):
        from pypaimon.write.backend_dispatch import create_python_write

        return self._configure_python_writer(create_python_write(
            self.table, self.commit_user, self.static_partition,
            fixed_bucket=True, bucket_plan=self._execution._bucket_plan, public_writer=self._public_writer))

    def _distributed_write_options(self, *, overwrite=False):
        if self._python_writer is not None:
            return self._python_writer._distributed_write_options(overwrite=overwrite)
        from pypaimon.write.input_adapter import postpone_distributed_write_options

        return postpone_distributed_write_options(self.table, self._execution._bucket_plan, overwrite)
