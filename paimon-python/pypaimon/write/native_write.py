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

from importlib import import_module

import pyarrow as pa

from pypaimon.common.options.core_options import MergeEngine
from pypaimon.schema.arrow_schema import arrow_schemas_compatible, normalize_arrow_strings
from pypaimon.schema.data_types import PyarrowFieldParser, is_blob_file_field
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.native_commit import (
    create_native_write_table, from_native_commit_messages,
)
from pypaimon.write.row_utils import row_to_named_values, row_values_to_arrow_table


def native_write_available() -> bool:
    """Whether the optional Rust bindings are installed."""
    try:
        import_module('pypaimon_rust.datafusion')
    except ImportError:
        return False
    return True


def _native_partition_types_supported(schema, partition_keys):
    """Partition keys which Rust can encode and use to locate existing files."""
    return not any(
        pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type)
        or pa.types.is_fixed_size_binary(data_type) or pa.types.is_floating(data_type)
        for data_type in (schema.field(name).type for name in partition_keys))


def _native_map_layouts_supported(table, schema):
    options = table.options.options.to_map()
    for field in schema:
        if table.options.map_storage_layout(field.name) != 'default':
            return False
        # Python rejects even an explicit default layout on a non-MAP field.
        if (not pa.types.is_map(field.type)
                and 'fields.{}.map.storage-layout'.format(field.name) in options):
            return False
    return True


def create_native_write(table, commit_user, static_partition=None, stream=False):
    """Return a native writer if the table can use the filesystem write path."""
    schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    sequence_fields = table.options.sequence_field()
    if table.is_primary_key_table and sequence_fields:
        # The native writer currently sorts sequence fields ascending and
        # does not implement Java's NaN/signed-zero ordering.
        if (not table.options.sequence_field_sort_order_is_ascending()
                or any(pa.types.is_floating(schema.field(name).type) for name in sequence_fields)):
            return None
    if (not native_write_available()
            # Rust does not produce the optional random-access .row sidecars.
            or (table.options.data_evolution_enabled()
                and table.options.data_evolution_row_sidecar_enabled())
            or table.options.data_file_external_paths()
            or table.bucket_mode() not in (BucketMode.HASH_FIXED,
                                           BucketMode.BUCKET_UNAWARE)
            or (table.options.deletion_vectors_enabled()
                and table.options.merge_engine() in (MergeEngine.PARTIAL_UPDATE,
                                                     MergeEngine.AGGREGATE))
            or table.options.changelog_file_format() not in (None, 'parquet')
            or table.options.file_format() != 'parquet'
            # The native writer does not implement MAP shared-shredding layouts.
            or not _native_map_layouts_supported(table, schema)
            # Rust cannot encode these partition keys yet.
            or not _native_partition_types_supported(schema, table.partition_keys)
            or any(is_blob_file_field(field) for field in table.table_schema.fields)):
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    if stream:
        builder = native_table.new_stream_write_builder().with_commit_user(commit_user)
    else:
        builder = native_table.new_batch_write_builder()._with_commit_user(commit_user)
        if static_partition is not None:
            builder = builder.with_overwrite(static_partition)
    return NativeTableWrite(table, commit_user, static_partition, stream,
                            builder.new_write())


class NativeTableWrite:
    """Use Rust for Arrow batches while retaining PyPaimon's commit-message API.

    Advanced Python writer methods switch to the Python writer before the first
    native data write. Once Rust has written data, switching would split one
    logical write across two writers, so it is rejected.
    """

    def __init__(self, table, commit_user, static_partition, stream, native_writer):
        self.table = table
        self.commit_user = commit_user
        self.static_partition = static_partition
        self.stream = stream
        self._native_writer = native_writer
        self._python_writer = None
        self._written = False
        self._schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)

    def _switch_to_python(self):
        if self._python_writer is not None:
            return self._python_writer
        if self._written:
            raise RuntimeError(
                'Cannot switch to the Python writer after native data was written')
        self._native_writer.close()
        self._native_writer = None
        from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite
        if self.stream:
            self._python_writer = StreamTableWrite(self.table, self.commit_user)
        else:
            self._python_writer = BatchTableWrite(
                self.table, self.commit_user, self.static_partition)
        return self._python_writer

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._switch_to_python(), name)

    def write_arrow(self, data):
        if self._python_writer is not None:
            return self._python_writer.write_arrow(data)
        if isinstance(data, pa.RecordBatch):
            return self.write_arrow_batch(data)
        for batch in data.to_batches():
            self.write_arrow_batch(batch)

    def write_arrow_batch(self, data):
        if self._python_writer is not None:
            return self._python_writer.write_arrow_batch(data)
        if not arrow_schemas_compatible(
                data.schema, self._schema, check_top_level_nullability=False,
                allow_binary_compatibility=True):
            raise ValueError(
                "Input schema isn't consistent with table schema and write cols. "
                f"Input schema is: {data.schema} Table schema is: {self._schema} "
                "Write cols is: None")
        data = normalize_arrow_strings(data)
        if data.num_rows:
            # A failed native write may already have produced files. Never
            # retry that batch through Python after this point.
            self._written = True
        self._native_writer.write_arrow(data)

    def write_pandas(self, dataframe):
        if self._python_writer is not None:
            return self._python_writer.write_pandas(dataframe)
        schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        self.write_arrow_batch(pa.RecordBatch.from_pandas(dataframe, schema=schema))

    def write_row(self, row):
        if self._python_writer is not None:
            return self._python_writer.write_row(row)
        values = row_to_named_values(row, self.table.table_schema.fields)
        names = list(self.table.field_names)
        self.write_arrow_batch(row_values_to_arrow_table(
            values, self.table.table_schema.fields, names).to_batches()[0])

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
            messages = self._native_writer.prepare_commit()
        return from_native_commit_messages(self.table, messages)

    def close(self):
        if self._python_writer is not None:
            self._python_writer.close()
        elif self._native_writer is not None:
            self._native_writer.close()
            self._native_writer = None

    def abort(self):
        if self._python_writer is not None:
            self._python_writer.abort()
        else:
            self.close()
