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

"""Optional native commits using the Java CommitMessage v14 bridge."""

from pypaimon.common.json_util import JSON
from pypaimon.read.native_plan import (
    _option_value_to_string, _resolved_schema_file_io_options, native_method_available)
from pypaimon.write.commit_message_serializer import serialize_commit_message


def native_commit_available() -> bool:
    """Whether the Rust runtime provides the required commit APIs."""
    return all(native_method_available(type_name, method) for type_name, method in (
        ('Table', 'from_resolved_schema'),
        ('CommitMessage', 'deserialize'),
        ('StreamWriteBuilder', 'with_commit_user'),
        ('BatchWriteBuilder', '_with_commit_user'),
        ('BatchWriteBuilder', 'with_overwrite'),
    ))


def native_messages_supported(table, messages) -> bool:
    for message in messages:
        if (message.compact_before or message.compact_after
                or message.compact_changelog_files
                or message.compact_index_adds or message.compact_index_deletes):
            return False
        # Python can rewrite stale row-id files before retrying. The native
        # committer does not yet implement that recovery path.
        if table.options.data_evolution_enabled() and (
                message.check_from_snapshot is not None
                or any(file.first_row_id is not None
                       for file in message.new_files + message.deleted_files)):
            return False
    return True


def create_native_commit(table, commit_user, overwrite_partition=None):
    """Return a native committer only when its publication protocol matches Python."""
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
    from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
    from pypaimon.table.file_store_table import FileStoreTable

    if not native_commit_available():
        return None
    # Native branch writes are not supported. Catalog-backed publication (REST
    # or custom version management) must continue through Python's environment.
    environment = table.catalog_environment
    if (type(table) is not FileStoreTable
            or type(environment) is not CatalogEnvironment
            or environment.supports_version_management
            or table.current_branch() != 'main'
            or table.options.query_auth_enabled
            or type(table.file_io) not in (LocalFileIO, PyArrowFileIO, ResolvingFileIO)):
        return None
    file_io_options = _resolved_schema_file_io_options(table)
    if file_io_options is None:
        return None

    from pypaimon_rust.datafusion import Table as NativeTable
    # Preserve the resolved schema and all effective table options, including
    # copy() overrides. Do not inject scan options or reload catalog schemas.
    options = {str(key): _option_value_to_string(value)
               for key, value in table.table_schema.options.items() if value is not None}
    # Python accepts boolean spellings such as "off"; pass the parsed value.
    options['dynamic-partition-overwrite'] = _option_value_to_string(
        table.options.dynamic_partition_overwrite())
    options['snapshot.ignore-empty-commit'] = _option_value_to_string(
        table.options.snapshot_ignore_empty_commit())
    native_table = NativeTable.from_resolved_schema(
        table.table_path, JSON.to_json(table.table_schema.copy(new_options=options)),
        database=table.identifier.get_database_name(),
        table=table.identifier.get_table_name(),
        options=file_io_options)
    if overwrite_partition is not None:
        return (native_table.new_batch_write_builder()
                ._with_commit_user(commit_user)
                .with_overwrite(overwrite_partition).new_commit())
    # Append commits use the stream committer with the Python writer's identity;
    # Python enforces each mode's lifecycle and empty-commit rules.
    return native_table.new_stream_write_builder().with_commit_user(commit_user).new_commit()


def to_native_commit_messages(table, messages):
    from pypaimon_rust.datafusion import CommitMessage as NativeCommitMessage
    # Convert the whole batch before any native mutation can begin.
    return [NativeCommitMessage.deserialize(
        serialize_commit_message(message, table.partition_keys_fields), version=14)
        for message in messages]
