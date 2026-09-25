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

from importlib import import_module

from pypaimon.common.json_util import JSON
from pypaimon.read.native_plan import (
    _catalog_context_options, _catalog_metastore, _option_value_to_string,
    _resolved_schema_file_io_options)
from pypaimon.utils.file_store_path_factory import canonical_data_file_path
from pypaimon.write.commit_message_serializer import serialize_commit_message


_DEFAULT_MANIFEST_TARGET_SIZE = 8 * 1024 * 1024


def native_commit_available() -> bool:
    """Whether the optional Rust bindings are installed."""
    try:
        import_module('pypaimon_rust.datafusion')
    except ImportError:
        return False
    return True


def _native_publication_supported(table) -> bool:
    # Data evolution needs sidecar ranges and row-id recovery. Custom manifest
    # targets need Java's forced size checks between manifest entry groups.
    return (not table.options.data_evolution_enabled()
            and table.options.manifest_target_size() == _DEFAULT_MANIFEST_TARGET_SIZE)


def native_messages_supported(table, messages) -> bool:
    if not _native_publication_supported(table):
        return False
    path_factory = table.path_factory()
    for message in messages:
        if (message.compact_before or message.compact_after
                or message.compact_changelog_files
                or message.compact_index_adds or message.compact_index_deletes):
            return False
        partition = tuple(message.partition)
        bucket_path = path_factory.bucket_path(
            partition, message.bucket, canonical_partition=True)
        for file in message.new_files + message.changelog_files:
            if file.external_path:
                continue
            expected = canonical_data_file_path(
                table, partition, message.bucket, file.file_name)
            if file.file_path:
                if str(file.file_path) != expected:
                    return False
            elif path_factory.bucket_path(partition, message.bucket) != bucket_path:
                # The message does not say which of the two partition layouts
                # contains the file. Use Python's path-aware commit and abort.
                return False
    return True


def create_native_commit(table, commit_user, overwrite_partition=None):
    """Return a native committer only when its publication protocol matches Python."""
    if (not _native_publication_supported(table)
            or not _rest_catalog_supported(table)
            or not native_commit_available()):
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    if overwrite_partition is not None:
        return (native_table.new_batch_write_builder()
                ._with_commit_user(commit_user)
                .with_overwrite(overwrite_partition).new_commit())
    # Append commits use the stream committer with the Python writer's identity;
    # Python enforces each mode's lifecycle and empty-commit rules.
    return native_table.new_stream_write_builder().with_commit_user(commit_user).new_commit()


def _rest_catalog_supported(table):
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.filesystem.oss_file_io import OssFileIO
    from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
    from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
    from pypaimon.table.file_store_table import FileStoreTable

    environment = table.catalog_environment
    loader = getattr(environment, 'catalog_loader', None)
    context = loader.context() if _catalog_metastore(loader) == 'rest' else None
    file_io = table.file_io
    if type(file_io) is CachingFileIO:
        file_io = file_io._delegate
    return (type(table) is FileStoreTable
            and type(environment) is CatalogEnvironment
            and type(file_io) in (LocalFileIO, PyArrowFileIO, OssFileIO,
                                  ResolvingFileIO, RESTTokenFileIO)
            and environment.supports_version_management
            and environment.uuid is not None
            and context is not None
            and context.options is not None
            and all(getattr(context, attr, None) is None for attr in (
                'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader')))


def _native_rest_table(table, schema_json):
    from pypaimon_rust.datafusion import PaimonCatalog as NativeCatalog

    catalog_options = _catalog_context_options(table)
    catalog_options['metastore'] = 'rest'
    native_table = NativeCatalog(catalog_options).get_table((
        table.identifier.get_database_name(), table.identifier.get_table_name()))
    if (native_table.location() != table.table_path
            or native_table.rest_table_uuid() != table.catalog_environment.uuid):
        return None
    return native_table.copy_with_resolved_schema(schema_json)


def create_native_write_table(table):
    """Preserve the resolved schema and the catalog's publication route."""
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
    from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
    from pypaimon.table.bucket_mode import BucketMode
    from pypaimon.table.file_store_table import FileStoreTable

    # Native branch and postpone writes are not supported.
    environment = table.catalog_environment
    if (type(table) is not FileStoreTable
            or type(environment) is not CatalogEnvironment
            or table.current_branch() != 'main'
            or table.bucket_mode() == BucketMode.POSTPONE_MODE
            or table.options.query_auth_enabled):
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
    schema_json = JSON.to_json(table.table_schema.copy(new_options=options))
    if environment.supports_version_management:
        if not _rest_catalog_supported(table):
            return None
        return _native_rest_table(table, schema_json)
    if type(table.file_io) not in (LocalFileIO, PyArrowFileIO, ResolvingFileIO):
        return None
    file_io_options = _resolved_schema_file_io_options(table)
    if file_io_options is None:
        return None
    return NativeTable.from_resolved_schema(
        table.table_path, schema_json,
        database=table.identifier.get_database_name(),
        table=table.identifier.get_table_name(),
        options=file_io_options)


def to_native_commit_messages(table, messages):
    from pypaimon_rust.datafusion import CommitMessage as NativeCommitMessage
    # Convert the whole batch before any native mutation can begin.
    return [NativeCommitMessage.deserialize(
        serialize_commit_message(message, table.partition_keys_fields), version=14)
        for message in messages]
