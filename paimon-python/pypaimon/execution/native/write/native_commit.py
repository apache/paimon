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

import json

from pypaimon.common.json_util import JSON
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.table import (
    _catalog_context_options, _catalog_metastore, _option_value_to_string, _options_to_native,
    _resolved_schema_file_io_options)
from pypaimon.utils.file_store_path_factory import canonical_data_file_path
from pypaimon.write.commit_message_serializer import (
    deserialize_commit_message, serialize_commit_message,
)


def native_messages_supported(table, messages) -> bool:
    for message in messages:
        if (message.compact_before or message.compact_after
                or message.compact_changelog_files
                or message.compact_index_adds or message.compact_index_deletes):
            return False
        partition = tuple(message.partition)
        for file in message.new_files + message.changelog_files:
            if file.external_path:
                continue
            expected = canonical_data_file_path(
                table, partition, message.bucket, file.file_name)
            if file.file_path:
                if str(file.file_path) != expected:
                    return False
    return True


def create_native_commit(table, commit_user, overwrite_partition=None, *, batch=False):
    """Return a native committer only when its publication protocol matches Python."""
    if not _rest_catalog_supported(table) or not native_runtime_available():
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    if batch or overwrite_partition is not None:
        builder = native_table.new_batch_write_builder().with_commit_user(commit_user)
        if overwrite_partition is not None:
            builder = builder.with_overwrite(overwrite_partition)
        return builder.new_commit()
    # Append commits use the stream committer with the Python writer's identity;
    # Python enforces each mode's lifecycle and empty-commit rules.
    return native_table.new_stream_write_builder().with_commit_user(commit_user).new_commit()


def _rest_catalog_supported(table):
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.filesystem.local_file_io import FuseLocalFileIO, LocalFileIO
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
            and type(file_io) in (LocalFileIO, FuseLocalFileIO, PyArrowFileIO, OssFileIO,
                                  ResolvingFileIO, RESTTokenFileIO)
            and environment.supports_version_management
            and _rest_source_uuid(table) is not None
            and context is not None
            and context.options is not None)


def _rest_source_uuid(table):
    """Identity of the catalog table used to reconstruct a copied FUSE view."""
    if table.catalog_environment.uuid is not None:
        return table.catalog_environment.uuid
    from pypaimon.execution.native.table import _resolved_rest_table_response, _validate_native_fuse_binding
    try:
        original = _validate_native_fuse_binding(table)
        if original is None or original == table.identifier:
            return None
        response = _resolved_rest_table_response(table, original)
        uuid = json.loads(response).get('id') if response is not None else None
        return uuid if isinstance(uuid, str) and uuid else None
    except (ValueError, RuntimeError):
        return None


def _native_rest_table(table, schema_json):
    from pypaimon_rust.datafusion import PaimonCatalog as NativeCatalog
    from pypaimon.execution.native.table import _native_table, _validate_native_fuse_binding

    identifier = _validate_native_fuse_binding(table) or table.identifier
    catalog_options = _catalog_context_options(table)
    catalog_options['metastore'] = 'rest'
    native_table = NativeCatalog(catalog_options, config_required=False).get_table((
        identifier.get_database_name(), identifier.get_object_name()))
    if (native_table.location() != table.table_path
            or native_table.rest_table_uuid() != _rest_source_uuid(table)):
        return None
    if identifier != table.identifier:
        native_table = native_table.copy({'branch': table.current_branch()})
    if native_table.location() != table.table_path or native_table.branch() != table.current_branch():
        return None
    if table.options.query_auth_enabled:
        # A catalog-loaded handle can predate ordinary schema evolution without
        # selecting a historical snapshot. Reuse the read transport's original
        # REST response and captured view instead of attaching its fields to a
        # newly loaded schema, which core correctly treats as an assembled table.
        # The fresh lookup above still refuses a replaced table or FileIO route.
        native_table = _native_table(table)
        if (native_table.location() != table.table_path
                or native_table.rest_table_uuid() != _rest_source_uuid(table)
                or native_table.branch() != table.current_branch()):
            return None
    return native_table.copy_with_resolved_schema(schema_json)


def create_native_write_table(table):
    """Preserve the resolved schema and the catalog's publication route."""
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
    from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
    from pypaimon.table.file_store_table import FileStoreTable

    environment = table.catalog_environment
    if (type(table) is not FileStoreTable
            or type(environment) is not CatalogEnvironment):
        return None

    from pypaimon_rust.datafusion import Table as NativeTable
    # Preserve the resolved schema and all effective table options, including
    # copy() overrides. Do not inject scan options or reload catalog schemas.
    options = _options_to_native(table.table_schema.options)
    # Python accepts boolean spellings such as "off"; pass the parsed value.
    options['dynamic-partition-overwrite'] = _option_value_to_string(
        table.options.dynamic_partition_overwrite())
    options['snapshot.ignore-empty-commit'] = _option_value_to_string(
        table.options.snapshot_ignore_empty_commit())
    options['row-tracking.partition-group-on-commit'] = _option_value_to_string(
        table.options.row_tracking_partition_group_on_commit())
    options['data-evolution.row-sidecar.enabled'] = _option_value_to_string(
        table.options.data_evolution_row_sidecar_enabled())
    options['query-auth.enabled'] = _option_value_to_string(table.options.query_auth_enabled)
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
        table=table.identifier.get_table_name(), branch=table.current_branch(),
        options=file_io_options)


def to_native_commit_messages(table, messages):
    from pypaimon_rust.datafusion import CommitMessage as NativeCommitMessage
    # Convert the whole batch before any native mutation can begin.
    return [NativeCommitMessage.deserialize(
        serialize_commit_message(message, table.partition_keys_fields), version=14)
        for message in messages]


def from_native_commit_messages(table, messages):
    """Decode native v14 messages for PyPaimon's existing commit API."""
    decoded = [deserialize_commit_message(
        message.serialize(), table.partition_keys_fields,
        table.trimmed_primary_keys_fields) for message in messages]
    for message in decoded:
        for file in message.new_files + message.changelog_files:
            file.file_path = file.physical_path() if file.external_path else canonical_data_file_path(
                table, message.partition, message.bucket, file.file_name)
    return decoded


def native_commit_error(error):
    """Keep retry classification structured; never infer it from error text."""
    try:
        from pypaimon_rust import RowIdExistenceConflict as NativeRowIdConflict
    except ImportError:
        return error
    from pypaimon.write.row_id_conflict import RowIdExistenceConflict

    if not isinstance(error, NativeRowIdConflict):
        return error
    return RowIdExistenceConflict._from_fields(
        file_name=error.file_name, first_row_id=error.first_row_id,
        row_count=error.row_count, bucket=error.bucket,
        current_first_row_id=error.current_first_row_id,
        current_row_count=error.current_row_count)


def prepare_native_row_id_conflict_check(table, messages):
    """Prepare a check without creating Python publication or conflict state."""
    from pypaimon.execution.native.write.commit import NativeTableCommit

    if not native_messages_supported(table, messages):
        return None
    native = create_native_commit(table, 'row-id-conflict-check', batch=True)
    if native is None:
        return None
    try:
        converted = to_native_commit_messages(table, messages)
    except BaseException:
        try:
            native.close()
        except BaseException:
            pass
        raise
    return NativeTableCommit(native, is_batch=True), converted


def native_commit_callback(table, callbacks):
    """Translate the native notification into the existing Python context."""
    from pypaimon.manifest.schema.manifest_entry import ManifestEntry
    from pypaimon.snapshot.snapshot import Snapshot
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.write.commit_callback import CommitCallbackContext

    def notify(native_context):
        entries = []
        for entry in native_context.commit_entries:
            message = from_native_commit_messages(table, [entry._to_commit_message()])[0]
            entries.append(ManifestEntry(
                entry.kind, GenericRow(list(message.partition), table.partition_keys_fields),
                entry.bucket, entry.total_buckets, message.new_files[0]))
        context = CommitCallbackContext(
            JSON.from_json(native_context._snapshot_json(), Snapshot), entries, native_context.identifier)
        for callback in callbacks:
            callback.call(context)

    return notify
