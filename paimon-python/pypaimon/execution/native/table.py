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

"""Adapt a resolved public table to the optional Native execution runtime."""

import json
import os
from threading import Lock
from types import SimpleNamespace
from typing import Optional

from pypaimon.common.options.config import CatalogOptions, FuseOptions, OssOptions
from pypaimon.common.options.options_utils import OptionsUtils
from pypaimon.filesystem.io_cache_routing import single_endpoint_options


def _native_blob_view_supported(table, read_names) -> bool:
    """Resolve Blob views natively only with the REST catalog environment."""
    if (not (table.options.blob_view_fields() & set(read_names))
            or not table.options.blob_view_resolve_enabled()):
        return True
    loader = getattr(getattr(table, 'catalog_environment', None), 'catalog_loader', None)
    # Python also leaves view structs unresolved without a loader.
    return loader is None or _catalog_metastore(loader) == 'rest'


def _partition_fields(table):
    """Ordered partition DataFields, used to decode the split partition bytes."""
    schema = table.table_schema
    by_name = {f.name: f for f in schema.fields}
    return [by_name[name] for name in schema.partition_keys]


def _catalog_metastore(loader) -> Optional[str]:
    """Return the Rust catalog kind for an exact built-in loader."""
    from pypaimon.catalog.filesystem_catalog_loader import FileSystemCatalogLoader
    from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader

    # Subclasses may override load() with routing or option semantics which
    # cannot be reproduced from context().options alone.
    if type(loader) is FileSystemCatalogLoader:
        return 'filesystem'
    if type(loader) is RESTCatalogLoader:
        return 'rest'
    return None


def _option_value_to_string(value) -> str:
    """Stringify an option value for the Rust catalog.

    Python bools stringify to ``'True'``/``'False'``; Rust parses booleans
    case-sensitively, so emit lowercase ``'true'``/``'false'`` instead.
    """
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return OptionsUtils.convert_to_string(value)


def _catalog_options(table) -> dict:
    """Catalog options that built this table, to reconstruct the Rust catalog."""
    loader = getattr(getattr(table, 'catalog_environment', None), 'catalog_loader', None)
    if loader is None:
        raise ValueError("native_plan requires a catalog-backed table (no catalog loader)")
    metastore = _catalog_metastore(loader)
    if metastore is None:
        raise ValueError("native_plan requires an exact built-in catalog loader")
    normalized = _catalog_context_options(table)
    normalized[CatalogOptions.METASTORE.key()] = metastore
    return normalized


def _catalog_context_options(table) -> dict:
    """Normalize catalog storage properties without rebuilding the metastore."""
    loader = table.catalog_environment.catalog_loader
    context_options = loader.context().options
    options = context_options.to_map()
    normalized = {
        str(key): _option_value_to_string(value)
        for key, value in options.items()
        if value is not None
    }
    if FuseOptions.FUSE_ENABLED.key() in normalized:
        normalized[FuseOptions.FUSE_ENABLED.key()] = _option_value_to_string(
            context_options.get(FuseOptions.FUSE_ENABLED))
    if str(getattr(table, 'table_path', '')).startswith('oss://'):
        from pypaimon.filesystem.jindo_file_system_handler import (
            JINDO_AVAILABLE,
        )
        impl = normalized.get(OssOptions.OSS_IMPL.key())
        if JINDO_AVAILABLE and (impl is None or impl.lower() == 'jindo'):
            # This catalog is only used for Rust scan planning.
            normalized[OssOptions.OSS_IMPL.key()] = 'jindo'
    return normalized


def _resolved_schema_file_io_options(table) -> Optional[dict]:
    """FileIO properties for tables whose metadata needs no catalog resolution."""
    environment = table.catalog_environment
    loader = environment.catalog_loader
    if loader is None:
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.filesystem.local_file_io import LocalFileIO
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
        from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
        # A custom environment or FileIO can supply metadata outside the path.
        if (type(environment) is not CatalogEnvironment
                or type(table.file_io) not in (LocalFileIO, PyArrowFileIO, ResolvingFileIO)):
            return None
        return {str(key): _option_value_to_string(value)
                for key, value in table.file_io.properties.to_map().items()
                if value is not None}
    from pypaimon.catalog.jdbc_catalog_loader import JdbcCatalogLoader
    if _catalog_metastore(loader) != 'filesystem' and type(loader) is not JdbcCatalogLoader:
        # REST tables must retain catalog snapshot loading and token refresh.
        return None
    context = loader.context()
    if context.options is None:
        return None
    # JDBC, like filesystem catalogs, uses on-disk snapshots. Its already
    # resolved table does not need another database connection during planning.
    return _catalog_context_options(table)


def _resolved_schema_json(table) -> str:
    """Preserve all effective table options as strings for Rust."""
    from pypaimon.common.json_util import JSON
    options = {str(key): _option_value_to_string(value)
               for key, value in table.table_schema.options.items() if value is not None}
    if 'query-auth.enabled' in options:
        # Preserve Python's effective policy. Forwarding an enabled spelling
        # such as "on" unchanged would disable authorization in Rust.
        options['query-auth.enabled'] = _option_value_to_string(OptionsUtils.convert_to_boolean(
            table.table_schema.options['query-auth.enabled']))
    return JSON.to_json(table.table_schema.copy(new_options=options))


def _resolved_rest_table_response(table, identifier=None):
    """Reuse REST metadata only when the standard loader can be reproduced."""
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader

    environment = table.catalog_environment
    identifier = table.identifier if identifier is None else identifier
    response = getattr(environment, 'rest_table_response', None)
    if (type(environment) is not CatalogEnvironment
            or type(environment.catalog_loader) is not RESTCatalogLoader
            or not isinstance(response, str)):
        return None
    metadata = json.loads(response)
    if (metadata.get('path') != table.table_path
            or metadata.get('name') != identifier.get_object_name()
            or ('database' in metadata
                and metadata['database'] != identifier.get_database_name())):
        return None
    return response


class _NativeRestTableCache:
    """One process-local native table, discarded during worker serialization."""

    def __init__(self):
        self._states = {}

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self.__init__()

    def get_or_create(self, key, create):
        pid = os.getpid()
        # Publish a complete state atomically; never touch an inherited lock.
        states = self._states
        state = states.get(pid)
        if state is None:
            state = states.setdefault(pid, SimpleNamespace(lock=Lock(), entry=None))
            self._states = {pid: state}
        with state.lock:
            if state.entry is None or state.entry[0] != key:
                state.entry = (key, create())
            return state.entry[1]

    def get(self, response, database, table, options, token_loader=None):
        from pypaimon_rust.datafusion import Table

        def create():
            token = token_loader() if token_loader else None
            if token is None:
                # A REST table can reach this path before its data token is available (for
                # example a copied branch or a token refresh race).  The native core sends
                # every OSS request to one endpoint, so routing keys in the catalog must not
                # leave its legacy fs.oss.endpoint pointed at a cache target.
                return Table.from_rest_response(
                    response, database=database, table=table,
                    rest_options=single_endpoint_options(options))
            # The native core cannot use io-cache targets per request, and fs.oss.endpoint may name the cache.
            return Table.from_rest_response_with_token(
                response, database=database, table=table, rest_options=options,
                data_token=single_endpoint_options(token.token, options),
                expires_at_millis=token.expire_at_millis)

        key = (response, database, table, tuple(sorted(options.items())))
        return self.get_or_create(key, create)


def _rest_data_token(table):
    """Reuse only an existing valid token bound to this exact table."""
    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.filesystem.caching_file_io import CachingFileIO

    file_io = table.file_io
    if type(file_io) is CachingFileIO:
        file_io = file_io._delegate
    if (table.current_branch() != 'main'
            or type(file_io) is not RESTTokenFileIO
            or file_io.identifier != table.identifier
            or file_io.path != table.table_path):
        return None
    return file_io._existing_valid_token()


def _validate_native_fuse_binding(table):
    """Find the catalog table whose construction reproduces the bound FileIO."""
    from pypaimon.catalog.rest.fuse_support import FusePathResolver
    from pypaimon.common.identifier import Identifier
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.filesystem.local_file_io import FuseLocalFileIO, LocalFileIO

    file_io = table.file_io
    if type(file_io) is CachingFileIO:
        file_io = file_io._delegate
    if not isinstance(file_io, FuseLocalFileIO):
        return
    loader = table.catalog_environment.catalog_loader
    if type(file_io) is not FuseLocalFileIO or _catalog_metastore(loader) != 'rest':
        raise RuntimeError('Native reads cannot reconstruct custom FUSE FileIO')
    options = loader.context().options
    if not options.get(FuseOptions.FUSE_ENABLED):
        raise RuntimeError('Native catalog would disable the bound FUSE mount')
    if file_io.path.rstrip('/') != table.table_path.rstrip('/'):
        raise RuntimeError('The bound FUSE FileIO belongs to a different table path')
    candidates = []
    response = table.catalog_environment.rest_table_response
    if isinstance(response, str):
        metadata = json.loads(response)
        if (metadata.get('path') == table.table_path
                and metadata.get('database', table.identifier.get_database_name())
                == table.identifier.get_database_name() and isinstance(metadata.get('name'), str)):
            original = Identifier.create(table.identifier.get_database_name(), metadata['name'])
            if original.get_table_name() == table.identifier.get_table_name():
                candidates.append(original)
    candidates.append(table.identifier)
    # A manually resolved view can omit the original REST response. Its usual
    # main-table binding can still be reconstructed by the same catalog.
    candidates.append(Identifier.create(table.identifier.get_database_name(), table.identifier.get_table_name()))
    resolver = FusePathResolver(options, None)
    local = LocalFileIO()
    mounted = local._to_file(file_io.fuse_path).absolute()
    for identifier in candidates:
        expected = resolver.resolve_local_path(table.table_path, identifier)
        if mounted == local._to_file(expected).absolute():
            return identifier
    raise RuntimeError('Native catalog cannot reconstruct the bound FUSE FileIO')


def _native_table(table):
    """Transport the resolved schema, shared FileIO and captured read view."""
    _validate_native_fuse_binding(table)
    if ('_read_snapshot' in vars(table)
            and _catalog_metastore(table.catalog_environment.catalog_loader) == 'rest'):
        from pypaimon.common.json_util import JSON
        environment = table.catalog_environment
        resources = vars(table).setdefault('_pinned_read_resources', {})
        cache = resources.setdefault('native_table', _NativeRestTableCache())
        key = (table.table_path, table.identifier.get_full_name(), table.file_io,
               environment.uuid, environment.rest_table_response,
               tuple(sorted(_catalog_options(table).items())),
               JSON.to_json(table._read_snapshot) if table._read_snapshot is not None else None)
        # Keep core's verified snapshot alive for this captured view, including
        # ordinary read-option copies. Pickling/forking starts a fresh cache.
        native = cache.get_or_create(key, lambda: _load_native_table(table))
        return _with_native_read_snapshot(table, _with_native_resolved_schema(table, native))
    return _load_native_table(table)


def _load_native_table(table):
    identifier = _validate_native_fuse_binding(table) or table.identifier
    rest_response = _resolved_rest_table_response(table, identifier)
    file_io_options = _resolved_schema_file_io_options(table)
    if rest_response is not None:
        cache = table.catalog_environment.__dict__.setdefault(
            '_native_rest_table_cache', _NativeRestTableCache())
        rt = cache.get(
            rest_response,
            database=identifier.get_database_name(),
            table=identifier.get_object_name(),
            options=_catalog_options(table),
            token_loader=lambda: _rest_data_token(table))
    elif file_io_options is not None:
        from pypaimon_rust.datafusion import Table
        # As with a token: the core cannot use io-cache targets per request, so it gets the origin's options.
        rt = Table.from_resolved_schema(
            table.table_path, _resolved_schema_json(table),
            database=table.identifier.get_database_name(),
            table=table.identifier.get_table_name(),
            branch=table.current_branch(), options=single_endpoint_options(file_io_options))
    else:
        from pypaimon_rust.datafusion import PaimonCatalog
        # The Python loader has already resolved /config, including typed
        # FUSE options. Match RESTCatalogLoader.load() and reuse that context.
        # This catalog is also a single-endpoint client.  Keep its metadata/catalog
        # configuration, but make its storage endpoint the origin when catalog-only
        # io-cache routing is enabled.
        catalog = PaimonCatalog(
            single_endpoint_options(_catalog_options(table)), config_required=False)
        # REST may keep branch schemas in the catalog only. Load the base
        # environment, then attach the schema/branch already resolved here.
        rt = catalog.get_table((
            identifier.get_database_name(), identifier.get_object_name()))
        if rt.location() != table.table_path:
            raise RuntimeError('Native catalog resolved a different table location')
    if identifier != table.identifier:
        # Construct the originally bound table, then use the existing copy API.
        # Core retains its FileIO while resolving the branch's REST identity.
        rt = rt.copy({'branch': table.current_branch()})
    if file_io_options is None:
        rt = _with_native_resolved_schema(table, rt)
    if table.current_branch() != 'main':
        if rt.branch() != table.current_branch():
            raise RuntimeError("Native table did not resolve the requested branch")
    return _with_native_read_snapshot(table, rt)


def _with_native_resolved_schema(table, native_table):
    # Resolve time travel through core before replacing options. Caller-supplied
    # fields cannot acquire the catalog's authorization session.
    from pypaimon.snapshot.time_travel_util import SCAN_KEYS
    resolved = _resolved_schema_json(table)
    options = json.loads(resolved)['options']
    historical = (options.get('query-auth.enabled') == 'true'
                  and any(key in options for key in SCAN_KEYS))
    snapshot = vars(table).get('_read_snapshot')
    if historical and snapshot is not None:
        from pypaimon.common.json_util import JSON
        # A captured view can intentionally retain the current read schema
        # while its files were written under an older snapshot schema.
        native_table = native_table.copy_with_pinned_snapshot(
            JSON.to_json(snapshot), with_schema=snapshot.schema_id == table.table_schema.id)
    elif historical:
        native_table = native_table.copy(options)
    return native_table.copy_with_resolved_schema(resolved, branch=table.current_branch())


def _with_native_read_snapshot(table, native_table):
    """Attach a captured read view, including a fixed empty snapshot."""
    # Only explicitly stored state captures a view; None means a fixed empty view.
    table_state = vars(table)
    if '_read_snapshot' in table_state:
        from pypaimon.common.json_util import JSON
        snapshot = table_state['_read_snapshot']
        native_table = native_table.copy_with_pinned_snapshot(JSON.to_json(snapshot) if snapshot is not None else None)
    return native_table


def _native_query_auth_supported(table):
    """Only reproduce authorization owned by the standard REST environment."""
    from pypaimon.catalog.catalog_environment import CatalogEnvironment, _TableQueryAuthFn

    environment = table.catalog_environment
    auth = environment.table_query_auth(table.options, table.identifier)
    if auth is None:
        return True
    if (type(environment) is not CatalogEnvironment
            or type(auth) is not _TableQueryAuthFn
            or auth._catalog_loader is not environment.catalog_loader
            or auth._identifier != table.identifier
            or not table.options.query_auth_enabled
            or _catalog_metastore(environment.catalog_loader) != 'rest'):
        return False
    return True
