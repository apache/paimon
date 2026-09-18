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

"""Plan splits with pypaimon_rust, decoded for the normal pypaimon reader.

Optional, lazily-imported dependency; enabled by ``scan.native-plan.enabled``.
Predicates and limits are pushed into Rust planning. The normal pypaimon reader
still applies them while reading, so pushdown remains an optimization.
"""

from typing import List, Optional, Tuple

from packaging.version import InvalidVersion, Version

from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options_utils import OptionsUtils
from pypaimon.common.predicate import Predicate
from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.read.split_serializer import deserialize_split_v1


def native_runtime_available() -> bool:
    """Whether an installed pypaimon-rust exposes the full split-planning API.

    Both entry points used by :func:`native_plan` are probed: ``PaimonCatalog.
    get_table`` (0.3.0) and ``Split.serialize`` (the split wire format). An
    intermediate build missing either must fall back, not fail mid-plan.
    """
    try:
        from pypaimon_rust.datafusion import PaimonCatalog, Split
    except ImportError:
        return False
    return hasattr(PaimonCatalog, 'get_table') and hasattr(Split, 'serialize')


def native_reader_available() -> bool:
    """Whether pypaimon-rust exposes the batch-read API used by PyPaimon."""
    return (native_runtime_available()
            and native_method_available('ReadBuilder', 'new_read')
            and native_method_available('TableRead', 'read'))


def native_blob_parallelism_available() -> bool:
    """Whether the native read builder accepts a per-read BLOB I/O limit."""
    return native_method_available('ReadBuilder', 'with_blob_parallelism')


def native_family_search_modes_available() -> bool:
    """Whether Rust supports family-specific global-index search modes."""
    return native_version_at_least(0, 4)


def native_version_at_least(major: int, minor: int, patch: int = 0) -> bool:
    """Compare complete package versions, including patch and pre-release ordering."""
    if not native_runtime_available():
        return False
    try:
        from importlib.metadata import PackageNotFoundError, version
    except ImportError:
        return False
    try:
        rust_version = Version(version('pypaimon-rust'))
    except (PackageNotFoundError, InvalidVersion):
        return False
    return rust_version >= Version('%d.%d.%d' % (major, minor, patch))


def native_method_available(type_name: str, method: str) -> bool:
    try:
        from pypaimon_rust import datafusion
        return callable(getattr(getattr(datafusion, type_name, None), method, None))
    except ImportError:
        return False


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
    options = loader.context().options.to_map()
    normalized = {
        str(key): _option_value_to_string(value)
        for key, value in options.items()
        if value is not None
    }
    if str(getattr(table, 'table_path', '')).startswith('oss://'):
        from pypaimon.filesystem.jindo_file_system_handler import (
            JINDO_AVAILABLE,
        )
        impl = normalized.get(OssOptions.OSS_IMPL.key())
        if JINDO_AVAILABLE and (impl is None or impl.lower() == 'jindo'):
            # This catalog is only used for Rust scan planning.
            normalized[OssOptions.OSS_IMPL.key()] = 'jindo'
    return normalized


def _read_options(table) -> dict:
    """Effective Rust read options, including FileStoreTable.copy overrides."""
    options = {
        CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): str(
            table.options.source_split_target_size()),
        CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): str(
            table.options.source_split_open_file_cost()),
        CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(): _option_value_to_string(
            table.options.options.get(CoreOptions.DELETION_VECTORS_MERGE_ON_READ)),
    }
    table_options = table.options.options
    for option in (
            CoreOptions.SCAN_VERSION,
            CoreOptions.SCAN_SNAPSHOT_ID,
            CoreOptions.SCAN_TAG_NAME,
            CoreOptions.SCAN_TIMESTAMP_MILLIS,
            CoreOptions.SCAN_WATERMARK,
            CoreOptions.GLOBAL_INDEX_SEARCH_MODE,
            CoreOptions.SCALAR_INDEX_SEARCH_MODE,
            CoreOptions.VECTOR_INDEX_SEARCH_MODE,
            CoreOptions.FULL_TEXT_INDEX_SEARCH_MODE):
        if table_options.contains_key(option.key()):
            options[option.key()] = _option_value_to_string(
                table_options.to_map()[option.key()])

    # Rust takes epoch millis but PyPaimon also accepts a timestamp string.
    if table_options.contains_key(CoreOptions.SCAN_TIMESTAMP.key()):
        from pypaimon.snapshot.time_travel_util import _parse_timestamp_to_millis
        options[CoreOptions.SCAN_TIMESTAMP_MILLIS.key()] = str(
            _parse_timestamp_to_millis(
                table_options.get(CoreOptions.SCAN_TIMESTAMP)))
    return options


def _resolved_schema_file_io_options(table) -> Optional[dict]:
    """FileIO properties for tables whose metadata needs no catalog resolution."""
    if not native_method_available('Table', 'from_resolved_schema'):
        return None
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
    if context.options is None or any(getattr(context, attr, None) is not None for attr in (
            'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader')):
        return None
    # JDBC, like filesystem catalogs, uses on-disk snapshots. Its already
    # resolved table does not need another database connection during planning.
    return _catalog_context_options(table)


def _resolved_schema_json(table) -> str:
    from pypaimon.common.json_util import JSON
    options = {str(key): _option_value_to_string(value)
               for key, value in table.table_schema.options.items() if value is not None}
    options.update(_read_options(table))
    # The timestamp string has already been converted to epoch millis.
    options.pop(CoreOptions.SCAN_TIMESTAMP.key(), None)
    return JSON.to_json(table.table_schema.copy(new_options=options))


def _predicate_to_native(predicate: Predicate) -> dict:
    """Convert PyPaimon's predicate tree to pypaimon-rust's dict API."""
    if predicate.method in ('and', 'or'):
        children = predicate.literals or []
        if not children:
            raise ValueError("Native compound predicate requires children")
        return {
            'method': predicate.method,
            'children': [_predicate_to_native(child) for child in children],
        }
    return {
        'method': predicate.method,
        'field': predicate.field,
        'literals': list(predicate.literals or []),
    }


def _restore_python_partition_paths(table, splits: List[Split]) -> None:
    """Restore legacy PyPaimon paths with one listing per bucket."""
    if not table.partition_keys:
        return
    path_factory = table.path_factory()
    bucket_files = {}
    for split in splits:
        bucket_path = path_factory.bucket_path(
            tuple(split.partition.values), split.bucket)
        candidates = []
        for data_file in split.files:
            python_path = "%s/%s" % (
                bucket_path.rstrip('/'), data_file.file_name)
            if (not data_file.external_path
                    and python_path != data_file.file_path):
                candidates.append((data_file, python_path))
        if not candidates:
            continue
        if bucket_path not in bucket_files:
            bucket_files[bucket_path] = {
                status.base_name
                for status in table.file_io.list_status(bucket_path)
            }
        for data_file, python_path in candidates:
            if data_file.file_name in bucket_files[bucket_path]:
                data_file.file_path = python_path


def _native_read_builder(table):
    """Reconstruct the Rust table and return a builder for the same schema."""
    file_io_options = _resolved_schema_file_io_options(table)
    if file_io_options is not None:
        from pypaimon_rust.datafusion import Table
        rt = Table.from_resolved_schema(
            table.table_path, _resolved_schema_json(table),
            database=table.identifier.get_database_name(),
            table=table.identifier.get_table_name(),
            branch=table.current_branch(), options=file_io_options)
        builder = rt.new_read_builder()
    else:
        from pypaimon_rust.datafusion import PaimonCatalog
        catalog = PaimonCatalog(_catalog_options(table))
        if native_method_available('Table', 'copy_with_resolved_schema'):
            # REST may keep branch schemas in the catalog only. Load the base
            # environment, then attach the schema/branch already resolved here.
            rt = catalog.get_table((
                table.identifier.get_database_name(), table.identifier.get_table_name()))
            if rt.location() != table.table_path:
                raise RuntimeError('Native catalog resolved a different table location')
            rt = rt.copy_with_resolved_schema(_resolved_schema_json(table), branch=table.current_branch())
            builder = rt.new_read_builder()
        else:
            rt = catalog.get_table(table.identifier.get_full_name())
            builder = rt.new_read_builder(_read_options(table))
    if table.current_branch() != 'main':
        branch = getattr(rt, 'branch', None)
        if not callable(branch) or branch() != table.current_branch():
            raise RuntimeError("Native table did not resolve the requested branch")
    return builder


def _configure_native_read_builder(builder, predicate, limit, projection):
    if projection is not None:
        builder = builder.with_projection(projection)
    if predicate is not None:
        builder = builder.with_filter(_predicate_to_native(predicate))
    if limit is not None:
        builder = builder.with_limit(limit)
    return builder


def native_read(table, splits, predicate: Optional[Predicate] = None,
                limit: Optional[int] = None,
                projection: Optional[List[str]] = None,
                blob_parallelism: Optional[int] = None):
    """Read Rust ``Split`` objects into PyArrow ``RecordBatch`` objects."""
    if not native_reader_available():
        raise RuntimeError(
            "read.native.enabled needs the pypaimon-rust native reader API")
    builder = _configure_native_read_builder(
        _native_read_builder(table), predicate, limit, projection)
    if blob_parallelism is not None:
        with_blob_parallelism = getattr(builder, 'with_blob_parallelism', None)
        if not callable(with_blob_parallelism):
            raise RuntimeError(
                "Installed pypaimon-rust does not support blob_parallelism")
        builder = with_blob_parallelism(blob_parallelism)
    reader = builder.new_read()
    read_arrow = getattr(reader, 'read_arrow', None)
    return (read_arrow(splits) if callable(read_arrow)
            else reader.read(splits))


def native_plan(
        table,
        predicate: Optional[Predicate] = None,
        limit: Optional[int] = None,
        projection: Optional[List[str]] = None,
        row_ranges: Optional[List[Tuple[int, int]]] = None,
        incremental_range: Optional[Tuple[int, int]] = None,
        row_position_slice: Optional[Tuple[int, int]] = None,
        row_position_shard: Optional[Tuple[int, int]] = None) -> Plan:
    """Plan with pypaimon_rust, preserving snapshot metadata.

    Native conversion or planning failures are handled by TableScan, which
    falls back to the Python planner.
    """
    if not native_runtime_available():
        raise RuntimeError(
            "scan.native-plan.enabled needs pypaimon-rust>=0.3.0 (split planning API)")
    builder = _configure_native_read_builder(
        _native_read_builder(table), predicate, limit, projection)
    if row_ranges is not None:
        builder = builder.with_row_ranges(row_ranges)
    scan = (builder.new_scan() if incremental_range is None
            else builder.new_incremental_scan(*incremental_range))
    if row_position_slice is not None:
        scan = scan.with_row_position_slice(*row_position_slice)
    if row_position_shard is not None:
        scan = scan.with_row_position_shard(*row_position_shard)
    rust_plan = scan.plan()
    rust_splits = rust_plan.splits()
    pfields = _partition_fields(table)
    # Trimmed primary keys decode per-file min/max keys (PK merge-on-read).
    kfields = table.trimmed_primary_keys_fields
    splits = [deserialize_split_v1(split.serialize(), pfields, kfields) for split in rust_splits]
    if table.options.native_read_enabled():
        # Retain the opaque Rust split next to the Python metadata view. The
        # normal planner/reader contract remains a Python Split list, while
        # native reads can consume the exact Rust split without a second lossy
        # conversion. Any Python split transformation creates a fresh object
        # without this marker and thus safely falls back to the Python reader.
        for split, rust_split in zip(splits, rust_splits):
            split._native_split = rust_split
    _restore_python_partition_paths(table, splits)
    snapshot_id = getattr(rust_plan, 'snapshot_id', None)
    if callable(snapshot_id):
        snapshot_id = snapshot_id()
    elif splits:
        snapshot_id = splits[0].snapshot_id
    else:
        # Older bindings cannot distinguish an empty committed snapshot from a
        # table without snapshots. Let the Python scanner recover the metadata.
        raise RuntimeError("Native runtime cannot report an empty plan's snapshot")
    return Plan(splits, snapshot_id=snapshot_id)
