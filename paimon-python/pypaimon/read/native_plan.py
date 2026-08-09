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

from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options_utils import OptionsUtils
from pypaimon.common.predicate import Predicate
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
    options = loader.context().options.to_map()
    normalized = {
        str(key): _option_value_to_string(value)
        for key, value in options.items()
        if value is not None
    }
    metastore = _catalog_metastore(loader)
    if metastore is None:
        raise ValueError("native_plan requires an exact built-in catalog loader")
    normalized[CatalogOptions.METASTORE.key()] = metastore
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
    }
    table_options = table.options.options
    for option in (
            CoreOptions.SCAN_SNAPSHOT_ID,
            CoreOptions.SCAN_TAG_NAME,
            CoreOptions.SCAN_TIMESTAMP_MILLIS):
        if table_options.contains_key(option.key()):
            options[option.key()] = _option_value_to_string(
                table_options.get(option))

    # Rust takes epoch millis but PyPaimon also accepts a timestamp string.
    if table_options.contains_key(CoreOptions.SCAN_TIMESTAMP.key()):
        from pypaimon.snapshot.time_travel_util import _parse_timestamp_to_millis
        options[CoreOptions.SCAN_TIMESTAMP_MILLIS.key()] = str(
            _parse_timestamp_to_millis(
                table_options.get(CoreOptions.SCAN_TIMESTAMP)))
    return options


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


def native_plan(
        table,
        predicate: Optional[Predicate] = None,
        limit: Optional[int] = None,
        projection: Optional[List[str]] = None,
        row_ranges: Optional[List[Tuple[int, int]]] = None) -> List[Split]:
    """Plan with pypaimon_rust and return the decoded pypaimon splits.

    Native conversion or planning failures are handled by TableScan, which
    falls back to the Python planner.
    """
    if not native_runtime_available():
        raise RuntimeError(
            "scan.native-plan.enabled needs pypaimon-rust>=0.3.0 (split planning API)")
    from pypaimon_rust.datafusion import PaimonCatalog

    rt = PaimonCatalog(_catalog_options(table)).get_table(table.identifier.get_full_name())
    builder = rt.new_read_builder(_read_options(table))
    if projection is not None:
        builder = builder.with_projection(projection)
    if predicate is not None:
        builder = builder.with_filter(_predicate_to_native(predicate))
    if limit is not None:
        builder = builder.with_limit(limit)
    if row_ranges is not None:
        builder = builder.with_row_ranges(row_ranges)
    rust_splits = builder.new_scan().plan().splits()
    pfields = _partition_fields(table)
    # Trimmed primary keys decode per-file min/max keys (PK merge-on-read).
    kfields = table.trimmed_primary_keys_fields
    splits = [deserialize_split_v1(s.serialize(), pfields, kfields) for s in rust_splits]
    _restore_python_partition_paths(table, splits)
    return splits
