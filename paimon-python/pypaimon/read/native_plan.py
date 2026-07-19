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
The predicate is applied pypaimon-side (partition pruning + row/limit filter),
so results match the normal path.
"""

from typing import List, Optional

from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options_utils import OptionsUtils
from pypaimon.read.split import Split
from pypaimon.read.split_serializer import deserialize_split_v1


def native_runtime_available() -> bool:
    """Whether an installed pypaimon-rust exposes the split-planning API."""
    try:
        from pypaimon_rust.datafusion import PaimonCatalog
    except ImportError:
        return False
    return hasattr(PaimonCatalog, 'get_table')


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


def _catalog_options(table) -> dict:
    """Catalog options that built this table, to reconstruct the Rust catalog."""
    loader = getattr(getattr(table, 'catalog_environment', None), 'catalog_loader', None)
    if loader is None:
        raise ValueError("native_plan requires a catalog-backed table (no catalog loader)")
    options = loader.context().options.to_map()
    normalized = {
        str(key): OptionsUtils.convert_to_string(value)
        for key, value in options.items()
        if value is not None
    }
    metastore = _catalog_metastore(loader)
    if metastore is None:
        raise ValueError("native_plan requires an exact built-in catalog loader")
    normalized[CatalogOptions.METASTORE.key()] = metastore
    return normalized


def _read_options(table) -> dict:
    """Effective split-shaping options, including FileStoreTable.copy overrides."""
    return {
        CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): str(
            table.options.source_split_target_size()),
        CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): str(
            table.options.source_split_open_file_cost()),
    }


def native_plan(table) -> List[Split]:
    """Plan with pypaimon_rust and return the decoded pypaimon splits.

    Predicate/limit are not pushed to the native planner (pushdown is a
    follow-up); pypaimon applies them at read time.
    """
    if not native_runtime_available():
        raise RuntimeError(
            "scan.native-plan.enabled needs pypaimon-rust>=0.3.0 (split planning API)")
    from pypaimon_rust.datafusion import PaimonCatalog

    rt = PaimonCatalog(_catalog_options(table)).get_table(table.identifier.get_full_name())
    rust_splits = rt.new_read_builder(_read_options(table)).new_scan().plan().splits()
    pfields = _partition_fields(table)
    # Trimmed primary keys decode per-file min/max keys (PK merge-on-read).
    kfields = table.trimmed_primary_keys_fields
    return [deserialize_split_v1(s.serialize(), pfields, kfields) for s in rust_splits]
