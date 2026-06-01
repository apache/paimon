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

"""Registry that maps a system-table short name to its implementation.

Implementation classes are referenced by dotted path and imported
lazily, so adding a new table is a single registry entry plus the
new module.

The following short names are intentionally not registered here yet:

  audit_log, binlog, read_optimized, consumers, statistics,
  aggregation_fields, file_key_ranges, table_indexes,
  row_tracking, all_tables, all_partitions, all_table_options,
  catalog_options
"""

import importlib
from typing import Callable, Dict, Optional, TYPE_CHECKING, Tuple

if TYPE_CHECKING:  # pragma: no cover - import for type hints only
    from pypaimon.table.file_store_table import FileStoreTable
    from pypaimon.table.system.system_table import SystemTable


SYSTEM_TABLES: Tuple[str, ...] = (
    "snapshots",
    "schemas",
    "options",
    "manifests",
    "files",
    "partitions",
    "buckets",
    "tags",
    "branches",
)


def _lazy(module_name: str, class_name: str) -> Callable[..., "SystemTable"]:
    """Build a factory that imports the implementation on first call."""
    def factory(base_table: "FileStoreTable") -> "SystemTable":
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls(base_table)
    factory.__name__ = "load_" + class_name
    return factory


SYSTEM_TABLE_LOADERS: Dict[str, Callable[..., "SystemTable"]] = {
    "snapshots": _lazy("pypaimon.table.system.snapshots_table", "SnapshotsTable"),
    "schemas": _lazy("pypaimon.table.system.schemas_table", "SchemasTable"),
    "options": _lazy("pypaimon.table.system.options_table", "OptionsTable"),
    "manifests": _lazy("pypaimon.table.system.manifests_table", "ManifestsTable"),
    "files": _lazy("pypaimon.table.system.files_table", "FilesTable"),
    "partitions": _lazy("pypaimon.table.system.partitions_table", "PartitionsTable"),
    "buckets": _lazy("pypaimon.table.system.buckets_table", "BucketsTable"),
    "tags": _lazy("pypaimon.table.system.tags_table", "TagsTable"),
    "branches": _lazy("pypaimon.table.system.branches_table", "BranchesTable"),
}


def load(name: str, base_table: "FileStoreTable") -> Optional["SystemTable"]:
    """Return the SystemTable implementation for ``name`` or ``None``.

    Callers (typically a Catalog) treat ``None`` as "no such system
    table" and surface a ``TableNotExistException`` to the user.
    """
    factory = SYSTEM_TABLE_LOADERS.get(name)
    if factory is None:
        return None
    return factory(base_table)
