#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Shared driver/worker helpers for the co-located Ray joins (bucket_join, range_join)."""

import threading
from typing import Dict, List, Optional, Sequence, Union

OnSpec = Union[str, Sequence[str]]


def norm_on(on: OnSpec) -> List[str]:
    return [on] if isinstance(on, str) else list(on)


def key_type(table, col):
    # Logical type without nullability -- a present key compares the same either way.
    return str(table.field_dict[col].type).replace(" NOT NULL", "")


# Per-worker table cache, keyed by schema id (so a schema change invalidates it) and
# lock-guarded against concurrent tasks. Planning always loads a fresh table.
_TABLE_CACHE: Dict = {}
_TABLE_CACHE_LOCK = threading.Lock()


def get_table(table_id, catalog_options, schema_id=None, join_name="join"):
    from pypaimon.catalog.catalog_factory import CatalogFactory
    if schema_id is None:  # planning: always load the latest schema
        return CatalogFactory.create(catalog_options).get_table(table_id)
    key = (table_id, tuple(sorted(catalog_options.items())), schema_id)
    with _TABLE_CACHE_LOCK:
        table = _TABLE_CACHE.get(key)
        if table is None:
            table = CatalogFactory.create(catalog_options).get_table(table_id)
            if table.table_schema.id != schema_id:
                # get_table loads the latest schema; a mismatch means the schema moved
                # after the driver planned, so the split plan is stale -- fail fast.
                raise ValueError(
                    f"{table_id} schema changed during {join_name} (planned {schema_id}, "
                    f"now {table.table_schema.id}); retry.")
            _TABLE_CACHE[key] = table
        return table


def read_builder(table_id, catalog_options, projection, schema_id=None, join_name="join"):
    rb = get_table(table_id, catalog_options, schema_id, join_name).new_read_builder()
    return rb.with_projection(projection) if projection is not None else rb


def read_splits(table_id, catalog_options, projection, splits, schema_id,
                join_name="join", predicate=None):
    # Snapshot-independent but schema-dependent -> cache by schema id (in get_table).
    rb = read_builder(table_id, catalog_options, projection, schema_id, join_name)
    if predicate is not None:
        rb = rb.with_filter(predicate)
    return rb.new_read().to_arrow(splits)


def pin_latest_snapshot(table) -> Optional[int]:
    """Pin the table instance to its latest snapshot; returns the snapshot id or None
    when the table is empty. Pinning keeps every manifest read of a plan consistent."""
    from pypaimon.common.options.core_options import CoreOptions
    snapshot = table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        return None
    # Drop any existing scan.mode / point-in-time options first so snapshot-id
    # doesn't clash with them.
    opts = table.options.options
    for key in (CoreOptions.SCAN_MODE, CoreOptions.SCAN_SNAPSHOT_ID,
                CoreOptions.SCAN_TAG_NAME, CoreOptions.SCAN_WATERMARK,
                CoreOptions.SCAN_TIMESTAMP, CoreOptions.SCAN_TIMESTAMP_MILLIS,
                CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
                CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                CoreOptions.SCAN_CREATION_TIME_MILLIS):
        opts.data.pop(key.key(), None)
    opts.set(CoreOptions.SCAN_SNAPSHOT_ID, snapshot.id)
    return snapshot.id
