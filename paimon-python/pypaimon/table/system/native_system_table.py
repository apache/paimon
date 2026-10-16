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

"""Plan REST metadata and read captured splits through Rust core."""

import pyarrow

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.read.native_plan import _native_table, native_method_available
from pypaimon.write.native_commit import _rest_catalog_supported

# Partitions need the catalog's list-partitions operation, which is not carried
# by a resolved Native Table. Keep that table on the existing catalog path.
_NATIVE_TABLES = frozenset((
    'snapshots', 'schemas', 'options', 'manifests', 'files', 'buckets',
    'tags', 'branches', 'file_key_ranges', 'table_indexes', 'consumers',
))


def try_plan_native_system_table(system_table):
    base = system_table.base_table
    name = system_table.system_table_name()
    if (name not in _NATIVE_TABLES or not base.options.native_read_enabled()
            or not native_method_available('Table', 'system_table')
            or not _rest_catalog_supported(base)):
        return None
    from pypaimon.table.system.system_table_scan import NativeSystemSplit
    system = _native_table(base).system_table(name)
    # Capture the full metadata view so a planned split remains reusable with
    # another projection/filter, as the existing Python SystemSplit does.
    return [NativeSystemSplit(split)
            for split in system.new_read_builder().new_scan().plan().splits()]


def read_native_system_splits(system_table, read_type, predicate, limit, splits):
    from pypaimon.read.native_plan import _predicate_to_native
    from pypaimon_rust.datafusion import SystemTableRead
    # A captured split is self-contained: reading it must not recreate REST or
    # token-backed FileIO, including after transport to a worker.
    reader_config = SystemTableRead(system_table.system_table_name())
    reader_config.with_projection([field.name for field in read_type])
    if predicate is not None:
        reader_config.with_filter(_predicate_to_native(predicate))
    if limit is not None:
        reader_config.with_limit(limit)
    reader = reader_config.read([split.native_split for split in splits])
    try:
        table = pyarrow.Table.from_batches(list(reader), schema=reader.schema)
        return table.cast(PyarrowFieldParser.from_paimon_schema(read_type))
    finally:
        reader.close()
