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

"""Read REST table metadata through the existing Rust system-table providers."""

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


def try_read_native_system_table(system_table):
    base = system_table.base_table
    name = system_table.system_table_name()
    if (name not in _NATIVE_TABLES or not base.options.native_read_enabled()
            or not native_method_available('Table', '_read_system_table')
            or not _rest_catalog_supported(base)):
        return None
    reader = _native_table(base)._read_system_table(name)
    try:
        # Preserve the Python system table's field selection and Arrow schema,
        # including an empty result. Metadata decoding and rendering run in Rust.
        schema = PyarrowFieldParser.from_paimon_schema(system_table.row_type().fields)
        batches = list(reader)
        table = pyarrow.Table.from_batches(batches, schema=reader.schema)
        return table.select(schema.names).cast(schema)
    finally:
        reader.close()
