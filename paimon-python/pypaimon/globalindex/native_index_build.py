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

"""Local sorted-index builds delegated to Rust, retaining Python's commit API."""

from pypaimon.read.native_plan import (
    _native_table, _option_value_to_string, _predicate_to_native, native_method_available)
from pypaimon.write.native_commit import _rest_catalog_supported, from_native_commit_messages


def build_native_sorted_index(builder, partition_filter):
    """Return unpublished messages, or None before building for an ineligible table.

    Rust owns snapshot selection, range planning, sorting and file generation.
    Build errors propagate: a failed native build must not start a second build.
    """
    table = builder._table
    if (builder._index_type not in ('btree', 'bitmap')
            or not table.options.native_write_enabled()
            or not table.options.data_evolution_enabled()
            or not table.options.global_index_enabled()
            or table.options.deletion_vectors_enabled()
            or table.options.query_auth_enabled
            or table.is_primary_key_table
            or table.options.file_format() != 'parquet'
            or not native_method_available('Table', 'new_sorted_global_index_build_builder')
            or not _rest_catalog_supported(table)):
        return None
    native = _native_table(table).new_sorted_global_index_build_builder()
    native.with_index_column(builder._index_columns[0]).with_index_type(builder._index_type)
    native.with_options({str(key): _option_value_to_string(value)
                         for key, value in builder._user_options.items() if value is not None})
    if partition_filter is not None:
        native.with_partition_filter(_predicate_to_native(partition_filter))
    return from_native_commit_messages(table, native.build())
