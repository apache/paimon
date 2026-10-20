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

"""Compatibility exports for Native planning and reading adapters."""

from pypaimon.execution.native.runtime import (
    _raise_if_native_fork_safety_error,
    native_runtime_available,
)
from pypaimon.execution.native.table import (
    _native_blob_view_supported,
    _partition_fields,
    _catalog_metastore,
    _option_value_to_string,
    _catalog_options,
    _catalog_context_options,
    _resolved_schema_file_io_options,
    _resolved_schema_json,
    _resolved_rest_table_response,
    _NativeRestTableCache,
    _rest_data_token,
    _validate_native_fuse_binding,
    _native_table,
    _load_native_table,
    _with_native_resolved_schema,
    _with_native_read_snapshot,
    _native_query_auth_supported,
)
from pypaimon.execution.native.read.builder import (
    _predicate_to_native,
    _native_read_builder,
    _configure_native_read_builder,
)
from pypaimon.execution.native.read.split_adapter import (
    _native_read_split,
    native_split_from_python,
)
from pypaimon.execution.native.read.table_read import (
    _prepare_native_read,
    native_read,
)
from pypaimon.execution.native.read.plan import (
    native_plan,
    native_stream_scan,
    _from_native_plan,
)
from pypaimon.read.split_serializer import (
    deserialize_split_v1,
    serialize_split_v1,
)

__all__ = [
    '_raise_if_native_fork_safety_error',
    'native_runtime_available',
    '_native_blob_view_supported',
    '_partition_fields',
    '_catalog_metastore',
    '_option_value_to_string',
    '_catalog_options',
    '_catalog_context_options',
    '_resolved_schema_file_io_options',
    '_resolved_schema_json',
    '_resolved_rest_table_response',
    '_NativeRestTableCache',
    '_rest_data_token',
    '_validate_native_fuse_binding',
    '_native_table',
    '_load_native_table',
    '_with_native_resolved_schema',
    '_with_native_read_snapshot',
    '_native_query_auth_supported',
    '_predicate_to_native',
    '_native_read_builder',
    '_configure_native_read_builder',
    '_native_read_split',
    'native_split_from_python',
    '_prepare_native_read',
    'native_read',
    'native_plan',
    'native_stream_scan',
    '_from_native_plan',
    'deserialize_split_v1',
    'serialize_split_v1',
]
