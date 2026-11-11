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

"""Translate canonical read requests into Native builders."""

import json
from typing import List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.execution.native import table as native_table
from pypaimon.schema.data_types import DataField, RowType


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
    result = {
        'method': predicate.method,
        'field': predicate.field,
        'literals': list(predicate.literals or []),
    }
    if predicate.field_path:
        result['field'] = predicate.field_path[0]
        if len(predicate.field_path) > 1:
            result['nested_path'] = list(predicate.field_path[1:])
    return result


def _native_read_builder(table):
    return native_table._native_table(table).new_read_builder()


def _configure_native_read_builder(builder, predicate, limit, projection,
                                   read_type: Optional[List[DataField]] = None,
                                   include_row_kind=False):
    if read_type is not None:
        builder = builder.with_read_type(
            json.dumps(RowType(True, read_type).to_dict()))
    if projection is not None and read_type is None:
        builder = builder.with_projection(projection)
    if predicate is not None:
        builder = builder.with_filter(_predicate_to_native(predicate))
    if limit is not None:
        builder = builder.with_limit(limit)
    if include_row_kind:
        builder = builder.with_include_row_kind(True)
    return builder
