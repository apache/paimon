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

"""Authorization-based pruning of the Python file scanner."""

import json as _json

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.push_down_utils import exclude_predicate_with_fields, predicate_field_names


def prune_scanner_by_auth(table, scanner, auth_result):
    masked = set(auth_result.column_masking or {}) if auth_result is not None else set()
    predicate = scanner._user_predicate
    effective = exclude_predicate_with_fields(predicate, masked)
    if effective != scanner.predicate:
        partition_key_predicate = scanner.partition_key_predicate
        scanner._set_predicate(effective)
        if scanner._explicit_partition_filter:
            scanner.partition_key_predicate = partition_key_predicate
    scanner.auth_partition_predicate = None
    scanner.auth_has_non_partition_filter = bool(
        predicate is not None and masked & predicate_field_names(predicate))
    if auth_result is None or not auth_result.filter:
        return
    partition_preds, has_non_partition = __split_auth_filter(table, auth_result)
    if partition_preds:
        combined = PredicateBuilder.and_predicates(partition_preds)
        scanner.auth_partition_predicate = combined
    scanner.auth_has_non_partition_filter |= has_non_partition


def __split_auth_filter(table, auth_result):
    partition_keys = list(table.partition_keys or [])
    if not partition_keys:
        return [], bool(auth_result.filter)

    partition_preds = []
    has_non_partition = False
    partition_key_set = set(partition_keys)
    partition_index_map = {name: i for i, name in enumerate(partition_keys)}

    for json_str in (auth_result.filter or []):
        pred = __try_parse_partition_predicate(table, json_str, partition_key_set, partition_index_map)
        if pred is not None:
            partition_preds.append(pred)
        else:
            has_non_partition = True
    return partition_preds, has_non_partition


def __try_parse_partition_predicate(table, json_str, partition_keys, partition_index_map):
    data = _json.loads(json_str)
    if data is None or data.get("kind") != "LEAF":
        return None
    transform = data.get("transform", {})
    if transform.get("name") != "FIELD_REF":
        return None
    field_name = transform.get("fieldRef", {}).get("name")
    if field_name is None or field_name not in partition_keys:
        return None
    field_index = partition_index_map.get(field_name)
    if field_index is None:
        return None

    partition_field_type = None
    for f in table.fields:
        if f.name == field_name:
            partition_field_type = getattr(f.type, 'type', '')
            break
    base_type = partition_field_type.split('(')[0] if partition_field_type else ''
    safe_types = {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'STRING', 'VARCHAR', 'CHAR'}
    if base_type not in safe_types:
        return None

    function = data.get("function", "")
    literals = data.get("literals", [])
    method_map = {
        "EQUAL": "equal", "NOT_EQUAL": "notEqual",
        "LESS_THAN": "lessThan", "LESS_OR_EQUAL": "lessOrEqual",
        "GREATER_THAN": "greaterThan", "GREATER_OR_EQUAL": "greaterOrEqual",
        "IS_NULL": "isNull", "IS_NOT_NULL": "isNotNull",
        "IN": "in", "NOT_IN": "notIn",
    }
    method = method_map.get(function)
    if method is None:
        return None
    return Predicate(method=method, index=field_index, field=field_name, literals=literals)
