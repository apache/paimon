################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################


"""Public MERGE condition helpers delegate to shared execution."""

from typing import Mapping, Optional, Set

import pyarrow as pa

from pypaimon.common.predicate import Predicate


def _load_datafusion():
    from pypaimon.execution.merge_condition import _load_datafusion as execute
    return execute()


def _strip_string_literals(condition: str) -> str:
    from pypaimon.execution.merge_condition import _strip_string_literals as execute
    return execute(condition)


def rewrite_condition(condition: str) -> str:
    from pypaimon.execution.merge_condition import rewrite_condition as execute
    return execute(condition)


def remap_source_on_keys(
    rewritten: str, on_map: Mapping[str, str],
) -> str:
    from pypaimon.execution.merge_condition import remap_source_on_keys as execute
    return execute(rewritten, on_map)


def filter_batch(
    batch: pa.Table, condition: str, _pre_rewritten: bool = False,
) -> pa.Table:
    from pypaimon.execution.merge_condition import filter_batch as execute
    return execute(batch, condition, _pre_rewritten)


def apply_condition(
    batch: pa.Table, rewritten: str, empty_schema: pa.Schema,
) -> pa.Table:
    from pypaimon.execution.merge_condition import apply_condition as execute
    return execute(batch, rewritten, empty_schema)


def extract_columns(condition: str) -> Set[str]:
    from pypaimon.execution.merge_condition import extract_columns as execute
    return execute(condition)


def extract_target_columns(condition: str) -> Set[str]:
    from pypaimon.execution.merge_condition import extract_target_columns as execute
    return execute(condition)


def try_parse_self_merge_predicate(condition, fields) -> Optional[Predicate]:
    from pypaimon.execution.merge_condition import try_parse_self_merge_predicate as execute
    return execute(condition, fields)


def _parse_self_merge_expression(condition, fields):
    from pypaimon.execution.merge_condition import _parse_self_merge_expression as execute
    return execute(condition, fields)


def _to_paimon_predicate(expression, builder, fields_by_name):
    from pypaimon.execution.merge_condition import _to_paimon_predicate as execute
    return execute(expression, builder, fields_by_name)


def _comparison_predicate(node, builder, fields_by_name):
    from pypaimon.execution.merge_condition import _comparison_predicate as execute
    return execute(node, builder, fields_by_name)


def _datafusion_field(expression, fields_by_name):
    from pypaimon.execution.merge_condition import _datafusion_field as execute
    return execute(expression, fields_by_name)


def _datafusion_literal(expression):
    from pypaimon.execution.merge_condition import _datafusion_literal as execute
    return execute(expression)


def _safe_literals(field, literals) -> bool:
    from pypaimon.execution.merge_condition import _safe_literals as execute
    return execute(field, literals)
