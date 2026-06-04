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

import re
from typing import Mapping, Set

import pyarrow as pa


_COL_REF_PATTERN = re.compile(r'\b([st])\.(\w+)\b')


def _require_datafusion():
    try:
        import datafusion
        return datafusion
    except ImportError:
        raise ImportError(
            "merge_into condition expressions require the 'datafusion' "
            "package. Install it with: pip install pypaimon[sql]"
        )


_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")


def _strip_string_literals(condition: str) -> str:
    return _STRING_LITERAL.sub('', condition)


def rewrite_condition(condition: str) -> str:
    parts, last = [], 0
    for m in _STRING_LITERAL.finditer(condition):
        parts.append(_COL_REF_PATTERN.sub(r'"\1.\2"', condition[last:m.start()]))
        parts.append(m.group())
        last = m.end()
    parts.append(_COL_REF_PATTERN.sub(r'"\1.\2"', condition[last:]))
    return ''.join(parts)


def remap_source_on_keys(
    rewritten: str, on_map: Mapping[str, str],
) -> str:
    for s_col, t_col in on_map.items():
        old, new = f'"s.{s_col}"', f'"t.{t_col}"'
        parts, last = [], 0
        for m in _STRING_LITERAL.finditer(rewritten):
            parts.append(rewritten[last:m.start()].replace(old, new))
            parts.append(m.group())
            last = m.end()
        parts.append(rewritten[last:].replace(old, new))
        rewritten = ''.join(parts)
    return rewritten


def filter_batch(
    batch: pa.Table, condition: str, _pre_rewritten: bool = False,
) -> pa.Table:
    if batch.num_rows == 0:
        return batch
    datafusion = _require_datafusion()
    rewritten = condition if _pre_rewritten else rewrite_condition(condition)
    ctx = datafusion.SessionContext()
    ctx.register_record_batches("_batch", [batch.to_batches()])
    result = ctx.sql(
        f'SELECT * FROM _batch WHERE {rewritten}'
    )
    return result.to_arrow_table()


def apply_condition(
    batch: pa.Table, rewritten: str, empty_schema: pa.Schema,
) -> pa.Table:
    batch = filter_batch(batch, rewritten, _pre_rewritten=True)
    if batch.num_rows == 0:
        return empty_schema.empty_table()
    return batch


def extract_columns(condition: str) -> Set[str]:
    stripped = _strip_string_literals(condition)
    return {f"{m.group(1)}.{m.group(2)}"
            for m in _COL_REF_PATTERN.finditer(stripped)}


def extract_target_columns(condition: str) -> Set[str]:
    stripped = _strip_string_literals(condition)
    return {m.group(2) for m in _COL_REF_PATTERN.finditer(stripped)
            if m.group(1) == "t"}
