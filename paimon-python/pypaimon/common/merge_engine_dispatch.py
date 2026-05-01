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

"""Centralised merge-engine dispatch.

Both the read path (``MergeFileSplitRead``) and the write path
(``KeyValueDataWriter``'s in-memory merge buffer) need to pick a
``MergeFunction`` based on the table's ``merge-engine`` option. This
module is the single source of truth so the two sides cannot drift.

Mirrors Java ``MergeFunctionFactory`` (paimon-core/.../mergetree/
compact/MergeFunctionFactory.java).
"""

from typing import List

from pypaimon.common.options.core_options import MergeEngine
from pypaimon.read.reader.deduplicate_merge_function import \
    DeduplicateMergeFunction
from pypaimon.read.reader.partial_update_merge_function import \
    PartialUpdateMergeFunction


# Boolean-valued options that, when truthy, opt the table into
# behaviour the Python PartialUpdateMergeFunction does not implement.
# Mirrors org.apache.paimon.CoreOptions and the fallback keys in
# PartialUpdateMergeFunction.java.
_PARTIAL_UPDATE_UNSUPPORTED_BOOLEAN_OPTIONS = (
    "ignore-delete",
    "partial-update.ignore-delete",
    "first-row.ignore-delete",
    "deduplicate.ignore-delete",
    "partial-update.remove-record-on-delete",
    "partial-update.remove-record-on-sequence-group",
)
_FIELDS_PREFIX = "fields."
_FIELD_SEQUENCE_GROUP_SUFFIX = ".sequence-group"
_FIELD_AGGREGATE_FUNCTION_SUFFIX = ".aggregate-function"
_DEFAULT_AGGREGATE_FUNCTION_KEY = "fields.default-aggregate-function"


def build_merge_function(
    *,
    engine: MergeEngine,
    raw_options: dict,
    key_arity: int,
    value_arity: int,
    value_field_nullables: List[bool],
):
    """Pick the MergeFunction for the table's ``merge-engine`` option.

    ``engine`` and ``raw_options`` come from the table's ``CoreOptions``
    (typically ``table.options.merge_engine()`` and
    ``table.options.options.to_map()``). ``key_arity`` / ``value_arity``
    / ``value_field_nullables`` describe the value-side schema the
    caller wants the merge function to operate on -- for the read path
    this is the projected read schema, for the write path it's the full
    table schema (minus primary keys).
    """
    if engine == MergeEngine.DEDUPLICATE:
        return DeduplicateMergeFunction()
    if engine == MergeEngine.PARTIAL_UPDATE:
        unsupported = partial_update_unsupported_options(raw_options)
        if unsupported:
            raise NotImplementedError(
                "merge-engine 'partial-update' is enabled together with "
                "options that pypaimon does not yet implement: {}. The "
                "supported subset is per-key last-non-null merge with "
                "no sequence-group, no per-field aggregator override, "
                "no ignore-delete and no partial-update.remove-record-on-* "
                "flags. Use the Java client for the full feature set, or "
                "open an issue to track Python support.".format(
                    ", ".join(sorted(unsupported))
                )
            )
        return PartialUpdateMergeFunction(
            key_arity=key_arity,
            value_arity=value_arity,
            nullables=list(value_field_nullables),
        )
    raise NotImplementedError(
        "merge-engine '{}' is not implemented in pypaimon yet "
        "(supported: deduplicate, partial-update). Use the Java "
        "client or open an issue to track support.".format(engine.value)
    )


def partial_update_unsupported_options(raw_options: dict):
    """Return the set of option keys this table sets that
    ``PartialUpdateMergeFunction`` does not yet support. Empty set
    means we can safely run the simple last-non-null merge.
    """
    flagged = set()
    for key, value in raw_options.items():
        if (key in _PARTIAL_UPDATE_UNSUPPORTED_BOOLEAN_OPTIONS
                and _option_is_truthy(value)):
            flagged.add(key)
        elif key == _DEFAULT_AGGREGATE_FUNCTION_KEY:
            flagged.add(key)
        elif key.startswith(_FIELDS_PREFIX) and (
                key.endswith(_FIELD_SEQUENCE_GROUP_SUFFIX)
                or key.endswith(_FIELD_AGGREGATE_FUNCTION_SUFFIX)):
            flagged.add(key)
    return flagged


def _option_is_truthy(raw):
    if raw is None:
        return False
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in ("true", "1", "yes", "on")
    return bool(raw)
