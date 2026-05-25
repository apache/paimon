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

"""Table-level validation for the configured ``merge-engine`` option.

Lives outside any specific ``SplitRead`` because the table's
merge-engine configuration is a property of the whole read, not of one
split. ``TableRead`` may dispatch the same logical scan across multiple
``SplitRead`` implementations (e.g. ``MergeFileSplitRead`` for splits
that need k-way merge, ``RawFileSplitRead`` for raw-convertible splits
where keys don't overlap). The unsupported-engine and
unsupported-options checks need to fire regardless of which dispatch
branch is picked — otherwise a single fresh snapshot whose splits are
all raw-convertible would silently bypass the guard and produce wrong
results when the table configures, e.g.,
``partial-update.remove-record-on-delete=true``.
"""

from typing import Set

from pypaimon.common.options.core_options import MergeEngine

# Boolean-valued options that, when truthy, opt the table into behaviour
# the Python ``PartialUpdateMergeFunction`` does not implement.
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


def check_supported(table) -> None:
    """Raise ``NotImplementedError`` if the table's merge-engine
    configuration is outside what pypaimon's read path implements.

    Non-PK tables are always fine (no merge function involved).
    """
    if not table.is_primary_key_table:
        return
    engine = table.options.merge_engine()
    if engine == MergeEngine.DEDUPLICATE:
        return
    if engine == MergeEngine.PARTIAL_UPDATE:
        unsupported = partial_update_unsupported_options(table)
        if unsupported:
            raise NotImplementedError(
                "merge-engine 'partial-update' is enabled together with "
                "options that pypaimon does not yet implement: {}. The "
                "supported subset is per-key last-non-null merge with "
                "no sequence-group, no per-field aggregator override, "
                "no ignore-delete and no partial-update.remove-record-"
                "on-* flags. Use the Java client for the full feature "
                "set, or open an issue to track Python support.".format(
                    ", ".join(sorted(unsupported))
                )
            )
        return
    if engine == MergeEngine.AGGREGATE:
        # AggregateMergeFunction is wired up; the unsupported-option
        # guard for aggregation (rejecting tables that configure
        # retract opt-ins or out-of-scope aggregators) lands in a
        # follow-up commit. For now non-supported configurations will
        # surface as a ValueError at merge-function construction time
        # (e.g. unknown aggregator identifier) or a NotImplementedError
        # at first DELETE / UPDATE_BEFORE row.
        return
    raise NotImplementedError(
        "merge-engine '{}' is not implemented in pypaimon yet "
        "(supported: deduplicate, partial-update, aggregation). "
        "Use the Java client or open an issue to track support.".format(
            engine.value
        )
    )


def partial_update_unsupported_options(table) -> Set[str]:
    """Return the set of option keys configured on this table that
    ``PartialUpdateMergeFunction`` does not yet support. Empty set
    means the simple last-non-null merge is safe to run.
    """
    flagged: Set[str] = set()
    raw = table.options.options.to_map()
    for key, value in raw.items():
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


def _option_is_truthy(raw) -> bool:
    if raw is None:
        return False
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in ("true", "1", "yes", "on")
    return bool(raw)
