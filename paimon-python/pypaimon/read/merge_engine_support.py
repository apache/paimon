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
# Boolean-valued options that, when truthy, opt the table into the
# retract / delete-removal behaviour the Python
# ``AggregateMergeFunction`` does not implement.
_AGGREGATION_UNSUPPORTED_BOOLEAN_OPTIONS = (
    "aggregation.remove-record-on-delete",
)
# Aggregator identifiers the ``aggregation`` engine knows how to
# build. Duplicated from the registration site in
# ``aggregate/aggregators.py`` so this guard has no import-time
# dependency on the read-pipeline modules; keep both sides in sync
# when adding new aggregators.
_AGGREGATION_SUPPORTED_AGG_FUNCS = frozenset([
    "primary_key",
    "last_value", "last_non_null_value",
    "first_value", "first_non_null_value",
    "sum", "max", "min",
    "bool_or", "bool_and",
])
_SEQUENCE_FIELD_KEY = "sequence.field"
_FIELDS_PREFIX = "fields."
_FIELD_SEQUENCE_GROUP_SUFFIX = ".sequence-group"
_FIELD_AGGREGATE_FUNCTION_SUFFIX = ".aggregate-function"
_FIELD_IGNORE_RETRACT_SUFFIX = ".ignore-retract"
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
                "on-* flags. These options are not yet supported; open "
                "an issue to track support.".format(
                    ", ".join(sorted(unsupported))
                )
            )
        return
    if engine == MergeEngine.AGGREGATE:
        unsupported = aggregation_unsupported_options(table)
        if unsupported:
            raise NotImplementedError(
                "merge-engine 'aggregation' is enabled together with "
                "options that pypaimon does not yet implement: {}. The "
                "supported subset is per-key field aggregation with the "
                "built-in aggregators ({}); retract opt-ins "
                "(aggregation.remove-record-on-delete, "
                "fields.<f>.ignore-retract), sequence-field handling "
                "and other aggregators (product / listagg / collect / "
                "merge_map* / nested_update* / theta_sketch / "
                "hll_sketch / roaring_bitmap_*) are not yet supported. "
                "Open an issue to track support.".format(
                    ", ".join(sorted(unsupported)),
                    ", ".join(sorted(_AGGREGATION_SUPPORTED_AGG_FUNCS)),
                )
            )
        return
    raise NotImplementedError(
        "merge-engine '{}' is not implemented in pypaimon yet "
        "(supported: deduplicate, partial-update, aggregation). "
        "Open an issue to track support.".format(
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


def aggregation_unsupported_options(table) -> Set[str]:
    """Return the set of option keys configured on this table that the
    ``AggregateMergeFunction`` does not yet support. Empty set means
    the configuration is safe to run.

    Three families of options are rejected:

    1. Retract opt-ins: ``aggregation.remove-record-on-delete`` and
       ``fields.<f>.ignore-retract`` only make sense in conjunction
       with DELETE / UPDATE_BEFORE handling, which the engine does not
       implement.
    2. Sequence-field configuration: ``sequence.field`` /
       ``fields.<f>.sequence-group`` are not supported; the merge
       function does not special-case sequence fields, so we refuse
       the table rather than silently merge them as ordinary value
       columns.
    3. Out-of-scope aggregator selections: ``fields.<f>.aggregate-
       function`` and ``fields.default-aggregate-function`` set to an
       identifier this engine doesn't support yet (e.g. ``collect``,
       ``nested_update``).
    """
    flagged: Set[str] = set()
    raw = table.options.options.to_map()
    for key, value in raw.items():
        if (key in _AGGREGATION_UNSUPPORTED_BOOLEAN_OPTIONS
                and _option_is_truthy(value)):
            flagged.add(key)
        elif key == _SEQUENCE_FIELD_KEY and value:
            flagged.add(key)
        elif key == _DEFAULT_AGGREGATE_FUNCTION_KEY:
            if value not in _AGGREGATION_SUPPORTED_AGG_FUNCS:
                flagged.add(key)
        elif key.startswith(_FIELDS_PREFIX):
            if key.endswith(_FIELD_IGNORE_RETRACT_SUFFIX):
                if _option_is_truthy(value):
                    flagged.add(key)
            elif key.endswith(_FIELD_SEQUENCE_GROUP_SUFFIX):
                flagged.add(key)
            elif key.endswith(_FIELD_AGGREGATE_FUNCTION_SUFFIX):
                if value not in _AGGREGATION_SUPPORTED_AGG_FUNCS:
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
