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

"""Shared Native eligibility checks that must agree for readers and writers.

Current Native/Rust runtimes do not implement
``nested-sequence-overwrite-on-equal=false`` for ``nested_update``. When that
option is configured, both Native read and Native write must yield to Python
so equal-sequence retention stays consistent. Default / explicit ``true``
keeps Native eligible.
"""

from typing import Optional

from pypaimon.common.options.core_options import CoreOptions


_FIELDS_PREFIX = CoreOptions.FIELDS_PREFIX + "."
_AGG_SUFFIX = ".aggregate-function"
_DEFAULT_AGG_KEY = _FIELDS_PREFIX + "default-aggregate-function"
_OVERWRITE_SUFFIX = "." + CoreOptions.NESTED_SEQUENCE_OVERWRITE_ON_EQUAL
_NESTED_UPDATE = "nested_update"
# Matches aggregation_merge_function.resolve_agg_func_name when neither a
# field-level nor table-wide aggregate-function is set.
_DEFAULT_AGG_FALLBACK = "last_non_null_value"


def _effective_aggregate_function(raw, field):
    """Resolve the aggregator that will actually run for ``field``.

    Same precedence as ``resolve_agg_func_name`` for non-PK / non-sequence
    fields: field-level ``aggregate-function``, then
    ``fields.default-aggregate-function``, then the factory default
    ``last_non_null_value``.
    """
    return (
        raw.get(_FIELDS_PREFIX + field + _AGG_SUFFIX)
        or raw.get(_DEFAULT_AGG_KEY)
        or _DEFAULT_AGG_FALLBACK
    )


def nested_update_overwrite_on_equal_false_reason(
        options: Optional[CoreOptions]) -> Optional[str]:
    """Return why Native must not run, or ``None`` when Native stays eligible.

    Fields with an explicit ``nested-sequence-overwrite-on-equal=false`` force
    Python when their effective aggregator is ``nested_update`` (field-level
    ``aggregate-function``, otherwise ``fields.default-aggregate-function``).
    Unset / ``true``, and false on non-``nested_update`` fields, leave Native
    eligible.
    """
    if options is None or not isinstance(options, CoreOptions):
        return None
    raw = options.options.to_map()
    for key in raw:
        if not (key.startswith(_FIELDS_PREFIX) and key.endswith(_OVERWRITE_SUFFIX)):
            continue
        field = key[len(_FIELDS_PREFIX):-len(_OVERWRITE_SUFFIX)]
        if options.field_nested_update_agg_nested_sequence_overwrite_on_equal(field):
            continue
        if _effective_aggregate_function(raw, field) != _NESTED_UPDATE:
            continue
        return (
            "fields.{0}.nested-sequence-overwrite-on-equal=false is not "
            "supported by the Native runtime; use the Python reader/writer "
            "so equal-sequence nested_update retention stays consistent"
            .format(field)
        )
    return None


def require_native_compatible(options, *, allow_python_fallback=True) -> Optional[str]:
    """Enforce Native eligibility for ``nested-sequence-overwrite-on-equal``.

    When Native is incompatible:
    - ``allow_python_fallback=True`` (auto routing): return the reason so the
      caller can select Python.
    - ``allow_python_fallback=False``: raise before any Native reader/writer
      can ignore the option and continue with unsupported semantics.
    """
    reason = nested_update_overwrite_on_equal_false_reason(options)
    if reason is None:
        return None
    if allow_python_fallback:
        return reason
    raise RuntimeError(
        "Native execution is required without a Python fallback, but %s"
        % reason
    )
