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
from pypaimon.common.options.options_utils import OptionsUtils


_FIELDS_PREFIX = CoreOptions.FIELDS_PREFIX + "."
_AGG_SUFFIX = ".aggregate-function"
_OVERWRITE_SUFFIX = "." + CoreOptions.NESTED_SEQUENCE_OVERWRITE_ON_EQUAL
_NESTED_UPDATE = "nested_update"


def nested_update_overwrite_on_equal_false_reason(
        options: Optional[CoreOptions]) -> Optional[str]:
    """Return why Native must not run, or ``None`` when Native stays eligible.

    Only ``aggregate-function=nested_update`` fields with an explicit
    ``nested-sequence-overwrite-on-equal=false`` force Python. Unset / ``true``
    leave Native eligible.
    """
    if options is None or not isinstance(options, CoreOptions):
        return None
    raw = options.options.to_map()
    for key, value in raw.items():
        if not (key.startswith(_FIELDS_PREFIX) and key.endswith(_AGG_SUFFIX)):
            continue
        if value != _NESTED_UPDATE:
            continue
        field = key[len(_FIELDS_PREFIX):-len(_AGG_SUFFIX)]
        overwrite_key = (
            _FIELDS_PREFIX + field + _OVERWRITE_SUFFIX
        )
        if overwrite_key not in raw:
            continue
        if OptionsUtils.convert_to_boolean(raw[overwrite_key]):
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
