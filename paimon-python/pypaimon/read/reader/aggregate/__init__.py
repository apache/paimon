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

"""FieldAggregator registry and factory entry point.

Looks up the registered factory for an aggregator identifier (``"sum"``,
``"last_value"``, ...) read from table options and builds an instance
for it. Concrete aggregators register themselves at import time via
:func:`register_aggregator`; importing this package eagerly imports the
built-in aggregator module so the registrations always happen,
regardless of which call site triggers the first lookup.
"""

from typing import Callable, Dict, TYPE_CHECKING

from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator
from pypaimon.schema.data_types import DataType

if TYPE_CHECKING:
    from pypaimon.common.options.core_options import CoreOptions


# Module-global registry keyed by aggregator identifier
# (``"sum"``, ``"last_value"`` ...).
_FACTORIES: Dict[str, Callable[[DataType, str, "CoreOptions"], FieldAggregator]] = {}


def register_aggregator(
    identifier: str,
    factory: Callable[[DataType, str, "CoreOptions"], FieldAggregator],
) -> None:
    """Register ``factory`` under ``identifier``.

    Re-registering an identifier replaces the existing factory. The
    built-in aggregators register themselves at module-import time from
    :mod:`aggregators`.
    """
    _FACTORIES[identifier] = factory


def create_field_aggregator(
    field_type: DataType,
    field_name: str,
    agg_func_name: str,
    options: "CoreOptions",
) -> FieldAggregator:
    """Build a ``FieldAggregator`` for ``agg_func_name``.

    Raises ``ValueError`` if the identifier was never registered, so
    typos or out-of-scope aggregators surface at merge-function
    construction time rather than at the first row.
    """
    factory = _FACTORIES.get(agg_func_name)
    if factory is None:
        raise ValueError(
            "Use unsupported aggregation '{}' or spell aggregate function "
            "incorrectly! Supported aggregators in pypaimon: {}".format(
                agg_func_name, sorted(_FACTORIES.keys())
            )
        )
    return factory(field_type, field_name, options)


# Eager-import the built-in aggregator module so its top-level
# ``register_aggregator(...)`` calls populate ``_FACTORIES`` before any
# caller looks anything up. Placed at the bottom of the module so the
# names ``register_aggregator`` / ``FieldAggregator`` aggregators
# imports back from here are already defined when its import runs.
from pypaimon.read.reader.aggregate import aggregators  # noqa: E402, F401
