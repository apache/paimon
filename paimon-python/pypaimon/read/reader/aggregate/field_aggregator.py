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

"""Per-field aggregator abstraction used by the ``aggregation`` merge
engine.

Each non-PK field is reduced across rows sharing the same primary key
by one ``FieldAggregator`` instance picked per field. The
``AggregateMergeFunction`` drives the lifecycle: ``reset()`` at the
start of each key group, ``agg()`` per input value, and the final
accumulator is read out to build the merged row.
"""

from abc import ABC, abstractmethod
from typing import Any

from pypaimon.schema.data_types import DataType


class FieldAggregator(ABC):
    """Per-field aggregator base class.

    Concrete subclasses implement :meth:`agg` and may override
    :meth:`reset`. :meth:`retract` is intentionally left as the default
    "refuse" implementation: pypaimon's ``AggregateMergeFunction``
    rejects ``DELETE`` / ``UPDATE_BEFORE`` rows up-front, so no
    aggregator's ``retract`` is reachable from the read path. The hook
    is kept so a future PR can add retract semantics without changing
    every subclass.
    """

    def __init__(self, name: str, field_type: DataType):
        self.name = name
        self.field_type = field_type

    @abstractmethod
    def agg(self, accumulator: Any, input_field: Any) -> Any:
        """Combine ``accumulator`` with ``input_field`` and return the
        new accumulator. Called once per row in the key group, in
        arrival order (sequence-number ascending). ``accumulator`` is
        ``None`` before the first add.
        """

    def reset(self) -> None:
        """Reset internal state at the start of a new key group.

        Default is a no-op. Aggregators that carry per-group bookkeeping
        beyond the externally-passed accumulator (e.g. ``first_value``'s
        "have we seen any row yet?" flag) must override this.
        """

    def retract(self, accumulator: Any, retract_field: Any) -> Any:
        """Refuse the retract operation by default.

        ``AggregateMergeFunction`` rejects retract rows at :meth:`add`
        time, so this path is currently unreachable from the read
        pipeline. The hook is kept for forward-compatibility: a future
        PR that wires retract through the merge function can override
        this on the aggregators that actually support it (sum, product,
        last_value, ...).
        """
        raise NotImplementedError(
            "Aggregator '{}' does not support retract; the aggregation "
            "merge engine does not implement DELETE / UPDATE_BEFORE "
            "handling.".format(self.name)
        )
