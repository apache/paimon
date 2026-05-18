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

"""Merge functions for primary key reduction.

A MergeFunction defines how multiple KeyValues sharing the same primary key
are reduced into one. SortMergeReader feeds a function with all KVs for a key
in ascending sequence order via reset()/add()*/get_result(), then moves on.

Phase 3 ships the production DeduplicateMergeFunction (kept identical to the
prior in-line implementation in sort_merge_reader.py) and stubs for the other
three engines so tables tagged with those engines fail loudly instead of
silently producing wrong data. Phase 6 will fill in the stubs.
"""

from abc import ABC, abstractmethod
from typing import Optional

from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.table.row.key_value import KeyValue


class MergeFunction(ABC):
    """Reduces a sequence of KeyValues sharing the same primary key into one."""

    @abstractmethod
    def reset(self) -> None:
        """Discard any state from the previous key."""

    @abstractmethod
    def add(self, kv: KeyValue) -> None:
        """Accept the next KV for the current key (caller delivers in seq order)."""

    @abstractmethod
    def get_result(self) -> Optional[KeyValue]:
        """Return the merged value for the current key, or None to drop the row."""


class MergeFunctionFactory(ABC):
    """A factory exists per-engine because some engines (PartialUpdate /
    Aggregate) build per-call instances bound to projected schemas."""

    @abstractmethod
    def create(self) -> MergeFunction:
        """Return a fresh MergeFunction. Caller owns it for one merge pass."""


class DeduplicateMergeFunction(MergeFunction):
    """Keep the latest KV (highest sequence number) for each key.

    Because SortMergeReader hands KVs over in ascending sequence order, the
    last one added is always the latest — no comparison needed here.
    """

    def __init__(self):
        self.latest_kv: Optional[KeyValue] = None

    def reset(self) -> None:
        self.latest_kv = None

    def add(self, kv: KeyValue) -> None:
        self.latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        return self.latest_kv


class DeduplicateMergeFunctionFactory(MergeFunctionFactory):
    def create(self) -> MergeFunction:
        return DeduplicateMergeFunction()


# --- Stubs reserved for Phase 6 ----------------------------------------------
# These exist so MergeFunctionFactory.create_for(options) can route every Java
# MergeEngine to a Python class today; tables tagged with these engines simply
# fail loudly instead of silently producing wrong results, and Phase 6 will
# fill in the bodies without changing any callers.


class _UnimplementedMergeFunction(MergeFunction):
    engine_name = "<unset>"

    def reset(self) -> None:
        raise NotImplementedError(
            f"MergeEngine '{self.engine_name}' compaction is not implemented yet "
            f"(planned for Phase 6)."
        )

    def add(self, kv: KeyValue) -> None:
        raise NotImplementedError(
            f"MergeEngine '{self.engine_name}' compaction is not implemented yet "
            f"(planned for Phase 6)."
        )

    def get_result(self) -> Optional[KeyValue]:
        raise NotImplementedError(
            f"MergeEngine '{self.engine_name}' compaction is not implemented yet "
            f"(planned for Phase 6)."
        )


class PartialUpdateMergeFunction(_UnimplementedMergeFunction):
    engine_name = "partial-update"


class AggregateMergeFunction(_UnimplementedMergeFunction):
    engine_name = "aggregation"


class FirstRowMergeFunction(_UnimplementedMergeFunction):
    engine_name = "first-row"


class _UnimplementedFactory(MergeFunctionFactory):
    def __init__(self, engine_name: str, impl_cls: type):
        self.engine_name = engine_name
        self.impl_cls = impl_cls

    def create(self) -> MergeFunction:
        # Build the instance now so callers see the failure at the first call
        # site they own, with the engine name in the traceback.
        return self.impl_cls()


def create_merge_function_factory(options: CoreOptions) -> MergeFunctionFactory:
    """Pick the correct factory for the table's configured merge engine.

    Unknown / unsupported engines raise here (rather than later inside the
    rewriter) so the failure points back at the configuration directly.
    """
    engine = options.merge_engine()
    if engine == MergeEngine.DEDUPLICATE:
        return DeduplicateMergeFunctionFactory()
    if engine == MergeEngine.PARTIAL_UPDATE:
        return _UnimplementedFactory("partial-update", PartialUpdateMergeFunction)
    if engine == MergeEngine.AGGREGATE:
        return _UnimplementedFactory("aggregation", AggregateMergeFunction)
    if engine == MergeEngine.FIRST_ROW:
        return _UnimplementedFactory("first-row", FirstRowMergeFunction)
    raise ValueError(f"Unsupported MergeEngine: {engine!r}")
