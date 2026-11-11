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


"""Typed split transport for Core's captured row-ID plans."""

from pypaimon.execution.native.runtime import native_runtime_available


def _with_bucket_path(table, split):
    from copy import copy
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.query_auth_split import QueryAuthSplit
    from pypaimon.read.split import DataSplit

    if isinstance(split, QueryAuthSplit):
        inner = _with_bucket_path(table, split.split)
        return split if inner is split.split else QueryAuthSplit(inner, split.auth_result)
    if isinstance(split, IndexedSplit):
        inner = _with_bucket_path(table, split.data_split())
        return split if inner is split.data_split() else IndexedSplit(inner, split.row_ranges(), split.scores())
    if not isinstance(split, DataSplit) or split.bucket_path:
        return split
    # Java keeps the table's canonical bucket path even when individual files
    # live elsewhere. Serialization retains each exact external/file path.
    captured = copy(split)
    captured.bucket_path = table.path_factory().bucket_path(
        tuple(split.partition.values), split.bucket, canonical_partition=True)
    return captured


def native_plan(table, plan):
    from pypaimon_rust.datafusion import Plan, Split
    from pypaimon.read.split_serializer import serialize_split_v1
    return Plan(plan.snapshot_id, [Split.deserialize(serialize_split_v1(_with_bucket_path(table, split)))
                                   for split in plan.splits()])


def create_native_row_id_plan(table, plan):
    if not (table.options.native_write_enabled() or table.options.native_read_enabled()
            or table.options.native_plan_enabled()) or not native_runtime_available():
        return None
    from pypaimon_rust.datafusion import RowIdPlan
    return RowIdPlan(native_plan(table, plan))


def python_groups(table, plan):
    from pypaimon.execution.native.table import _partition_fields
    from pypaimon.read.split_serializer import deserialize_split_v1
    return [(start, end, deserialize_split_v1(split.serialize(), _partition_fields(table),
                                              table.trimmed_primary_keys_fields))
            for start, end, split in plan.groups()]


class NativeRowIdPlan:
    """Transport Arrow row IDs to Core without reading or replanning files."""

    def __init__(self, table, plan, native=None, groups=None):
        self.table = table
        self.plan = plan
        self._native = native
        self.groups = groups if groups is not None else python_groups(table, self._ensure_native())

    def _ensure_native(self):
        if self._native is None:
            from pypaimon_rust.datafusion import RowIdPlan
            self._native = RowIdPlan(native_plan(self.table, self.plan))
        return self._native

    def route(self, row_ids):
        return self._ensure_native().route(row_ids)

    def route_by_bucket(self, row_ids):
        return self._ensure_native().route_by_bucket(row_ids)

    def select(self, row_ids):
        from pypaimon.execution.native.read.plan import _from_native_plan
        selected = _from_native_plan(self.table, self._ensure_native().select(row_ids))
        return self.plan._with_splits(selected.splits())

    def for_plan(self, plan, groups):
        return NativeRowIdPlan(self.table, plan, groups=groups)

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_native'] = None
        return state
