# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Replayable Paimon source for resumable Ray writes."""

import hashlib
import pickle
import types
from typing import Any, Callable, List, Optional


def _code_identity(code):
    def constant(value):
        if isinstance(value, types.CodeType):
            return _code_identity(value)
        if isinstance(value, tuple):
            return tuple(constant(item) for item in value)
        if isinstance(value, frozenset):
            return tuple(sorted(
                (constant(item) for item in value), key=repr))
        return value

    return (
        code.co_argcount,
        getattr(code, "co_posonlyargcount", 0),
        code.co_kwonlyargcount,
        code.co_flags,
        code.co_code,
        tuple(constant(value) for value in code.co_consts),
        code.co_names,
        code.co_varnames,
        code.co_freevars,
        code.co_cellvars,
    )


def _stable_units(units):
    """Order source units deterministically for checkpoint offsets."""
    keyed = [
        (hashlib.sha256(pickle.dumps(unit, protocol=4)).digest(), unit)
        for unit in units
    ]
    keyed.sort(key=lambda item: item[0])
    return [item[1] for item in keyed], tuple(item[0] for item in keyed)


class PaimonOffsetSource:
    """A snapshot-pinned Paimon source processed in stable offset units.

    ``transform`` is rebuilt for each read window. It must be deterministic
    and must not depend on rows from another window.
    """

    def __init__(
            self,
            table_identifier: str,
            *,
            projection: Optional[List[str]] = None,
            filter=None,
            transform: Optional[Callable[[Any], Any]] = None):
        if not isinstance(table_identifier, str) or not table_identifier:
            raise ValueError("table_identifier is required.")
        if transform is not None and not callable(transform):
            raise ValueError("transform must be callable.")
        self.table_identifier = table_identifier
        self.projection = (
            list(projection) if projection is not None else None)
        self.filter = filter
        self.transform = transform

    def _bind(
            self,
            catalog,
            checkpoint_plan=None,
            source_tag=None,
            retained_snapshot_id=None,
            splits_per_window=1):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.read.read_builder import ReadBuilder

        checkpoint_snapshot_id = (
            checkpoint_plan.get("snapshot_id")
            if checkpoint_plan is not None else None)
        if (checkpoint_plan is not None
                and checkpoint_plan.get("table") != self.table_identifier):
            raise ValueError(
                "PaimonOffsetSource does not match the saved checkpoint.")
        snapshot_ids = {
            value for value in (checkpoint_snapshot_id, retained_snapshot_id)
            if value is not None}
        if len(snapshot_ids) > 1:
            raise ValueError(
                "PaimonOffsetSource snapshot differs from its checkpoint.")

        table = catalog.get_table(self.table_identifier)
        snapshot_id = next(iter(snapshot_ids), None)
        if snapshot_id is None:
            latest = table.snapshot_manager().get_latest_snapshot()
            snapshot_id = latest.id if latest is not None else None
        if snapshot_id is None:
            raise ValueError("PaimonOffsetSource requires a source snapshot.")

        if source_tag is not None:
            table = table.copy({CoreOptions.SCAN_TAG_NAME.key(): source_tag})
        else:
            table = table.copy({
                CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot_id),
            })

        read_builder = ReadBuilder(table)
        if self.filter is not None:
            read_builder = read_builder.with_filter(self.filter)
        if self.projection is not None:
            read_builder = read_builder.with_projection(self.projection)

        read_type = read_builder.read_type()
        nested_name_paths = read_builder._nested_name_paths()
        units, unit_ids = _stable_units(
            read_builder.new_scan().plan().splits())
        fingerprint = hashlib.sha256(pickle.dumps((
            self.table_identifier, snapshot_id, self.projection,
            self.filter, self._transform_identity(), read_type, unit_ids),
            protocol=4)).hexdigest()
        plan = {
            "table": self.table_identifier,
            "snapshot_id": snapshot_id,
            "fingerprint": fingerprint,
            "num_units": len(units),
            "splits_per_window": splits_per_window,
        }
        expected_plan = dict(checkpoint_plan or {})
        if checkpoint_plan is not None:
            expected_plan.setdefault("splits_per_window", splits_per_window)
        if checkpoint_plan is not None and plan != expected_plan:
            raise ValueError(
                "PaimonOffsetSource does not match the saved checkpoint.")
        self._table = table
        self._units = units
        self._read_type = read_type
        self._nested_name_paths = nested_name_paths
        self.plan = plan
        return self

    def _transform_identity(self):
        if self.transform is None:
            return None
        function = getattr(
            self.transform, "__func__",
            getattr(self.transform, "func", self.transform))
        code = getattr(function, "__code__", None)
        if code is None:
            call = getattr(self.transform, "__call__", None)
            function = getattr(call, "__func__", call)
            code = getattr(function, "__code__", None)
        identity = (
            getattr(function, "__module__", None),
            getattr(function, "__qualname__", None),
            _code_identity(code) if code is not None else None,
            type(self.transform).__module__,
            type(self.transform).__qualname__,
        )
        return hashlib.sha256(pickle.dumps(identity, protocol=4)).hexdigest()

    @property
    def num_units(self):
        return len(self._units)

    def read_window(self, start, end):
        import ray.data

        from pypaimon.read.datasource.ray_datasource import RayDatasource
        from pypaimon.read.datasource.split_provider import (
            PreResolvedSplitProvider,
        )

        provider = PreResolvedSplitProvider(
            self._table,
            self._units[start:end],
            self._read_type,
            predicate=self.filter,
            nested_name_paths=self._nested_name_paths,
        )
        dataset = ray.data.read_datasource(RayDatasource(provider))
        if self.transform is not None:
            dataset = self.transform(dataset)
        return dataset
