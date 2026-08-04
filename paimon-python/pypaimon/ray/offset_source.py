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
import marshal
import pickle
from typing import Any, Callable, Dict, List, Optional

__all__ = ["PaimonOffsetSource"]

_ROWS_PER_UNIT = 1_000_000


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
            transform: Optional[Callable[[Any], Any]] = None,
            snapshot_id: Optional[int] = None,
            ray_remote_args: Optional[Dict[str, Any]] = None,
            concurrency: Optional[int] = None,
            override_num_blocks: Optional[int] = None,
            **read_args):
        if not isinstance(table_identifier, str) or not table_identifier:
            raise ValueError("table_identifier is required.")
        if transform is not None and not callable(transform):
            raise ValueError("transform must be callable.")
        if snapshot_id is not None and (
                isinstance(snapshot_id, bool)
                or not isinstance(snapshot_id, int)
                or snapshot_id <= 0):
            raise ValueError("snapshot_id must be a positive integer.")
        self.table_identifier = table_identifier
        self.projection = (
            list(projection) if projection is not None else None)
        self.filter = filter
        self.transform = transform
        self.snapshot_id = snapshot_id
        self.ray_remote_args = (
            dict(ray_remote_args) if ray_remote_args is not None else None)
        self.concurrency = concurrency
        self.override_num_blocks = override_num_blocks
        self.read_args = dict(read_args)

    def _bind(
            self,
            catalog,
            checkpoint_plan=None,
            source_tag=None,
            retained_snapshot_id=None):
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
            value for value in (
                self.snapshot_id,
                checkpoint_snapshot_id,
                retained_snapshot_id,
            ) if value is not None
        }
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
        units = _build_offset_units(
            table, read_builder.new_scan().plan().splits())
        plan = {
            "kind": "paimon-units-v1",
            "table": self.table_identifier,
            "snapshot_id": snapshot_id,
            "fingerprint": self._fingerprint(snapshot_id, units, read_type),
            "num_units": len(units),
        }
        if checkpoint_plan is not None and plan != checkpoint_plan:
            raise ValueError(
                "PaimonOffsetSource does not match the saved checkpoint.")
        return _BoundPaimonOffsetSource(
            self, table, units, read_type, nested_name_paths, plan)

    def _fingerprint(self, snapshot_id, units, read_type):
        payload = (
            self.table_identifier,
            snapshot_id,
            self.projection,
            self.filter,
            self._transform_identity(),
            read_type,
            units,
            sorted(self.read_args.items()),
        )
        return hashlib.sha256(
            pickle.dumps(payload, protocol=4)).hexdigest()

    def _transform_identity(self):
        if self.transform is None:
            return None
        try:
            import ray.cloudpickle as cloudpickle

            encoded = cloudpickle.dumps(self.transform)
        except Exception:
            function = getattr(self.transform, "__func__", self.transform)
            code = getattr(function, "__code__", None)
            if code is None:
                call = getattr(self.transform, "__call__", None)
                function = getattr(call, "__func__", call)
                code = getattr(function, "__code__", None)
            if code is None:
                raise ValueError(
                    "PaimonOffsetSource transform must be serializable.")
            encoded = pickle.dumps((
                getattr(function, "__module__", None),
                getattr(function, "__qualname__", None),
                marshal.dumps(code),
                repr(getattr(function, "__defaults__", None)),
                repr(getattr(function, "__kwdefaults__", None)),
            ), protocol=4)
        return hashlib.sha256(encoded).hexdigest()


def _build_offset_units(table, splits):
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.sliced_split import SlicedSplit
    from pypaimon.read.split import DataSplit
    from pypaimon.utils.range import Range

    units = []
    for split in splits:
        if not isinstance(split, DataSplit):
            units.append(split)
            continue
        if (table.options.data_evolution_enabled()
                and all(data_file.first_row_id is not None
                        for data_file in split.files)):
            ranges = Range.sort_and_merge_overlap(
                [data_file.row_id_range() for data_file in split.files],
                True,
                True,
            )
            for row_range in ranges:
                for start in range(
                        row_range.from_, row_range.to + 1, _ROWS_PER_UNIT):
                    end = min(start + _ROWS_PER_UNIT - 1, row_range.to)
                    units.append(IndexedSplit(split, [Range(start, end)]))
            continue
        if table.is_primary_key_table:
            units.append(split)
            continue

        deletion_files = split.data_deletion_files
        for index, data_file in enumerate(split.files):
            deletion_file = (
                deletion_files[index]
                if deletion_files is not None else None)
            single = DataSplit(
                [data_file],
                split.partition,
                split.bucket,
                raw_convertible=split.raw_convertible,
                data_deletion_files=(
                    [deletion_file] if deletion_file is not None else None),
                snapshot_id=split.snapshot_id,
            )
            for start in range(0, data_file.row_count, _ROWS_PER_UNIT):
                end = min(start + _ROWS_PER_UNIT, data_file.row_count)
                if start == 0 and end == data_file.row_count:
                    units.append(single)
                else:
                    units.append(SlicedSplit(
                        single, {data_file.file_name: (start, end)}))
    return units


class _BoundPaimonOffsetSource:

    def __init__(
            self,
            source,
            table,
            units,
            read_type,
            nested_name_paths,
            plan):
        self.source = source
        self.table = table
        self.units = units
        self.read_type = read_type
        self.nested_name_paths = nested_name_paths
        self.plan = plan

    @property
    def num_units(self):
        return len(self.units)

    def windows(self, next_offset):
        for start in range(next_offset, self.num_units):
            yield start, start + 1

    def read_window(self, start, end):
        import ray.data

        from pypaimon.read.datasource.ray_datasource import RayDatasource
        from pypaimon.read.datasource.split_provider import (
            PreResolvedSplitProvider,
        )

        provider = PreResolvedSplitProvider(
            self.table,
            self.units[start:end],
            self.read_type,
            predicate=self.source.filter,
            nested_name_paths=self.nested_name_paths,
        )
        dataset = ray.data.read_datasource(
            RayDatasource(provider),
            ray_remote_args=self.source.ray_remote_args,
            concurrency=self.source.concurrency,
            override_num_blocks=self.source.override_num_blocks,
            **self.source.read_args,
        )
        if self.source.transform is not None:
            dataset = self.source.transform(dataset)
        return dataset
