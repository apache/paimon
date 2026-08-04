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
from typing import Any, Callable, List, Optional


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
        units = list(read_builder.new_scan().plan().splits())
        import ray.cloudpickle as cloudpickle

        try:
            transform = cloudpickle.dumps(self.transform)
        except Exception as error:
            raise ValueError(
                "PaimonOffsetSource transform must be serializable.") \
                from error
        fingerprint = hashlib.sha256(pickle.dumps((
            self.table_identifier, snapshot_id, self.projection,
            self.filter, transform, read_type, units), protocol=4)).hexdigest()
        plan = {
            "table": self.table_identifier,
            "snapshot_id": snapshot_id,
            "fingerprint": fingerprint,
            "num_units": len(units),
        }
        if checkpoint_plan is not None and plan != checkpoint_plan:
            raise ValueError(
                "PaimonOffsetSource does not match the saved checkpoint.")
        self._table = table
        self._units = units
        self._read_type = read_type
        self._nested_name_paths = nested_name_paths
        self.plan = plan
        return self

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
