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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Replayable Paimon source for resumable Ray updates."""

import hashlib
import marshal
import pickle
from typing import Any, Callable, Dict, List, Optional

__all__ = ["PaimonCoBucketedJoinOffsetSource", "PaimonOffsetSource"]


class PaimonOffsetSource:
    """A snapshot-pinned Paimon source processed in offset units.

    ``transform`` is rebuilt for every window. It must be deterministic and
    must not depend on rows from another window. Append and data-evolution
    sources are sliced by ``rows_per_unit``; other sources use scan splits.
    """

    def __init__(
            self,
            table_identifier: str,
            *,
            projection: Optional[List[str]] = None,
            filter=None,
            transform: Optional[Callable[[Any], Any]] = None,
            snapshot_id: Optional[int] = None,
            rows_per_unit: int = 1_000_000,
            units_per_checkpoint: int = 1,
            ray_remote_args: Optional[Dict[str, Any]] = None,
            concurrency: Optional[int] = None,
            override_num_blocks: Optional[int] = None,
            **read_args):
        if not isinstance(table_identifier, str) or not table_identifier:
            raise ValueError("table_identifier is required.")
        if (isinstance(rows_per_unit, bool)
                or not isinstance(rows_per_unit, int)
                or rows_per_unit <= 0):
            raise ValueError(
                "rows_per_unit must be a positive integer.")
        if (isinstance(units_per_checkpoint, bool)
                or not isinstance(units_per_checkpoint, int)
                or units_per_checkpoint <= 0):
            raise ValueError(
                "units_per_checkpoint must be a positive integer.")
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
        self.rows_per_unit = rows_per_unit
        self.units_per_checkpoint = units_per_checkpoint
        self.ray_remote_args = (
            dict(ray_remote_args) if ray_remote_args is not None else None)
        self.concurrency = concurrency
        self.override_num_blocks = override_num_blocks
        self.read_args = dict(read_args)

    def _retention_tables(self):
        return {"source": self.table_identifier}

    def _resolve_snapshot_id(
            self,
            catalog,
            checkpoint_plan=None,
            retained_snapshot_id=None):
        checkpoint_snapshot_id = (
            checkpoint_plan.get("snapshot_id")
            if checkpoint_plan is not None else None)
        if (checkpoint_plan is not None
                and checkpoint_plan.get("table") != self.table_identifier):
            raise ValueError(
                "PaimonOffsetSource does not match the saved checkpoint.")
        snapshot_ids = {
            snapshot_id
            for snapshot_id in (
                self.snapshot_id,
                checkpoint_snapshot_id,
                retained_snapshot_id,
            )
            if snapshot_id is not None
        }
        if len(snapshot_ids) > 1:
            raise ValueError(
                "PaimonOffsetSource snapshot_id differs from its checkpoint.")
        snapshot_id = (
            self.snapshot_id
            or checkpoint_snapshot_id
            or retained_snapshot_id
        )
        if snapshot_id is None:
            latest = (
                catalog.get_table(self.table_identifier)
                .snapshot_manager()
                .get_latest_snapshot()
            )
            snapshot_id = latest.id if latest is not None else None
        return snapshot_id

    def _bind(
            self,
            catalog,
            checkpoint_plan=None,
            retention_tags=None,
            retained_snapshot_ids=None,
            catalog_options=None):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.read.read_builder import ReadBuilder

        retention_tags = retention_tags or {}
        retained_snapshot_ids = retained_snapshot_ids or {}
        tag_name = retention_tags.get("source")
        table = catalog.get_table(self.table_identifier)
        snapshot_id = self._resolve_snapshot_id(
            catalog,
            checkpoint_plan,
            retained_snapshot_ids.get("source"),
        )
        if snapshot_id is None:
            raise ValueError(
                "PaimonOffsetSource requires a source snapshot.")

        if tag_name is not None:
            table = table.copy({
                CoreOptions.SCAN_TAG_NAME.key(): tag_name,
            })
        elif snapshot_id is not None:
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
        splits = read_builder.new_scan().plan().splits()
        units = _build_offset_units(table, splits, self.rows_per_unit)
        fingerprint = self._fingerprint(snapshot_id, units, read_type)
        plan = {
            "kind": "paimon-units-v1",
            "table": self.table_identifier,
            "snapshot_id": snapshot_id,
            "fingerprint": fingerprint,
            "num_units": len(units),
            "rows_per_unit": self.rows_per_unit,
            "units_per_checkpoint": self.units_per_checkpoint,
            "retentions": [{
                "role": "source",
                "table": self.table_identifier,
                "snapshot_id": snapshot_id,
            }],
        }
        if checkpoint_plan is not None and plan != checkpoint_plan:
            raise ValueError(
                "PaimonOffsetSource does not match the saved checkpoint.")
        return _BoundPaimonOffsetSource(
            self, table, units, read_type, nested_name_paths, plan)

    def _fingerprint(self, snapshot_id, units, read_type):
        transform_identity = self._transform_identity()
        payload = (
            self.table_identifier,
            snapshot_id,
            self.projection,
            self.filter,
            transform_identity,
            read_type,
            units,
            self.concurrency,
            self.override_num_blocks,
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
            function = getattr(
                self.transform, "__func__", self.transform)
            code = getattr(function, "__code__", None)
            if code is None:
                call = getattr(self.transform, "__call__", None)
                function = getattr(call, "__func__", call)
                code = getattr(function, "__code__", None)
            if code is None:
                raise ValueError(
                    "PaimonOffsetSource transform must be a Python "
                    "function or a serializable callable.")
            encoded = pickle.dumps((
                getattr(function, "__module__", None),
                getattr(function, "__qualname__", None),
                marshal.dumps(code),
                repr(getattr(function, "__defaults__", None)),
                repr(getattr(function, "__kwdefaults__", None)),
            ), protocol=4)
        return hashlib.sha256(encoded).hexdigest()


class PaimonCoBucketedJoinOffsetSource(PaimonOffsetSource):
    """A replayable join of two co-bucketed Paimon tables.

    The bucket join is materialized into an internal Paimon table before
    ``transform`` runs. The table is bucketed by the target file group, so a
    target group belongs to exactly one checkpoint window. ``transform`` may
    filter routed row ids, but must not add or change them.
    """

    _needs_target_read_plan = True

    def __init__(
            self,
            left: str,
            right: str,
            *,
            on,
            left_projection: Optional[List[str]] = None,
            right_projection: Optional[List[str]] = None,
            transform: Optional[Callable[[Any], Any]] = None,
            row_id_col: str = "row_id",
            left_snapshot_id: Optional[int] = None,
            right_snapshot_id: Optional[int] = None,
            units_per_checkpoint: int = 1,
            routing_buckets: int = 256,
            route_units_per_commit: int = 64,
            ray_remote_args: Optional[Dict[str, Any]] = None):
        super().__init__(
            left,
            projection=left_projection,
            transform=transform,
            snapshot_id=left_snapshot_id,
            rows_per_unit=1,
            units_per_checkpoint=units_per_checkpoint,
            ray_remote_args=ray_remote_args,
        )
        if transform is None:
            raise ValueError("transform is required.")
        if not isinstance(right, str) or not right:
            raise ValueError("right is required.")
        if not isinstance(row_id_col, str) or not row_id_col:
            raise ValueError("row_id_col is required.")
        if row_id_col in {
                "_ROW_ID", "_ROUTE_KIND", "_SOURCE_UNIT", "_TARGET_GROUP"}:
            raise ValueError("row_id_col is reserved.")
        for name, value in (
                ("routing_buckets", routing_buckets),
                ("route_units_per_commit", route_units_per_commit)):
            if (isinstance(value, bool)
                    or not isinstance(value, int)
                    or value <= 0):
                raise ValueError("{} must be a positive integer.".format(name))
        if right_snapshot_id is not None and (
                isinstance(right_snapshot_id, bool)
                or not isinstance(right_snapshot_id, int)
                or right_snapshot_id <= 0):
            raise ValueError(
                "right_snapshot_id must be a positive integer.")
        self.right_table_identifier = right
        self.on = on
        self.left_projection = self.projection
        self.right_projection = (
            list(right_projection)
            if right_projection is not None else None)
        if (self.right_projection is not None
                and row_id_col not in self.right_projection):
            raise ValueError(
                "right_projection must include row_id_col {!r}.".format(
                    row_id_col))
        self.row_id_col = row_id_col
        self.right_snapshot_id = right_snapshot_id
        self.routing_buckets = routing_buckets
        self.route_units_per_commit = route_units_per_commit

    def _retention_tables(self):
        return {
            "source": self.table_identifier,
            "join-right": self.right_table_identifier,
        }

    def _resolve_right_snapshot_id(
            self,
            catalog,
            checkpoint_plan=None,
            retained_snapshot_id=None):
        if (checkpoint_plan is not None
                and checkpoint_plan.get("right_table")
                != self.right_table_identifier):
            raise ValueError(
                "PaimonCoBucketedJoinOffsetSource does not match the saved "
                "checkpoint.")
        checkpoint_snapshot_id = (
            checkpoint_plan.get("right_snapshot_id")
            if checkpoint_plan is not None else None)
        snapshot_ids = {
            snapshot_id
            for snapshot_id in (
                self.right_snapshot_id,
                checkpoint_snapshot_id,
                retained_snapshot_id,
            )
            if snapshot_id is not None
        }
        if len(snapshot_ids) > 1:
            raise ValueError(
                "right_snapshot_id differs from the saved checkpoint.")
        snapshot_id = (
            self.right_snapshot_id
            or checkpoint_snapshot_id
            or retained_snapshot_id
        )
        if snapshot_id is None:
            latest = (
                catalog.get_table(self.right_table_identifier)
                .snapshot_manager()
                .get_latest_snapshot()
            )
            snapshot_id = latest.id if latest is not None else None
        return snapshot_id

    def _bind(
            self,
            catalog,
            checkpoint_plan=None,
            retention_tags=None,
            retained_snapshot_ids=None,
            catalog_options=None,
            target=None,
            operation_id=None,
            target_snapshot_id=None):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.ray.bucket_join import (
            _plan_table_splits_by_bucket,
            _validate_bucket_join,
        )

        retention_tags = retention_tags or {}
        retained_snapshot_ids = retained_snapshot_ids or {}
        left_snapshot_id = self._resolve_snapshot_id(
            catalog,
            checkpoint_plan,
            retained_snapshot_ids.get("source"),
        )
        right_snapshot_id = self._resolve_right_snapshot_id(
            catalog,
            checkpoint_plan,
            retained_snapshot_ids.get("join-right"),
        )
        if left_snapshot_id is None or right_snapshot_id is None:
            raise ValueError(
                "PaimonCoBucketedJoinOffsetSource requires snapshots on both "
                "tables.")

        left_table = _pin_table(
            catalog.get_table(self.table_identifier),
            left_snapshot_id,
            retention_tags.get("source"),
            CoreOptions,
        )
        right_table = _pin_table(
            catalog.get_table(self.right_table_identifier),
            right_snapshot_id,
            retention_tags.get("join-right"),
            CoreOptions,
        )
        if self.row_id_col not in right_table.field_names:
            raise ValueError(
                "right table is missing row_id_col {!r}.".format(
                    self.row_id_col))
        on_cols, bucket_count = _validate_bucket_join(
            self.table_identifier,
            self.right_table_identifier,
            left_table,
            right_table,
            self.on,
            self.left_projection,
            self.right_projection,
            "inner",
        )
        left_by_bucket, left_schema_id = (
            _plan_table_splits_by_bucket(
                left_table, self.left_projection, bucket_count))
        right_by_bucket, right_schema_id = (
            _plan_table_splits_by_bucket(
                right_table, self.right_projection, bucket_count))
        units = [
            (bucket, tuple(left_by_bucket[bucket]))
            for bucket in sorted(left_by_bucket)
        ]
        if (target is None or operation_id is None
                or target_snapshot_id is None):
            raise ValueError(
                "target, operation_id, and target_snapshot_id are required "
                "for a bucket join offset source.")
        route_table = _route_table_identifier(target, operation_id)
        route_tag = "_pypaimon_route_complete"
        payload = (
            self.table_identifier,
            left_snapshot_id,
            self.right_table_identifier,
            right_snapshot_id,
            on_cols,
            self.left_projection,
            self.right_projection,
            units,
            sorted(
                (bucket, tuple(splits))
                for bucket, splits in right_by_bucket.items()),
            self.row_id_col,
            self._transform_identity(),
            self.routing_buckets,
            self.route_units_per_commit,
            route_table,
            target_snapshot_id,
        )
        fingerprint = hashlib.sha256(
            pickle.dumps(payload, protocol=4)).hexdigest()
        plan = {
            "kind": "paimon-bucket-join-v1",
            "table": self.table_identifier,
            "snapshot_id": left_snapshot_id,
            "right_table": self.right_table_identifier,
            "right_snapshot_id": right_snapshot_id,
            "fingerprint": fingerprint,
            "num_units": self.routing_buckets,
            "route_num_units": len(units),
            "units_per_checkpoint": self.units_per_checkpoint,
            "route_units_per_commit": self.route_units_per_commit,
            "routing_buckets": self.routing_buckets,
            "row_id_col": self.row_id_col,
            "route_table": route_table,
            "route_tag": route_tag,
            "target_snapshot_id": target_snapshot_id,
            "retentions": [
                {
                    "role": "source",
                    "table": self.table_identifier,
                    "snapshot_id": left_snapshot_id,
                },
                {
                    "role": "join-right",
                    "table": self.right_table_identifier,
                    "snapshot_id": right_snapshot_id,
                },
            ],
        }
        if checkpoint_plan is not None and plan != checkpoint_plan:
            raise ValueError(
                "PaimonCoBucketedJoinOffsetSource does not match the saved "
                "checkpoint.")
        return _BoundPaimonCoBucketedJoinOffsetSource(
            self,
            units,
            right_by_bucket,
            left_schema_id,
            right_schema_id,
            on_cols,
            plan,
            dict(catalog_options or {}),
        )


def _route_table_identifier(target, operation_id):
    database, separator, _ = target.rpartition(".")
    if not separator:
        raise ValueError("target must be a fully qualified table name.")
    digest = hashlib.sha256(
        (target + "\0" + operation_id).encode("utf-8")).hexdigest()[:24]
    return database + ".__pypaimon_ray_route_" + digest


def _pin_table(table, snapshot_id, tag_name, core_options):
    if tag_name is not None:
        return table.copy({
            core_options.SCAN_TAG_NAME.key(): tag_name,
        })
    return table.copy({
        core_options.SCAN_SNAPSHOT_ID.key(): str(snapshot_id),
    })


def _build_offset_units(table, splits, rows_per_unit):
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
                        row_range.from_, row_range.to + 1, rows_per_unit):
                    end = min(
                        start + rows_per_unit - 1, row_range.to)
                    units.append(IndexedSplit(
                        split, [Range(start, end)]))
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
                    [deletion_file]
                    if deletion_file is not None else None),
                snapshot_id=split.snapshot_id,
            )
            for start in range(0, data_file.row_count, rows_per_unit):
                end = min(start + rows_per_unit, data_file.row_count)
                if start == 0 and end == data_file.row_count:
                    units.append(single)
                else:
                    units.append(SlicedSplit(
                        single,
                        {data_file.file_name: (start, end)},
                    ))
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
        step = self.source.units_per_checkpoint
        for start in range(next_offset, self.num_units, step):
            yield start, min(start + step, self.num_units)

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


class _BoundPaimonCoBucketedJoinOffsetSource:

    def __init__(
            self,
            source,
            units,
            right_by_bucket,
            left_schema_id,
            right_schema_id,
            on_cols,
            plan,
            catalog_options):
        self.source = source
        self.units = units
        self.right_by_bucket = right_by_bucket
        self.left_schema_id = left_schema_id
        self.right_schema_id = right_schema_id
        self.on_cols = on_cols
        self.plan = plan
        self.catalog_options = catalog_options
        self.route_table = None
        self.route_splits_by_bucket = None
        self.route_read_type = None
        self.route_nested_name_paths = None

    @property
    def num_units(self):
        return self.source.routing_buckets

    @property
    def route_num_units(self):
        return len(self.units)

    def windows(self, next_offset):
        step = self.source.units_per_checkpoint
        buckets = sorted(
            bucket for bucket in self.route_splits_by_bucket
            if bucket >= next_offset
        )
        for index in range(0, len(buckets), step):
            last = min(index + step, len(buckets))
            end = buckets[last - 1] + 1
            if last == len(buckets):
                end = self.num_units
            yield next_offset, end
            next_offset = end

    def route_windows(self, completed_units):
        step = self.source.route_units_per_commit
        for start in range(0, self.route_num_units, step):
            missing = [
                unit for unit in range(
                    start, min(start + step, self.route_num_units))
                if unit not in completed_units
            ]
            if missing:
                yield missing

    def read_join_units(self, unit_indexes):
        from pypaimon.ray.bucket_join import (
            _bucket_join_from_split_plans,
        )

        left_by_bucket = dict(
            self.units[index] for index in unit_indexes)
        return _bucket_join_from_split_plans(
            self.source.table_identifier,
            self.source.right_table_identifier,
            self.catalog_options,
            self.on_cols,
            self.source.left_projection,
            self.source.right_projection,
            "inner",
            left_by_bucket,
            self.right_by_bucket,
            self.left_schema_id,
            self.right_schema_id,
            self.source.ray_remote_args,
        )

    def configure_route(
            self,
            route_table,
            route_splits_by_bucket,
            read_type,
            nested_name_paths):
        self.route_table = route_table
        self.route_splits_by_bucket = route_splits_by_bucket
        self.route_read_type = read_type
        self.route_nested_name_paths = nested_name_paths

    def read_window(self, start, end):
        import ray.data

        from pypaimon.read.datasource.ray_datasource import RayDatasource
        from pypaimon.read.datasource.split_provider import (
            PreResolvedSplitProvider,
        )

        if self.route_table is None:
            raise RuntimeError("Bucket join routing is not prepared.")
        splits = []
        for bucket in range(start, end):
            splits.extend(self.route_splits_by_bucket.get(bucket, ()))
        provider = PreResolvedSplitProvider(
            self.route_table,
            splits,
            self.route_read_type,
            nested_name_paths=self.route_nested_name_paths,
        )
        dataset = ray.data.read_datasource(
            RayDatasource(provider),
            ray_remote_args=self.source.ray_remote_args,
        )
        if self.source.transform is not None:
            dataset = self.source.transform(dataset)
        return dataset
