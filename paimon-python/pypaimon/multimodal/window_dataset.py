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

"""Snapshot-pinned PyTorch Dataset for contiguous Paimon row windows."""

import copy
import math
import operator

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import torch
from torch.utils.data import Dataset

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.multimodal.blob_read import fetch_blob_bodies
from pypaimon.multimodal.query import ScanQuery, _PreFilterQuery
from pypaimon.read.datasource.torch_dataset import (
    SplitRangeIndex,
    row_ranges_for_split,
    select_indexed_splits,
)
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.schema.data_types import (
    is_array_blob_type, is_blob_file_type, is_map_blob_type,
)
from pypaimon.snapshot.time_travel_util import SCAN_KEYS
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range


class ContiguousWindowDataset(Dataset):
    """Map-style training samples from a single table of contiguous frames.

    The in-memory index contains only group values, order bounds, and Paimon
    row IDs, stored in Arrow and NumPy arrays. Each ``__getitem__`` reads the
    projected rows from the snapshot resolved while the index was built, reusing
    that snapshot's authorized scan plan instead of planning again. Within each
    group, ``order_key`` must contain non-null integers that increase by exactly
    one; rows from different groups never share a window. ``frame_offsets``
    selects relative frames independently for each projected column. Unspecified
    columns use ``[0]``. Alternatively, ``delta_timestamps`` specifies offsets in
    seconds on a regular grid defined by ``fps``. ``boundary`` handles scheduled
    anchors whose offsets extend beyond either end of a group:

    * ``drop`` omits them;
    * ``pad`` repeats the closest endpoint, or uses the column's ``pad_values``;
    * ``error`` rejects the dataset.

    The result contains scalar group/order values identifying the anchor, a
    list for each value column (including singleton windows), and a Boolean
    ``<column>_is_pad`` tensor per column. Offsets retain their order and
    duplicates. Padding happens before ``column_transforms`` convert each list;
    ``adapter`` then adapts the complete mapping to a model-specific contract.
    Both transforms operate on independent copies of the requested values.

    For compatibility, ``window_size`` selects forward consecutive frames for
    all columns except ``anchor_columns``, which use ``[0]``. These calls retain
    the original ``tail`` argument and single ``is_pad`` mask in their output.
    Only one of the three window definitions may be supplied. Without any,
    every column uses ``[0]``. ``stride`` schedules anchors from each group's
    first row; boundary handling never shifts that schedule.
    ``blob_parallelism`` controls concurrent BLOB reads for each item or batch.
    Video frame columns are not supported yet, because a window read would drop
    the frame metadata carried by their descriptors.
    """

    _BOUNDARY_POLICIES = ("drop", "pad", "error")

    def __init__(
            self,
            query,
            *,
            window_size=None,
            columns=None,
            anchor_columns=None,
            group_key="episode_index",
            order_key="frame_index",
            stride=1,
            tail=None,
            column_transforms=None,
            pad_values=None,
            adapter=None,
            blob_parallelism=64,
            frame_offsets=None,
            delta_timestamps=None,
            fps=None,
            tolerance_s=None,
            boundary=None):
        if not isinstance(query, ScanQuery) or isinstance(query, _PreFilterQuery):
            raise TypeError(
                "ContiguousWindowDataset is only supported on scan(), "
                "not search queries.")
        self.window_size = (
            _positive_int(window_size, "window_size")
            if window_size is not None else None)
        self.stride = _positive_int(stride, "stride")
        if sum(value is not None for value in (
                window_size, frame_offsets, delta_timestamps)) > 1:
            raise ValueError(
                "Specify only one of window_size, frame_offsets, "
                "or delta_timestamps.")
        if window_size is None and (anchor_columns is not None or tail is not None):
            raise ValueError("anchor_columns and tail require window_size.")
        if tail is not None and boundary is not None:
            raise ValueError("Specify boundary or tail, not both.")
        self.boundary = boundary if boundary is not None else (
            tail if tail is not None else "drop")
        if self.boundary not in self._BOUNDARY_POLICIES:
            raise ValueError(
                "boundary must be one of %s; got %r."
                % (self._BOUNDARY_POLICIES, self.boundary))
        if self.window_size is not None:
            self.tail = self.boundary
        self.group_key = _column(query, group_key, "group_key")
        self.order_key = _column(query, order_key, "order_key")
        if self.group_key == self.order_key:
            raise ValueError("group_key and order_key must name different columns.")
        if self.window_size is not None and "is_pad" in (self.group_key, self.order_key):
            raise ValueError("group_key and order_key must not be is_pad.")
        self.columns = _columns(
            query, columns, self.group_key, self.order_key)
        _reject_video_columns(query._table, self.columns)
        self.anchor_columns = _anchor_columns(anchor_columns, self.columns)
        self.frame_offsets = _frame_offsets(
            self.columns, frame_offsets, delta_timestamps, fps, tolerance_s,
            self.window_size, self.anchor_columns)
        reserved = {"is_pad"} if self.window_size is not None else {
            "%s_is_pad" % name for name in self.columns}
        collisions = reserved.intersection(
            self.columns + [self.group_key, self.order_key])
        if collisions:
            raise ValueError(
                "Generated padding mask names conflict with output "
                "columns: %s." % sorted(collisions))
        self.column_transforms = _column_transforms(
            column_transforms, self.columns)
        self.pad_values = _pad_values(pad_values, self.columns)
        self._read_groups = {}
        for name, offsets in self.frame_offsets.items():
            key = (offsets, self.boundary == "pad" and name in self.pad_values)
            self._read_groups.setdefault(key, []).append(name)
        if adapter is not None and not callable(adapter):
            raise TypeError("adapter must be callable or None.")
        self.adapter = adapter
        self.blob_parallelism = _positive_int(
            blob_parallelism, "blob_parallelism")

        if not query._table.options.row_tracking_enabled():
            raise ValueError(
                "ContiguousWindowDataset requires row-tracking.enabled=true.")

        index, snapshot_id = _read_window_index(
            query, self.group_key, self.order_key)
        self.snapshot_id = snapshot_id
        self._table = _pin_table(query._table, snapshot_id)
        self._plans = {}
        self._build_index(index)

    @classmethod
    def from_query(cls, query, **kwargs):
        """Build a contiguous-window Dataset from a ``ScanQuery``."""
        return cls(query, **kwargs)

    def __len__(self):
        return int(self._anchor_groups.size)

    def __getitem__(self, index):
        """Read one sample, or a list of samples for a Python slice.

        Negative indices follow Python sequence semantics. The return value is
        the pre-adapter mapping described by the class, or the adapter result
        when an adapter is configured.
        """
        if isinstance(index, slice):
            return self.__getitems__(range(*index.indices(len(self))))
        return self.__getitems__([index])[0]

    def __getitems__(self, indices):
        """Read several Dataset indices while coalescing overlapping row IDs.

        The returned list preserves the requested index order and duplicates.
        Coalescing affects only physical reads, not logical sample cardinality.
        """
        windows = [self._resolve_window(index) for index in indices]
        if not windows:
            return []
        reads = {}
        for (offsets, constant_pad), columns in self._read_groups.items():
            plans = [self._column_window(anchor, offsets, constant_pad)
                     for anchor in windows]
            row_ids = list(dict.fromkeys(
                row_id for ids, _ in plans for row_id in ids
                if row_id is not None))
            rows = self._read_rows(row_ids, columns) if row_ids else []
            by_id = dict(zip(row_ids, rows))
            for name in columns:
                reads[name] = (plans, by_id)
        return [
            self._sample(anchor, position, reads)
            for position, anchor in enumerate(windows)
        ]

    def __getstate__(self):
        # Cached plans hold planning state of one process; each DataLoader
        # worker plans the pinned snapshot once for itself.
        state = self.__dict__.copy()
        state["_plans"] = {}
        return state

    def _resolve_window(self, index):
        index = operator.index(index)
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("window index out of range")

        group_index = int(self._anchor_groups[index])
        start = int(self._anchor_starts[index])
        return group_index, start

    def _column_window(self, anchor, offsets, constant_pad):
        group_index, start = anchor
        length = int(self._group_lengths[group_index])
        group_start = int(self._group_starts[group_index])
        positions = [start + delta for delta in offsets]
        padding = [position < 0 or position >= length for position in positions]
        row_ids = [
            None if padded and constant_pad else int(self._row_ids[
                group_start + min(max(position, 0), length - 1)])
            for position, padded in zip(positions, padding)
        ]
        return row_ids, padding

    def _sample(self, anchor, position, reads):
        group_index, start = anchor
        sample = {
            self.group_key: self._group_keys[group_index],
            self.order_key: int(self._group_first_orders[group_index]) + start,
        }
        if self.window_size is not None:
            available = int(self._group_lengths[group_index]) - start
            sample["is_pad"] = torch.arange(self.window_size) >= available
        for name in self.columns:
            plans, rows = reads[name]
            row_ids, padding = plans[position]
            values = [copy.deepcopy(
                self.pad_values[name] if row_id is None else rows[row_id][name])
                for row_id in row_ids]
            if self.window_size is None:
                sample["%s_is_pad" % name] = torch.tensor(padding, dtype=torch.bool)
            transform = self.column_transforms.get(name)
            sample[name] = transform(values) if transform is not None else values
        if self.adapter is not None:
            return self.adapter(sample)
        return sample

    def _build_index(self, index):
        """Validate index rows, then store row IDs, groups, and window anchors.

        Args:
            index: Arrow table containing ``group_key``, ``order_key``, and
                Paimon's ``_ROW_ID`` for the resolved snapshot.

        The index is kept as NumPy arrays of row IDs, per-group offsets, and
        window anchors, plus one Python group value per group. Order values are
        not stored per row: contiguity makes them the group's first value plus
        the offset inside the group.
        """
        group_column = index.column(self.group_key)
        order_column = index.column(self.order_key)
        row_id_column = index.column(SpecialFields.ROW_ID.name)
        if row_id_column.null_count:
            raise ValueError(
                "ContiguousWindowDataset requires readable Paimon row IDs, "
                "but %s contains null values." % SpecialFields.ROW_ID.name)
        if group_column.null_count:
            raise ValueError(
                "%s must not contain null values." % self.group_key)
        if order_column.null_count:
            raise ValueError(
                "%s must not contain null values." % self.order_key)
        if not pa.types.is_integer(order_column.type):
            raise ValueError(
                "%s must contain integer values." % self.order_key)
        if pa.types.is_floating(group_column.type) and pc.any(
                pc.is_nan(group_column)).as_py():
            raise ValueError(
                "%s must not contain NaN values, which never compare equal to "
                "themselves and would split one group." % self.group_key)

        try:
            ordered = index.sort_by([
                (self.group_key, "ascending"),
                (self.order_key, "ascending"),
            ])
        except pa.ArrowNotImplementedError:
            raise ValueError(
                "%s values must be mutually orderable." % self.group_key)

        group_values = _contiguous_array(ordered.column(self.group_key))
        order_values = _integer_numpy(
            _contiguous_array(ordered.column(self.order_key)))
        self._row_ids = _integer_numpy(
            _contiguous_array(ordered.column(SpecialFields.ROW_ID.name)))
        self._group_starts = _group_starts(group_values)
        self._group_lengths = np.diff(
            np.append(self._group_starts, len(self._row_ids)))
        self._group_keys = group_values.take(
            pa.array(self._group_starts, type=pa.int64())).to_pylist()
        self._group_first_orders = order_values[self._group_starts]
        self._validate_contiguity(order_values)
        self._anchor_groups, self._anchor_starts = self._build_anchors()

    def _validate_contiguity(self, order_values):
        if len(order_values) < 2:
            return
        same_group = np.ones(len(order_values) - 1, dtype=bool)
        same_group[self._group_starts[1:] - 1] = False
        steps = np.diff(order_values)
        duplicate = same_group & (steps == 0)
        if duplicate.any():
            position = int(np.flatnonzero(duplicate)[0])
            raise ValueError(
                "Group %s has duplicate order value %r in %s."
                % (self._group_of(position),
                   int(order_values[position]), self.order_key))
        broken = same_group & (steps != 1)
        if broken.any():
            position = int(np.flatnonzero(broken)[0])
            raise ValueError(
                "Group %s is not contiguous in %s: %s followed by %s."
                % (self._group_of(position), self.order_key,
                   int(order_values[position]), int(order_values[position + 1])))

    def _group_of(self, position):
        group_index = int(np.searchsorted(
            self._group_starts, position, side="right")) - 1
        return self._group_keys[group_index]

    def _build_anchors(self):
        groups = []
        starts = []
        minimum = min(min(offsets) for offsets in self.frame_offsets.values())
        maximum = max(max(offsets) for offsets in self.frame_offsets.values())
        # Legacy windows schedule their full horizon even with anchor-only
        # columns, so their cardinality and global mask remain unchanged.
        if self.window_size is not None:
            maximum = max(maximum, self.window_size - 1)
        for group_index, length in enumerate(self._group_lengths):
            length = int(length)
            positions = np.arange(0, length, self.stride, dtype=np.int64)
            lower = min(length, max(0, -minimum))
            upper = max(0, min(length, length - maximum))
            valid = (positions >= lower) & (positions < upper)
            incomplete = np.flatnonzero(~valid)
            if incomplete.size:
                if self.boundary == "error":
                    first = int(incomplete[0])
                    size = "window_size=%s, " % self.window_size \
                        if self.window_size is not None else ""
                    raise ValueError(
                        "Group %s has an incomplete window at %s: "
                        "%soffset bounds=[%d, %d]."
                        % (self._group_keys[group_index],
                           int(self._group_first_orders[group_index])
                           + int(positions[first]),
                           size, minimum, maximum))
                if self.boundary == "drop":
                    positions = positions[valid]
            if positions.size:
                groups.append(np.full(positions.size, group_index, dtype=np.int64))
                starts.append(positions)
        if not groups:
            empty = np.zeros(0, dtype=np.int64)
            return empty, empty.copy()
        return np.concatenate(groups), np.concatenate(starts)

    def _read_rows(self, row_ids, columns=None):
        """Read projected rows by ID from the pinned snapshot.

        Args:
            row_ids: Paimon row IDs to read. Their order and duplicates define
                the returned row order.
            columns: Projected value columns, or all Dataset columns when
                omitted.

        Returns:
            A list of row dictionaries aligned one-for-one with ``row_ids``.
            The internal ``_ROW_ID`` field is removed, and BLOB descriptors are
            resolved to their bodies.
        """
        columns = self.columns if columns is None else columns
        rows = self._plan_for(columns).read(row_ids)
        row_id_column = SpecialFields.ROW_ID.name
        by_row_id = {}
        for row in rows:
            by_row_id[int(row.pop(row_id_column))] = row
        missing = [row_id for row_id in row_ids if row_id not in by_row_id]
        if missing:
            raise RuntimeError(
                "Pinned snapshot %s did not return indexed row IDs %s."
                % (self.snapshot_id, missing))
        return [by_row_id[row_id] for row_id in row_ids]

    def _plan_for(self, columns):
        key = tuple(columns)
        plan = self._plans.get(key)
        if plan is None:
            plan = _PinnedRowIdPlan(
                self._table, columns, self.blob_parallelism, self.snapshot_id)
            self._plans[key] = plan
        return plan


class _PinnedRowIdPlan:
    """One authorized scan plan of the pinned snapshot, read by row ID.

    Planning happens once per projection. Every read then narrows the cached
    splits to the row ranges covering the requested row IDs, so repeated item
    and batch reads never revisit the snapshot's manifests.
    """

    def __init__(self, table, columns, blob_parallelism, snapshot_id):
        self._blob_parallelism = blob_parallelism
        self._snapshot_id = snapshot_id
        self._blob_columns = [
            field.name for field in table.fields
            if field.name in columns
            and is_blob_file_type(field.type)
        ]
        self._map_blob_columns = {
            field.name for field in table.fields
            if field.name in self._blob_columns and is_map_blob_type(field.type)
        }
        self._array_blob_columns = {
            field.name for field in table.fields
            if field.name in self._blob_columns and is_array_blob_type(field.type)
        }
        blob_column_set = set(self._blob_columns)
        self._projection = (
            [name for name in columns if name not in blob_column_set]
            + [SpecialFields.ROW_ID.name]
            + self._blob_columns
        )
        self._table = (
            table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
            if self._blob_columns else table
        )
        plan = self._new_read_builder().new_scan().plan()
        # A tag can be replaced between indexing and this deferred plan, so the
        # plan is only usable while it still resolves the indexed snapshot.
        if snapshot_id is not None and plan.snapshot_id != snapshot_id:
            raise RuntimeError(
                "The window index was built from snapshot %s, but reading its "
                "rows now resolves snapshot %s; the pinned snapshot moved."
                % (snapshot_id, plan.snapshot_id))
        self._splits = plan.splits()
        _reject_masked_row_ids(self._splits)
        self._split_ranges = [
            row_ranges_for_split(split) for split in self._splits
        ]
        self._split_range_index = SplitRangeIndex(self._split_ranges)

    def read(self, row_ids):
        """Return raw row dictionaries, including ``_ROW_ID``, for ``row_ids``."""
        requested = list(dict.fromkeys(row_ids))
        ranges = Range.to_ranges(requested)
        splits = select_indexed_splits(
            self._splits, self._split_ranges, self._split_range_index, ranges)
        if not splits:
            return []
        read_builder = self._new_read_builder()
        predicate = read_builder.new_predicate_builder().is_in(
            SpecialFields.ROW_ID.name, requested)
        arrow = read_builder.with_filter(predicate).new_read().to_arrow(splits)
        # Row ranges are merged per split, so a read can still return rows
        # between requested IDs; drop them before BLOB bodies are fetched.
        row_id_column = arrow.column(SpecialFields.ROW_ID.name)
        arrow = arrow.filter(pc.is_in(
            row_id_column,
            value_set=pa.array(requested, type=row_id_column.type)))
        if not self._blob_columns:
            return arrow.to_pylist()

        bodies = fetch_blob_bodies(
            self._table.file_io,
            arrow.select(self._blob_columns).to_pydict(),
            self._blob_columns,
            self._blob_parallelism,
            self._map_blob_columns,
            self._array_blob_columns)
        blob_column_set = set(self._blob_columns)
        rows = arrow.select([
            name for name in arrow.column_names if name not in blob_column_set
        ]).to_pylist()
        for name in self._blob_columns:
            values = bodies[name]
            if len(values) != len(rows):
                raise RuntimeError(
                    "BLOB column %s is not row-aligned with a window read."
                    % name)
            for row, value in zip(rows, values):
                row[name] = value
        return rows

    def _new_read_builder(self):
        return self._table.new_read_builder().with_projection(self._projection)


def _masked_columns(split):
    if not isinstance(split, QueryAuthSplit):
        return ()
    masking = getattr(split.auth_result, "column_masking", None)
    return tuple(masking) if masking else ()


def _reject_masked_row_ids(splits):
    for split in splits:
        if SpecialFields.ROW_ID.name in _masked_columns(split):
            raise ValueError(
                "ContiguousWindowDataset requires readable Paimon row IDs, but "
                "query authorization masks %s for this table; read the rows "
                "with scan().to_arrow() instead."
                % SpecialFields.ROW_ID.name)


def _reject_masked_index_keys(splits, group_key, order_key):
    for split in splits:
        masked = _masked_columns(split)
        keys = [name for name in (group_key, order_key) if name in masked]
        if keys:
            raise ValueError(
                "ContiguousWindowDataset groups and orders rows by %s, but "
                "query authorization masks %s for this table; a mask can merge "
                "distinct groups or break contiguity, so its windows would no "
                "longer follow the stored rows."
                % ([group_key, order_key], keys))


def _contiguous_array(column):
    combined = column.combine_chunks()
    if not isinstance(combined, pa.ChunkedArray):
        return combined
    if combined.num_chunks == 1:
        return combined.chunk(0)
    if not combined.num_chunks:
        return pa.nulls(0, type=combined.type)
    return pa.concat_arrays(list(combined.iterchunks()))


def _integer_numpy(array):
    # Cast narrow widths so differences of adjacent values cannot overflow.
    if not pa.types.is_uint64(array.type):
        array = array.cast(pa.int64())
    return array.to_numpy(zero_copy_only=False)


def _group_starts(group_values):
    count = len(group_values)
    if not count:
        return np.zeros(0, dtype=np.int64)
    changed = pc.not_equal(
        group_values.slice(1), group_values.slice(0, count - 1))
    return np.concatenate((
        [0],
        np.flatnonzero(changed.to_numpy(zero_copy_only=False)) + 1,
    )).astype(np.int64, copy=False)


def _read_window_index(query, group_key, order_key):
    index_query = copy.copy(query)
    index_query._projection = [group_key, order_key]
    index_query._include_row_id = True
    read_builder = index_query._configured_read_builder()
    plan = read_builder.new_scan().plan()
    splits = plan.splits()
    _reject_masked_row_ids(splits)
    _reject_masked_index_keys(splits, group_key, order_key)
    index = read_builder.new_read().to_arrow(splits)
    if index.num_rows and plan.snapshot_id is None:
        raise RuntimeError("Cannot pin the snapshot used to build the window index.")
    return index, plan.snapshot_id


def _pin_table(table, snapshot_id):
    """Pin a table copy to ``snapshot_id``, or reuse it when unresolved.

    A scan already pinned by ``scan.tag-name`` keeps that tag: the tag retains
    its snapshot's metadata after the main snapshot file expires, which reading
    by raw snapshot ID cannot.
    """
    if snapshot_id is None:
        return table
    tag_name = table.options.scan_tag_name()
    if tag_name is not None:
        _require_tag_snapshot(table, tag_name, snapshot_id)
    scan_keys = set(SCAN_KEYS)
    scan_keys.update(option.key() for option in (
        CoreOptions.SCAN_MODE,
        CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
        CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
        CoreOptions.SCAN_CREATION_TIME_MILLIS,
    ))
    if tag_name is not None:
        scan_keys.discard(CoreOptions.SCAN_TAG_NAME.key())
    options = {
        key: None for key in scan_keys
        if table.options.options.contains_key(key)
    }
    if tag_name is None:
        options[CoreOptions.SCAN_SNAPSHOT_ID.key()] = str(snapshot_id)
    if not options:
        return table
    return table.copy(options)


def _require_tag_snapshot(table, tag_name, snapshot_id):
    tag = table.tag_manager().get(tag_name)
    if tag is None:
        raise RuntimeError(
            "Tag %r used to build the window index no longer exists." % tag_name)
    resolved = tag.trim_to_snapshot().id
    if resolved != snapshot_id:
        raise RuntimeError(
            "Tag %r now resolves to snapshot %s, but the window index was "
            "built from snapshot %s." % (tag_name, resolved, snapshot_id))


def _reject_video_columns(table, columns):
    video_columns = [
        name for name in columns if name in table.options.video_frame_fields()
    ]
    if video_columns:
        raise ValueError(
            "ContiguousWindowDataset does not support video frame columns %s "
            "yet: a window read would drop their frame metadata, such as "
            "frame_index." % video_columns)


def _frame_offsets(columns, offsets, timestamps, fps, tolerance_s,
                   window_size, anchor_columns):
    """Normalize the public window forms to nonempty integer offset tuples."""
    if timestamps is not None:
        tolerance = _finite_number(
            1e-4 if tolerance_s is None else tolerance_s, "tolerance_s")
        if tolerance < 0:
            raise ValueError("tolerance_s must be non-negative.")
        if fps is None:
            raise ValueError("delta_timestamps requires an explicit fps.")
        fps = _finite_number(fps, "fps")
        if fps <= 0:
            raise ValueError("fps must be positive.")
    elif fps is not None or tolerance_s is not None:
        raise ValueError("fps and tolerance_s are only used with delta_timestamps.")
    if window_size is not None:
        forward = tuple(range(window_size))
        return {
            name: (0,) if name in anchor_columns else forward
            for name in columns
        }
    parameter = "delta_timestamps" if timestamps is not None else "frame_offsets"
    supplied = _mapping(timestamps if timestamps is not None else offsets, parameter)
    _validate_mapping_columns(supplied, columns, parameter)
    result = {}
    for name in columns:
        if name not in supplied:
            result[name] = (0,)
            continue
        values = supplied[name]
        if isinstance(values, (str, bytes, dict)):
            raise TypeError("%s[%r] must be a sequence of offsets." % (parameter, name))
        try:
            values = list(values)
        except TypeError:
            raise TypeError("%s[%r] must be a sequence of offsets." % (parameter, name))
        if not values:
            raise ValueError("%s[%r] must not be empty." % (parameter, name))
        converted = []
        for value in values:
            if timestamps is not None:
                seconds = _finite_number(value, "delta_timestamps[%r]" % name)
                scaled = seconds * fps
                if not math.isfinite(scaled):
                    raise ValueError("delta_timestamps * fps must be finite.")
                delta = round(scaled)
                if abs(seconds - delta / fps) > tolerance:
                    raise ValueError(
                        "delta_timestamps for %s must align to multiples of 1/fps."
                        % name)
            else:
                if isinstance(value, (bool, np.bool_)):
                    raise TypeError("frame_offsets must contain integers, not booleans.")
                try:
                    delta = operator.index(value)
                except TypeError:
                    raise TypeError("frame_offsets must contain integer offsets.")
            converted.append(delta)
        result[name] = tuple(converted)
    return result


def _finite_number(value, name):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("%s must be a finite number, not a boolean." % name)
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("%s must be a finite number." % name)
    if not math.isfinite(number):
        raise ValueError("%s must be a finite number." % name)
    return number


def _columns(query, columns, group_key, order_key):
    available = {field.name for field in query._table.fields}
    if columns is None:
        if query._projection is None:
            columns = [field.name for field in query._table.fields]
        else:
            columns = list(query._projection)
        columns = [name for name in columns
                   if name not in (group_key, order_key)]
    elif isinstance(columns, str):
        columns = [columns]
    else:
        try:
            columns = list(columns)
        except TypeError:
            raise TypeError(
                "columns must be a non-empty sequence of column names.")
    if not columns:
        raise ValueError("columns must contain at least one value column.")
    if any(not isinstance(name, str) or not name for name in columns):
        raise TypeError("columns must contain only non-empty column names.")
    if len(set(columns)) != len(columns):
        raise ValueError("columns must not contain duplicates.")
    invalid = [name for name in columns if name not in available]
    if invalid:
        raise ValueError("columns do not exist: %s." % invalid)
    reserved = [name for name in columns
                if name in (group_key, order_key)]
    if reserved:
        raise ValueError(
            "columns must not include group_key or order_key: %s."
            % reserved)
    return columns


def _anchor_columns(value, columns):
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    else:
        try:
            value = list(value)
        except TypeError:
            raise TypeError(
                "anchor_columns must be a sequence of projected column names.")
    if any(not isinstance(name, str) or not name for name in value):
        raise TypeError(
            "anchor_columns must contain only non-empty column names.")
    if len(set(value)) != len(value):
        raise ValueError("anchor_columns must not contain duplicates.")
    invalid = [name for name in value if name not in columns]
    if invalid:
        raise ValueError(
            "anchor_columns must be included in columns: %s." % invalid)
    return value


def _column_transforms(value, columns):
    transforms = _mapping(value, "column_transforms")
    _validate_mapping_columns(transforms, columns, "column_transforms")
    invalid = [name for name, transform in transforms.items()
               if not callable(transform)]
    if invalid:
        raise TypeError(
            "column_transforms values must be callable: %s." % invalid)
    return transforms


def _pad_values(value, columns):
    values = _mapping(value, "pad_values")
    _validate_mapping_columns(values, columns, "pad_values")
    return values


def _mapping(value, name):
    if value is None:
        return {}
    try:
        return dict(value)
    except (TypeError, ValueError):
        raise TypeError("%s must be a mapping or None." % name)


def _validate_mapping_columns(value, columns, name):
    invalid = [column for column in value if column not in columns]
    if invalid:
        raise ValueError("%s contains unknown columns: %s." % (name, invalid))


def _column(query, value, name):
    if not isinstance(value, str) or not value:
        raise TypeError("%s must be a non-empty column name." % name)
    available = {field.name for field in query._table.fields}
    if value not in available:
        raise ValueError("%s column %r does not exist." % (name, value))
    return value


def _positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("%s must be a positive int." % name)
    return value


__all__ = ["ContiguousWindowDataset"]
