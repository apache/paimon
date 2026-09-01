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
import operator
from collections import defaultdict
from numbers import Integral

import torch
from torch.utils.data import Dataset

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.multimodal.query import ScanQuery
from pypaimon.schema.data_types import is_blob_type
from pypaimon.snapshot.time_travel_util import SCAN_KEYS
from pypaimon.table.special_fields import SpecialFields


class ContiguousWindowDataset(Dataset):
    """Map-style Dataset which reads fixed row windows on demand.

    The in-memory index contains only group values, order values, and Paimon
    row IDs. Each ``__getitem__`` reads the projected rows from the snapshot
    resolved while the index was built. Within each group, ``order_key`` must
    contain non-null integers that increase by exactly one; rows from different
    groups never share a window. ``tail`` controls scheduled anchors whose
    remaining rows are shorter than ``window_size``:

    * ``drop`` omits them;
    * ``pad`` repeats final values and marks repeats in ``is_pad``;
    * ``error`` rejects the dataset.

    The raw result mapping contains scalar group and order values, a
    length-``window_size`` Boolean ``is_pad`` tensor, one-element lists for
    ``anchor_columns``, and length-``window_size`` lists for other projected
    columns. ``anchor_columns`` therefore avoids loading repeated context such
    as observation images or initial robot state. ``column_transforms`` then
    convert individual column lists before ``adapter`` adapts the complete
    mapping to a model-specific contract.
    ``blob_parallelism`` controls concurrent BLOB reads for each item or batch.
    """

    _TAIL_POLICIES = ("drop", "pad", "error")

    def __init__(
            self,
            query,
            *,
            window_size,
            columns=None,
            anchor_columns=None,
            group_key="episode_id",
            order_key="step_idx",
            stride=1,
            tail="drop",
            column_transforms=None,
            pad_values=None,
            adapter=None,
            blob_parallelism=64):
        if getattr(query, "_result_factory", None) is not None:
            raise TypeError(
                "ContiguousWindowDataset is only supported on scan(), "
                "not search queries.")
        self.window_size = _positive_int(window_size, "window_size")
        self.stride = _positive_int(stride, "stride")
        if tail not in self._TAIL_POLICIES:
            raise ValueError(
                "tail must be one of %s; got %r."
                % (self._TAIL_POLICIES, tail))
        self.tail = tail
        self.group_key = _column(query, group_key, "group_key")
        self.order_key = _column(query, order_key, "order_key")
        if self.group_key == self.order_key:
            raise ValueError("group_key and order_key must name different columns.")
        if "is_pad" in (self.group_key, self.order_key):
            raise ValueError("group_key and order_key must not be is_pad.")
        self.columns = _columns(
            query, columns, self.group_key, self.order_key)
        self.anchor_columns = _anchor_columns(anchor_columns, self.columns)
        anchor_column_set = set(self.anchor_columns)
        self._window_columns = [
            name for name in self.columns if name not in anchor_column_set
        ]
        self.column_transforms = _column_transforms(
            column_transforms, self.columns)
        self.pad_values = _pad_values(pad_values, self.columns)
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
        self._groups, self._anchors = self._build_index(index)

    @classmethod
    def from_query(cls, query, **kwargs):
        """Build a contiguous-window Dataset from a ``ScanQuery``."""
        return cls(query, **kwargs)

    def __len__(self):
        return len(self._anchors)

    def __getitem__(self, index):
        """Read one window by map-style Dataset index.

        Negative indices follow Python sequence semantics. The return value is
        the pre-adapter mapping described by the class, or the adapter result
        when an adapter is configured.
        """
        anchor, row_ids = self._resolve_window(index)
        rows = self._read_window_rows(row_ids)
        anchor_row = (
            self._read_rows(row_ids[:1], self.anchor_columns)[0]
            if self.anchor_columns else None
        )
        return self._sample(anchor, rows, anchor_row)

    def __getitems__(self, indices):
        """Read several Dataset indices while coalescing overlapping row IDs.

        The returned list preserves the requested index order and duplicates.
        Coalescing affects only physical reads, not logical sample cardinality.
        """
        windows = [self._resolve_window(index) for index in indices]
        if not windows:
            return []
        row_ids = list(dict.fromkeys(
            row_id for _, window_row_ids in windows
            for row_id in window_row_ids
        ))
        rows_by_id = dict(zip(row_ids, self._read_window_rows(row_ids)))
        anchor_row_ids = list(dict.fromkeys(
            window_row_ids[0] for _, window_row_ids in windows
        ))
        anchor_rows_by_id = (
            dict(zip(
                anchor_row_ids,
                self._read_rows(anchor_row_ids, self.anchor_columns),
            ))
            if self.anchor_columns else {}
        )
        return [
            self._sample(
                anchor,
                [rows_by_id[row_id] for row_id in window_row_ids],
                anchor_rows_by_id.get(window_row_ids[0]),
            )
            for anchor, window_row_ids in windows
        ]

    def _resolve_window(self, index):
        index = operator.index(index)
        if index < 0:
            index += len(self._anchors)
        if index < 0 or index >= len(self._anchors):
            raise IndexError("window index out of range")

        anchor = self._anchors[index]
        group_index, start, valid_count = anchor
        row_ids = self._groups[group_index][2]
        return anchor, row_ids[start:start + valid_count]

    def _sample(self, anchor, rows, anchor_row=None):
        group_index, start, valid_count = anchor
        group_key, order_values, _ = self._groups[group_index]
        padding_count = self.window_size - valid_count
        padding_mask = torch.zeros(self.window_size, dtype=torch.bool)
        if padding_count:
            padding_mask[valid_count:] = True
        sample = {
            self.group_key: group_key,
            self.order_key: order_values[start],
            "is_pad": padding_mask,
        }
        for name in self.columns:
            if name in self.anchor_columns:
                values = [anchor_row[name]]
            else:
                values = [row[name] for row in rows]
            if padding_count and name not in self.anchor_columns:
                pad_value = self.pad_values.get(name, values[-1])
                values.extend(
                    copy.deepcopy(pad_value) for _ in range(padding_count))
            transform = self.column_transforms.get(name)
            sample[name] = transform(values) if transform is not None else values
        if self.adapter is not None:
            return self.adapter(sample)
        return sample

    def _build_index(self, index):
        """Validate index rows and return grouped row IDs plus window anchors.

        Args:
            index: Arrow table containing ``group_key``, ``order_key``, and
                Paimon's ``_ROW_ID`` for the resolved snapshot.

        Returns:
            ``(groups, anchors)``. Each group stores its key, ordered positions,
            and row IDs. Each anchor stores group index, start offset, and the
            number of real rows available before optional padding.
        """
        group_values = index.column(self.group_key).to_pylist()
        order_values = index.column(self.order_key).to_pylist()
        row_ids = index.column(SpecialFields.ROW_ID.name).to_pylist()
        grouped = defaultdict(list)
        for group_key, order_value, row_id in zip(
                group_values, order_values, row_ids):
            if group_key is None:
                raise ValueError("%s must not contain null values." % self.group_key)
            if order_value is None:
                raise ValueError("%s must not contain null values." % self.order_key)
            if isinstance(order_value, bool) or not isinstance(order_value, Integral):
                raise ValueError(
                    "%s must contain integer values." % self.order_key)
            try:
                grouped[group_key].append((int(order_value), int(row_id)))
            except TypeError:
                raise ValueError(
                    "%s values must be hashable." % self.group_key)

        groups = []
        anchors = []
        try:
            sorted_groups = sorted(grouped.items(), key=lambda item: item[0])
        except TypeError:
            raise ValueError(
                "%s values must be mutually orderable." % self.group_key)
        for group_key, members in sorted_groups:
            try:
                members.sort(key=lambda item: item[0])
            except TypeError:
                raise ValueError(
                    "%s values in group %r must be mutually orderable."
                    % (self.order_key, group_key))
            for previous, current in zip(members, members[1:]):
                if previous[0] == current[0]:
                    raise ValueError(
                        "Group %s has duplicate order value %r in %s."
                        % (group_key, current[0], self.order_key))
                if current[0] != previous[0] + 1:
                    raise ValueError(
                        "Group %s is not contiguous in %s: %s followed by %s."
                        % (group_key, self.order_key,
                           previous[0], current[0]))

            group_index = len(groups)
            group_orders = [member[0] for member in members]
            group_row_ids = [member[1] for member in members]
            groups.append((group_key, group_orders, group_row_ids))
            for start in range(0, len(members), self.stride):
                valid_count = min(self.window_size, len(members) - start)
                if valid_count < self.window_size:
                    if self.tail == "drop":
                        continue
                    if self.tail == "error":
                        raise ValueError(
                            "Group %s has an incomplete window at %s: "
                            "window_size=%d, available=%d."
                            % (group_key, group_orders[start],
                               self.window_size, valid_count))
                anchors.append((group_index, start, valid_count))
        return groups, anchors

    def _read_window_rows(self, row_ids):
        if not self._window_columns:
            return [{} for _ in row_ids]
        return self._read_rows(row_ids, self._window_columns)

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
        query = ScanQuery(self._table)
        predicate_builder = (
            self._table.new_read_builder()
            .with_projection(
                [field.name for field in self._table.fields]
                + [SpecialFields.ROW_ID.name])
            .new_predicate_builder()
        )
        query._predicate = predicate_builder.is_in(
            SpecialFields.ROW_ID.name, row_ids)
        query._projection = list(columns)
        query._include_row_id = True

        blob_columns = [
            field.name for field in self._table.fields
            if field.name in columns and is_blob_type(field.type)
        ]
        if blob_columns:
            scalar, blobs = query.read_blobs(
                blob_columns, parallelism=self.blob_parallelism)
            rows = scalar.to_pylist()
            for name in blob_columns:
                values = blobs[name]
                if len(values) != len(rows):
                    raise RuntimeError(
                        "BLOB column %s is not row-aligned with a window read."
                        % name)
                for row, value in zip(rows, values):
                    row[name] = value
        else:
            rows = query.to_arrow().to_pylist()

        by_row_id = {}
        row_id_column = SpecialFields.ROW_ID.name
        for row in rows:
            row_id = int(row[row_id_column])
            del row[row_id_column]
            by_row_id[row_id] = row
        missing = [row_id for row_id in row_ids if row_id not in by_row_id]
        if missing:
            raise RuntimeError(
                "Pinned snapshot %s did not return indexed row IDs %s."
                % (self.snapshot_id, missing))
        return [by_row_id[row_id] for row_id in row_ids]


def _read_window_index(query, group_key, order_key):
    index_query = copy.copy(query)
    index_query._projection = [group_key, order_key]
    index_query._include_row_id = True
    read_builder = index_query._configured_read_builder()
    plan = read_builder.new_scan().plan()
    index = read_builder.new_read().to_arrow(plan.splits())
    if index.num_rows and plan.snapshot_id is None:
        raise RuntimeError("Cannot pin the snapshot used to build the window index.")
    return index, plan.snapshot_id


def _pin_table(table, snapshot_id):
    """Pin a table copy to ``snapshot_id``, or reuse it when unresolved."""
    if snapshot_id is None:
        return table
    options = {
        key: None for key in SCAN_KEYS
        if table.options.options.contains_key(key)
    }
    options[CoreOptions.SCAN_SNAPSHOT_ID.key()] = str(snapshot_id)
    return table.copy(options)


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
                if name in (group_key, order_key, "is_pad")]
    if reserved:
        raise ValueError(
            "columns must not include group_key, order_key, or is_pad: %s."
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
