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

"""Translate ordinary snapshot-and-split plans to and from Rust Core."""

from copy import copy, deepcopy

from pypaimon.common.json_util import JSON
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.execution.native.table import (_partition_fields)
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.source.search_types import (
    FullTextScanPlan, VectorSearchScanPlan,
    PrimaryKeyFullTextScanPlan, PrimaryKeyFullTextSearchSplit,
    PrimaryKeyVectorScanPlan, PrimaryKeyVectorSearchSplit,
)
from pypaimon.table.source.primary_key_scored_result import PrimaryKeyScoredResult
from pypaimon.utils.range import Range
from pypaimon.write.commit_message_serializer import _index_from_row, _index_row


def _range(value):
    return Range(*value)


def _files(values):
    return [_index_from_row(bytes(value._serialize())) for value in values]


def _split_from_native(table, split, full_text):
    from pypaimon_rust import datafusion as sdk
    from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit, RawFullTextSearchSplit
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit

    native_pk = sdk.PrimaryKeyFullTextSearchSplit if full_text else sdk.PrimaryKeyVectorSearchSplit
    native_index = sdk.IndexFullTextSearchSplit if full_text else sdk.IndexVectorSearchSplit
    native_raw = sdk.RawFullTextSearchSplit if full_text else sdk.RawVectorSearchSplit
    if isinstance(split, native_pk):
        data_split = deserialize_split_v1(bytes(split.data_split().serialize()), _partition_fields(table),
                                          table.trimmed_primary_keys_fields)
        ranges = {name: tuple(_range(value) for value in values)
                  for name, values in split.row_ranges_by_file().items()}
        cls = PrimaryKeyFullTextSearchSplit if full_text else PrimaryKeyVectorSearchSplit
        return cls(data_split, tuple(_files(split.payloads())), tuple(split.uncovered_data_files()), ranges)
    if isinstance(split, native_index):
        args = (split.row_range_start(), split.row_range_end(),
                _files(split.full_text_index_files() if full_text else split.vector_index_files()),
                _files(split.scalar_index_files()))
        return IndexFullTextSearchSplit(split.column_name(), *args) if full_text else IndexVectorSearchSplit(*args)
    if isinstance(split, native_raw):
        ranges = [_range(value) for value in split.row_ranges()]
        return RawFullTextSearchSplit(ranges) if full_text else RawVectorSearchSplit(
            ranges, _files(split.scalar_index_files()), split.index_type())
    raise TypeError('Unsupported Native %s search split: %s' % (
        'full-text' if full_text else 'vector', type(split).__name__))


def _plan_from_native(table, native, full_text, primary_key):
    captured = native.snapshot()
    snapshot = None if captured is None else JSON.from_json(captured._to_json(), Snapshot)
    splits = [_split_from_native(table, split, full_text) for split in native.splits()]
    if primary_key:
        cls = PrimaryKeyFullTextScanPlan if full_text else PrimaryKeyVectorScanPlan
        return cls(0 if snapshot is None else snapshot.id, splits, snapshot)
    return (FullTextScanPlan if full_text else VectorSearchScanPlan)(splits, snapshot)


def _split_to_native(split, full_text, snapshot=None):
    from pypaimon_rust import datafusion as sdk
    from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit, RawFullTextSearchSplit
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit

    expected = ((PrimaryKeyFullTextSearchSplit, IndexFullTextSearchSplit, RawFullTextSearchSplit) if full_text else
                (PrimaryKeyVectorSearchSplit, IndexVectorSearchSplit, RawVectorSearchSplit))
    if not isinstance(split, expected):
        raise TypeError('Unsupported %s search split: %s' % (
            'full-text' if full_text else 'vector', type(split).__name__))

    def files(values):
        return [sdk.IndexFileMeta._deserialize(_index_row(value)) for value in values]

    def ranges(values):
        return [(value.from_, value.to) for value in values]

    if isinstance(split, (PrimaryKeyVectorSearchSplit, PrimaryKeyFullTextSearchSplit)):
        cls = sdk.PrimaryKeyFullTextSearchSplit if full_text else sdk.PrimaryKeyVectorSearchSplit
        data_split = split.data_split
        if data_split.snapshot_id is None and snapshot is not None:
            data_split = copy(data_split)
            data_split.snapshot_id = snapshot.id
        return cls(sdk.Split.deserialize(serialize_split_v1(data_split)), files(split.payloads),
                   list(split.uncovered_data_files),
                   {name: ranges(values) for name, values in split.row_ranges_by_file.items()})
    if isinstance(split, IndexFullTextSearchSplit):
        return sdk.IndexFullTextSearchSplit(split.column_name, split.row_range_start, split.row_range_end,
                                            files(split.full_text_index_files), files(split.scalar_index_files))
    if isinstance(split, IndexVectorSearchSplit):
        return sdk.IndexVectorSearchSplit(split.row_range_start, split.row_range_end,
                                          files(split.vector_index_files), files(split.scalar_index_files))
    if isinstance(split, RawVectorSearchSplit):
        return sdk.RawVectorSearchSplit(ranges(split.row_ranges), files(split.scalar_index_files), split.index_type)
    return sdk.RawFullTextSearchSplit(ranges(split.row_ranges))


def _plan_to_native(plan, full_text):
    from pypaimon_rust import datafusion as sdk

    captured = plan.snapshot()
    snapshot = None if captured is None else sdk.Snapshot._from_json(JSON.to_json(captured))
    cls = sdk.FullTextScanPlan if full_text else sdk.VectorScanPlan
    return cls([_split_to_native(split, full_text, captured) for split in plan.splits()], snapshot)


def _capture_builder(builder):
    # Public builders contain only table configuration. Freeze query values and
    # options for reader transport without copying the catalog or live SDK cache.
    captured = copy(builder)
    captured.__dict__ = {key: value if key == '_table' else deepcopy(value)
                         for key, value in builder.__dict__.items()}
    return captured


def _result(table, value, primary_key):
    return (PrimaryKeyScoredResult._from_native(table, value)
            if primary_key else DictBasedScoredIndexResult(value.row_ids()))


class NativeSearchScan:
    """Convert Rust scan metadata without selecting another executor."""

    def __init__(self, table, scan, full_text, primary_key, builder=None, batch=False):
        self._table = table
        self._scan = scan
        self._full_text = full_text
        self._primary_key = primary_key
        self._builder = builder
        self._batch = batch

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_scan'] = None
        return state

    def available(self):
        return self._scan is not None or self._builder is not None

    def scan(self):
        if self._scan is None:
            native = _builder_to_native(self._builder, self._full_text, self._batch, scan=True)
            self._scan = native.new_full_text_scan() if self._full_text else native.new_vector_search_scan()
        return _plan_from_native(self._table, self._scan.scan(), self._full_text, self._primary_key)


class NativeSearchRead:
    """Convert source metadata and delegate complete queries to Rust Core."""

    def __init__(self, table, native, primary_key, full_text=False, builder=None, batch=False):
        self._table = table
        self._native = native
        self._primary_key = primary_key
        self._full_text = full_text
        self._builder = builder
        self._batch = batch

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_native'] = None
        return state

    def _get_native_reader(self):
        if self._native is None:
            native = _builder_to_native(self._builder, self._full_text, self._batch)
            self._native = (native.new_full_text_read() if self._full_text else
                            native.new_batch_vector_search_read() if self._batch else
                            native.new_vector_search_read())
        return self._native

    def _plan(self, plan):
        if (self._primary_key and isinstance(plan, (PrimaryKeyVectorScanPlan, PrimaryKeyFullTextScanPlan))
                and plan.snapshot() is None and plan.snapshot_id):
            snapshot = self._table.snapshot_manager().get_snapshot_by_id(plan.snapshot_id)
            if snapshot is None:
                raise ValueError('Captured search snapshot %s is unavailable' % plan.snapshot_id)
            plan = copy(plan)
            plan._snapshot = snapshot
        return _plan_to_native(plan, self._full_text)

    def read_plan(self, plan, executor=None):
        native = self._get_native_reader()
        plan = self._plan(plan)
        if executor is None:
            result = native.read_plan(plan)
        else:
            self._validate_task_execution()
            from pypaimon.execution.native.read.search_execution import native_task_executor
            callback = native_task_executor(self._table, executor)
            try:
                result = native._read_plan_with_executor(plan, callback)
            finally:
                callback.cancel()
                callback.raise_if_failed()
        return _result(self._table, result, self._primary_key)

    def read_batch_plan(self, plan, executor=None):
        native = self._get_native_reader()
        plan = self._plan(plan)
        if executor is None:
            results = native.read_batch_plan(plan)
        else:
            self._validate_task_execution()
            from pypaimon.execution.native.read.search_execution import native_task_executor
            callback = native_task_executor(self._table, executor)
            try:
                results = native._read_batch_plan_with_executor(plan, callback)
            finally:
                callback.cancel()
                callback.raise_if_failed()
        return [_result(self._table, result, False)
                for result in results]

    def _validate_task_execution(self):
        if self._primary_key or not self._table.options.data_evolution_enabled():
            raise ValueError('Distributed search supports only data-evolution tables')

    def read(self, splits):
        return self.read_plan(self._split_plan(splits))

    def read_batch(self, splits):
        return self.read_batch_plan(self._split_plan(splits))

    def _split_plan(self, splits):
        from pypaimon.snapshot.time_travel_util import TimeTravelUtil
        snapshot = TimeTravelUtil.resolve_snapshot(self._table)
        if self._primary_key:
            cls = PrimaryKeyFullTextScanPlan if self._full_text else PrimaryKeyVectorScanPlan
            return cls(0 if snapshot is None else snapshot.id, splits, snapshot)
        return (FullTextScanPlan if self._full_text else VectorSearchScanPlan)(splits, snapshot)


def _builder_to_native(builder, full_text, batch=False, scan=False):
    if full_text:
        from pypaimon.execution.native.read.native_full_text_search import _native_full_text_builder
        native = _native_full_text_builder(builder, require_limit=not scan)
    else:
        from pypaimon.execution.native.read.native_vector_search import _native_vector_builder
        native = _native_vector_builder(builder, batch)
        if native is not None and not scan:
            if batch:
                native.with_query_vectors(builder._query_vectors)
            else:
                native.with_query_vector(builder._query_vector)
    if native is None:
        raise RuntimeError('Native search runtime is unavailable for the captured reader configuration')
    return native


def native_vector_scan(builder, batch=False):
    from pypaimon.execution.native.read.native_vector_search import _native_vector_builder
    native = _native_vector_builder(builder, batch)
    return None if native is None else NativeSearchScan(
        builder._table, native.new_vector_search_scan(), False,
        builder._is_primary_key_vector_search(), _capture_builder(builder), batch)


def native_vector_read(builder, batch=False):
    from pypaimon.execution.native.read.native_vector_search import _native_vector_builder
    native = _native_vector_builder(builder, batch)
    if native is None:
        return None
    if batch:
        native.with_query_vectors(builder._query_vectors)
        reader = native.new_batch_vector_search_read()
    else:
        native.with_query_vector(builder._query_vector)
        reader = native.new_vector_search_read()
    return NativeSearchRead(
        builder._table, reader, builder._is_primary_key_vector_search(), False,
        _capture_builder(builder), batch)


def native_full_text_scan(builder):
    from pypaimon.execution.native.read.native_full_text_search import _native_full_text_builder
    native = _native_full_text_builder(builder, require_limit=False)
    return None if native is None else NativeSearchScan(
        builder._table, native.new_full_text_scan(), True,
        builder._primary_key_full_text_definition() is not None, _capture_builder(builder))


def native_full_text_read(builder):
    from pypaimon.execution.native.read.native_full_text_search import _native_full_text_builder
    native = _native_full_text_builder(builder)
    return None if native is None else NativeSearchRead(
        builder._table, native.new_full_text_read(),
        builder._primary_key_full_text_definition() is not None, True, _capture_builder(builder))
