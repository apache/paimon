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

"""Native search plans with ordinary, transportable Python source metadata."""

from copy import copy
import hashlib
import json
import pickle

from pypaimon.common.json_util import JSON
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.read.native_plan import deserialize_split_v1, _partition_fields
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.source.full_text_scan import FullTextScanPlan
from pypaimon.table.source.primary_key_full_text_scan import (
    PrimaryKeyFullTextScanPlan, PrimaryKeyFullTextSearchSplit,
)
from pypaimon.table.source.primary_key_scored_result import PrimaryKeyScoredResult
from pypaimon.table.source.primary_key_vector_scan import PrimaryKeyVectorScanPlan, PrimaryKeyVectorSearchSplit
from pypaimon.table.source.vector_search_scan import VectorSearchScanPlan
from pypaimon.utils.range import Range
from pypaimon.write.commit_message_serializer import _index_from_row


def _metadata_fingerprint(plan):
    # Only captured metadata participates: never pickle a Table, catalog or
    # live native handle. This also detects edits through a shallow copy.
    metadata = (plan.snapshot(), plan.splits(), getattr(plan, 'snapshot_id', None))
    return hashlib.sha256(pickle.dumps(metadata, protocol=4)).digest()


class _NativePlan:
    def __getstate__(self):
        # Transport immutable snapshot data and ordinary Python splits. The
        # original result and its metadata remain unchanged by serialization.
        state = self.__dict__.copy()
        state.pop('_native_plan', None)
        state.pop('_native_fingerprint', None)
        return state

    def _unchanged_native_plan(self):
        native = getattr(self, '_native_plan', None)
        return native if native is not None and self._native_fingerprint == _metadata_fingerprint(self) else None


class _NativeVectorPlan(_NativePlan, VectorSearchScanPlan):
    pass


class _NativeFullTextPlan(_NativePlan, FullTextScanPlan):
    pass


class _NativePkVectorPlan(_NativePlan, PrimaryKeyVectorScanPlan):
    pass


class _NativePkFullTextPlan(_NativePlan, PrimaryKeyFullTextScanPlan):
    pass


def _range(value):
    return Range(value['from'], value['to'])


def _files(values):
    return [_index_from_row(bytes(value)) for value in values]


def _plan_from_native(table, native, full_text, primary_key, source_modes):
    from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit, RawFullTextSearchSplit
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit

    metadata = json.loads(native._metadata())
    snapshot = JSON.from_json(json.dumps(metadata['snapshot']), Snapshot) if metadata['snapshot'] is not None else None
    splits = []
    for value in metadata['splits']:
        if value['kind'] == 'primary-key':
            split = deserialize_split_v1(bytes(value['data_split']), _partition_fields(table),
                                         table.trimmed_primary_keys_fields)
            ranges = {name: tuple(_range(range_) for range_ in ranges)
                      for name, ranges in value['row_ranges_by_file'].items()}
            cls = PrimaryKeyFullTextSearchSplit if full_text else PrimaryKeyVectorSearchSplit
            splits.append(cls(split, tuple(_files(value['payloads'])), tuple(value['uncovered_data_files']), ranges))
        elif value['kind'] == 'index':
            row_range = _range(value['row_range'])
            args = (row_range.from_, row_range.to, _files(value['index_files']), _files(value['scalar_index_files']))
            splits.append(IndexFullTextSearchSplit(value['column'], *args)
                          if full_text else IndexVectorSearchSplit(*args))
        else:
            ranges = [_range(range_) for range_ in value['row_ranges']]
            splits.append(RawFullTextSearchSplit(ranges) if full_text else RawVectorSearchSplit(
                ranges, _files(value['scalar_index_files']), value['index_type']))
    if primary_key:
        cls = _NativePkFullTextPlan if full_text else _NativePkVectorPlan
        plan = cls(metadata['snapshot_id'] or 0, splits)
        plan._snapshot = snapshot
    else:
        cls = _NativeFullTextPlan if full_text else _NativeVectorPlan
        plan = cls(splits, snapshot)
    plan._native_plan = native
    plan._source_modes = source_modes
    plan._table_options = dict(table.options.options.to_map())
    plan._native_fingerprint = _metadata_fingerprint(plan)
    return plan


def _result(table, value, primary_key):
    return (PrimaryKeyScoredResult._from_native(table, value)
            if primary_key else DictBasedScoredIndexResult(value.row_ids()))


class _NativeScan:
    def __init__(self, table, scan, full_text, primary_key, python, source_modes):
        self._table = table
        self._scan = scan
        self._full_text = full_text
        self._primary_key = primary_key
        self._python = python
        self._source_modes = source_modes

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_scan'] = None
        return state

    def scan(self):
        if self._scan is None:
            return self._python.scan()
        return _plan_from_native(self._table, self._scan.scan(), self._full_text, self._primary_key, self._source_modes)


class _NativeRead:
    def __init__(self, table, native, python, primary_key, source_modes):
        self._table = table
        self._native = native
        self._python = python
        self._primary_key = primary_key
        self._source_modes = source_modes
        self._table_options = dict(table.options.options.to_map())

    def __getattr__(self, name):
        python = self.__dict__.get('_python')
        if python is None:
            raise AttributeError(name)
        return getattr(python, name)

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_native'] = None
        return state

    def read_plan(self, plan):
        native = self._native_plan(plan)
        if native is not None and self._native is not None:
            # Once execution starts, propagate errors without a second search.
            return _result(self._table, self._native.read_plan(native), self._primary_key)
        reader = copy(self._python)
        if plan.snapshot() is not None:
            reader._table = reader._table._copy_with_snapshot(plan.snapshot())
        return reader.read_plan(plan)

    def read_batch_plan(self, plan):
        native = self._native_plan(plan)
        if native is not None and self._native is not None:
            return [_result(self._table, result, False) for result in self._native.read_batch_plan(native)]
        reader = copy(self._python)
        if plan.snapshot() is not None:
            reader._table = reader._table._copy_with_snapshot(plan.snapshot())
        return reader.read_batch_plan(plan)

    def _native_plan(self, plan):
        # Search modes choose source work at scan time. A new reader may use
        # another query or limit, but must still consume these captured splits.
        if (isinstance(plan, _NativePlan) and plan._source_modes == self._source_modes
                and plan._table_options == self._table_options):
            return plan._unchanged_native_plan()
        return None


def _source_modes(builder, full_text=False):
    options = dict(builder._table.options.options.to_map())
    options.update(getattr(builder, '_options', {}))
    core = CoreOptions(Options(options))
    return ((core.full_text_index_search_mode(),) if full_text else
            (core.vector_index_search_mode(), core.scalar_index_search_mode()))


def python_search_reader(reader):
    """Keep the existing Ray split workers independent of a live native reader."""
    return reader._python if isinstance(reader, _NativeRead) else reader


def native_vector_scan(builder, python, batch=False):
    from pypaimon.table.source.native_vector_search import _native_vector_builder
    native = _native_vector_builder(builder, batch)
    return python if native is None else _NativeScan(
        builder._table, native.new_vector_search_scan(), False, builder._is_primary_key_vector_search(), python,
        _source_modes(builder))


def native_vector_read(builder, python, batch=False):
    from pypaimon.table.source.native_vector_search import _native_vector_builder
    native = _native_vector_builder(builder, batch)
    if native is None:
        return python
    if batch:
        native.with_query_vectors(builder._query_vectors)
        reader = native.new_batch_vector_search_read()
    else:
        native.with_query_vector(builder._query_vector)
        reader = native.new_vector_search_read()
    return _NativeRead(builder._table, reader, python, builder._is_primary_key_vector_search(), _source_modes(builder))


def native_full_text_scan(builder, python):
    from pypaimon.table.source.native_full_text_search import _native_full_text_builder
    native = _native_full_text_builder(builder, require_limit=False)
    return python if native is None else _NativeScan(
        builder._table, native.new_full_text_scan(), True,
        builder._primary_key_full_text_definition() is not None, python, _source_modes(builder, True))


def native_full_text_read(builder, python):
    from pypaimon.table.source.native_full_text_search import _native_full_text_builder
    native = _native_full_text_builder(builder)
    return python if native is None else _NativeRead(
        builder._table, native.new_full_text_read(), python, builder._primary_key_full_text_definition() is not None,
        _source_modes(builder, True))
