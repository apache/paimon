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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from dataclasses import dataclass

from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _map_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.data_evolution_global_index_scanner import _create_inner_readers
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pksorted.pk_sorted_bucket_index_state import PkSortedBucketIndexState
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilePlan:
    source_split: object
    file_index: int
    groups: dict

    @property
    def data_file(self):
        return self.source_split.files[self.file_index]


@dataclass(frozen=True)
class Plan:
    snapshot_id: int
    files: tuple


@dataclass(frozen=True)
class EvaluatedFile:
    file: FilePlan
    result: object


@dataclass(frozen=True)
class EvaluatedPlan:
    snapshot_id: int
    files: tuple


def plan(snapshot_id, data_splits, definitions, index_entries):
    payloads_by_bucket = {}
    for entry in index_entries:
        meta = entry.index_file.global_index_meta
        if entry.kind != 0 or meta is None or meta.source_meta is None:
            continue
        payloads_by_bucket.setdefault(_bucket_key(entry.partition, entry.bucket), []).append(
            entry.index_file)

    data_files_by_bucket = {}
    for split in data_splits:
        data_files_by_bucket.setdefault(_bucket_key(split.partition, split.bucket), []).extend(
            split.files)

    groups_by_bucket = {}
    for bucket, data_files in data_files_by_bucket.items():
        payloads = payloads_by_bucket.get(bucket, [])
        active_sources = {
            PrimaryKeyIndexSourceFile(f.file_name, f.row_count) for f in data_files
        }
        by_source = {}
        for definition in definitions:
            definition_payloads = [
                payload for payload in payloads
                if payload.index_type == definition.index_type
                and payload.global_index_meta.index_field_id == definition.field_id
            ]
            try:
                state = PkSortedBucketIndexState.from_active_data_files(
                    definition.field_id, definition.index_type,
                    data_files, definition_payloads)
                for group in state.groups:
                    for source in group.source_files:
                        if source in active_sources:
                            by_source.setdefault(source.file_name, {})[definition.field_id] = group
            except Exception as exc:
                LOG.warning("Failed to plan primary-key sorted index for field %s: %s",
                            definition.field_id, exc)
        groups_by_bucket[bucket] = by_source

    files = []
    for split in data_splits:
        by_source = groups_by_bucket.get(_bucket_key(split.partition, split.bucket), {})
        for index, data_file in enumerate(split.files):
            files.append(FilePlan(split, index, by_source.get(data_file.file_name, {})))
    return Plan(snapshot_id, tuple(files))


def evaluate(index_plan, fields, predicate, definitions, reader_factory):
    definitions_by_field = {definition.field_id: definition for definition in definitions}
    evaluated = []
    shared_readers = {}
    try:
        for file_plan in index_plan.files:
            def readers_function(field):
                definition = definitions_by_field.get(field.id)
                group = file_plan.groups.get(field.id)
                if definition is None or group is None:
                    return []
                key = id(group)
                reader = shared_readers.get(key)
                if reader is None:
                    reader = _SharedGlobalIndexReader(
                        reader_factory(file_plan, definition, list(group.payloads)),
                        group.source_files)
                    shared_readers[key] = reader
                return [_FileLocalGlobalIndexReader(
                    reader, group.source_files, file_plan.data_file)]

            evaluator = GlobalIndexEvaluator(fields, readers_function)
            try:
                result = evaluator.evaluate(predicate)
            except Exception as exc:
                LOG.warning("Failed to evaluate primary-key sorted index for %s: %s",
                            file_plan.data_file.file_name, exc)
                result = None
            finally:
                evaluator.close()
            evaluated.append(EvaluatedFile(file_plan, result))
    finally:
        for reader in shared_readers.values():
            try:
                reader.close()
            except Exception as exc:
                LOG.warning("Failed to close primary-key sorted-index reader: %s", exc)
    return EvaluatedPlan(index_plan.snapshot_id, tuple(evaluated))


def reader_factory(table):
    path_factory = table.path_factory()

    def create(file_plan, definition, payloads):
        field = next(field for field in table.fields if field.id == definition.field_id)
        if path_factory.index_file_in_data_file_dir:
            index_path = path_factory.bucket_path(
                tuple(file_plan.source_split.partition.values),
                file_plan.source_split.bucket)
        else:
            index_path = path_factory.global_index_path_factory().index_path()
        io_metas = [
            GlobalIndexIOMeta(
                payload.file_name, payload.file_size,
                payload.global_index_meta.index_meta, payload.external_path)
            for payload in payloads
        ]
        readers = _create_inner_readers(
            definition.index_type, table.file_io, index_path,
            field, io_metas, options=CoreOptions(definition.options))
        if len(readers) != 1:
            raise ValueError("Expected exactly one primary-key sorted-index reader.")
        return readers[0]

    return create


class _FileLocalGlobalIndexReader(GlobalIndexReader):
    def __init__(self, wrapped, source_files, data_file):
        self._wrapped = wrapped
        target = PrimaryKeyIndexSourceFile(data_file.file_name, data_file.row_count)
        self._source_index = tuple(source_files).index(target)

    def _localize(self, future):
        return self._wrapped.localize(future, self._source_index)

    def close(self):
        pass


class _SharedGlobalIndexReader(GlobalIndexReader):
    def __init__(self, wrapped, source_files):
        self._wrapped = wrapped
        self._results = {}
        self._localized_results = {}
        self._source_offsets = [0]
        for source in source_files:
            self._source_offsets.append(
                self._source_offsets[-1] + source.row_count)

    def _query(self, name, args):
        key = (name,) + tuple(_query_key(value) for value in args)
        result = self._results.get(key)
        if result is None:
            result = getattr(self._wrapped, name)(*args)
            self._results[key] = result
        return result

    def localize(self, future, source_index):
        partitions = self._localized_results.get(future)
        if partitions is None:
            partitions = _map_future(future, self._partition_by_source)
            self._localized_results[future] = partitions
        return _map_future(partitions, lambda results: results[source_index])

    def _partition_by_source(self, result):
        source_count = len(self._source_offsets) - 1
        if result is None:
            return [None] * source_count
        if source_count == 1:
            return [result]

        partitions = [RoaringBitmap64() for _ in range(source_count)]
        total_row_count = self._source_offsets[-1]
        source_index = 0
        for position in result.results():
            if position < 0 or position >= total_row_count:
                return [
                    _invalid_local_result(
                        self._source_offsets[index + 1]
                        - self._source_offsets[index])
                    for index in range(source_count)
                ]
            while position >= self._source_offsets[source_index + 1]:
                source_index += 1
            partitions[source_index].add(
                position - self._source_offsets[source_index])
        return [GlobalIndexResult.create(partition) for partition in partitions]

    def close(self):
        self._wrapped.close()


def _delegate(name):
    def method(self, *args):
        return self._localize(getattr(self._wrapped, name)(*args))
    return method


for _method in (
    "visit_equal", "visit_not_equal", "visit_less_than", "visit_less_or_equal",
    "visit_greater_than", "visit_greater_or_equal", "visit_is_null",
    "visit_is_not_null", "visit_in", "visit_not_in", "visit_starts_with",
    "visit_ends_with", "visit_contains", "visit_like", "visit_between",
    "visit_not_between",
):
    setattr(_FileLocalGlobalIndexReader, _method, _delegate(_method))


def _shared_delegate(name):
    def method(self, *args):
        return self._query(name, args)
    return method


for _method in (
    "visit_equal", "visit_not_equal", "visit_less_than", "visit_less_or_equal",
    "visit_greater_than", "visit_greater_or_equal", "visit_is_null",
    "visit_is_not_null", "visit_in", "visit_not_in", "visit_starts_with",
    "visit_ends_with", "visit_contains", "visit_like", "visit_between",
    "visit_not_between",
):
    setattr(_SharedGlobalIndexReader, _method, _shared_delegate(_method))


def _query_key(value):
    if hasattr(value, "name") and hasattr(value, "data_type"):
        return value.name, value.data_type
    if isinstance(value, list):
        return tuple(_query_key(item) for item in value)
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


def _invalid_local_result(row_count):
    bitmap = RoaringBitmap64()
    bitmap.add(row_count)
    return GlobalIndexResult.create(bitmap)


def _bucket_key(partition, bucket):
    return tuple(partition.values), bucket
