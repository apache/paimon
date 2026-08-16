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

import unittest
from concurrent.futures import Future
from types import SimpleNamespace

from pypaimon.common.options.options import Options
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.index.pk.primary_key_index_definition import (
    PrimaryKeyIndexDefinition, PrimaryKeyIndexFamily)
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source import primary_key_sorted_index_scan as scan
from pypaimon.utils.range import Range


class _Reader(GlobalIndexReader):
    def __init__(self, ranges=None, result=None, close_error=False):
        self.ranges = ranges or [Range(1, 1), Range(3, 3)]
        self.result = result
        self.close_error = close_error

    def visit_equal(self, field_ref, literal):
        future = Future()
        future.set_result(self.result or GlobalIndexResult.from_ranges(self.ranges))
        return future

    def close(self):
        if self.close_error:
            raise IOError("close failed")


class PrimaryKeySortedIndexScanTest(unittest.TestCase):
    def test_localizes_group_result_to_each_source_file(self):
        field = DataField(3, "value", AtomicType("INT"))
        definition = PrimaryKeyIndexDefinition(
            "value", 3, "btree", Options.from_none(), PrimaryKeyIndexFamily.BTREE)
        files = [
            SimpleNamespace(file_name="a", row_count=2, level=1, file_source=1),
            SimpleNamespace(file_name="b", row_count=2, level=1, file_source=1),
        ]
        partition = GenericRow([], [])
        split = DataSplit(files, partition, 0, raw_convertible=True)
        source_meta = PrimaryKeyIndexSourceMeta(
            1, [PrimaryKeyIndexSourceFile("a", 2),
                PrimaryKeyIndexSourceFile("b", 2)]).serialize()
        payload = IndexFileMeta(
            "btree", "index", 1, 4,
            global_index_meta=GlobalIndexMeta(0, 3, 3, source_meta=source_meta))
        entry = IndexManifestEntry(0, partition, 0, payload)
        predicate = PredicateBuilder([field]).equal("value", 1)

        planned = scan.plan(9, [split], [definition], [entry])
        created = []

        def reader_factory(*ignored):
            created.append(_Reader())
            return created[-1]

        evaluated = scan.evaluate(
            planned, [field], predicate, [definition], reader_factory)

        self.assertEqual(1, len(created))
        self.assertEqual([1], evaluated.files[0].result.results().to_list())
        self.assertEqual([1], evaluated.files[1].result.results().to_list())

    def test_shared_result_is_partitioned_only_once(self):
        class CountingResult(GlobalIndexResult):
            def __init__(self):
                self.calls = 0
                self.value = GlobalIndexResult.from_ranges(
                    [Range(1, 1), Range(3, 3)]).results()

            def results(self):
                self.calls += 1
                return self.value

        field = DataField(3, "value", AtomicType("INT"))
        definition = PrimaryKeyIndexDefinition(
            "value", 3, "btree", Options.from_none(), PrimaryKeyIndexFamily.BTREE)
        files = [
            SimpleNamespace(file_name="a", row_count=2, level=1, file_source=1),
            SimpleNamespace(file_name="b", row_count=2, level=1, file_source=1),
        ]
        partition = GenericRow([], [])
        split = DataSplit(files, partition, 0, raw_convertible=True)
        source_meta = PrimaryKeyIndexSourceMeta(
            1, [PrimaryKeyIndexSourceFile("a", 2),
                PrimaryKeyIndexSourceFile("b", 2)]).serialize()
        payload = IndexFileMeta(
            "btree", "index", 1, 4,
            global_index_meta=GlobalIndexMeta(0, 3, 3, source_meta=source_meta))
        planned = scan.plan(
            9, [split], [definition], [IndexManifestEntry(0, partition, 0, payload)])
        counting_result = CountingResult()

        evaluated = scan.evaluate(
            planned, [field], PredicateBuilder([field]).equal("value", 1),
            [definition], lambda *ignored: _Reader(result=counting_result))

        self.assertEqual(2, len(evaluated.files))
        self.assertEqual(1, counting_result.calls)

    def test_invalid_group_position_forces_each_source_file_to_fallback(self):
        field = DataField(3, "value", AtomicType("INT"))
        definition = PrimaryKeyIndexDefinition(
            "value", 3, "btree", Options.from_none(), PrimaryKeyIndexFamily.BTREE)
        files = [
            SimpleNamespace(file_name="a", row_count=2, level=1, file_source=1),
            SimpleNamespace(file_name="b", row_count=2, level=1, file_source=1),
        ]
        partition = GenericRow([], [])
        split = DataSplit(files, partition, 0, raw_convertible=True)
        source_meta = PrimaryKeyIndexSourceMeta(
            1, [PrimaryKeyIndexSourceFile("a", 2),
                PrimaryKeyIndexSourceFile("b", 2)]).serialize()
        payload = IndexFileMeta(
            "btree", "index", 1, 4,
            global_index_meta=GlobalIndexMeta(0, 3, 3, source_meta=source_meta))
        planned = scan.plan(
            9, [split], [definition], [IndexManifestEntry(0, partition, 0, payload)])

        evaluated = scan.evaluate(
            planned, [field], PredicateBuilder([field]).equal("value", 1),
            [definition], lambda *ignored: _Reader([Range(4, 4)]))

        self.assertEqual([2], evaluated.files[0].result.results().to_list())
        self.assertEqual([2], evaluated.files[1].result.results().to_list())

    def test_reader_close_failure_does_not_discard_evaluated_results(self):
        field = DataField(3, "value", AtomicType("INT"))
        definition = PrimaryKeyIndexDefinition(
            "value", 3, "btree", Options.from_none(), PrimaryKeyIndexFamily.BTREE)
        data_file = SimpleNamespace(
            file_name="a", row_count=2, level=1, file_source=1)
        partition = GenericRow([], [])
        split = DataSplit([data_file], partition, 0, raw_convertible=True)
        source_meta = PrimaryKeyIndexSourceMeta(
            1, [PrimaryKeyIndexSourceFile("a", 2)]).serialize()
        payload = IndexFileMeta(
            "btree", "index", 1, 2,
            global_index_meta=GlobalIndexMeta(0, 1, 3, source_meta=source_meta))
        planned = scan.plan(
            9, [split], [definition], [IndexManifestEntry(0, partition, 0, payload)])

        evaluated = scan.evaluate(
            planned, [field], PredicateBuilder([field]).equal("value", 1),
            [definition], lambda *ignored: _Reader(
                ranges=[Range(1, 1)], close_error=True))

        self.assertEqual([1], evaluated.files[0].result.results().to_list())


if __name__ == "__main__":
    unittest.main()
