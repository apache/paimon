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
from unittest import mock
from types import SimpleNamespace

from pypaimon.index.pk.primary_key_index_definitions import PrimaryKeyIndexDefinitions
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.primary_key_full_text_read import PrimaryKeyFullTextRead
from pypaimon.table.source.primary_key_full_text_scan import PrimaryKeyFullTextScan
from pypaimon.table.source.primary_key_full_text_scan import (
    _current_payloads, _should_read_source as _full_text_should_read_source)
from pypaimon.table.source.primary_key_vector_read import (
    PrimaryKeyVectorRead, _allowed, _include_row_ids)
from pypaimon.table.source.primary_key_vector_scan import (
    PrimaryKeyVectorScan, _bucket_splits, _should_read_source)
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition)
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


class PrimaryKeyIndexDefinitionsTest(unittest.TestCase):
    def test_definitions_follow_schema_order(self):
        schema = TableSchema(
            fields=[DataField(0, "id", AtomicType("INT")),
                    DataField(1, "name", AtomicType("STRING"))],
            options={"pk-btree.index.columns": "name",
                     "pk-bitmap.index.columns": "id",
                     "fields.name.pk-btree.index.options":
                         '{"block-size": "4096"}'},
        )

        definitions = PrimaryKeyIndexDefinitions.create(schema).definitions

        self.assertEqual(["id", "name"], [d.column for d in definitions])
        self.assertEqual(["bitmap", "btree"], [d.index_type for d in definitions])
        self.assertEqual(
            "4096", definitions[1].options.to_map()["btree-index.block-size"])

    def test_source_meta_uses_java_binary_layout(self):
        meta = PrimaryKeyIndexSourceMeta(
            2, [PrimaryKeyIndexSourceFile("数据-\0-😀.parquet", 7)])

        restored = PrimaryKeyIndexSourceMeta.deserialize(meta.serialize())

        self.assertEqual(meta, restored)

    def test_vector_and_full_text_definitions(self):
        schema = TableSchema(
            fields=[DataField(0, "embedding", AtomicType("VECTOR(3)")),
                    DataField(1, "content", AtomicType("STRING"))],
            options={
                "pk-vector.index.columns": "embedding",
                "fields.embedding.pk-vector.index.type": "vindex",
                "fields.embedding.pk-vector.index.options": '{"m": "16"}',
                "pk-full-text.index.columns": "content",
                "fields.content.pk-full-text.index.options": '{"analyzer": "english"}',
            })

        definitions = PrimaryKeyIndexDefinitions.create(schema).definitions

        self.assertEqual(["vindex", "full-text"], [d.index_type for d in definitions])
        self.assertEqual("16", definitions[0].options.to_map()["vindex.m"])
        self.assertEqual(
            "inner_product", definitions[0].options.to_map()["vindex.metric"])
        self.assertEqual(
            "english", definitions[1].options.to_map()["full-text.analyzer"])

    def test_builders_route_primary_key_searches(self):
        schema = TableSchema(
            fields=[DataField(0, "embedding", AtomicType("VECTOR(3)")),
                    DataField(1, "content", AtomicType("STRING"))],
            options={
                "pk-vector.index.columns": "embedding",
                "fields.embedding.pk-vector.index.type": "vindex",
                "pk-full-text.index.columns": "content",
            })
        table = SimpleNamespace(
            fields=schema.fields, table_schema=schema, partition_keys=[])

        vector = (VectorSearchBuilderImpl(table)
                  .with_vector_column("embedding")
                  .with_query_vector([1.0, 2.0, 3.0])
                  .with_limit(2))
        full_text = (FullTextSearchBuilderImpl(table)
                     .with_query("content", "paimon")
                     .with_limit(2))

        self.assertIsInstance(vector.new_vector_search_scan(), PrimaryKeyVectorScan)
        self.assertIsInstance(vector.new_vector_search_read(), PrimaryKeyVectorRead)
        self.assertIsInstance(full_text.new_full_text_scan(), PrimaryKeyFullTextScan)
        self.assertIsInstance(full_text.new_full_text_read(), PrimaryKeyFullTextRead)

    def test_primary_key_full_text_only_supports_fast_mode(self):
        fields = [DataField(0, "id", AtomicType("INT")),
                  DataField(1, "content", AtomicType("STRING"))]

        def builder(mode):
            schema = TableSchema(
                fields=fields,
                options={"pk-full-text.index.columns": "content",
                         "global-index.search-mode": mode})
            table = SimpleNamespace(
                fields=fields, table_schema=schema, partition_keys=[])
            return (FullTextSearchBuilderImpl(table)
                    .with_query("content", "paimon").with_limit(2))

        self.assertIsInstance(
            builder("fast").new_full_text_read(), PrimaryKeyFullTextRead)
        for mode in ("full", "detail"):
            with self.subTest(mode=mode), self.assertRaisesRegex(
                    NotImplementedError, "only supports the FAST"):
                builder(mode).new_full_text_read()

    def test_primary_key_full_text_reader_cannot_bypass_fast_mode(self):
        field = DataField(1, "content", AtomicType("STRING"))
        for mode in ("full", "detail"):
            schema = TableSchema(
                fields=[field],
                options={"global-index.search-mode": mode})
            table = SimpleNamespace(table_schema=schema)
            with self.subTest(mode=mode), self.assertRaisesRegex(
                    NotImplementedError, "only supports the FAST"):
                PrimaryKeyFullTextRead(
                    table, 1, field, "paimon", definition=None)

    def test_scored_result_uses_file_local_positions(self):
        partition = GenericRow([], [])
        data_file = DataFileMeta(
            file_name="data.parquet", file_size=10, row_count=5,
            min_key=None, max_key=None, key_stats=None, value_stats=None,
            min_sequence_number=0, max_sequence_number=0,
            schema_id=0, level=1, extra_files=[])
        source = DataSplit([data_file], partition, 3)
        partition_bytes = repr(tuple(partition.values)).encode("utf-8")

        result = PrimaryKeyScoredResult(7, [source], [
            PrimaryKeySearchPosition(partition_bytes, 3, "data.parquet", 1, 0.9),
            PrimaryKeySearchPosition(partition_bytes, 3, "data.parquet", 2, 0.8),
            PrimaryKeySearchPosition(partition_bytes, 3, "data.parquet", 4, 0.7),
        ])

        self.assertEqual(7, result.snapshot_id)
        self.assertEqual([(1, 2), (4, 4)],
                         [(r.from_, r.to) for r in result.splits[0].row_ranges()])
        self.assertEqual([0.9, 0.8, 0.7], result.splits[0].scores())

    def test_pk_vector_filter_is_applied_before_top_k(self):
        split = SimpleNamespace(row_ranges_by_file={
            "first.parquet": (Range(1, 2),),
            "second.parquet": (Range(0, 1),),
        })
        sources = [
            PrimaryKeyIndexSourceFile("first.parquet", 4),
            PrimaryKeyIndexSourceFile("second.parquet", 3),
        ]
        deleted = {(id(split), "first.parquet"): {1}}

        include = _include_row_ids(split, sources, deleted)

        # first[2] maps to payload row 2; second[0,1] map to rows 4 and 5.
        self.assertEqual([2, 4, 5], list(include))

    def test_empty_pk_vector_ranges_mean_no_candidates(self):
        split = SimpleNamespace(row_ranges_by_file={"data.parquet": tuple()})
        source = PrimaryKeyIndexSourceFile("data.parquet", 3)

        include = _include_row_ids(split, [source], {})

        self.assertEqual([], list(include))
        self.assertFalse(_allowed(split, "data.parquet", 0))

    def test_pk_vector_refine_factor_overflow_is_rejected_before_search(self):
        table = SimpleNamespace(options=SimpleNamespace(
            primary_key_vector_index_type=lambda column: "ivf-pq"))
        reader = PrimaryKeyVectorRead(
            table, 2147483647, DataField(1, "embedding", AtomicType("VECTOR(1)")),
            [0.0], options={"ivf.refine_factor": "2"})

        from pypaimon.table.source.primary_key_vector_scan import (
            PrimaryKeyVectorScanPlan)
        plan = PrimaryKeyVectorScanPlan(1, [])
        with self.assertRaisesRegex(ValueError, "limit overflow"):
            reader.read_plan(plan)

    def test_full_text_payload_planner_rejects_duplicate_and_stale_levels(self):
        files = [SimpleNamespace(file_name="a.parquet", row_count=2, level=1)]
        source_meta = PrimaryKeyIndexSourceMeta(
            1, [PrimaryKeyIndexSourceFile("a.parquet", 2)])

        def payload(name, row_count=2, start=0, end=1):
            return SimpleNamespace(
                file_name=name, row_count=row_count,
                global_index_meta=SimpleNamespace(
                    row_range_start=start, row_range_end=end))

        current = payload("current")
        duplicate = payload("duplicate")
        stale = payload("stale", end=0)
        with mock.patch.object(
                PrimaryKeyIndexSourceMeta, "from_index_file",
                return_value=source_meta):
            selected, covered = _current_payloads(
                files, [current, duplicate, stale])

        self.assertEqual([], selected)
        self.assertEqual(set(), covered)

    def test_vector_payload_planner_rejects_duplicate_level(self):
        partition = GenericRow([], [])
        data_file = DataFileMeta(
            file_name="a.parquet", file_size=10, row_count=2,
            min_key=None, max_key=None, key_stats=None, value_stats=None,
            min_sequence_number=0, max_sequence_number=0,
            schema_id=0, level=1, extra_files=[], file_source=1)
        split = DataSplit([data_file], partition, 0)
        source_meta = PrimaryKeyIndexSourceMeta(
            1, [PrimaryKeyIndexSourceFile("a.parquet", 2)])

        def payload(name):
            return SimpleNamespace(
                file_name=name, row_count=2,
                global_index_meta=SimpleNamespace(
                    row_range_start=0, row_range_end=1))

        entries = [
            SimpleNamespace(partition=partition, bucket=0,
                            index_file=payload("first")),
            SimpleNamespace(partition=partition, bucket=0,
                            index_file=payload("duplicate")),
        ]
        with mock.patch.object(
                PrimaryKeyIndexSourceMeta, "from_index_file",
                return_value=source_meta):
            planned = _bucket_splits([(split, {})], entries)

        self.assertEqual(tuple(), planned[0].payloads)
        self.assertEqual(("a.parquet",), planned[0].uncovered_data_files)

    def test_full_text_source_policy_matches_java(self):
        self.assertTrue(_full_text_should_read_source(
            SimpleNamespace(file_source=1, level=1)))
        self.assertFalse(_full_text_should_read_source(
            SimpleNamespace(file_source=1, level=0)))
        self.assertFalse(_full_text_should_read_source(
            SimpleNamespace(file_source=0, level=2)))

    def test_pk_vector_scan_pins_source_and_index_to_same_snapshot(self):
        schema = TableSchema(
            fields=[DataField(1, "embedding", AtomicType("VECTOR(1)"))],
            options={"pk-vector.index.columns": "embedding",
                     "scan.tag-name": "old-tag"})
        snapshot = SimpleNamespace(id=17)
        copied_options = []
        index_snapshots = []

        class _ScanTable:
            table_schema = schema
            fields = schema.fields

            def new_read_builder(self):
                plan = SimpleNamespace(splits=lambda: [])
                scan = SimpleNamespace(plan=lambda: plan)
                return SimpleNamespace(new_scan=lambda: scan)

        class _Table:
            table_schema = schema
            fields = schema.fields

            def tag_manager(self):
                return None

            def snapshot_manager(self):
                return SimpleNamespace(get_latest_snapshot=lambda: snapshot)

            def copy(self, options):
                copied_options.append(options)
                return _ScanTable()

        class _IndexHandler:
            def __init__(self, table):
                self.table = table

            def scan(self, selected_snapshot, entry_filter):
                index_snapshots.append(selected_snapshot)
                return []

        with mock.patch(
                "pypaimon.table.source.primary_key_vector_scan."
                "TimeTravelUtil.try_travel_to_snapshot",
                return_value=snapshot), mock.patch(
                "pypaimon.table.source.primary_key_vector_scan.IndexFileHandler",
                _IndexHandler):
            plan = PrimaryKeyVectorScan(
                _Table(), schema.fields[0], index_type="vindex").scan()

        self.assertEqual(17, plan.snapshot_id)
        self.assertEqual([snapshot], index_snapshots)
        self.assertEqual("from-snapshot", copied_options[0]["scan.mode"])
        self.assertEqual("17", copied_options[0]["scan.snapshot-id"])
        self.assertIsNone(copied_options[0]["scan.tag-name"])

    def test_pk_index_source_policy_matches_java(self):
        compact = SimpleNamespace(file_source=1, level=1)
        append = SimpleNamespace(file_source=0, level=1)
        level_zero = SimpleNamespace(file_source=1, level=0)
        legacy = SimpleNamespace(file_source=None, level=1)

        self.assertTrue(_should_read_source(compact))
        self.assertFalse(_should_read_source(append))
        self.assertFalse(_should_read_source(level_zero))
        self.assertFalse(_should_read_source(legacy))


if __name__ == "__main__":
    unittest.main()
