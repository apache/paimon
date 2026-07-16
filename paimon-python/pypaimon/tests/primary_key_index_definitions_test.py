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
from types import SimpleNamespace

from pypaimon.index.pk.primary_key_index_definitions import PrimaryKeyIndexDefinitions
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.primary_key_full_text_read import PrimaryKeyFullTextRead
from pypaimon.table.source.primary_key_full_text_scan import PrimaryKeyFullTextScan
from pypaimon.table.source.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.table.source.primary_key_vector_scan import PrimaryKeyVectorScan
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition)
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow


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


if __name__ == "__main__":
    unittest.main()
