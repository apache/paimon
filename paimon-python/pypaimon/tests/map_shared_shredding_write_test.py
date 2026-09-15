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

import shutil
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.data.map_shared_shredding import (
    is_shared_shredding,
    parse_shared_shredding_metadata,
)


class MapSharedShreddingWriteTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.catalog = CatalogFactory.create({"warehouse": self.temp_dir})
        self.catalog.create_database("default", True)
        self.arrow_schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("metrics", pa.map_(pa.string(), pa.int64())),
        ])

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_and_read_parquet(self):
        expected = [
            [("hot", 10), ("warm", 20), ("overflow", 30)],
            [("hot", None), ("new", 40)],
            [],
            None,
        ]
        data = pa.Table.from_pydict({
            "id": [1, 2, 3, 4],
            "metrics": expected,
        }, schema=self.arrow_schema)

        table = self._create_table("parquet", max_columns=2)
        messages = self._write(table, data)

        physical_field = pq.read_schema(
            messages[0].new_files[0].file_path).field("metrics")
        self.assertTrue(pa.types.is_struct(physical_field.type))
        self.assertTrue(is_shared_shredding(physical_field))
        name_by_id, num_columns = \
            parse_shared_shredding_metadata(physical_field)
        self.assertEqual(2, num_columns)
        self.assertEqual(
            {"hot", "warm", "overflow", "new"},
            set(name_by_id.values()),
        )

        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        self.assertEqual(expected, result.column("metrics").to_pylist())

        selected = table.new_read_builder().with_projection([
            "id", "metrics['hot']", "metrics['overflow']",
        ])
        result = selected.new_read().to_arrow(
            selected.new_scan().plan().splits())
        self.assertEqual(
            [10, None, None, None],
            result.column("metrics_hot").to_pylist(),
        )
        self.assertEqual(
            [30, None, None, None],
            result.column("metrics_overflow").to_pylist(),
        )

    def test_reject_orc(self):
        with self.assertRaisesRegex(
                ValueError,
                "PyPaimon MAP shared-shredding writes only support parquet"):
            writer = self._create_table(
                "orc", max_columns=2).new_batch_write_builder().new_write()
            writer.write_arrow(pa.Table.from_pydict({
                "id": [1],
                "metrics": [[("key", 1)]],
            }, schema=self.arrow_schema))

    def test_adapts_physical_column_count_between_files(self):
        table = self._create_table(
            "parquet", max_columns=4,
            extra_options={
                "data-evolution.enabled": "true",
                "row-tracking.enabled": "true",
                "target-file-row-num": "2",
            },
        )
        data = pa.Table.from_pydict({
            "id": [1, 2, 3, 4],
            "metrics": [
                [("a", 1)],
                [("a", 2)],
                [("a", 3), ("b", 4)],
                [("b", 5)],
            ],
        }, schema=self.arrow_schema)

        messages = self._write(table, data)
        files = [file for message in messages for file in message.new_files]
        self.assertEqual(2, len(files))
        counts = []
        for file in files:
            field = pq.read_schema(file.file_path).field("metrics")
            counts.append(parse_shared_shredding_metadata(field)[1])
        self.assertEqual([4, 1], counts)

        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        self.assertEqual(4, result.num_rows)

    def _create_table(
            self, file_format, max_columns, extra_options=None):
        options = {
            "file.format": file_format,
            "fields.metrics.map.storage-layout": "shared-shredding",
            "fields.metrics.map.shared-shredding.max-columns": str(max_columns),
        }
        options.update(extra_options or {})
        name = "default.map_write_{}_{}".format(
            file_format, len(self.catalog.list_tables("default")))
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(self.arrow_schema, options=options),
            False,
        )
        return self.catalog.get_table(name)

    @staticmethod
    def _write(table, data):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        builder.new_commit().commit(messages)
        writer.close()
        return messages

if __name__ == "__main__":
    unittest.main()
