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

import unittest

import pyarrow as pa

from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
)
from pypaimon.utils.range import Range


class GlobalIndexBuildTest(
        BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'global-index.enabled': 'true',
        'bucket': '-1',
        'file.format': 'parquet',
    }

    def test_create_btree_global_index_from_python(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {
                'id': [3, 1, 2, 2],
                'name': ['c', 'a', 'b1', 'b2'],
                'age': [30, 10, 20, 21],
                'city': ['z', 'x', 'y', 'y2'],
            },
            schema=self.pa_schema,
        ))

        added = table.create_global_index(
            'id',
            options={'sorted-index.records-per-range': '2'},
        )

        self.assertEqual(2, added)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertIsNotNone(snapshot.index_manifest)

        entries = IndexFileHandler(table).scan(snapshot)
        self.assertEqual(2, len(entries))
        self.assertEqual({'btree'}, {e.index_file.index_type for e in entries})
        self.assertEqual({0}, {e.index_file.global_index_meta.row_range_start for e in entries})
        self.assertEqual({3}, {e.index_file.global_index_meta.row_range_end for e in entries})

        read_builder = table.new_read_builder()
        predicate = read_builder.new_predicate_builder().equal('id', 2)
        with GlobalIndexScanner.create(
                table,
                predicate=predicate,
                snapshot=snapshot) as scanner:
            result = scanner.scan(predicate)

        self.assertEqual(
            [Range(2, 3)],
            result.results().to_range_list(),
        )

        table_name = table.identifier.get_full_name()
        table_indexes = self.catalog.get_table(table_name + '$table_indexes')
        index_read_builder = table_indexes.new_read_builder().with_projection([
            'index_type',
            'index_field_name',
            'row_range_start',
            'row_range_end',
        ])
        index_table = index_read_builder.new_read().to_arrow(
            index_read_builder.new_scan().plan().splits())
        self.assertEqual(2, index_table.num_rows)
        self.assertEqual(['btree', 'btree'],
                         index_table.column('index_type').to_pylist())
        self.assertEqual(['id', 'id'],
                         index_table.column('index_field_name').to_pylist())
        self.assertEqual([0, 0],
                         index_table.column('row_range_start').to_pylist())
        self.assertEqual([3, 3],
                         index_table.column('row_range_end').to_pylist())

        key_ranges = self.catalog.get_table(table_name + '$file_key_ranges')
        range_read_builder = key_ranges.new_read_builder().with_projection([
            'file_path',
            'record_count',
            'first_row_id',
        ])
        range_table = range_read_builder.new_read().to_arrow(
            range_read_builder.new_scan().plan().splits())
        self.assertEqual(1, range_table.num_rows)
        self.assertEqual([4], range_table.column('record_count').to_pylist())
        self.assertEqual([0], range_table.column('first_row_id').to_pylist())

        self.assertEqual(2, table.drop_global_index('id', dry_run=True))
        self.assertEqual(2, len(IndexFileHandler(table).scan(
            table.snapshot_manager().get_latest_snapshot())))

        self.assertEqual(2, table.drop_global_index('id'))
        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual([], IndexFileHandler(table).scan(latest_snapshot))

        index_read_builder = table_indexes.new_read_builder()
        index_table = index_read_builder.new_read().to_arrow(
            index_read_builder.new_scan().plan().splits())
        self.assertEqual(0, index_table.num_rows)


if __name__ == "__main__":
    unittest.main()
