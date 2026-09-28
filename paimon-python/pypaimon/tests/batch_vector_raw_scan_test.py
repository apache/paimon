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

from pypaimon.table.source.vector_search_read import BatchVectorSearchReadImpl
from pypaimon.tests.data_evolution_test_helpers import BatchModeMixin, DataEvolutionTestBase
from pypaimon.utils.range import Range


def _scores(result):
    getter = result.score_getter()
    return {row_id: getter(row_id) for row_id in result.results()}


class BatchVectorRawScanTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    pa_schema = pa.schema([
        ('id', pa.int32()), ('embedding', pa.list_(pa.float32())), ('pt', pa.int32()),
    ])
    table_options = {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'global-index.enabled': 'true', 'bucket': '-1', 'file.format': 'parquet',
        'vector-index.search-mode': 'full',
    }

    def _data(self, vectors, partition=0):
        return pa.table({'id': list(range(len(vectors))), 'embedding': vectors,
                         'pt': [partition] * len(vectors)}, schema=self.pa_schema)

    def _reader(self, table, queries, metric='l2', **kwargs):
        return BatchVectorSearchReadImpl(
            table, limit=2, vector_column=table.field_dict['embedding'],
            query_vectors=queries, options={'ivf-flat.metric': metric}, **kwargs)

    def test_shared_scan_matches_individual_queries_for_all_metrics(self):
        table = self._create_table()
        self._write_arrow(table, self._data([
            [1, 0], [0, 1], None, [0, 0], [1, 0], [-1, 2], [2, -1],
        ]))
        queries = [[1, 0], [0, 1], [0, 0], [1, 0]]
        ranges = [Range(0, 4), Range(3, 6)]
        for metric in ('l2', 'cosine', 'inner_product'):
            with self.subTest(metric=metric):
                reader = self._reader(table, queries, metric)
                expected = [_scores(reader._read_raw_search(
                    ranges, None, query, 'ivf-flat')) for query in queries]
                actual = reader._read_raw_batch_search(ranges, None, 'ivf-flat')
                self.assertEqual(expected, [_scores(result) for result in actual])
                self.assertTrue(all(len(result.results()) <= 2 for result in actual))

    def test_filters_and_partition_are_applied_to_the_shared_scan(self):
        from pypaimon.common.predicate import Predicate
        table = self._create_table(partition_keys=['pt'])
        self._write_arrow(table, self._data([[10, 0], [11, 0]], partition=0))
        self._write_arrow(table, self._data([[0, 0], [1, 0], [2, 0]], partition=1))
        partition_pred = Predicate(method="equal", index=0, field="pt", literals=[1])
        reader = self._reader(table, [[1, 0], [0, 1]], 'l2',
                              partition_filter=partition_pred)
        actual = reader._read_raw_batch_search(
            [Range(0, 2), Range(2, 4)], None, 'ivf-flat')
        ids_0 = sorted(actual[0].results())
        ids_1 = sorted(actual[1].results())
        self.assertTrue(all(rid >= 2 for rid in ids_0))
        self.assertTrue(all(rid >= 2 for rid in ids_1))

    def test_null_vectors_are_excluded(self):
        table = self._create_table()
        self._write_arrow(table, self._data([
            [1, 0], None, [0, 1],
        ]))
        reader = self._reader(table, [[1, 0]], 'l2')
        actual = reader._read_raw_batch_search([Range(0, 2)], None, 'ivf-flat')
        for row_id in actual[0].results():
            self.assertNotEqual(1, row_id)

    def test_public_batch_search_uses_planned_snapshot(self):
        table = self._create_table()
        self._write_arrow(table, self._data([[0, 0]]))
        builder = table.new_batch_vector_search_builder().with_vector_column(
            'embedding').with_query_vectors([[2, 0], [0, 0]]).with_limit(1)
        plan = builder.new_vector_search_scan().scan()
        self._write_arrow(table, self._data([[2, 0]]))
        reader = builder.new_batch_vector_search_read()
        old = reader.read_batch_plan(plan)
        self.assertAlmostEqual(0.2, list(_scores(old[0]).values())[0], places=5)
        self.assertAlmostEqual(1.0, list(_scores(old[1]).values())[0], places=5)
        current = builder.execute_batch_local()
        self.assertGreaterEqual(len(current[0].results()), 1)

    def test_public_batch_search_preserves_split_parallelism(self):
        table = self._create_table()
        self._write_arrow(table, self._data([[1, 0], [0, 1], None, [0, 0]]))
        self._write_arrow(table, self._data([[1, 0], [-1, 2], [2, -1]], partition=0))
        for metric in ('l2', 'cosine', 'inner_product'):
            for queries in ([[1, 0]], [[1, 0], [0, 1]]):
                for parallelism in (1, 2, 4, None):
                    with self.subTest(metric=metric, queries=queries, parallelism=parallelism):
                        opts = {'ivf-flat.metric': metric}
                        if parallelism is not None:
                            opts['global-index.thread-num'] = str(parallelism)
                        builder = (table.new_batch_vector_search_builder()
                                   .with_vector_column('embedding')
                                   .with_query_vectors(queries)
                                   .with_limit(2)
                                   .with_options(opts))
                        results = builder.execute_batch_local()
                        self.assertEqual(len(queries), len(results))
                        for result in results:
                            self.assertGreater(len(result.results()), 0)
                            self.assertLessEqual(len(result.results()), 2)


if __name__ == '__main__':
    unittest.main()
