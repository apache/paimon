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
from unittest.mock import patch

import pyarrow as pa

from pypaimon.read.table_read import TableRead
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
        'read.batch-size': '2',
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
                with patch.object(reader, '_plan_raw_read', wraps=reader._plan_raw_read) as plan, \
                        patch.object(TableRead, 'to_arrow', side_effect=AssertionError(
                            'Batch fallback must not materialize the full table')):
                    actual = reader._read_raw_batch_search(ranges, None, 'ivf-flat')
                plan.assert_called_once()
                self.assertEqual(expected, [_scores(result) for result in actual])
                self.assertTrue(all(len(result.results()) <= 2 for result in actual))

    def test_filters_and_partition_are_applied_to_the_shared_scan(self):
        table = self._create_table(partition_keys=['pt'])
        self._write_arrow(table, self._data([[10, 0], [11, 0]], partition=0))
        self._write_arrow(table, self._data([[0, 0], [1, 0], [2, 0]], partition=1))
        predicates = table.new_read_builder().new_predicate_builder()
        reader = self._reader(
            table, [[1, 0], [2, 0]],
            filter_=predicates.greater_or_equal('id', 1),
            partition_filter=predicates.equal('pt', 1))
        ranges = [Range(0, 99)]
        pre_filter = [Range(1, 99)]
        expected = [_scores(reader._read_raw_search(
            ranges, pre_filter, q, 'ivf-flat')) for q in reader._query_vectors]
        with patch.object(reader, '_plan_raw_read', wraps=reader._plan_raw_read) as plan:
            actual = reader._read_raw_batch_search(ranges, pre_filter, 'ivf-flat')
        plan.assert_called_once()
        self.assertEqual(expected, [_scores(r) for r in actual])
        self.assertTrue(all(len(r.results()) == 2 for r in actual))
        self.assertTrue(all(max(_scores(r).values()) == 1.0 for r in actual))

    def test_empty_prefilter_and_empty_queries_do_not_read(self):
        table = self._create_table()
        for queries, ranges, pre_filter in (
            ([[0, 0], [1, 1]], [Range(0, 9)], []),
            ([[0, 0]], [], None),
            ([], [Range(0, 9)], None),
        ):
            with self.subTest(queries=queries, ranges=ranges):
                reader = self._reader(table, queries)
                with patch.object(reader, '_plan_raw_read') as plan:
                    results = reader._read_raw_batch_search(ranges, pre_filter, 'ivf-flat')
                plan.assert_not_called()
                self.assertEqual([{} for _ in queries], [_scores(r) for r in results])

    def test_empty_and_null_only_data_return_empty_results(self):
        for vectors in ([], [None, None, None]):
            with self.subTest(vectors=vectors):
                table = self._create_table()
                if vectors:
                    self._write_arrow(table, self._data(vectors))
                reader = self._reader(table, [[0, 0], [1, 1]])
                results = reader._read_raw_batch_search([Range(0, 99)], None, 'ivf-flat')
                self.assertEqual([{}, {}], [_scores(r) for r in results])

    def test_scoring_finishes_each_batch_before_reading_the_next(self):
        table = self._create_table()
        self._write_arrow(table, self._data([[1, 0], [0, 1], [2, 0], [0, 2]]))
        reader = self._reader(table, [[1, 0], [0, 1]])
        original = TableRead._new_arrow_batch_reader
        original_generator = TableRead._arrow_batch_generator
        generators = []
        batch_sizes = []
        module = 'pypaimon.table.source.vector_search_read'
        from pypaimon.table.source.vector_search_read import _compute_score

        def batches(table_read, splits):
            arrow, generator = original(table_read, splits)
            generators.append(generator)
            return arrow, generator

        def tracked_generator(table_read, *args):
            source = original_generator(table_read, *args)
            expected_scores = 0
            try:
                for batch in source:
                    batch_sizes.append(batch.num_rows)
                    expected_scores += batch.num_rows * 2
                    yield batch
                    self.assertEqual(expected_scores, score.call_count)
            finally:
                source.close()

        with patch.object(TableRead, '_new_arrow_batch_reader', batches), \
                patch.object(TableRead, '_arrow_batch_generator', tracked_generator), \
                patch(module + '._compute_score', wraps=_compute_score) as score:
            reader._read_raw_batch_search([Range(0, 3)], None, 'ivf-flat')
        self.assertEqual(8, score.call_count)
        self.assertEqual(1, len(generators))
        self.assertGreater(len(batch_sizes), 1)
        self.assertIsNone(generators[0].gi_frame)

    def test_dimension_and_read_failures_close_suspended_iterator(self):
        table = self._create_table()
        self._write_arrow(table, self._data([[1, 0], [0, 1]]))
        schema = pa.schema([('embedding', pa.list_(pa.float32())), ('_ROW_ID', pa.int64())])
        for failure in ('dimension', 'read'):
            with self.subTest(failure=failure):
                closed = []

                def generate():
                    try:
                        yield pa.RecordBatch.from_arrays([
                            pa.array([[1, 0]], type=pa.list_(pa.float32())),
                            pa.array([0], type=pa.int64()),
                        ], schema=schema)
                        raise RuntimeError('injected read failure')
                    finally:
                        closed.append(True)

                generator = generate()
                arrow = pa.RecordBatchReader.from_batches(schema, generator)
                reader = self._reader(table, [[1]] if failure == 'dimension' else [[1, 0]])
                exception = ValueError if failure == 'dimension' else RuntimeError
                message = 'dimension mismatch' if failure == 'dimension' else 'injected read failure'
                with patch.object(TableRead, '_new_arrow_batch_reader', return_value=(arrow, generator)):
                    with self.assertRaisesRegex(exception, message):
                        reader._read_raw_batch_search([Range(0, 1)], None, 'ivf-flat')
                self.assertEqual([True], closed)
                self.assertIsNone(generator.gi_frame)

    def test_public_batch_search_uses_planned_snapshot(self):
        table = self._create_table()
        self._write_arrow(table, self._data([[0, 0]]))
        builder = table.new_batch_vector_search_builder().with_vector_column(
            'embedding').with_query_vectors([[2, 0], [0, 0]]).with_limit(1)
        plan = builder.new_vector_search_scan().scan()
        self._write_arrow(table, self._data([[2, 0]]))
        reader = builder.new_batch_vector_search_read()
        old = reader.read_batch_plan(plan)
        self.assertEqual([0.2], list(_scores(old[0]).values()))
        self.assertEqual([1.0], list(_scores(old[1]).values()))
        current = builder.execute_batch_local()
        self.assertEqual([1.0], list(_scores(current[0]).values()))
        self.assertNotEqual(list(old[0].results()), list(current[0].results()))


if __name__ == '__main__':
    unittest.main()
