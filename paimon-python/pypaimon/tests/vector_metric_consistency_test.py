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

import importlib.util
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon.table.source.vector_search_read import (
    BatchVectorSearchReadImpl, DataEvolutionVectorRead, _raw_search_metric)
from pypaimon.tests.vector_search_filter_test import (
    _StubTable, _entry, _field, _install_raw_vector_read_builder)
from pypaimon.table.source.vector_search_split import RawVectorSearchSplit
from pypaimon.utils.range import Range
from pypaimon.tests.data_evolution_test_helpers import BatchModeMixin, DataEvolutionTestBase


def _scores(result):
    getter = result.score_getter()
    return {row_id: getter(row_id) for row_id in result.results()}


@unittest.skipUnless(importlib.util.find_spec("paimon_vindex"), "paimon-vindex is not installed")
class NativeVectorMetricTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):
    pa_schema = pa.schema([('embedding', pa.list_(pa.float32()))])
    table_options = {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'global-index.enabled': 'true', 'bucket': '-1', 'file.format': 'parquet',
        'vector-index.search-mode': 'full',
    }

    def _append(self, table, vectors):
        self._write_arrow(table, pa.table({'embedding': vectors}, schema=self.pa_schema))

    def _build(self, table, metric=None):
        options = {'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1'}
        if metric is not None:
            options['ivf-flat.distance.metric'] = metric
        self.assertEqual(1, table.create_global_index('embedding', index_type='ivf-flat', options=options))

    def _builder(self, table, batch=False):
        if batch:
            builder = table.new_batch_vector_search_builder().with_query_vectors([[1, 0], [1, 0]])
        else:
            builder = table.new_vector_search_builder().with_query_vector([1, 0])
        return builder.with_vector_column('embedding').with_limit(2).with_option('ivf.nprobe', '1')

    def _execute(self, builder, batch):
        return builder.execute_batch_local() if batch else [builder.execute_local()]

    def test_mixed_search_uses_persisted_metric(self):
        for metric, expected in ((None, {0: 2.0, 1: 3.0}),
                                 ('inner_product', {0: 2.0, 1: 3.0}),
                                 ('cosine', {0: 1.0, 1: 1.0}),
                                 ('l2', {0: 0.5, 1: 0.2})):
            with self.subTest(metric=metric):
                table = self._create_table()
                self._append(table, [[2, 0]])
                self._build(table, metric)
                self._append(table, [[3, 0]])
                for batch in (False, True):
                    for result in self._execute(self._builder(table, batch), batch):
                        actual = _scores(result)
                        self.assertEqual(set(expected), set(actual))
                        for row_id, score in expected.items():
                            self.assertAlmostEqual(score, actual[row_id], places=6)

    def test_refinement_uses_persisted_metric(self):
        for metric, expected in ((None, {1: 3.0}), ('cosine', {0: 1.0}), ('l2', {0: 0.5})):
            with self.subTest(metric=metric):
                table = self._create_table()
                self._append(table, [[2, 0], [3, 0]])
                self._build(table, metric)
                for batch in (False, True):
                    builder = self._builder(table, batch).with_limit(1).with_option('ivf.refine_factor', '2')
                    for result in self._execute(builder, batch):
                        self.assertEqual(expected, _scores(result))

    def test_persisted_metric_overrides_changed_table_options(self):
        table = self._create_table()
        self._append(table, [[2, 0]])
        self._build(table)
        self._append(table, [[3, 0]])
        changed = table.copy({'fields.embedding.distance.metric': 'l2'})
        for batch in (False, True):
            for result in self._execute(self._builder(changed, batch), batch):
                self.assertEqual({0: 2.0, 1: 3.0}, _scores(result))

    def test_incompatible_query_metric_is_rejected(self):
        table = self._create_table()
        self._append(table, [[2, 0]])
        self._build(table)
        for batch in (False, True):
            builder = self._builder(table, batch).with_option('metric', 'l2')
            with self.assertRaisesRegex(ValueError, "Query vector metric 'l2'.*index metric 'inner_product'"):
                self._execute(builder, batch)

    def test_incompatible_shard_metrics_are_rejected(self):
        table = self._create_table()
        self._append(table, [[2, 0]])
        self._build(table)
        self._append(table, [[3, 0]])
        self._build(table, 'cosine')
        for batch in (False, True):
            with self.assertRaisesRegex(ValueError, 'Cannot merge vector indexes with different metrics'):
                self._execute(self._builder(table, batch), batch)

    def test_default_metric_is_consistent_before_and_after_build(self):
        table = self._create_table()
        self._append(table, [[2, 0], [3, 0]])
        for indexed in (False, True):
            if indexed:
                self._build(table)
            for batch in (False, True):
                builder = self._builder(table, batch).with_option('index-type', 'ivf-flat').with_limit(1)
                for result in self._execute(builder, batch):
                    self.assertEqual({1: 3.0}, _scores(result))


class VectorMetricResolutionTest(unittest.TestCase):
    def setUp(self):
        self.column = _field(1, 'embedding', 'FLOAT')
        self.table = _StubTable(fields=[self.column], entries=[])
        self.table.table_schema.options = {}

    def _reader(self, options=None, batch=False):
        kwargs = dict(table=self.table, vector_column=self.column, limit=1, options=options)
        if batch:
            return BatchVectorSearchReadImpl(query_vectors=[[1.0]], **kwargs)
        return DataEvolutionVectorRead(query_vector=[1.0], **kwargs)

    def test_query_metric_aliases_are_validated(self):
        for key in ('fields.embedding.pk-vector.distance.metric',
                    'fields.embedding.distance.metric', 'fields.embedding.metric',
                    'ivf-flat.distance.metric', 'ivf-flat.metric', 'distance.metric', 'metric'):
            with self.subTest(key=key):
                native = Mock(spec=['vector_metric'])
                native.vector_metric.return_value = 'inner_product'
                reader = self._reader({key: 'inner-product'})
                reader._record_index_metric(native, 'ivf-flat')
                self.assertEqual('inner_product', reader._search_metric('ivf-flat'))
                reader = self._reader({key: 'l2'})
                with self.assertRaisesRegex(ValueError, 'does not match index metric'):
                    reader._record_index_metric(native, 'ivf-flat')

    def test_other_columns_do_not_override_persisted_metric(self):
        reader = self._reader({'fields.other.metric': 'l2'})
        native = Mock(spec=['vector_metric'])
        native.vector_metric.return_value = 'cosine'
        reader._record_index_metric(native, 'ivf-flat')
        self.assertEqual('cosine', reader._search_metric('ivf-flat'))
        self.assertEqual('inner_product', _raw_search_metric(
            self.table, self.column, {'fields.other.metric': 'l2'}, 'ivf-flat'))

    def test_raw_only_defaults_match_vindex_writers(self):
        for kind in ('ivf-flat', 'ivf-pq', 'ivf-sq', 'ivf-rq', 'diskann'):
            self.assertEqual('inner_product', _raw_search_metric(self.table, self.column, {}, kind))
            self.assertEqual('cosine', _raw_search_metric(
                self.table, self.column, {'metric': 'cosine'}, kind))
        self.assertEqual('l2', _raw_search_metric(self.table, self.column, {}))
        self.assertEqual('l2', _raw_search_metric(self.table, self.column, {}, 'lumina'))

    def test_reader_closes_on_metadata_and_metric_errors(self):
        entry = _entry(None, field_id=1, index_type='ivf-flat', file_name='vectors.index',
                       row_range_start=0, row_range_end=1)
        for failure in ('metadata', 'query', 'shard'):
            with self.subTest(failure=failure):
                native = Mock(spec=['vector_metric', 'close'])
                native.vector_metric.return_value = 'inner_product'
                reader = self._reader({'metric': 'l2'} if failure == 'query' else {})
                if failure == 'metadata':
                    native.vector_metric.side_effect = RuntimeError('invalid index metadata')
                if failure == 'shard':
                    previous = Mock(spec=['vector_metric'])
                    previous.vector_metric.return_value = 'cosine'
                    reader._record_index_metric(previous, 'ivf-flat')
                with patch('pypaimon.table.source.vector_search_read._create_vector_reader', return_value=native):
                    with self.assertRaises((RuntimeError, ValueError)):
                        reader._open_offset_reader([entry.index_file], 0, 1)
                native.close.assert_called_once_with()

    def test_metric_does_not_leak_between_read_calls(self):
        _install_raw_vector_read_builder(self.table, 'embedding', {0: [2.0], 1: [3.0]})
        split = RawVectorSearchSplit([Range(0, 1)], [], 'ivf-flat')
        for batch in (False, True):
            reader = self._reader(batch=batch)
            native = Mock(spec=['vector_metric'])
            native.vector_metric.return_value = 'l2'
            reader._record_index_metric(native, 'ivf-flat')
            results = reader.read_batch([split]) if batch else [reader.read([split])]
            self.assertEqual([{1: 3.0}], [_scores(r) for r in results])


if __name__ == '__main__':
    unittest.main()
