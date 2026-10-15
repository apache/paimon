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

import os
import sys
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.dataset as ds

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_vortex_reader import FormatVortexReader
from pypaimon.schema.data_types import PyarrowFieldParser


@unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
class VortexReaderTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.path = os.path.join(self.tempdir.name, 'data.vortex')
        self.file_io = LocalFileIO()
        self.data = pa.table({
            'id': pa.array(range(6), type=pa.int32()),
            'text': ['short', None, '', 'long string ' * 20, '日本語', 'last'],
            'binary': [b'a', b'\x00\xff' * 20, None, b'', b'four', b'five'],
        })
        self.file_io.write_vortex(self.path, self.data)

    def read(self, schema=None, predicate=None, **kwargs):
        schema = self.data.schema if schema is None else schema
        reader = FormatVortexReader(
            self.file_io, self.path, PyarrowFieldParser.to_paimon_schema(schema),
            predicate, batch_size=2, **kwargs)
        batches = []
        try:
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                if not kwargs:
                    self.assertLessEqual(batch.num_rows, 2)
                batches.append(batch)
            if not batches:
                return pa.Table.from_batches([], schema=reader.record_batch_reader.schema)
            return pa.Table.from_batches(batches)
        finally:
            reader.close()

    def test_full_scan_uses_native_arrow_conversion(self):
        with mock.patch.object(FormatVortexReader, '_cast_view_types',
                               side_effect=AssertionError('per-batch conversion used')):
            actual = self.read()
        self.assertEqual(actual, self.data)
        self.assertEqual(actual['text'].type, pa.string())
        self.assertEqual(actual['binary'].type, pa.binary())

    def test_projected_filter(self):
        expected = self.data.slice(3).select(['binary', 'text'])
        actual = self.read(expected.schema, ds.field('id') >= 3)
        self.assertEqual(actual, expected)

    def test_missing_fields(self):
        schema = pa.schema([('missing', pa.string()), ('text', pa.string())])
        actual = self.read(schema)
        expected = pa.table({'missing': pa.nulls(6, type=pa.string()), 'text': self.data['text']})
        self.assertEqual(actual, expected)

    def test_all_fields_missing(self):
        actual = self.read(pa.schema([('missing', pa.string())]))
        self.assertEqual(actual, pa.table({'missing': pa.nulls(6, type=pa.string())}))

    def test_row_selection(self):
        for options, indices in [({'row_indices': [1, 3, 5]}, [1, 3, 5]),
                                 ({'shard_range': (2, 5)}, [2, 3, 4])]:
            with self.subTest(options=options):
                self.assertEqual(self.read(**options), self.data.take(indices))

    def test_physical_type_preserved_for_schema_evolution(self):
        actual = self.read(pa.schema([('id', pa.int64())]))
        self.assertEqual(actual, self.data.select(['id']))

    def test_empty_filter(self):
        actual = self.read(predicate=ds.field('id') > 10)
        self.assertEqual(actual, self.data.slice(0, 0))

    def test_natural_splits_are_sliced_without_copying(self):
        native_batches = [self.data.slice(0, 5).to_batches()[0],
                          self.data.slice(5).to_batches()[0]]
        native_reader = pa.RecordBatchReader.from_batches(self.data.schema, native_batches)
        vortex_file = mock.Mock()
        vortex_file.dtype.to_arrow_schema.return_value = self.data.schema
        vortex_file.to_arrow.return_value = native_reader
        with mock.patch('vortex.open', return_value=vortex_file):
            reader = FormatVortexReader(
                self.file_io, self.path,
                PyarrowFieldParser.to_paimon_schema(self.data.schema),
                None, batch_size=2)
        try:
            self.assertNotIn('batch_size', vortex_file.to_arrow.call_args.kwargs)
            batches = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                batches.append(batch)
            self.assertEqual([len(batch) for batch in batches], [2, 2, 1, 1])
            self.assertEqual(pa.Table.from_batches(batches), self.data)
            for batch in batches[:3]:
                for i in range(batch.num_columns):
                    self.assertEqual(
                        [b.address if b is not None else None for b in batch.column(i).buffers()],
                        [b.address if b is not None else None for b in native_batches[0].column(i).buffers()])
        finally:
            reader.close()

    def test_invalid_batch_size(self):
        for batch_size in [0, -1]:
            with self.subTest(batch_size=batch_size):
                with self.assertRaisesRegex(ValueError, 'batch_size must be positive'):
                    FormatVortexReader(self.file_io, self.path, [], None, batch_size=batch_size)
