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

import glob
import os
import tempfile
import unittest
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
from parameterized import parameterized

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.arrow_schema import arrow_schemas_compatible, normalize_arrow_strings
from pypaimon.write.table_write import TableWrite


class ArrowSchemaTest(unittest.TestCase):
    def test_compatibility_keeps_non_string_contracts(self):
        target = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('text', pa.struct([pa.field('value', pa.string(), nullable=False)])),
            ('blob', pa.large_binary()),
        ])
        source = target.set(1, pa.field('text', pa.struct([
            pa.field('value', pa.large_string(), nullable=False),
        ])))
        self.assertTrue(arrow_schemas_compatible(source, target))
        for invalid in [
            source.set(0, pa.field('id', pa.int64(), nullable=False)),
            source.set(1, pa.field('text', pa.struct([('value', pa.large_string())]))),
            source.set(2, pa.field('blob', pa.binary())),
            pa.schema(list(reversed(list(source)))),
        ]:
            self.assertFalse(arrow_schemas_compatible(invalid, target))

    @parameterized.expand([(False,), (True,)])
    def test_map_sorting_contract_is_preserved(self, sorted_keys):
        small = pa.schema([('v', pa.map_(pa.string(), pa.string(), keys_sorted=sorted_keys))])
        large = pa.schema([('v', pa.map_(pa.large_string(), pa.large_string(), keys_sorted=sorted_keys))])
        different_sorting = pa.schema([('v', pa.map_(pa.string(), pa.string(), keys_sorted=not sorted_keys))])
        self.assertTrue(arrow_schemas_compatible(small, large))
        self.assertFalse(arrow_schemas_compatible(large, different_sorting))

    @parameterized.expand([(False, False), (False, True), (True, False), (True, True)])
    def test_normalization_preserves_schema_and_other_buffers(self, record_batch, empty):
        schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            pa.field('text', pa.large_string(), metadata={b'description': b'label'}),
            ('blob', pa.large_binary()),
            ('bytes', pa.binary(1)),
        ], metadata={b'source': b'input'})
        values = [[], [], [], []] if empty else [[1, 2], ['中文', None], [b'blob', None], [b'x', b'y']]
        arrays = [pa.array(items, type=field.type) for items, field in zip(values, schema)]
        factory = pa.RecordBatch if record_batch else pa.Table
        source = factory.from_arrays(arrays, schema=schema)
        result = normalize_arrow_strings(source)
        self.assertIsInstance(result, factory)
        self.assertEqual(result.schema, schema.set(1, schema[1].with_type(pa.string())))
        self.assertEqual(result.schema.metadata, schema.metadata)
        self.assertEqual(result.schema[1].metadata, schema[1].metadata)
        self.assertEqual(result.to_pydict(), source.to_pydict())
        self.assertIs(normalize_arrow_strings(result), result)
        if not empty:
            blob = result.column(2) if record_batch else result.column(2).chunk(0)
            self.assertEqual(blob.buffers()[2].address, arrays[2].buffers()[2].address)

    @parameterized.expand([('table',), ('batch',), ('bucket',), ('postpone',), ('postpone_batch',)])
    def test_core_writes_canonical_strings(self, entry):
        with tempfile.TemporaryDirectory() as directory:
            source_schema = pa.schema([
                pa.field('id', pa.int64(), nullable=False), ('text', pa.large_string()),
            ])
            catalog = CatalogFactory.create({'warehouse': directory})
            catalog.create_database('default', False)
            catalog.create_table('default.strings', Schema.from_pyarrow_schema(
                source_schema, primary_keys=['id'] if entry.startswith('postpone') else [],
                options={'bucket': '-2' if entry.startswith('postpone') else '1', 'file.format': 'parquet'},
            ), False)
            table = catalog.get_table('default.strings')
            builder = (table.new_postpone_fixed_bucket_write_builder()
                       if entry.startswith('postpone') else table.new_batch_write_builder())
            writer, commit = builder.new_write(), builder.new_commit()
            expected = {'id': [1, 2], 'text': ['中文', None]}
            source = pa.Table.from_pydict(expected, schema=source_schema)
            try:
                if entry in ('batch', 'postpone_batch'):
                    writer.write_arrow_batch(source.to_batches()[0])
                elif entry == 'bucket':
                    writer.write_arrow_batch_to_bucket(source.to_batches()[0], 0)
                else:
                    writer.write_arrow(source)
                commit.commit(writer.prepare_commit())
            except Exception:
                writer.abort()
                raise
            finally:
                writer.close()
                commit.close()
            files = glob.glob(os.path.join(directory, '**', '*.parquet'), recursive=True)
            self.assertTrue(files)
            for path in files:
                self.assertEqual(pq.read_schema(path).field('text').type, pa.string())
            reader = table.new_read_builder()
            result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
            actual = result.to_pydict()
            self.assertEqual(sorted(zip(actual['id'], actual['text'])), [(1, '中文'), (2, None)])

    @unittest.skipUnless(int(pa.__version__.split('.')[0]) == 6, 'Arrow 6 lacks struct cast kernels')
    def test_unsupported_nested_cast_fails_before_routing(self):
        writer = object.__new__(TableWrite)
        writer.file_store_write = Mock(write_cols=None)
        writer.row_key_extractor = Mock()
        writer.table_pyarrow_schema = pa.schema([('nested', pa.struct([('text', pa.string())]))])
        source = pa.Table.from_pydict(
            {'nested': [{'text': '中文'}]},
            schema=pa.schema([('nested', pa.struct([('text', pa.large_string())]))]),
        )
        with self.assertRaisesRegex(ValueError, 'Cannot convert large_string input to string'):
            writer.write_arrow(source)
        with self.assertRaisesRegex(ValueError, 'Cannot convert large_string input to string'):
            writer.write_arrow_batch(source.to_batches()[0])
        writer.row_key_extractor.extract_partition_bucket_groups.assert_not_called()
        writer.file_store_write.write.assert_not_called()
