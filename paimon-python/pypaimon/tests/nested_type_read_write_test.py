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
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class NestedTypeReadWriteTest(unittest.TestCase):
    """Read/write of nested map types on primary-key tables.

    Primary-key tables read through a row-based merge path that converts each
    arrow batch with polars before merging, so reading a map nested inside a
    struct exercises that conversion. This guards against regressions there
    (e.g. polars releases that cannot decode a struct-nested arrow Map).
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, name, pa_schema):
        self.catalog.create_table(
            'default.' + name,
            Schema.from_pyarrow_schema(
                pa_schema, primary_keys=['id'],
                options={'bucket': '1', 'file.format': 'parquet'}),
            False)
        return self.catalog.get_table('default.' + name)

    @staticmethod
    def _write(table, pa_schema, ids, values):
        data = pa.table(
            {'id': pa.array(ids, pa_schema.field('id').type),
             pa_schema.field(1).name: pa.array(
                 values, type=pa_schema.field(1).type)},
            schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        write.write_arrow(data)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

    @staticmethod
    def _read_sorted(table):
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        rows = read_builder.new_read().to_arrow(splits).to_pylist()
        rows.sort(key=lambda r: r['id'])
        return rows

    def test_pk_merge_read_map_nested_in_struct(self):
        # row<latest_version str, latest_value row<...>,
        #     all_versioned_values map<string, row<...>>>
        inner = pa.struct([
            pa.field('audio_vae_version', pa.string()),
            pa.field('audio_vae_result_path', pa.string()),
            pa.field('audio_vae_latent_shape', pa.string()),
        ])
        top = pa.struct([
            pa.field('latest_version', pa.string()),
            pa.field('latest_value', inner),
            pa.field('all_versioned_values', pa.map_(pa.string(), inner)),
        ])
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            pa.field('info', top),
        ])

        def value(ver, path, shape):
            return {'audio_vae_version': ver,
                    'audio_vae_result_path': path,
                    'audio_vae_latent_shape': shape}

        table = self._create_pk_table('nested_map_in_struct', pa_schema)

        # first version
        self._write(table, pa_schema, [1], [{
            'latest_version': 'v2',
            'latest_value': value('v2', '/p/v2', '[1,2,3]'),
            'all_versioned_values': [
                ('v1', value('v1', '/p/v1', '[1,2]')),
                ('v2', value('v2', '/p/v2', '[1,2,3]')),
            ],
        }])
        # same pk again -> forces a merge read (the polars conversion path)
        self._write(table, pa_schema, [1], [{
            'latest_version': 'v3',
            'latest_value': value('v3', '/p/v3', '[1,2,3,4]'),
            'all_versioned_values': [
                ('v1', value('v1', '/p/v1', '[1,2]')),
                ('v2', value('v2', '/p/v2', '[1,2,3]')),
                ('v3', value('v3', '/p/v3', '[1,2,3,4]')),
            ],
        }])

        rows = self._read_sorted(table)
        self.assertEqual(1, len(rows))
        info = rows[0]['info']
        self.assertEqual('v3', info['latest_version'])
        self.assertEqual(value('v3', '/p/v3', '[1,2,3,4]'),
                         info['latest_value'])
        self.assertEqual(
            [('v1', value('v1', '/p/v1', '[1,2]')),
             ('v2', value('v2', '/p/v2', '[1,2,3]')),
             ('v3', value('v3', '/p/v3', '[1,2,3,4]'))],
            info['all_versioned_values'])

    def test_pk_merge_read_top_level_map(self):
        # map<string, row<a string, b int>> as a top-level value column
        row_ab = pa.struct([
            pa.field('a', pa.string()),
            pa.field('b', pa.int32()),
        ])
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            pa.field('v', pa.map_(pa.string(), row_ab)),
        ])

        table = self._create_pk_table('top_level_map', pa_schema)

        self._write(table, pa_schema, [1, 2], [
            [('k1', {'a': 'OLD', 'b': 1})],
            [('k2', {'a': 'keep', 'b': 2})],
        ])
        # overwrite id=1, leave id=2 untouched
        self._write(table, pa_schema, [1], [
            [('k1', {'a': 'NEW', 'b': 100}), ('k1b', {'a': 'extra', 'b': 101})],
        ])

        rows = self._read_sorted(table)
        self.assertEqual(2, len(rows))
        self.assertEqual(
            [('k1', {'a': 'NEW', 'b': 100}), ('k1b', {'a': 'extra', 'b': 101})],
            rows[0]['v'])
        self.assertEqual([('k2', {'a': 'keep', 'b': 2})], rows[1]['v'])


if __name__ == '__main__':
    unittest.main()
