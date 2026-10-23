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
import os
import shutil
import sys
import tempfile
import unittest

import pyarrow as pa

from pypaimon.common.file_io import FileIO
from pypaimon.execution.python.read.reader import vortex_utils


@unittest.skipIf(sys.version_info < (3, 11) or importlib.util.find_spec('vortex') is None,
                 "vortex-data requires Python >= 3.11")
class VortexUtilsTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = FileIO.get(self.temp_dir, {})

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_local_file_round_trip_reuses_footer(self):
        path = os.path.join(self.temp_dir, 'data', 'a.vortex')
        table = pa.table({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
        vortex_utils.write_vortex_file(self.file_io, path, table)

        first = vortex_utils.open_vortex_file(self.file_io, path)
        self.assertIn(path, vortex_utils._footer_cache)

        # The second open takes the cached footer and reads the same rows.
        second = vortex_utils.open_vortex_file(self.file_io, path)
        self.assertEqual(len(first), len(second))
        result = second.to_arrow(schema=pa.schema([('id', pa.int64()), ('name', pa.utf8())]))
        self.assertEqual(result.read_all().to_pydict(), table.to_pydict())

    def test_store_is_shared_and_keys_are_relative_to_the_bucket(self):
        import vortex
        from vortex.io import write

        url = 'file://' + os.path.join(self.temp_dir, 'b.vortex')
        store, key = vortex_utils._store_for(url, {})
        same_store, _ = vortex_utils._store_for('file://' + os.path.join(self.temp_dir, 'c'), {})
        self.assertIs(store, same_store)
        self.assertEqual(key, os.path.join(self.temp_dir, 'b.vortex').lstrip('/'))

        write(vortex.array(pa.table({'x': [1, 2]})), key, store=store)
        self.assertEqual(len(vortex.open(key, store=store)), 2)

    def test_compact_encodings_make_smaller_files_with_the_same_rows(self):
        table = pa.table({'email': [f'user-{i * 7919 % 100003}@example.com' for i in range(200_000)]})
        default_path = os.path.join(self.temp_dir, 'default.vortex')
        compact_path = os.path.join(self.temp_dir, 'compact.vortex')
        vortex_utils.write_vortex_file(self.file_io, default_path, table)
        vortex_utils.write_vortex_file(self.file_io, compact_path, table, compact=True)

        self.assertLess(os.path.getsize(compact_path), os.path.getsize(default_path))
        compact = vortex_utils.open_vortex_file(self.file_io, compact_path)
        rows = compact.to_arrow(schema=pa.schema([('email', pa.utf8())])).read_all()
        self.assertEqual(rows.to_pydict(), table.to_pydict())

    def test_table_option_selects_compact_encodings(self):
        from unittest import mock

        from pypaimon import CatalogFactory, Schema

        catalog = CatalogFactory.create({'warehouse': os.path.join(self.temp_dir, 'wh')})
        catalog.create_database('db', True)
        pa_schema = pa.schema([('id', pa.int64())])
        catalog.create_table('db.t', Schema.from_pyarrow_schema(
            pa_schema, options={'file.format': 'vortex', 'vortex.compact-encodings': 'true'}), False)
        table = catalog.get_table('db.t')

        with mock.patch.object(vortex_utils, 'write_vortex_file',
                               wraps=vortex_utils.write_vortex_file) as write:
            builder = table.new_batch_write_builder()
            table_write = builder.new_write()
            commit = builder.new_commit()
            table_write.write_arrow(pa.table({'id': [1, 2, 3]}, schema=pa_schema))
            commit.commit(table_write.prepare_commit())
            table_write.close()
            commit.close()
        self.assertTrue(write.call_args.kwargs['compact'])

        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3])

    def test_reader_slices_native_chunks_to_the_batch_size(self):
        from pypaimon.execution.python.read.reader.format_vortex_reader import FormatVortexReader
        from pypaimon.schema.data_types import AtomicType, DataField

        path = os.path.join(self.temp_dir, 'd.vortex')
        vortex_utils.write_vortex_file(self.file_io, path, pa.table({'id': list(range(5))}))

        reader = FormatVortexReader(self.file_io, path, [DataField(0, 'id', AtomicType('BIGINT'))],
                                    None, batch_size=2)
        sizes = []
        values = []
        batch = reader.read_arrow_batch()
        while batch is not None:
            sizes.append(batch.num_rows)
            values.extend(batch.column(0).to_pylist())
            batch = reader.read_arrow_batch()
        reader.close()

        self.assertEqual(sizes, [2, 2, 1])
        self.assertEqual(values, [0, 1, 2, 3, 4])


if __name__ == '__main__':
    unittest.main()
