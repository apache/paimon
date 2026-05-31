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
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.table.row.generic_row import GenericRow


class IndexManifestWriteTest(unittest.TestCase):

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('vec', pa.string()),
    ])

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _table(self):
        name = f'default.idx_{uuid.uuid4().hex[:8]}'
        s = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table(name, s, False)
        return self.catalog.get_table(name)

    def _entry(self, file_name, field_id, meta=b'm'):
        partition = GenericRow([], [])
        index_file = IndexFileMeta(
            index_type='BTREE',
            file_name=file_name,
            file_size=123,
            row_count=10,
            global_index_meta=GlobalIndexMeta(
                row_range_start=0,
                row_range_end=10,
                index_field_id=field_id,
                extra_field_ids=[field_id + 1],
                index_meta=meta,
            ),
        )
        return IndexManifestEntry(kind=0, partition=partition, bucket=0, index_file=index_file)

    def test_write_read_roundtrip(self):
        imf = IndexManifestFile(self._table())
        name = imf.write([self._entry('idx-a', 1), self._entry('idx-b', 2)])
        out = imf.read(name)
        self.assertEqual(2, len(out))
        by_name = {e.index_file.file_name: e for e in out}
        a = by_name['idx-a']
        self.assertEqual('BTREE', a.index_file.index_type)
        self.assertEqual(123, a.index_file.file_size)
        self.assertEqual(10, a.index_file.row_count)
        self.assertEqual(0, a.kind)
        gim = a.index_file.global_index_meta
        self.assertEqual(1, gim.index_field_id)
        self.assertEqual(0, gim.row_range_start)
        self.assertEqual(10, gim.row_range_end)
        self.assertEqual([2], gim.extra_field_ids)
        self.assertEqual(b'm', bytes(gim.index_meta))

    def test_combine_drops_named_files(self):
        imf = IndexManifestFile(self._table())
        previous = imf.write([self._entry('idx-a', 1), self._entry('idx-b', 2)])
        deletes = [self._entry('idx-a', 1)]
        new_name = imf.combine_deletes(previous, deletes)
        self.assertNotEqual(previous, new_name)
        survivors = {e.index_file.file_name for e in imf.read(new_name)}
        self.assertEqual({'idx-b'}, survivors)

    def test_combine_unknown_delete_is_noop_on_content(self):
        imf = IndexManifestFile(self._table())
        previous = imf.write([self._entry('idx-a', 1)])
        new_name = imf.combine_deletes(previous, [self._entry('idx-zzz', 9)])
        survivors = {e.index_file.file_name for e in imf.read(new_name)}
        self.assertEqual({'idx-a'}, survivors)

    def test_combine_empty_deletes_returns_previous(self):
        imf = IndexManifestFile(self._table())
        previous = imf.write([self._entry('idx-a', 1)])
        self.assertEqual(previous, imf.combine_deletes(previous, []))

    def test_combine_all_deleted_returns_none(self):
        imf = IndexManifestFile(self._table())
        previous = imf.write([self._entry('idx-a', 1)])
        self.assertIsNone(imf.combine_deletes(previous, [self._entry('idx-a', 1)]))


if __name__ == '__main__':
    unittest.main()
