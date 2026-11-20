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
import struct
import tempfile
import unittest
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write.native_commit import native_messages_supported
from pypaimon.execution.python.globalindex.build_plan import indexed_row_ranges
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta, has_centroid_scalar_index
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage


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

    def _entry(self, file_name, field_id, meta=b'm', source_meta=None):
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
                source_meta=source_meta,
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

    def test_write_read_primary_key_source_meta(self):
        source_meta = b'primary-key-source-meta'
        imf = IndexManifestFile(self._table())
        name = imf.write([self._entry('idx-pk', 1, source_meta=source_meta)])

        result = imf.read(name)[0].index_file.global_index_meta

        self.assertEqual(source_meta, bytes(result.source_meta))

    def test_combine_drops_named_files(self):
        imf = IndexManifestFile(self._table())
        previous = imf.write([self._entry('idx-a', 1), self._entry('idx-b', 2)])
        deletes = [self._entry('idx-a', 1)]
        new_name = imf.combine_deletes(previous, deletes)
        self.assertNotEqual(previous, new_name)
        survivors = {e.index_file.file_name for e in imf.read(new_name)}
        self.assertEqual({'idx-b'}, survivors)

    def test_centroid_scalar_group_owns_vector_range(self):
        imf = IndexManifestFile(self._table())
        router = self._entry('centroid-router', 1, b'PAIMCENT\x02\x01' + struct.pack('>iii', 1, 1, 0) + b'm')
        router.index_file.index_type = 'ivf-flat'
        previous = imf.write([router])
        range_entry = self._entry('vector-range', 1)
        range_entry.index_file.index_type = 'ivf-flat'
        range_entry.index_file.global_index_meta.extra_field_ids = None
        with self.assertRaisesRegex(RuntimeError, 'overlapping row range'):
            imf.combine_changes(previous, [range_entry], [])
        range_entry.index_file.index_type = 'ivf-pq'
        combined = imf.combine_changes(previous, [range_entry], [])
        self.assertEqual({'ivf-flat', 'ivf-pq'}, {entry.index_file.index_type for entry in imf.read(combined)})

    def test_python_commit_rejects_updates_to_centroid_bitmap_fields_under_ignore(self):
        name = f'default.centroid_{uuid.uuid4().hex[:8]}'
        schema = Schema.from_pyarrow_schema(self.pa_schema, options={
            'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'global-index.column-update-action': 'IGNORE',
        })
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)
        builder = table.new_batch_write_builder()

        def publish(messages):
            commit = builder.new_commit()
            try:
                commit.commit(messages)
            finally:
                commit.close()

        writer = builder.new_write()
        try:
            writer.write_arrow(pa.table({'id': [7], 'vec': ['before']}, schema=self.pa_schema))
            publish(writer.prepare_commit())
        finally:
            writer.close()

        field_id, extra_id = (field.id for field in table.fields)
        router = self._entry('centroid-router', field_id,
                             b'PAIMCENT\x02\x01' + struct.pack('>iii', 1, 1, 0) + b'm')
        router.index_file.index_type = 'ivf-flat'
        router.index_file.global_index_meta.row_range_end = 0
        router.index_file.global_index_meta.extra_field_ids = [extra_id]
        index_message = CommitMessage((), 0, [], index_adds=[router])
        self.assertFalse(native_messages_supported(table, [index_message], for_commit=True))
        publish([index_message])
        snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertTrue(has_centroid_scalar_index(table, 'ivf-flat', 'id', None))
        ranges = indexed_row_ranges(table, snapshot, None, field_id, 'ivf-flat')
        self.assertEqual([(0, 0)], [(r.from_, r.to) for r in ranges])

        update = builder.new_update().new_update_by_row_id()
        messages = update.update_columns(pa.table({'_ROW_ID': [0], 'vec': ['after']}), ['vec'])
        self.assertTrue(messages)
        with self.assertRaisesRegex(RuntimeError, 'active centroid index'):
            publish(messages)
        self.assertEqual(snapshot.id, table.snapshot_manager().get_latest_snapshot().id)
        self.assertEqual(1, table.drop_global_index('id', index_type='ivf-flat', dry_run=True))
        self.assertEqual(1, table.drop_global_index('id', index_type='ivf-flat'))
        self.assertFalse(has_centroid_scalar_index(table, 'ivf-flat', 'id', None))

    def test_combine_unknown_global_index_delete_is_rejected(self):
        imf = IndexManifestFile(self._table())
        previous = imf.write([self._entry('idx-a', 1)])
        with self.assertRaisesRegex(RuntimeError, 'does not exist'):
            imf.combine_deletes(previous, [self._entry('idx-zzz', 9)])
        self.assertEqual(['idx-a'], [e.index_file.file_name for e in imf.read(previous)])

    def test_combine_global_index_delete_retains_other_type_with_same_name(self):
        imf = IndexManifestFile(self._table())
        btree = self._entry('shared-name', 1)
        bitmap = self._entry('shared-name', 1)
        bitmap.index_file.index_type = 'bitmap'
        previous = imf.write([btree, bitmap])
        current = imf.combine_deletes(previous, [btree])
        self.assertEqual(['bitmap'], [e.index_file.index_type for e in imf.read(current)])

    def test_combine_hash_index_delete_keeps_bucketed_stale_delete_semantics(self):
        imf = IndexManifestFile(self._table())
        current = self._entry('current-hash', 1)
        current.index_file.index_type = 'HASH'
        current.index_file.global_index_meta = None
        previous = imf.write([current])
        stale = self._entry('old-hash', 1)
        stale.index_file.index_type = 'HASH'
        stale.index_file.global_index_meta = None
        self.assertIsNone(imf.combine_deletes(previous, [stale]))

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
