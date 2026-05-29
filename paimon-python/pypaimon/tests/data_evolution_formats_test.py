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

"""Data evolution tests covering parquet + blob + vector (vortex) formats.

Each test writes data using different file format combinations and reads it
back, verifying correctness of the data evolution merge path across formats.
"""

import os
import shutil
import sys
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


class DataEvolutionFormatsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Parquet-format data evolution
    # ------------------------------------------------------------------

    def test_parquet_column_subset_write_and_merge_read(self):
        """Write disjoint column subsets as parquet, merge-read via data evolution."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('score', pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'parquet',
        })
        self.catalog.create_table('default.fmt_parquet_subset', schema, False)
        table = self.catalog.get_table('default.fmt_parquet_subset')
        wb = table.new_batch_write_builder()

        # commit 1: write id + name
        w0 = wb.new_write().with_write_type(['id', 'name'])
        w1 = wb.new_write().with_write_type(['score'])
        c = wb.new_commit()
        w0.write_arrow(pa.Table.from_pydict(
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c']},
            schema=pa.schema([('id', pa.int32()), ('name', pa.string())])))
        w1.write_arrow(pa.Table.from_pydict(
            {'score': [1.1, 2.2, 3.3]},
            schema=pa.schema([('score', pa.float64())])))
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # verify file format
        all_files = [nf for m in cmts for nf in m.new_files]
        for f in all_files:
            self.assertTrue(f.file_name.endswith('.parquet'),
                            f"Expected parquet file, got {f.file_name}")

        # read back
        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        expect = pa.Table.from_pydict(
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'score': [1.1, 2.2, 3.3]},
            schema=pa_schema)
        self.assertEqual(actual, expect)

    def test_parquet_overwrite_column(self):
        """Write all columns, then overwrite one column via a second commit."""
        pa_schema = pa.schema([
            ('k', pa.int64()),
            ('v', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'parquet',
        })
        self.catalog.create_table('default.fmt_parquet_overwrite', schema, False)
        table = self.catalog.get_table('default.fmt_parquet_overwrite')
        wb = table.new_batch_write_builder()

        # commit 1: full row
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'k': [10, 20], 'v': ['old1', 'old2']}, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: overwrite v only (first_row_id=0)
        tw = wb.new_write().with_write_type(['v'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'v': ['new1', 'new2']}, schema=pa.schema([('v', pa.string())])))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        expect = pa.Table.from_pydict(
            {'k': [10, 20], 'v': ['new1', 'new2']}, schema=pa_schema)
        self.assertEqual(actual, expect)

    def test_parquet_append_new_rows(self):
        """Append new rows (new first_row_id) with column subsets, merge-read all."""
        pa_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.string()),
            ('c', pa.float32()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'parquet',
        })
        self.catalog.create_table('default.fmt_parquet_append', schema, False)
        table = self.catalog.get_table('default.fmt_parquet_append')
        wb = table.new_batch_write_builder()

        # commit 1: 2 full rows
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'a': [1, 2], 'b': ['x', 'y'], 'c': [0.1, 0.2]}, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: append 2 new rows with column subsets, first_row_id=2
        w_ab = wb.new_write().with_write_type(['a', 'b'])
        w_c = wb.new_write().with_write_type(['c'])
        tc = wb.new_commit()
        w_ab.write_arrow(pa.Table.from_pydict(
            {'a': [3, 4], 'b': ['z', 'w']},
            schema=pa.schema([('a', pa.int32()), ('b', pa.string())])))
        w_c.write_arrow(pa.Table.from_pydict(
            {'c': [0.3, 0.4]},
            schema=pa.schema([('c', pa.float32())])))
        cmts = w_ab.prepare_commit() + w_c.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 2
        tc.commit(cmts)
        w_ab.close()
        w_c.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 4)
        expect = pa.Table.from_pydict(
            {'a': [1, 2, 3, 4], 'b': ['x', 'y', 'z', 'w'],
             'c': [0.1, 0.2, 0.3, 0.4]},
            schema=pa_schema)
        self.assertEqual(actual, expect)

    # ------------------------------------------------------------------
    # Blob-format data evolution
    # ------------------------------------------------------------------

    def test_blob_write_and_read(self):
        """Write a table with normal + blob columns, read back and verify."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_blob_basic', schema, False)
        table = self.catalog.get_table('default.fmt_blob_basic')
        wb = table.new_batch_write_builder()

        blobs = [b'hello world', b'\x00\x01\x02\xff', b'paimon blob']
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': [1, 2, 3], 'payload': blobs}, schema=pa_schema))
        cmts = tw.prepare_commit()
        tc.commit(cmts)
        tw.close()
        tc.close()

        # verify we produced both parquet and blob files
        all_files = [nf for m in cmts for nf in m.new_files]
        parquet_files = [f for f in all_files if f.file_name.endswith('.parquet')]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]
        self.assertGreater(len(parquet_files), 0)
        self.assertGreater(len(blob_files), 0)

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])
        self.assertEqual(actual.column('payload').to_pylist(), blobs)

    def test_blob_column_subset_evolution(self):
        """Write normal+blob cols in one commit, overwrite normal col in another, merge-read."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('doc', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_blob_evolution', schema, False)
        table = self.catalog.get_table('default.fmt_blob_evolution')
        wb = table.new_batch_write_builder()

        # commit 1: write id + doc (normal + blob together)
        tw = wb.new_write().with_write_type(['id', 'doc'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': [1, 2], 'doc': [b'doc_alice', b'doc_bob']},
            schema=pa.schema([('id', pa.int32()), ('doc', pa.large_binary())])))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: write name for the same rows (first_row_id=0)
        tw = wb.new_write().with_write_type(['name'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'name': ['Alice', 'Bob']},
            schema=pa.schema([('name', pa.string())])))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 2)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2])
        self.assertEqual(actual.column('name').to_pylist(), ['Alice', 'Bob'])
        self.assertEqual(actual.column('doc').to_pylist(), [b'doc_alice', b'doc_bob'])

    def test_blob_append_with_subset_evolution(self):
        """Write normal+blob subset in first commit, add remaining col via evolution."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('tag', pa.string()),
            ('picture', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_blob_append_evo', schema, False)
        table = self.catalog.get_table('default.fmt_blob_append_evo')
        wb = table.new_batch_write_builder()

        # commit 1: id + picture (normal + blob)
        tw = wb.new_write().with_write_type(['id', 'picture'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': [1, 2], 'picture': [b'pic1', b'pic2']},
            schema=pa.schema([('id', pa.int32()), ('picture', pa.large_binary())])))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: add tag for the same rows
        tw = wb.new_write().with_write_type(['tag'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'tag': ['t1', 't2']},
            schema=pa.schema([('tag', pa.string())])))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 2)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2])
        self.assertEqual(actual.column('tag').to_pylist(), ['t1', 't2'])
        self.assertEqual(actual.column('picture').to_pylist(), [b'pic1', b'pic2'])

    def test_blob_multiple_blob_columns(self):
        """Table with two blob columns, write and read both."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('audio', pa.large_binary()),
            ('video', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_blob_multi', schema, False)
        table = self.catalog.get_table('default.fmt_blob_multi')
        wb = table.new_batch_write_builder()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict({
            'id': [1, 2],
            'audio': [b'audio_1', b'audio_2'],
            'video': [b'video_1', b'video_2'],
        }, schema=pa_schema))
        cmts = tw.prepare_commit()
        tc.commit(cmts)
        tw.close()
        tc.close()

        # verify blob files were produced
        all_files = [nf for m in cmts for nf in m.new_files]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]
        self.assertGreaterEqual(len(blob_files), 2)

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 2)
        self.assertEqual(actual.column('audio').to_pylist(), [b'audio_1', b'audio_2'])
        self.assertEqual(actual.column('video').to_pylist(), [b'video_1', b'video_2'])

    # ------------------------------------------------------------------
    # Vortex-format data evolution
    # ------------------------------------------------------------------

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vortex_column_subset_write_and_merge_read(self):
        """Write disjoint column subsets as vortex, merge-read via data evolution."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('tag', pa.string()),
            ('val', pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vortex_subset', schema, False)
        table = self.catalog.get_table('default.fmt_vortex_subset')
        wb = table.new_batch_write_builder()

        w0 = wb.new_write().with_write_type(['id', 'tag'])
        w1 = wb.new_write().with_write_type(['val'])
        c = wb.new_commit()
        w0.write_arrow(pa.Table.from_pydict(
            {'id': [10, 20, 30], 'tag': ['p', 'q', 'r']},
            schema=pa.schema([('id', pa.int32()), ('tag', pa.string())])))
        w1.write_arrow(pa.Table.from_pydict(
            {'val': [1.5, 2.5, 3.5]},
            schema=pa.schema([('val', pa.float64())])))
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        # verify vortex files
        all_files = [nf for m in cmts for nf in m.new_files]
        for f in all_files:
            self.assertTrue(f.file_name.endswith('.vortex'),
                            f"Expected vortex file, got {f.file_name}")

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        expect = pa.Table.from_pydict(
            {'id': [10, 20, 30], 'tag': ['p', 'q', 'r'], 'val': [1.5, 2.5, 3.5]},
            schema=pa_schema)
        self.assertEqual(actual, expect)

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vortex_overwrite_column(self):
        """Full row write then overwrite one column, all in vortex format."""
        pa_schema = pa.schema([
            ('k', pa.int64()),
            ('v', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vortex_overwrite', schema, False)
        table = self.catalog.get_table('default.fmt_vortex_overwrite')
        wb = table.new_batch_write_builder()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'k': [100, 200], 'v': ['old', 'old']}, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        tw = wb.new_write().with_write_type(['v'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'v': ['new', 'new']}, schema=pa.schema([('v', pa.string())])))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual, pa.Table.from_pydict(
            {'k': [100, 200], 'v': ['new', 'new']}, schema=pa_schema))

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vortex_append_new_rows(self):
        """Append new rows with column subsets in vortex format."""
        pa_schema = pa.schema([
            ('x', pa.int32()),
            ('y', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vortex_append', schema, False)
        table = self.catalog.get_table('default.fmt_vortex_append')
        wb = table.new_batch_write_builder()

        # commit 1
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'x': [1, 2], 'y': ['a', 'b']}, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: append with subsets, first_row_id=2
        w_x = wb.new_write().with_write_type(['x'])
        w_y = wb.new_write().with_write_type(['y'])
        tc = wb.new_commit()
        w_x.write_arrow(pa.Table.from_pydict(
            {'x': [3]}, schema=pa.schema([('x', pa.int32())])))
        w_y.write_arrow(pa.Table.from_pydict(
            {'y': ['c']}, schema=pa.schema([('y', pa.string())])))
        cmts = w_x.prepare_commit() + w_y.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 2
        tc.commit(cmts)
        w_x.close()
        w_y.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        expect = pa.Table.from_pydict(
            {'x': [1, 2, 3], 'y': ['a', 'b', 'c']}, schema=pa_schema)
        self.assertEqual(actual, expect)

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vortex_with_row_id_and_filter(self):
        """Write vortex data, read with _ROW_ID projection and filter."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('val', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vortex_rowid_filter', schema, False)
        table = self.catalog.get_table('default.fmt_vortex_rowid_filter')
        wb = table.new_batch_write_builder()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': list(range(10)), 'val': [f'v{i}' for i in range(10)]},
            schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # full read
        rb = table.new_read_builder()
        full = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(full.num_rows, 10)

        # filter by _ROW_ID
        rb_rid = table.new_read_builder().with_projection(['id', 'val', '_ROW_ID'])
        pb = rb_rid.new_predicate_builder()
        rb_f = table.new_read_builder().with_filter(pb.equal('_ROW_ID', 5))
        actual = rb_f.new_read().to_arrow(rb_f.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 1)
        self.assertEqual(actual.column('id')[0].as_py(), 5)
        self.assertEqual(actual.column('val')[0].as_py(), 'v5')

    # ------------------------------------------------------------------
    # Vector (vortex) file format for embedding columns
    # ------------------------------------------------------------------

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vector_vortex_write_and_read(self):
        """Write table with normal + vector columns using vortex vector format."""
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('embed', pa.list_(pa.float32(), 4)),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
            'vector.file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vec_vortex', schema, False)
        table = self.catalog.get_table('default.fmt_vec_vortex')

        embeddings = [1.0, 0.0, 0.0, 0.0,
                      0.0, 1.0, 0.0, 0.0,
                      0.0, 0.0, 1.0, 0.0]
        test_data = pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array(embeddings, type=pa.float32()), 4),
        })

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_arrow(test_data)
        cmts = tw.prepare_commit()

        # should produce both normal and vector files
        all_files = [nf for m in cmts for nf in m.new_files]
        normal_files = [f for f in all_files if not DataFileMeta.is_vector_file(f.file_name)]
        vector_files = [f for f in all_files if DataFileMeta.is_vector_file(f.file_name)]
        self.assertGreater(len(normal_files), 0)
        self.assertGreater(len(vector_files), 0)
        for vf in vector_files:
            self.assertIn('.vector.vortex', vf.file_name)

        wb.new_commit().commit(cmts)
        tw.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])
        embed_col = actual.column('embed')
        self.assertEqual(embed_col[0].as_py(), [1.0, 0.0, 0.0, 0.0])
        self.assertEqual(embed_col[1].as_py(), [0.0, 1.0, 0.0, 0.0])
        self.assertEqual(embed_col[2].as_py(), [0.0, 0.0, 1.0, 0.0])

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vector_vortex_multiple_appends(self):
        """Append multiple batches of normal+vector data and read all back."""
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('label', pa.string()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
            'vector.file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vec_vortex_append', schema, False)
        table = self.catalog.get_table('default.fmt_vec_vortex_append')
        wb = table.new_batch_write_builder()

        # commit 1
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'label': pa.array(['cat', 'dog']),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], type=pa.float32()), 3),
        }))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: append
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([3], type=pa.int64()),
            'label': pa.array(['bird']),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([0.7, 0.8, 0.9], type=pa.float32()), 3),
        }))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])
        self.assertEqual(actual.column('label').to_pylist(), ['cat', 'dog', 'bird'])
        embed_col = actual.column('embed')
        self.assertAlmostEqual(embed_col[0].as_py()[0], 0.1, places=5)
        self.assertAlmostEqual(embed_col[2].as_py()[2], 0.9, places=5)

    # ------------------------------------------------------------------
    # Mixed formats: parquet + blob + vector in one table
    # ------------------------------------------------------------------

    def test_parquet_and_blob_mixed_append(self):
        """Table with normal parquet cols + blob col, append new rows."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('image', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_mixed_parquet_blob', schema, False)
        table = self.catalog.get_table('default.fmt_mixed_parquet_blob')
        wb = table.new_batch_write_builder()

        # commit 1: first batch
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['a', 'b'],
            'image': [b'img1', b'img2'],
        }, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: append more rows
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['c', 'd'],
            'image': [b'img3', b'img4'],
        }, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 4)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3, 4])
        self.assertEqual(actual.column('name').to_pylist(), ['a', 'b', 'c', 'd'])
        self.assertEqual(actual.column('image').to_pylist(),
                         [b'img1', b'img2', b'img3', b'img4'])

    @unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed")
    def test_vortex_and_vector_vortex_mixed(self):
        """Table with normal (vortex) + vector (vortex) columns, write and read.

        Verifies that the writer produces separate .vortex and .vector.vortex files,
        and the data evolution merge reader stitches them back together.
        """
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'vortex',
            'vector.file.format': 'vortex',
        })
        self.catalog.create_table('default.fmt_vortex_vector', schema, False)
        table = self.catalog.get_table('default.fmt_vortex_vector')
        wb = table.new_batch_write_builder()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'name': pa.array(['cat', 'dog', 'bird']),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
                         type=pa.float32()), 3),
        }))
        cmts = tw.prepare_commit()
        tc.commit(cmts)
        tw.close()
        tc.close()

        # verify two file types: .vortex + .vector.vortex
        all_files = [nf for m in cmts for nf in m.new_files]
        normal_files = [f for f in all_files if not DataFileMeta.is_vector_file(f.file_name)]
        vector_files = [f for f in all_files if DataFileMeta.is_vector_file(f.file_name)]
        self.assertGreater(len(normal_files), 0, "should produce normal vortex files")
        self.assertGreater(len(vector_files), 0, "should produce vector files")
        for nf in normal_files:
            self.assertTrue(nf.file_name.endswith('.vortex'))
        for vf in vector_files:
            self.assertIn('.vector.vortex', vf.file_name)

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])
        self.assertEqual(actual.column('name').to_pylist(), ['cat', 'dog', 'bird'])
        embed = actual.column('embed')
        self.assertAlmostEqual(embed[0].as_py()[0], 0.1, places=5)
        self.assertAlmostEqual(embed[2].as_py()[2], 0.9, places=5)

    def test_blob_and_vector_inline_mixed(self):
        """Table with normal + blob + vector(inline) columns, write and read.

        When blob columns are present, vector columns are stored inline in the
        parquet file (not as separate .vector files). This test verifies the
        blob+inline-vector path works correctly.
        """
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('doc', pa.large_binary()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_blob_vector_inline', schema, False)
        table = self.catalog.get_table('default.fmt_blob_vector_inline')
        wb = table.new_batch_write_builder()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'doc': pa.array([b'doc1', b'doc2'], type=pa.large_binary()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], type=pa.float32()), 3),
        }))
        cmts = tw.prepare_commit()
        tc.commit(cmts)
        tw.close()
        tc.close()

        # verify parquet + blob files
        all_files = [nf for m in cmts for nf in m.new_files]
        parquet_files = [f for f in all_files if f.file_name.endswith('.parquet')]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]
        self.assertGreater(len(parquet_files), 0, "should produce parquet files")
        self.assertGreater(len(blob_files), 0, "should produce blob files")

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 2)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2])
        self.assertEqual(actual.column('doc').to_pylist(), [b'doc1', b'doc2'])
        embed = actual.column('embed')
        self.assertAlmostEqual(embed[0].as_py()[0], 0.1, places=5)
        self.assertAlmostEqual(embed[1].as_py()[2], 0.6, places=5)

    def test_blob_and_vector_with_vector_file_format(self):
        """Table with blob + vector columns and explicit vector.file.format.

        DedicatedFormatWriter splits data three ways: normal columns to .parquet,
        blob columns to .blob, and vector columns to .vector.<format> files.
        """
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('doc', pa.large_binary()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'vector.file.format': 'parquet',
        })
        self.catalog.create_table('default.fmt_blob_vec_format', schema, False)
        table = self.catalog.get_table('default.fmt_blob_vec_format')
        wb = table.new_batch_write_builder()

        # commit 1: write all columns
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'doc': pa.array([b'aaa', b'bbb', b'ccc'], type=pa.large_binary()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 0.0, 0.0,
                          0.0, 1.0, 0.0,
                          0.0, 0.0, 1.0], type=pa.float32()), 3),
        }))
        cmts = tw.prepare_commit()
        tc.commit(cmts)
        tw.close()
        tc.close()

        # DedicatedFormatWriter produces parquet + blob + vector files
        all_files = [nf for m in cmts for nf in m.new_files]
        parquet_files = [f for f in all_files
                         if f.file_name.endswith('.parquet')
                         and not DataFileMeta.is_vector_file(f.file_name)]
        blob_files = [f for f in all_files if f.file_name.endswith('.blob')]
        vector_files = [f for f in all_files if DataFileMeta.is_vector_file(f.file_name)]
        self.assertGreater(len(parquet_files), 0, "should produce normal parquet files")
        self.assertGreater(len(blob_files), 0, "should produce blob files")
        self.assertGreater(len(vector_files), 0, "should produce vector files")
        for vf in vector_files:
            self.assertIn('.vector.parquet', vf.file_name)

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])
        self.assertEqual(actual.column('doc').to_pylist(), [b'aaa', b'bbb', b'ccc'])
        self.assertEqual(actual.column('embed')[0].as_py(), [1.0, 0.0, 0.0])
        self.assertEqual(actual.column('embed')[1].as_py(), [0.0, 1.0, 0.0])
        self.assertEqual(actual.column('embed')[2].as_py(), [0.0, 0.0, 1.0])

        # commit 2: append more rows
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([4, 5], type=pa.int64()),
            'doc': pa.array([b'ddd', b'eee'], type=pa.large_binary()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([0.5, 0.5, 0.0,
                          0.0, 0.5, 0.5], type=pa.float32()), 3),
        }))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        actual2 = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual2.num_rows, 5)
        self.assertEqual(actual2.column('id').to_pylist(), [1, 2, 3, 4, 5])
        self.assertEqual(actual2.column('doc').to_pylist(),
                         [b'aaa', b'bbb', b'ccc', b'ddd', b'eee'])

    def test_blob_vector_partial_write_vector_only(self):
        """Blob+vector table with with_write_type(['embed']) — vector-only partial write.

        When normal_column_names is empty, the writer must still flush vector
        metadata without crashing on an empty normal data path.
        """
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('doc', pa.large_binary()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'vector.file.format': 'parquet',
        })
        self.catalog.create_table('default.fmt_blob_vec_partial', schema, False)
        table = self.catalog.get_table('default.fmt_blob_vec_partial')
        wb = table.new_batch_write_builder()

        # commit 1: write all columns
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'doc': pa.array([b'aaa', b'bbb', b'ccc'], type=pa.large_binary()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 0.0, 0.0,
                          0.0, 1.0, 0.0,
                          0.0, 0.0, 1.0], type=pa.float32()), 3),
        }))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        # commit 2: write only vector column — no normal columns
        tw = wb.new_write().with_write_type(['embed'])
        tc = wb.new_commit()
        tw.write_arrow(pa.table({
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([0.5, 0.5, 0.0,
                          0.0, 0.5, 0.5,
                          0.5, 0.0, 0.5], type=pa.float32()), 3),
        }))
        cmts = tw.prepare_commit()

        # should produce only vector files, no normal or blob files
        all_files = [nf for m in cmts for nf in m.new_files]
        self.assertGreater(len(all_files), 0, "should produce vector files")
        for f in all_files:
            self.assertTrue(DataFileMeta.is_vector_file(f.file_name),
                            f"Expected vector file, got {f.file_name}")

        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        # read back and verify the vector column was updated
        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 3)
        self.assertEqual(actual.column('id').to_pylist(), [1, 2, 3])
        self.assertEqual(actual.column('doc').to_pylist(), [b'aaa', b'bbb', b'ccc'])
        embed = actual.column('embed')
        self.assertEqual(embed[0].as_py(), [0.5, 0.5, 0.0])
        self.assertEqual(embed[1].as_py(), [0.0, 0.5, 0.5])
        self.assertEqual(embed[2].as_py(), [0.5, 0.0, 0.5])

    # ------------------------------------------------------------------
    # Projection and _ROW_ID across formats
    # ------------------------------------------------------------------

    def test_blob_with_row_id_projection(self):
        """Read blob table with _ROW_ID projection."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('data', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        self.catalog.create_table('default.fmt_blob_rowid', schema, False)
        table = self.catalog.get_table('default.fmt_blob_rowid')
        wb = table.new_batch_write_builder()

        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': [10, 20], 'data': [b'aa', b'bb']}, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder()
        rb.with_projection(['id', 'data', '_ROW_ID'])
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, 2)
        self.assertEqual(actual.column('_ROW_ID').to_pylist(), [0, 1])
        self.assertEqual(actual.column('id').to_pylist(), [10, 20])
        self.assertEqual(actual.column('data').to_pylist(), [b'aa', b'bb'])

    def test_parquet_large_data_evolution(self):
        """Larger dataset: 1000 rows, column-subset write+merge."""
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('col_a', pa.string()),
            ('col_b', pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'parquet',
        })
        self.catalog.create_table('default.fmt_parquet_large', schema, False)
        table = self.catalog.get_table('default.fmt_parquet_large')
        wb = table.new_batch_write_builder()

        n = 1000
        w0 = wb.new_write().with_write_type(['id', 'col_a'])
        w1 = wb.new_write().with_write_type(['col_b'])
        c = wb.new_commit()
        w0.write_arrow(pa.Table.from_pydict(
            {'id': list(range(n)), 'col_a': [f's{i}' for i in range(n)]},
            schema=pa.schema([('id', pa.int32()), ('col_a', pa.string())])))
        w1.write_arrow(pa.Table.from_pydict(
            {'col_b': [float(i) for i in range(n)]},
            schema=pa.schema([('col_b', pa.float64())])))
        cmts = w0.prepare_commit() + w1.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        c.commit(cmts)
        w0.close()
        w1.close()
        c.close()

        rb = table.new_read_builder()
        actual = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(actual.num_rows, n)
        self.assertEqual(actual.column('id').to_pylist(), list(range(n)))
        self.assertEqual(actual.column('col_a').to_pylist(), [f's{i}' for i in range(n)])
        self.assertEqual(actual.column('col_b').to_pylist(), [float(i) for i in range(n)])


if __name__ == '__main__':
    unittest.main()
