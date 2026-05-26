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
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.vector import Vector


class VectorClassTest(unittest.TestCase):

    def test_basic_operations(self):
        v = Vector([1.0, 2.0, 3.0])
        self.assertEqual(len(v), 3)
        self.assertEqual(v[0], 1.0)
        self.assertEqual(v[1], 2.0)
        self.assertEqual(v[2], 3.0)

    def test_to_list(self):
        v = Vector([1.0, 2.0, 3.0])
        self.assertEqual(v.to_list(), [1.0, 2.0, 3.0])

    def test_from_list(self):
        v = Vector.from_list([4.0, 5.0, 6.0])
        self.assertEqual(v.to_list(), [4.0, 5.0, 6.0])

    def test_equality(self):
        v1 = Vector([1.0, 2.0, 3.0])
        v2 = Vector([1.0, 2.0, 3.0])
        v3 = Vector([4.0, 5.0, 6.0])
        self.assertEqual(v1, v2)
        self.assertNotEqual(v1, v3)

    def test_hash(self):
        v1 = Vector([1.0, 2.0, 3.0])
        v2 = Vector([1.0, 2.0, 3.0])
        self.assertEqual(hash(v1), hash(v2))

    def test_str_repr(self):
        v = Vector([1.0, 2.0])
        self.assertEqual(str(v), "Vector([1.0, 2.0])")
        self.assertEqual(repr(v), "Vector([1.0, 2.0])")

    def test_integer_vector(self):
        v = Vector([1, 2, 3])
        self.assertEqual(v.to_list(), [1.0, 2.0, 3.0])

    def test_empty_vector(self):
        v = Vector([])
        self.assertEqual(len(v), 0)
        self.assertEqual(v.to_list(), [])


class VectorFileDetectionTest(unittest.TestCase):

    def test_is_vector_file(self):
        self.assertTrue(DataFileMeta.is_vector_file("data-uuid-0.vector.lance"))
        self.assertTrue(DataFileMeta.is_vector_file("data-uuid-0.vector.parquet"))
        self.assertFalse(DataFileMeta.is_vector_file("data-uuid-0.parquet"))
        self.assertFalse(DataFileMeta.is_vector_file("data-uuid-0.lance"))
        self.assertFalse(DataFileMeta.is_vector_file("data-uuid-0.blob"))


class VectorOnlyTableTest(unittest.TestCase):
    """Vector-only tables (no normal columns) must be rejected at schema creation."""

    def test_vector_only_table_rejected(self):
        pa_schema = pa.schema([
            ('embed1', pa.list_(pa.float32(), 3)),
            ('embed2', pa.list_(pa.float32(), 2)),
        ])
        with self.assertRaises(ValueError) as ctx:
            Schema.from_pyarrow_schema(
                pa_schema,
                options={
                    'vector.file.format': 'vortex',
                    'row-tracking.enabled': 'true',
                    'data-evolution.enabled': 'true',
                    'bucket': '-1',
                }
            )
        self.assertIn("must have other normal columns", str(ctx.exception))

    def test_vector_dedicated_missing_row_tracking_rejected(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])
        with self.assertRaises(ValueError) as ctx:
            Schema.from_pyarrow_schema(
                pa_schema,
                options={
                    'vector.file.format': 'vortex',
                    'data-evolution.enabled': 'true',
                    'bucket': '-1',
                }
            )
        self.assertIn("row-tracking.enabled", str(ctx.exception))


class VectorTableWriteReadTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('test_db', False)

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree(cls.temp_dir)
        except OSError:
            pass

    def test_inline_vector_write_read(self):
        """Write and read vector data stored inline (no vector.file.format)."""
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('test_db.inline_vector', schema, False)
        table = self.catalog.get_table('test_db.inline_vector')

        test_data = pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], type=pa.float32()),
                3
            ),
        })

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Read back
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 3)
        self.assertEqual(result.column('id').to_pylist(), [1, 2, 3])
        embed_col = result.column('embed')
        self.assertTrue(pa.types.is_fixed_size_list(embed_col.type))
        self.assertEqual(embed_col.type.list_size, 3)
        self.assertEqual(embed_col[0].as_py(), [1.0, 2.0, 3.0])
        self.assertEqual(embed_col[1].as_py(), [4.0, 5.0, 6.0])
        self.assertEqual(embed_col[2].as_py(), [7.0, 8.0, 9.0])

    def test_get_vector_row_access(self):
        """Test get_vector() returns Vector objects from InternalRow."""
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])

        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('test_db.row_vector', schema, False)
        table = self.catalog.get_table('test_db.row_vector')

        test_data = pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], type=pa.float32()),
                3
            ),
        })

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Read rows and use get_vector() — collect vectors eagerly since OffsetRow is reused
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        vectors = set()
        count = 0
        for row in read_builder.new_read().to_iterator(splits):
            vec = row.get_vector(1)
            self.assertIsInstance(vec, Vector)
            self.assertEqual(len(vec), 3)
            vectors.add(tuple(vec.to_list()))
            count += 1

        self.assertEqual(count, 2)
        self.assertIn((1.0, 2.0, 3.0), vectors)
        self.assertIn((4.0, 5.0, 6.0), vectors)

    @unittest.skipUnless(
        __import__('importlib').util.find_spec('vortex') is not None,
        "vortex not installed"
    )
    def test_vector_dedicated_format_write_read_vortex(self):
        """Write vector data to separate .vector.vortex files."""
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'file.format': 'vortex',
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'vector.file.format': 'vortex',
            }
        )

        self.catalog.create_table('test_db.dedicated_vector_vortex', schema, False)
        table = self.catalog.get_table('test_db.dedicated_vector_vortex')

        test_data = pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], type=pa.float32()),
                3
            ),
        })

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()

        # Verify file names: should have both normal files and .vector.vortex
        all_files = []
        for msg in commit_messages:
            all_files.extend(msg.new_files)

        normal_files = [f for f in all_files if not DataFileMeta.is_vector_file(f.file_name)]
        vector_files = [f for f in all_files if DataFileMeta.is_vector_file(f.file_name)]

        self.assertGreater(len(normal_files), 0, "Should have normal data files")
        self.assertGreater(len(vector_files), 0, "Should have vector files")
        for vf in vector_files:
            self.assertIn('.vector.vortex', vf.file_name)

        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Read back
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 3)
        ids = sorted(result.column('id').to_pylist())
        self.assertEqual(ids, [1, 2, 3])

        embed_by_id = {}
        for i in range(result.num_rows):
            embed_by_id[result.column('id')[i].as_py()] = result.column('embed')[i].as_py()
        self.assertEqual(embed_by_id[1], [1.0, 2.0, 3.0])
        self.assertEqual(embed_by_id[2], [4.0, 5.0, 6.0])
        self.assertEqual(embed_by_id[3], [7.0, 8.0, 9.0])

    @unittest.skipUnless(
        __import__('importlib').util.find_spec('lance') is not None,
        "lance not installed"
    )
    def test_vector_dedicated_format_write_read_lance(self):
        """Write vector data to separate .vector.lance files."""
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('embed', pa.list_(pa.float32(), 3)),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'vector.file.format': 'lance',
            }
        )

        self.catalog.create_table('test_db.dedicated_vector_lance', schema, False)
        table = self.catalog.get_table('test_db.dedicated_vector_lance')

        test_data = pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'embed': pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], type=pa.float32()),
                3
            ),
        })

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()

        all_files = []
        for msg in commit_messages:
            all_files.extend(msg.new_files)

        vector_files = [f for f in all_files if DataFileMeta.is_vector_file(f.file_name)]
        self.assertGreater(len(vector_files), 0, "Should have vector files")
        for vf in vector_files:
            self.assertIn('.vector.lance', vf.file_name)

        write_builder.new_commit().commit(commit_messages)
        writer.close()

        # Read back
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)

        self.assertEqual(result.num_rows, 3)


if __name__ == '__main__':
    unittest.main()
