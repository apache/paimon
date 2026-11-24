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
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.offset_row import OffsetRow
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

    def test_offset_row_legacy_positional_vector_indices(self):
        row = OffsetRow(([1.0, 2.0],), 0, 1, None, None, {0})

        self.assertEqual(row.get_vector(0).to_list(), [1.0, 2.0])
        self.assertEqual(row._descriptor_field_indices, frozenset())


class VectorFileDetectionTest(unittest.TestCase):

    def test_is_vector_file(self):
        self.assertTrue(DataFileMeta.is_vector_file("data-uuid-0.vector.vortex"))
        self.assertTrue(DataFileMeta.is_vector_file("data-uuid-0.vector.parquet"))
        self.assertFalse(DataFileMeta.is_vector_file("data-uuid-0.parquet"))
        self.assertFalse(DataFileMeta.is_vector_file("data-uuid-0.vortex"))
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

    @pytest.mark.python_plan
    @pytest.mark.python_write
    @pytest.mark.python_commit
    def test_vector_table_partial_update_shared_vector_file(self):
        from pypaimon.utils.range import Range

        pytest.importorskip('vortex')
        vector_type = pa.list_(pa.float32(), 3)
        initial = pa.table({
            'id': pa.array([1, 2], type=pa.int32()),
            'embedding': pa.array([[1., 0., 0.], [0., 1., 0.]], type=vector_type),
            'embedding_v2': pa.array([[0., 1., 0.], [0., 0., 1.]], type=vector_type),
        })
        table_name = 'test_db.partial_update_shared_vector_file'
        schema = Schema.from_pyarrow_schema(initial.schema, options={
            'bucket': '-1',
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'vector.file.format': 'vortex',
        })
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        def read(projection, row_ranges=None):
            builder = table.new_read_builder().with_projection(projection)
            scan = builder.new_scan()
            if row_ranges is not None:
                scan = scan.with_row_ranges(row_ranges)
            return builder.new_read().to_arrow(scan.plan().splits())

        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(initial)
        messages = writer.prepare_commit()
        self.assertEqual([['embedding', 'embedding_v2']], [
            file.write_cols for message in messages for file in message.new_files
            if DataFileMeta.is_vector_file(file.file_name)
        ])
        commit = builder.new_commit()
        commit.commit(messages)
        writer.close()
        commit.close()

        before = read(initial.column_names + ['_ROW_ID']).sort_by('id')
        self.assertEqual(initial.to_pydict(), before.select(initial.column_names).to_pydict())
        row_id = before['_ROW_ID'][0].as_py()

        updates = pa.table({
            '_ROW_ID': pa.array([row_id], type=pa.int64()),
            'embedding': pa.array([[0., 0., 1.]], type=vector_type),
        })
        builder = table.new_batch_write_builder()
        updater = builder.new_update().with_update_type(['embedding'])
        messages = updater.update_by_arrow_with_row_id(updates)
        self.assertEqual([['embedding']], [
            file.write_cols for message in messages for file in message.new_files
            if DataFileMeta.is_vector_file(file.file_name)
        ])
        commit = builder.new_commit()
        commit.commit(messages)
        commit.close()

        table = self.catalog.get_table(table_name)
        expected = {
            'embedding': [[0., 0., 1.], [0., 1., 0.]],
            'embedding_v2': initial['embedding_v2'].to_pylist(),
        }
        for projection in (['embedding'], ['embedding_v2'], ['embedding', 'embedding_v2']):
            with self.subTest(projection=projection):
                self.assertEqual({name: expected[name] for name in projection},
                                 read(projection).to_pydict())
        self.assertEqual({name: values[:1] for name, values in expected.items()},
                         read(list(expected), [Range(row_id, row_id)]).to_pydict())

        # The newer file starts at the same row ID but covers only the first row.
        second_row_id = before['_ROW_ID'][1].as_py()
        builder = table.new_batch_write_builder()
        writer = builder.new_write().with_write_type(['embedding'])
        writer.write_arrow(pa.table({'embedding': pa.array([None], type=vector_type)}))
        messages = writer.prepare_commit()
        files = [file for message in messages for file in message.new_files]
        self.assertEqual(1, len(files))
        self.assertTrue(DataFileMeta.is_vector_file(files[0].file_name))
        files[0].first_row_id = row_id
        commit = builder.new_commit()
        commit.commit(messages)
        writer.close()
        commit.close()

        table = self.catalog.get_table(table_name)
        expected['embedding'][0] = None
        self.assertEqual(dict(expected, _ROW_ID=[row_id, second_row_id]),
                         read(['embedding_v2', '_ROW_ID', 'embedding']).to_pydict())

    @pytest.mark.python_plan
    @pytest.mark.python_write
    @pytest.mark.python_commit
    def test_vector_updates_across_inline_and_dedicated_files(self):
        from pypaimon.schema.schema_change import SchemaChange

        pytest.importorskip('vortex')
        vector_type = pa.list_(pa.float32(), 2)
        initial = pa.table({
            'id': pa.array([0], type=pa.int32()),
            'embedding': pa.array([[1., 1.]], type=vector_type),
            'embedding_v2': pa.array([[4., 4.]], type=vector_type),
        })
        table_name = 'test_db.vector_storage_change'
        schema = Schema.from_pyarrow_schema(initial.schema, options={
            'bucket': '-1',
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'vector.file.format': 'vortex',
        })
        self.catalog.create_table(table_name, schema, False)
        self._write_vector_batches(self.catalog.get_table(table_name), initial)

        for change, value in (
                (SchemaChange.remove_option('vector.file.format'), 2.),
                (SchemaChange.set_option('vector.file.format', 'vortex'), 3.)):
            self.catalog.alter_table(table_name, [change], False)
            table = self.catalog.get_table(table_name)
            builder = table.new_batch_write_builder()
            messages = builder.new_update().with_update_type(['embedding']).update_by_arrow_with_row_id(pa.table({
                '_ROW_ID': pa.array([0], type=pa.int64()),
                'embedding': pa.array([[value, value]], type=vector_type),
            }))
            commit = builder.new_commit()
            commit.commit(messages)
            commit.close()

        table = self.catalog.get_table(table_name)
        for projection in (['embedding'], initial.column_names):
            builder = table.new_read_builder().with_projection(projection)
            result = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pydict()
            expected = initial.select(projection).to_pydict()
            expected['embedding'] = [[3., 3.]]
            self.assertEqual(expected, result)

    @pytest.mark.python_plan
    @pytest.mark.python_write
    @pytest.mark.python_commit
    def test_null_predicates_after_switching_to_dedicated_vector_files(self):
        from pypaimon.schema.schema_change import SchemaChange

        pytest.importorskip('vortex')
        vector_type = pa.list_(pa.float32(), 2)
        for initial_value, updated_value in ((None, [3., 4.]), ([1., 2.], None)):
            with self.subTest(initial_value=initial_value, updated_value=updated_value):
                table_name = 'test_db.vector_null_filter_' + ('was_null' if initial_value is None else 'was_set')
                initial = pa.table({
                    'id': pa.array([0], type=pa.int32()),
                    'embedding': pa.array([initial_value], type=vector_type),
                })
                schema = Schema.from_pyarrow_schema(initial.schema, options={
                    'bucket': '-1',
                    'row-tracking.enabled': 'true',
                    'data-evolution.enabled': 'true',
                    'file.format': 'parquet',
                })
                self.catalog.create_table(table_name, schema, False)
                self._write_vector_batches(self.catalog.get_table(table_name), initial)
                self.catalog.alter_table(
                    table_name, [SchemaChange.set_option('vector.file.format', 'vortex')], False)
                table = self.catalog.get_table(table_name)
                builder = table.new_batch_write_builder()
                update = pa.table({
                    '_ROW_ID': pa.array([0], type=pa.int64()),
                    'embedding': pa.array([updated_value], type=vector_type),
                })
                messages = builder.new_update().with_update_type(['embedding']).update_by_arrow_with_row_id(update)
                commit = builder.new_commit()
                commit.commit(messages)
                commit.close()

                table = self.catalog.get_table(table_name)
                reader = table.new_read_builder()
                expected = {'id': [0], 'embedding': [updated_value]}
                self.assertEqual(expected, reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pydict())
                predicate_builder = reader.new_predicate_builder()
                predicate = (predicate_builder.is_null('embedding') if updated_value is None
                             else predicate_builder.is_not_null('embedding'))
                reader = reader.with_filter(predicate)
                splits = reader.new_scan().plan().splits()
                self.assertTrue(splits)
                self.assertEqual(expected, reader.new_read().to_arrow(splits).to_pydict())

    @pytest.mark.python_plan
    @pytest.mark.python_write
    @pytest.mark.python_commit
    def test_rolled_vector_updates(self):
        vortex = pytest.importorskip('vortex')
        cases = [
            ('1KB', '1KB', 80, False), ('1MB', '1KB', 80, False), ('1MB', '1KB', 72, False),
            ('1MB', '1KB', 80, True), ('1MB', '1KB', 72, True), ('1KB', '2KB', 64, True),
        ]
        for case in cases:
            with self.subTest(layout=case):
                self._assert_rolled_vector_update(vortex, *case)

    @pytest.mark.python_plan
    @pytest.mark.python_write
    @pytest.mark.python_commit
    def test_rolled_update_of_shared_vector_file(self):
        vortex = pytest.importorskip('vortex')
        for updated_row_count in (80, 72):
            with self.subTest(updated_row_count=updated_row_count):
                self._assert_rolled_vector_update(
                    vortex, '1MB', '1KB', updated_row_count, False, shared_vector_file=True)

    @pytest.mark.python_plan
    @pytest.mark.python_write
    @pytest.mark.python_commit
    def test_rolled_vector_backfill_decodes_parquet_anchor_once(self):
        from pypaimon.execution.python.read.reader.format_pyarrow_reader import FormatPyArrowReader
        from pypaimon.schema.data_types import AtomicType, VectorType
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.utils.range import Range

        pytest.importorskip('vortex')
        table_name = 'test_db.partial_rolled_vector_backfill'
        initial = pa.table({'id': pa.array(range(80), type=pa.int32())})
        schema = Schema.from_pyarrow_schema(initial.schema, options={
            'bucket': '-1',
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'parquet',
            'vector.file.format': 'vortex',
            'vector.target-file-size': '1KB',
            'parquet.filter.columnindex.enabled': 'false',
        })
        self.catalog.create_table(table_name, schema, False)
        self._write_vector_batches(self.catalog.get_table(table_name), initial)
        self.catalog.alter_table(table_name, [
            SchemaChange.add_column('embedding', VectorType(True, AtomicType('FLOAT'), 32))
        ], False)
        vectors = [[float(i)] * 32 for i in range(72)]
        self._write_vector_batches(self.catalog.get_table(table_name), pa.table({
            'embedding': pa.array(vectors, type=pa.list_(pa.float32(), 32)),
        }), first_row_id=0)

        table = self.catalog.get_table(table_name)
        builder = table.new_read_builder()
        splits = builder.new_scan().with_row_ranges([Range(0, 79)]).plan().splits()
        original = FormatPyArrowReader._read_parquet_row_group_batches
        decoded_rows = []

        def count_decoded(reader, row_group, columns):
            for batch in original(reader, row_group, columns):
                decoded_rows.append(batch.num_rows)
                yield batch

        with patch.object(FormatPyArrowReader, '_read_parquet_row_group_batches', count_decoded):
            result = builder.new_read().to_arrow(splits).to_pydict()
        self.assertEqual({'id': list(range(80)), 'embedding': vectors + [None] * 8}, result)
        self.assertEqual(80, sum(decoded_rows))

    def _assert_rolled_vector_update(
            self, vortex, initial_file_size, updated_file_size, updated_row_count, alter_schema,
            shared_vector_file=False):
        from pypaimon.execution.python.read.split_read import DataEvolutionSplitRead
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.utils.range import Range

        row_count = 80
        vector_type = pa.list_(pa.float32(), 32)
        initial = pa.table({
            'id': pa.array(range(row_count), type=pa.int32()),
            'embedding': pa.array([[float(i)] * 32 for i in range(row_count)], type=vector_type),
        })
        updated_column = 'embedding'
        if shared_vector_file:
            updated_column = 'embedding_v2'
            initial = initial.append_column(
                updated_column, pa.array([[float(i + 200)] * 32 for i in range(row_count)], type=vector_type))
        updated = pa.array([[float(i + 100)] * 32 for i in range(updated_row_count)], type=vector_type)
        table_name = ('test_db.rolled_vector_updates_'
                      f'{initial_file_size}_{updated_row_count}_{alter_schema}_{shared_vector_file}')
        schema = Schema.from_pyarrow_schema(initial.schema, options={
            'bucket': '-1',
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true' if shared_vector_file else 'false',
            'read.batch-size': '5' if shared_vector_file else '1024',
            'file.format': 'vortex',
            'vector.file.format': 'vortex',
            'vector.target-file-size': initial_file_size if alter_schema else updated_file_size,
        })
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        self._write_vector_batches(table.copy({'vector.target-file-size': initial_file_size}), initial)
        initial_schema_id = table.table_schema.id
        if alter_schema:
            self.catalog.alter_table(
                table_name, [SchemaChange.set_option('vector.target-file-size', updated_file_size)], False)
            table = self.catalog.get_table(table_name)
            self.assertGreater(table.table_schema.id, initial_schema_id)
        updated_rows_per_file = 16 if updated_file_size == '2KB' else 8
        self._write_vector_batches(table, pa.table({updated_column: updated}), first_row_id=0,
                                   rows_per_batch=updated_rows_per_file)
        table = self.catalog.get_table(table_name)
        builder = table.new_read_builder()
        splits = builder.new_scan().plan().splits()
        files = [file for split in splits for file in split.files]
        normal_files = [file for file in files if not DataFileMeta.is_vector_file(file.file_name)]
        self.assertEqual([row_count], [file.row_count for file in normal_files])
        updated_ranges = [(start, updated_rows_per_file)
                          for start in range(0, updated_row_count, updated_rows_per_file)]
        initial_rows_per_file = 8 if initial_file_size == '1KB' else row_count
        initial_ranges = [(start, initial_rows_per_file) for start in range(0, row_count, initial_rows_per_file)]
        expected_ranges = [(initial_schema_id, start, count) for start, count in initial_ranges]
        expected_ranges.extend((table.table_schema.id, start, count) for start, count in updated_ranges)
        self.assertEqual(sorted(expected_ranges), sorted(
            (file.schema_id, file.first_row_id, file.row_count) for file in files
            if DataFileMeta.is_vector_file(file.file_name)))

        with patch.object(vortex, 'open', wraps=vortex.open) as open_file:
            result = builder.new_read().to_arrow(splits).sort_by('id')
        expected = initial.to_pydict()
        expected[updated_column][:updated_row_count] = updated.to_pylist()
        self.assertEqual(expected, result.to_pydict())
        opened_files = [os.path.basename(call.args[0]) for call in open_file.call_args_list]
        self.assertIn(normal_files[0].file_name, opened_files)
        self.assertCountEqual(opened_files, set(opened_files))

        if shared_vector_file:
            deleted = [7, 23, 24, 73, 78]
            write_builder = table.new_batch_write_builder()
            messages = write_builder.new_update().delete_by_row_id(deleted)
            commit = write_builder.new_commit()
            commit.commit(messages)
            commit.close()
            builder = self.catalog.get_table(table_name).new_read_builder()

            selected = [Range(6, 10), Range(23, 24), Range(70, 74), Range(79, 79)]
            row_ids = [row_id for row_range in selected
                       for row_id in range(row_range.from_, row_range.to + 1) if row_id not in deleted]
            splits = builder.new_scan().with_row_ranges(selected).plan().splits()
            with patch.object(DataEvolutionSplitRead, 'file_reader_supplier', autospec=True,
                              side_effect=DataEvolutionSplitRead.file_reader_supplier) as create_file_reader:
                result = builder.new_read().to_arrow(splits)
            self.assertEqual({name: [values[row_id] for row_id in row_ids] for name, values in expected.items()},
                             result.to_pydict())
            if updated_row_count == row_count:
                for call in create_file_reader.call_args_list:
                    file = call.kwargs['file']
                    if DataFileMeta.is_vector_file(file.file_name):
                        self.assertEqual(Range.and_(selected, [file.non_null_row_id_range()]),
                                         call.kwargs['row_ranges'])

    @staticmethod
    def _write_vector_batches(table, data, first_row_id=None, rows_per_batch=8):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        if first_row_id is not None:
            writer = writer.with_write_type(data.column_names)
        for batch in data.to_batches(max_chunksize=rows_per_batch):
            writer.write_arrow(pa.Table.from_batches([batch]))
        messages = writer.prepare_commit()
        if first_row_id is not None:
            # Stage a replacement for each rolled vector range.
            for message in messages:
                for file in message.new_files:
                    file.first_row_id = first_row_id
                    first_row_id += file.row_count
        commit = builder.new_commit()
        commit.commit(messages)
        writer.close()
        commit.close()

    def test_vector_table_partial_update_non_vector_column(self):
        vector_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('embedding', pa.list_(pa.float32(), 4)),
        ])
        opts = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'data-evolution.write-cols-optimization.enabled': 'true',
            'vector.file.format': 'parquet',
        }
        s = Schema.from_pyarrow_schema(vector_schema, options=opts)
        table_name = 'test_db.vector_de_seq'
        self.catalog.create_table(table_name, s, False)

        table = self.catalog.get_table(table_name)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {
                'id': [1, 2],
                'name': ['a', 'b'],
                'embedding': [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]],
            },
            schema=vector_schema,
        ))
        initial_messages = w.prepare_commit()
        initial_files = [
            file for message in initial_messages for file in message.new_files
        ]
        normal_file = next(
            file for file in initial_files
            if not DataFileMeta.is_vector_file(file.file_name)
        )
        vector_file = next(
            file for file in initial_files
            if DataFileMeta.is_vector_file(file.file_name)
        )
        self.assertIsNone(normal_file.write_cols)
        self.assertEqual(['embedding'], vector_file.write_cols)
        wb.new_commit().commit(initial_messages)
        w.close()

        table = self.catalog.get_table(table_name)
        rb = table.new_read_builder()
        rb = rb.with_projection(['name', '_ROW_ID'])
        splits = rb.new_scan().plan().splits()
        source = rb.new_read().to_arrow(splits)

        update_data = pa.table({
            '_ROW_ID': source.column('_ROW_ID'),
            'name': pa.array(['updated', 'updated'], type=pa.string()),
        })
        update_builder = table.new_batch_write_builder()
        updater = update_builder.new_update().new_update_by_row_id()
        msgs = updater.update_columns(update_data, ['name'])
        update_builder.new_commit().commit(msgs)

        table = self.catalog.get_table(table_name)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        result = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(result['name'], ['updated', 'updated'])

    def test_backfill_vector_column_added_after_existing_rows(self):
        from pypaimon.schema.data_types import AtomicType, VectorType
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.utils.range import Range

        table_name = 'test_db.vector_backfill_added_column'
        base_schema = pa.schema([
            ('id', pa.int64()),
            ('image', pa.large_binary()),
        ])
        opts = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'vector.file.format': 'parquet',
        }
        self.catalog.create_table(
            table_name,
            Schema.from_pyarrow_schema(base_schema, options=opts),
            False,
        )
        table = self.catalog.get_table(table_name)
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(pa.table({
            'id': pa.array([1, 2, 3], type=pa.int64()),
            'image': pa.array([b'a', b'b', b'c'], type=pa.large_binary()),
        }))
        commit = wb.new_commit()
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

        self.catalog.alter_table(
            table_name,
            [SchemaChange.add_column(
                'embedding', VectorType(True, AtomicType('FLOAT'), 2))],
            False,
        )
        table = self.catalog.get_table(table_name)
        read_builder = table.new_read_builder().with_projection(
            ['image', 'embedding', '_ROW_ID'])
        read_builder.with_filter(
            read_builder.new_predicate_builder().is_null('embedding'))
        splits = (
            read_builder.new_scan()
            .with_row_ranges([Range(0, 2)])
            .plan()
            .splits()
        )
        pending = read_builder.new_read().to_arrow(splits).sort_by('_ROW_ID')
        self.assertEqual([0, 1, 2], pending['_ROW_ID'].to_pylist())
        self.assertEqual([b'a', b'b', b'c'], pending['image'].to_pylist())

        updates = pa.table({
            '_ROW_ID': pending['_ROW_ID'],
            'embedding': pa.array(
                [[1.0, 0.0], [0.5, 0.5], [0.0, 1.0]],
                type=pa.list_(pa.float32(), 2),
            ),
        })
        wb = table.new_batch_write_builder()
        messages = (
            wb.new_update()
            .with_update_type(['embedding'])
            .update_by_arrow_with_row_id(updates)
        )
        commit = wb.new_commit()
        commit.commit(messages)
        commit.close()

        table = self.catalog.get_table(table_name)
        read_builder = table.new_read_builder().with_projection(
            ['id', 'embedding'])
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits()).sort_by('id')
        self.assertEqual(
            [[1.0, 0.0], [0.5, 0.5], [0.0, 1.0]],
            result['embedding'].to_pylist(),
        )

        retry_builder = table.new_read_builder().with_projection(
            ['embedding', '_ROW_ID'])
        retry_builder.with_filter(
            retry_builder.new_predicate_builder().is_null('embedding'))
        retry_splits = (
            retry_builder.new_scan()
            .with_row_ranges([Range(0, 2)])
            .plan()
            .splits()
        )
        self.assertEqual(
            0,
            retry_builder.new_read().to_arrow(retry_splits).num_rows,
        )

    def test_vector_table_partial_update_non_vector_column_with_rolling_files(self):
        vector_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('embedding', pa.list_(pa.float32(), 4)),
        ])
        opts = {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'vector.file.format': 'parquet',
            'target-file-size': '1KB',
        }
        s = Schema.from_pyarrow_schema(vector_schema, options=opts)
        table_name = 'test_db.vector_de_seq_rolling'
        self.catalog.create_table(table_name, s, False)

        write_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        table = self.catalog.get_table(table_name)
        wb = table.new_batch_write_builder()
        w = wb.new_write().with_write_type(['id', 'name'])
        for start in (0, 1000):
            ids = list(range(start, start + 1000))
            w.write_arrow(pa.Table.from_pydict(
                {
                    'id': ids,
                    'name': [f'name_{i}_' + 'x' * 2048 for i in ids],
                },
                schema=write_schema,
            ))
        commit_messages = w.prepare_commit()
        normal_files = [
            f for msg in commit_messages for f in msg.new_files
            if not DataFileMeta.is_vector_file(f.file_name)
        ]
        self.assertGreaterEqual(len(normal_files), 2)
        for file in normal_files:
            self.assertEqual(file.min_sequence_number, 0)
            self.assertEqual(file.max_sequence_number, file.row_count - 1)
        wb.new_commit().commit(commit_messages)
        w.close()

        table = self.catalog.get_table(table_name)
        rb = table.new_read_builder().with_projection(['id', 'name', '_ROW_ID'])
        splits = rb.new_scan().plan().splits()
        source = rb.new_read().to_arrow(splits).sort_by('id')

        update_data = pa.table({
            '_ROW_ID': source.column('_ROW_ID'),
            'name': pa.array(['updated'] * source.num_rows, type=pa.string()),
        })
        update_builder = table.new_batch_write_builder()
        updater = update_builder.new_update().new_update_by_row_id()
        msgs = updater.update_columns(update_data, ['name'])
        update_normal_files = [
            f for msg in msgs for f in msg.new_files
            if not DataFileMeta.is_vector_file(f.file_name)
        ]
        self.assertGreaterEqual(len(update_normal_files), 2)
        for file in update_normal_files:
            self.assertEqual(file.min_sequence_number, 0)
            self.assertEqual(file.max_sequence_number, file.row_count - 1)
        update_builder.new_commit().commit(msgs)

        table = self.catalog.get_table(table_name)
        rb = table.new_read_builder().with_projection(['id', 'name'])
        splits = rb.new_scan().plan().splits()
        result = rb.new_read().to_arrow(splits).sort_by('id').to_pydict()
        self.assertEqual(result['id'], list(range(2000)))
        self.assertEqual(result['name'], ['updated'] * 2000)


if __name__ == '__main__':
    unittest.main()
