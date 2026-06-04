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

"""Schema-evolution read tests for nested (struct/array/map) types.

Two layers are covered:

* Whole-column evolution of a top-level struct/array/map column
  (add / drop / rename / projection) -- aligned by the column's field id.
* Sub-field evolution INSIDE a struct (add/rename/update-type/drop a nested
  field via a dotted ``field_names`` path) -- this is NOT implemented:
  ``schema_manager`` only operates on the top-level ``field_names[-1]``.
  ``SchemaEvolutionNestedGapTest`` locks in the current behaviour with
  explicit assertions so the gap is documented and any future fix is
  noticed.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange


def _paimon_type(pa_type, nullable=True):
    return PyarrowFieldParser.to_paimon_type(pa_type, nullable)


_MV_PA = pa.struct([('latest_version', pa.int64()), ('latest_value', pa.string())])


class _NestedBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, name, pa_schema, primary_keys=None, bucket='-1'):
        options = {'bucket': bucket}
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=list(primary_keys) if primary_keys else None,
            options=options,
        )
        full = 'default.{}'.format(name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def _write(self, table, pa_table):
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa_table)
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    def _read_sorted(self, table, key='id', projection=None):
        rb = table.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        splits = rb.new_scan().plan().splits()
        rows = rb.new_read().to_arrow(splits).to_pylist() if splits else []
        return sorted(rows, key=lambda r: r[key])


class SchemaEvolutionNestedReadTest(_NestedBase):
    """Top-level struct/array/map column evolution (works)."""

    # -- C1: add a new struct top-level column ---------------------------

    def test_add_struct_column(self):
        s0 = pa.schema([('id', pa.int64()), ('val', pa.string())])
        table = self._create('nested_add_struct', s0)
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 2], 'val': ['x', 'y']}, schema=s0))

        self.catalog.alter_table(
            'default.nested_add_struct',
            [SchemaChange.add_column('mv', _paimon_type(_MV_PA))], False)
        table = self.catalog.get_table('default.nested_add_struct')
        s1 = pa.schema([('id', pa.int64()), ('val', pa.string()), ('mv', _MV_PA)])
        self._write(table, pa.Table.from_pylist(
            [{'id': 3, 'val': 'z',
              'mv': {'latest_version': 300, 'latest_value': 'c'}}], schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'val': 'x', 'mv': None},
            {'id': 2, 'val': 'y', 'mv': None},
            {'id': 3, 'val': 'z',
             'mv': {'latest_version': 300, 'latest_value': 'c'}}])

    # -- C2: drop a struct top-level column ------------------------------

    def test_drop_struct_column(self):
        s0 = pa.schema([('id', pa.int64()), ('mv', _MV_PA), ('val', pa.string())])
        table = self._create('nested_drop_struct', s0)
        self._write(table, pa.Table.from_pylist([
            {'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
            {'id': 2, 'mv': {'latest_version': 200, 'latest_value': 'b'}, 'val': 'y'},
        ], schema=s0))

        self.catalog.alter_table(
            'default.nested_drop_struct', [SchemaChange.drop_column('mv')], False)
        table = self.catalog.get_table('default.nested_drop_struct')
        s1 = pa.schema([('id', pa.int64()), ('val', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [3], 'val': ['z']}, schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'val': 'x'}, {'id': 2, 'val': 'y'}, {'id': 3, 'val': 'z'}])

    # -- C4: nested leaf projection under evolution ----------------------

    def test_nested_projection_after_add_column(self):
        s0 = pa.schema([('id', pa.int64()), ('mv', _MV_PA), ('val', pa.string())])
        table = self._create('nested_proj_evo', s0)
        self._write(table, pa.Table.from_pylist([
            {'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
        ], schema=s0))

        self.catalog.alter_table(
            'default.nested_proj_evo',
            [SchemaChange.add_column('extra', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.nested_proj_evo')
        s1 = pa.schema([('id', pa.int64()), ('mv', _MV_PA),
                        ('val', pa.string()), ('extra', pa.string())])
        self._write(table, pa.Table.from_pylist([
            {'id': 2, 'mv': {'latest_version': 200, 'latest_value': 'b'},
             'val': 'y', 'extra': 'E'},
        ], schema=s1))

        rows = self._read_sorted(
            table, projection=['id', 'mv.latest_version', 'extra'])
        self.assertEqual(rows, [
            {'id': 1, 'mv_latest_version': 100, 'extra': None},
            {'id': 2, 'mv_latest_version': 200, 'extra': 'E'}])

    # -- C5: PK table struct column + add column merge read --------------

    def test_pk_struct_add_column_merge(self):
        s0 = pa.schema([pa.field('id', pa.int64(), nullable=False),
                        ('mv', _MV_PA)])
        table = self._create('nested_pk_struct', s0,
                             primary_keys=['id'], bucket='1')
        rows0 = [{'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}}]
        self._write(table, pa.Table.from_pylist(rows0, schema=s0))

        self.catalog.alter_table(
            'default.nested_pk_struct',
            [SchemaChange.add_column('w', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.nested_pk_struct')
        s1 = pa.schema([pa.field('id', pa.int64(), nullable=False),
                        ('mv', _MV_PA), ('w', pa.string())])
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'mv': {'latest_version': 101, 'latest_value': 'a2'},
              'w': 'W'}], schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'mv': {'latest_version': 101, 'latest_value': 'a2'},
             'w': 'W'}])

    # -- C6: array / map top-level column add ----------------------------

    def test_add_array_and_map_columns(self):
        s0 = pa.schema([('id', pa.int64()), ('val', pa.string())])
        table = self._create('nested_add_arr_map', s0)
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'val': ['x']}, schema=s0))

        arr_pa = pa.list_(pa.int64())
        map_pa = pa.map_(pa.string(), pa.int64())
        self.catalog.alter_table(
            'default.nested_add_arr_map', [
                SchemaChange.add_column('arr', _paimon_type(arr_pa)),
                SchemaChange.add_column('m', _paimon_type(map_pa)),
            ], False)
        table = self.catalog.get_table('default.nested_add_arr_map')
        s1 = pa.schema([('id', pa.int64()), ('val', pa.string()),
                        ('arr', arr_pa), ('m', map_pa)])
        self._write(table, pa.Table.from_pylist(
            [{'id': 2, 'val': 'y', 'arr': [1, 2, 3],
              'm': [('k', 7)]}], schema=s1))

        rows = self._read_sorted(table)
        self.assertEqual(rows[0], {'id': 1, 'val': 'x', 'arr': None, 'm': None})
        self.assertEqual(rows[1]['arr'], [1, 2, 3])
        self.assertEqual(dict(rows[1]['m']), {'k': 7})

    # -- C3: rename a struct top-level column ----------------------------

    def test_rename_struct_column(self):
        # Renaming the top-level struct column keeps its field id, so old rows
        # read their original struct back under the new name.
        s0 = pa.schema([('id', pa.int64()), ('mv', _MV_PA), ('val', pa.string())])
        table = self._create('nested_rename_struct', s0)
        self._write(table, pa.Table.from_pylist([
            {'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
        ], schema=s0))

        self.catalog.alter_table(
            'default.nested_rename_struct',
            [SchemaChange.rename_column('mv', 'mv2')], False)
        table = self.catalog.get_table('default.nested_rename_struct')
        s1 = pa.schema([('id', pa.int64()), ('mv2', _MV_PA), ('val', pa.string())])
        self._write(table, pa.Table.from_pylist([
            {'id': 2, 'mv2': {'latest_version': 200, 'latest_value': 'b'}, 'val': 'y'},
        ], schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'mv2': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
            {'id': 2, 'mv2': {'latest_version': 200, 'latest_value': 'b'}, 'val': 'y'}])


class SchemaEvolutionNestedGapTest(_NestedBase):
    """Sub-field-level evolution inside a struct is NOT implemented.

    schema_manager handles only the top-level ``field_names[-1]``; a dotted
    path like ``['mv', 'latest_value']`` never recurses into the RowType.
    These tests assert the current behaviour (silent top-level mutation, or
    ColumnNotExistException) so the gap is documented.
    """

    def _create_struct_table(self, name):
        s0 = pa.schema([('id', pa.int64()), ('mv', _MV_PA), ('val', pa.string())])
        table = self._create(name, s0)
        self._write(table, pa.Table.from_pylist([
            {'id': 1, 'mv': {'latest_version': 100, 'latest_value': 'a'}, 'val': 'x'},
        ], schema=s0))
        return table

    def _mv_subfield_names(self, table_name):
        schema = self.catalog.get_table(
            'default.{}'.format(table_name)).table_schema
        mv = next(f for f in schema.fields if f.name == 'mv')
        return [sf.name for sf in mv.type.fields]

    def _top_level_names(self, table_name):
        schema = self.catalog.get_table(
            'default.{}'.format(table_name)).table_schema
        return [f.name for f in schema.fields]

    # -- C7: add nested sub-field -> silently adds a TOP-LEVEL column ----

    def test_nested_add_subfield_mutates_top_level(self):
        # GAP: add_column(['mv','new_inner']) does NOT add new_inner inside
        # mv; it silently appends a top-level column 'new_inner' instead.
        self._create_struct_table('gap_add')
        self.catalog.alter_table(
            'default.gap_add',
            [SchemaChange.add_column(['mv', 'new_inner'], AtomicType('INT'))],
            False)
        # mv's sub-fields are unchanged; a stray top-level column appeared.
        self.assertEqual(self._mv_subfield_names('gap_add'),
                         ['latest_version', 'latest_value'])
        self.assertIn('new_inner', self._top_level_names('gap_add'))

    # -- C8/C9/C10: rename / update-type / drop nested sub-field ---------

    def test_nested_rename_subfield_raises(self):
        # GAP: field_names[-1]='latest_value' is looked up at the TOP level,
        # where it does not exist -> ColumnNotExistException (wrapped).
        self._create_struct_table('gap_rename')
        with self.assertRaises(RuntimeError) as cm:
            self.catalog.alter_table(
                'default.gap_rename',
                [SchemaChange.rename_column(['mv', 'latest_value'], 'lv')], False)
        self.assertIn('latest_value', str(cm.exception))

    def test_nested_update_subfield_type_raises(self):
        self._create_struct_table('gap_update')
        with self.assertRaises(RuntimeError) as cm:
            self.catalog.alter_table(
                'default.gap_update',
                [SchemaChange.update_column_type(
                    ['mv', 'latest_version'], AtomicType('BIGINT'))], False)
        self.assertIn('latest_version', str(cm.exception))

    def test_nested_drop_subfield_raises(self):
        self._create_struct_table('gap_drop')
        with self.assertRaises(RuntimeError) as cm:
            self.catalog.alter_table(
                'default.gap_drop',
                [SchemaChange.drop_column(['mv', 'latest_value'])], False)
        self.assertIn('latest_value', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
