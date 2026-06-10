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
  field via a dotted ``field_names`` path), including sub-fields of a ROW
  nested in an ARRAY/MAP. Sub-fields are aligned by field id, so a rename
  follows the data, an added sub-field reads NULL for old rows, a dropped one
  is not revived, and a type change is cast at read time.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.casting.data_type_casts import supports_cast
from pypaimon.schema.data_types import (AtomicInteger, AtomicType, DataField,
                                        PyarrowFieldParser, RowType,
                                        collect_field_ids,
                                        current_highest_field_id,
                                        reassign_field_id)
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


class SchemaEvolutionNestedSubfieldTest(_NestedBase):
    """Sub-field evolution inside a struct, aligned by field id."""

    def _create_struct_table(self, name, primary_keys=None, bucket='-1'):
        s0 = pa.schema([('id', pa.int64()), ('mv', _MV_PA), ('val', pa.string())])
        table = self._create(name, s0, primary_keys=primary_keys, bucket=bucket)
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

    def test_add_subfield_goes_inside_struct_and_pads_null(self):
        table = self._create_struct_table('nsub_add')
        self.catalog.alter_table(
            'default.nsub_add',
            [SchemaChange.add_column(['mv', 'score'], AtomicType('INT'))], False)
        # The sub-field lands inside mv, not as a stray top-level column.
        self.assertEqual(self._mv_subfield_names('nsub_add'),
                         ['latest_version', 'latest_value', 'score'])
        self.assertNotIn('score', self._top_level_names('nsub_add'))

        table = self.catalog.get_table('default.nsub_add')
        s1 = pa.schema([
            ('id', pa.int64()),
            ('mv', pa.struct([('latest_version', pa.int64()),
                              ('latest_value', pa.string()),
                              ('score', pa.int32())])),
            ('val', pa.string())])
        self._write(table, pa.Table.from_pylist([
            {'id': 2, 'mv': {'latest_version': 200, 'latest_value': 'b', 'score': 7},
             'val': 'y'}], schema=s1))
        rows = self._read_sorted(table)
        # Old row reads NULL for the added sub-field; new row carries it.
        self.assertEqual(rows[0]['mv'],
                         {'latest_version': 100, 'latest_value': 'a', 'score': None})
        self.assertEqual(rows[1]['mv'],
                         {'latest_version': 200, 'latest_value': 'b', 'score': 7})

    def test_rename_subfield_follows_field_id(self):
        table = self._create_struct_table('nsub_rename')
        self.catalog.alter_table(
            'default.nsub_rename',
            [SchemaChange.rename_column(['mv', 'latest_value'], 'lv')], False)
        self.assertEqual(self._mv_subfield_names('nsub_rename'),
                         ['latest_version', 'lv'])
        table = self.catalog.get_table('default.nsub_rename')
        rows = self._read_sorted(table)
        # Old data follows the renamed sub-field by id, not by name.
        self.assertEqual(rows[0]['mv'], {'latest_version': 100, 'lv': 'a'})

    def test_update_subfield_type_casts(self):
        s0 = pa.schema([('id', pa.int64()),
                        ('mv', pa.struct([('v', pa.int32()), ('s', pa.string())]))])
        table = self._create('nsub_type', s0)
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'mv': {'v': 10, 's': 'a'}}], schema=s0))
        self.catalog.alter_table(
            'default.nsub_type',
            [SchemaChange.update_column_type(['mv', 'v'], AtomicType('BIGINT'))], False)
        table = self.catalog.get_table('default.nsub_type')
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        arrow = rb.new_read().to_arrow(splits)
        self.assertEqual(arrow.schema.field('mv').type.field('v').type, pa.int64())
        self.assertEqual(arrow.to_pylist()[0]['mv'], {'v': 10, 's': 'a'})

    def test_drop_subfield_not_revived(self):
        table = self._create_struct_table('nsub_drop')
        self.catalog.alter_table(
            'default.nsub_drop',
            [SchemaChange.drop_column(['mv', 'latest_value'])], False)
        self.assertEqual(self._mv_subfield_names('nsub_drop'), ['latest_version'])
        table = self.catalog.get_table('default.nsub_drop')
        rows = self._read_sorted(table)
        # The dropped sub-field's old data is gone, not revived under its id.
        self.assertEqual(rows[0]['mv'], {'latest_version': 100})

    def test_drop_all_subfields_rejected(self):
        self._create_struct_table('nsub_dropall')
        self.catalog.alter_table(
            'default.nsub_dropall',
            [SchemaChange.drop_column(['mv', 'latest_value'])], False)
        with self.assertRaises(RuntimeError):
            self.catalog.alter_table(
                'default.nsub_dropall',
                [SchemaChange.drop_column(['mv', 'latest_version'])], False)

    def test_unsupported_subfield_cast_rejected(self):
        self._create_struct_table('nsub_badcast')
        with self.assertRaises(RuntimeError) as cm:
            self.catalog.alter_table(
                'default.nsub_badcast',
                [SchemaChange.update_column_type(
                    ['mv', 'latest_version'], AtomicType('DATE'))], False)
        self.assertIn('cannot be converted', str(cm.exception))

    def test_nested_projection_after_rename_subfield(self):
        # Projecting a renamed leaf must follow the field id into old files,
        # not look the new name up in the file's physical schema.
        s0 = pa.schema([('id', pa.int64()),
                        ('mv', pa.struct([('v', pa.int32()), ('s', pa.string())]))])
        table = self._create('nsub_proj_rename', s0)
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'mv': {'v': 10, 's': 'a'}}], schema=s0))
        self.catalog.alter_table(
            'default.nsub_proj_rename',
            [SchemaChange.rename_column(['mv', 's'], 'ss')], False)
        table = self.catalog.get_table('default.nsub_proj_rename')
        s1 = pa.schema([('id', pa.int64()),
                        ('mv', pa.struct([('v', pa.int32()), ('ss', pa.string())]))])
        self._write(table, pa.Table.from_pylist(
            [{'id': 2, 'mv': {'v': 20, 'ss': 'b'}}], schema=s1))

        rows = self._read_sorted(table, projection=['id', 'mv.ss'])
        self.assertEqual(rows, [
            {'id': 1, 'mv_ss': 'a'},
            {'id': 2, 'mv_ss': 'b'},
        ])

    def test_nested_projection_after_update_subfield_type(self):
        # Projecting a type-changed leaf must cast old batches to the latest
        # type instead of emitting mixed-type batches.
        s0 = pa.schema([('id', pa.int64()),
                        ('mv', pa.struct([('v', pa.int32()), ('s', pa.string())]))])
        table = self._create('nsub_proj_type', s0)
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'mv': {'v': 10, 's': 'a'}}], schema=s0))
        self.catalog.alter_table(
            'default.nsub_proj_type',
            [SchemaChange.update_column_type(['mv', 'v'], AtomicType('BIGINT'))], False)
        table = self.catalog.get_table('default.nsub_proj_type')
        s1 = pa.schema([('id', pa.int64()),
                        ('mv', pa.struct([('v', pa.int64()), ('s', pa.string())]))])
        self._write(table, pa.Table.from_pylist(
            [{'id': 2, 'mv': {'v': 20, 's': 'b'}}], schema=s1))

        rb = table.new_read_builder().with_projection(['id', 'mv.v'])
        splits = rb.new_scan().plan().splits()
        arrow = rb.new_read().to_arrow(splits)
        self.assertEqual(arrow.schema.field('mv_v').type, pa.int64())
        rows = sorted(arrow.to_pylist(), key=lambda r: r['id'])
        self.assertEqual(rows, [
            {'id': 1, 'mv_v': 10},
            {'id': 2, 'mv_v': 20},
        ])

    def test_pk_nested_subfield_evolution_merge(self):
        s0 = pa.schema([('id', pa.int64()), ('mv', _MV_PA)])
        table = self._create('nsub_pk', s0, primary_keys=['id'], bucket='1')
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'mv': {'latest_version': 1, 'latest_value': 'a'}}], schema=s0))
        self.catalog.alter_table(
            'default.nsub_pk',
            [SchemaChange.add_column(['mv', 'score'], AtomicType('INT'))], False)
        table = self.catalog.get_table('default.nsub_pk')
        s1 = pa.schema([('id', pa.int64()),
                        ('mv', pa.struct([('latest_version', pa.int64()),
                                          ('latest_value', pa.string()),
                                          ('score', pa.int32())]))])
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'mv': {'latest_version': 2, 'latest_value': 'b', 'score': 9}}],
            schema=s1))
        rows = self._read_sorted(table)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['mv'],
                         {'latest_version': 2, 'latest_value': 'b', 'score': 9})


class SchemaEvolutionNestedContainerTest(_NestedBase):
    """Sub-field evolution of a ROW nested inside an ARRAY / MAP."""

    def test_array_of_row_add_and_rename_subfield(self):
        elem = pa.struct([('a', pa.int64()), ('b', pa.string())])
        s0 = pa.schema([('id', pa.int64()), ('arr', pa.list_(elem))])
        table = self._create('narr', s0)
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'arr': [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}]}], schema=s0))
        # Descend through the array element into the ROW.
        self.catalog.alter_table('default.narr', [
            SchemaChange.add_column(['arr', 'element', 'c'], AtomicType('INT')),
            SchemaChange.rename_column(['arr', 'element', 'b'], 'bb'),
        ], False)
        table = self.catalog.get_table('default.narr')
        rows = self._read_sorted(table)
        self.assertEqual(rows[0]['arr'],
                         [{'a': 1, 'bb': 'x', 'c': None},
                          {'a': 2, 'bb': 'y', 'c': None}])

    def test_map_of_row_add_subfield(self):
        val = pa.struct([('a', pa.int64()), ('b', pa.string())])
        s0 = pa.schema([('id', pa.int64()), ('m', pa.map_(pa.string(), val))])
        table = self._create('nmap', s0)
        self._write(table, pa.Table.from_pylist(
            [{'id': 1, 'm': [('k', {'a': 1, 'b': 'x'})]}], schema=s0))
        # Descend through the map value into the ROW.
        self.catalog.alter_table(
            'default.nmap',
            [SchemaChange.add_column(['m', 'value', 'c'], AtomicType('INT'))], False)
        table = self.catalog.get_table('default.nmap')
        rows = self._read_sorted(table)
        self.assertEqual(rows[0]['m'], [('k', {'a': 1, 'b': 'x', 'c': None})])


class NestedFieldIdModelTest(unittest.TestCase):
    """Globally-unique nested field ids, mirrored from the engine id model."""

    def test_nested_ids_are_globally_unique(self):
        s = pa.schema([('id', pa.int64()), ('mv', _MV_PA), ('x', pa.string())])
        fields = PyarrowFieldParser.to_paimon_schema(s)
        ids = set()
        for f in fields:
            ids.add(f.id)
            collect_field_ids(f.type, ids)
        # id(0), mv(1), latest_version(2), latest_value(3), x(4)
        self.assertEqual(ids, {0, 1, 2, 3, 4})
        self.assertEqual(current_highest_field_id(fields), 4)

    def test_flat_schema_ids_unchanged(self):
        fields = PyarrowFieldParser.to_paimon_schema(
            pa.schema([('a', pa.int64()), ('b', pa.string()), ('c', pa.int32())]))
        self.assertEqual([f.id for f in fields], [0, 1, 2])

    def test_reassign_field_id_depth_first_order(self):
        inner = RowType(True, [DataField(0, 'c', AtomicType('INT'))])
        mid = RowType(True, [DataField(0, 'b', inner)])
        outer = RowType(True, [DataField(0, 'a', mid),
                               DataField(0, 'd', AtomicType('INT'))])
        result = reassign_field_id(outer, AtomicInteger(2))
        ids = set()
        collect_field_ids(result, ids)
        self.assertEqual(ids, {3, 4, 5, 6})

    def test_duplicate_field_id_raises(self):
        bad = [
            DataField(0, 'a', AtomicType('INT')),
            DataField(1, 'b', RowType(True, [DataField(0, 'c', AtomicType('INT'))])),
        ]
        with self.assertRaises(ValueError):
            current_highest_field_id(bad)


class SupportsCastTest(unittest.TestCase):

    def test_supported_casts(self):
        for src, dst in [('INT', 'BIGINT'), ('FLOAT', 'DOUBLE'), ('INT', 'STRING'),
                         ('DOUBLE', 'INT'), ('DECIMAL(10, 4)', 'DECIMAL(10, 2)')]:
            self.assertTrue(supports_cast(AtomicType(src), AtomicType(dst)),
                            '{} -> {}'.format(src, dst))

    def test_unsupported_casts(self):
        for src, dst in [('BIGINT', 'DATE'), ('BOOLEAN', 'DATE')]:
            self.assertFalse(supports_cast(AtomicType(src), AtomicType(dst)),
                             '{} -> {}'.format(src, dst))


if __name__ == '__main__':
    unittest.main()
