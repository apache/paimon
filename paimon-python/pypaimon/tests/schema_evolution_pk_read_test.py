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

"""Schema-evolution read tests for primary-key tables (merge engines).

Each test creates a PK table (``bucket=1`` so same-PK rows share a bucket
and the read goes through the merge reader), writes a batch under the
original schema, evolves the schema via ``catalog.alter_table``, writes a
second batch under the new schema, and reads back the merged result that
spans both schema versions.

Covers add / drop / type promotion / position as well as the field-id
isolation cases (rename, drop-then-readd same name) that a name-based
alignment would get wrong -- the merged result must follow field id.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import Move, SchemaChange


class SchemaEvolutionPkReadTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, name, pa_schema, merge_engine='deduplicate',
                primary_keys=('id',), partition_keys=None, extra=None):
        options = {'bucket': '1', 'merge-engine': merge_engine}
        if extra:
            options.update(extra)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=list(primary_keys),
            partition_keys=list(partition_keys) if partition_keys else None,
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

    def _read_sorted(self, table, key='id'):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        rows = rb.new_read().to_arrow(splits).to_pylist() if splits else []
        return sorted(rows, key=lambda r: r[key])

    @staticmethod
    def _pk(name, arrow_type, nullable=True):
        return pa.field(name, arrow_type, nullable=nullable)

    # -- B1: add column + deduplicate ------------------------------------

    def test_pk_add_column_deduplicate(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string())])
        table = self._create('pk_add_dedup', s0, 'deduplicate')
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 2], 'v': ['a', 'b']}, schema=s0))

        self.catalog.alter_table(
            'default.pk_add_dedup',
            [SchemaChange.add_column('w', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.pk_add_dedup')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string()), ('w', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 3], 'v': ['a2', 'c'], 'w': ['w1', 'w3']}, schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'v': 'a2', 'w': 'w1'},   # latest wins
            {'id': 2, 'v': 'b', 'w': None},    # old file, w padded NULL
            {'id': 3, 'v': 'c', 'w': 'w3'}])

    # -- B2: add column + partial-update ---------------------------------

    def test_pk_add_column_partial_update(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string())])
        table = self._create('pk_add_partial', s0, 'partial-update')
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': ['A']}, schema=s0))

        self.catalog.alter_table(
            'default.pk_add_partial',
            [SchemaChange.add_column('b', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.pk_add_partial')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string()), ('b', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': [None], 'b': ['B']}, schema=s1))

        # partial-update merges non-null per field across the two versions.
        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'a': 'A', 'b': 'B'}])

    # -- B3: type promotion on a value column + deduplicate --------------

    def test_pk_type_promotion_deduplicate(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('n', pa.int32())])
        table = self._create('pk_promote', s0, 'deduplicate')
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 2], 'n': [10, 20]}, schema=s0))

        self.catalog.alter_table(
            'default.pk_promote',
            [SchemaChange.update_column_type('n', AtomicType('BIGINT'))], False)
        table = self.catalog.get_table('default.pk_promote')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('n', pa.int64())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 3], 'n': [100, 30]}, schema=s1))

        rows = self._read_sorted(table)
        self.assertEqual(rows, [
            {'id': 1, 'n': 100}, {'id': 2, 'n': 20}, {'id': 3, 'n': 30}])

    # -- B5: drop a value column + partial-update ------------------------

    def test_pk_drop_column_partial_update(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string()), ('b', pa.string())])
        table = self._create('pk_drop_partial', s0, 'partial-update')
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': ['A'], 'b': [None]}, schema=s0))

        self.catalog.alter_table(
            'default.pk_drop_partial', [SchemaChange.drop_column('b')], False)
        table = self.catalog.get_table('default.pk_drop_partial')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': ['A2']}, schema=s1))

        self.assertEqual(self._read_sorted(table), [{'id': 1, 'a': 'A2'}])

    # -- B6: reorder a value column (position) + deduplicate -------------

    def test_pk_column_position_deduplicate(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string()), ('b', pa.string())])
        table = self._create('pk_position', s0, 'deduplicate')
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': ['a1'], 'b': ['b1']}, schema=s0))

        self.catalog.alter_table(
            'default.pk_position',
            [SchemaChange.update_column_position(Move.first('b'))], False)
        table = self.catalog.get_table('default.pk_position')
        s1 = pa.schema([('b', pa.string()),
                        self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'b': ['b2'], 'id': [1], 'a': ['a2']}, schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'b': 'b2', 'id': 1, 'a': 'a2'}])

    # -- B7: first-row engine + add column -------------------------------

    def test_pk_first_row_add_column(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string())])
        table = self._create('pk_first_row', s0, 'first-row')
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'v': ['first']}, schema=s0))

        self.catalog.alter_table(
            'default.pk_first_row',
            [SchemaChange.add_column('w', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.pk_first_row')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string()), ('w', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'v': ['second'], 'w': ['W']}, schema=s1))

        # first-row keeps the earliest row; it predates column w, so w is NULL.
        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'v': 'first', 'w': None}])

    # -- B8: multi-version chain (add + promotion) + partial-update ------

    def test_pk_multi_version_chain_partial_update(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string()), ('n', pa.int32())])
        table = self._create('pk_chain', s0, 'partial-update')
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': ['A'], 'n': [10]}, schema=s0))

        self.catalog.alter_table(
            'default.pk_chain',
            [SchemaChange.add_column('b', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.pk_chain')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string()), ('n', pa.int32()), ('b', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': [None], 'n': [None], 'b': ['B']}, schema=s1))

        self.catalog.alter_table(
            'default.pk_chain',
            [SchemaChange.update_column_type('n', AtomicType('BIGINT'))], False)
        table = self.catalog.get_table('default.pk_chain')
        s2 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('a', pa.string()), ('n', pa.int64()), ('b', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1], 'a': [None], 'n': [99], 'b': [None]}, schema=s2))

        # partial-update across three schema versions: a from v0, b from v1,
        # n (promoted) latest non-null from v2.
        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'a': 'A', 'n': 99, 'b': 'B'}])

    # -- B9 / B10: negative constraints ----------------------------------

    def test_pk_cannot_update_primary_key_type(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string())])
        self._create('pk_no_pk_type', s0, 'deduplicate')
        with self.assertRaises(RuntimeError) as cm:
            self.catalog.alter_table(
                'default.pk_no_pk_type',
                [SchemaChange.update_column_type('id', AtomicType('BIGINT'))],
                False)
        self.assertIn('primary key', str(cm.exception))

    def test_pk_cannot_drop_or_rename_partition_key(self):
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        self._pk('dt', pa.string(), nullable=False),
                        ('v', pa.string())])
        # PK must contain the partition key.
        self._create('pk_part_guard', s0, 'deduplicate',
                     primary_keys=('id', 'dt'), partition_keys=('dt',))
        with self.assertRaises(RuntimeError) as cm:
            self.catalog.alter_table(
                'default.pk_part_guard', [SchemaChange.drop_column('dt')], False)
        self.assertIn('partition', str(cm.exception).lower())
        with self.assertRaises(RuntimeError):
            self.catalog.alter_table(
                'default.pk_part_guard',
                [SchemaChange.rename_column('dt', 'dt2')], False)

    # -- B4: rename a value column ---------------------------------------

    def test_pk_rename_value_column(self):
        # After rename, the old file's value (written under the old name) must
        # still merge in under the new name -- alignment follows field id, so
        # id=2 keeps its old 'b'.
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string())])
        table = self._create('pk_rename', s0, 'deduplicate')
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 2], 'v': ['a', 'b']}, schema=s0))

        self.catalog.alter_table(
            'default.pk_rename',
            [SchemaChange.rename_column('v', 'v2')], False)
        table = self.catalog.get_table('default.pk_rename')
        s1 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v2', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 3], 'v2': ['a2', 'c']}, schema=s1))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'v2': 'a2'}, {'id': 2, 'v2': 'b'}, {'id': 3, 'v2': 'c'}])

    # -- B11: drop a value column then re-add the same name --------------

    def test_pk_drop_then_readd_same_name(self):
        # The re-added v (new field id) must not revive the dropped column's
        # old data: aligned by field id, id=2 (only in the old file) reads
        # NULL for the new v.
        s0 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string())])
        table = self._create('pk_drop_readd', s0, 'deduplicate')
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 2], 'v': ['old1', 'old2']}, schema=s0))

        self.catalog.alter_table(
            'default.pk_drop_readd', [SchemaChange.drop_column('v')], False)
        self.catalog.alter_table(
            'default.pk_drop_readd',
            [SchemaChange.add_column('v', AtomicType('STRING'))], False)
        table = self.catalog.get_table('default.pk_drop_readd')
        s2 = pa.schema([self._pk('id', pa.int64(), nullable=False),
                        ('v', pa.string())])
        self._write(table, pa.Table.from_pydict(
            {'id': [1, 3], 'v': ['new1', 'new3']}, schema=s2))

        self.assertEqual(self._read_sorted(table), [
            {'id': 1, 'v': 'new1'}, {'id': 2, 'v': None}, {'id': 3, 'v': 'new3'}])


if __name__ == '__main__':
    unittest.main()
