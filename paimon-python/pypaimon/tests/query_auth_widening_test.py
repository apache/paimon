################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

import json
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.schema.data_types import AtomicType, DataField


def _field_ref(index, name, type_name="STRING"):
    return {"name": "FIELD_REF",
            "fieldRef": {"index": index, "name": name, "type": type_name}}


def _null_mask():
    return json.dumps({"name": "NULL"})


def _leaf(index, name, function, literals, type_name="INT"):
    return json.dumps({
        "kind": "LEAF",
        "transform": _field_ref(index, name, type_name),
        "function": function,
        "literals": literals,
    })


def _fields(*names):
    return [DataField(i, n, AtomicType("STRING")) for i, n in enumerate(names)]


class TestExtraFieldsWidening(unittest.TestCase):

    def _extra(self, result, read, table):
        return [f.name for f in result.get_extra_fields(
            _fields(*read), _fields(*table))]

    def test_filter_operands_widen(self):
        result = TableQueryAuthResult(
            filter=[_leaf(2, "score", "GREATER_THAN", [5])], column_masking=None)
        self.assertEqual(self._extra(result, ["id"], ["id", "name", "score"]),
                         ["score"])

    def test_readable_mask_input_widens(self):
        result = TableQueryAuthResult(filter=None, column_masking={
            "secret": json.dumps(_field_ref(2, "salt"))})
        self.assertEqual(
            self._extra(result, ["id", "secret"], ["id", "secret", "salt"]),
            ["salt"])

    def test_unreadable_mask_target_widens_nothing(self):
        result = TableQueryAuthResult(filter=None, column_masking={
            "secret": json.dumps(_field_ref(2, "salt"))})
        self.assertEqual(self._extra(result, ["id"], ["id", "secret", "salt"]), [])

    def test_widening_is_transitive(self):
        result = TableQueryAuthResult(filter=None, column_masking={
            "a": json.dumps(_field_ref(1, "b")),
            "b": json.dumps(_field_ref(2, "c")),
        })
        self.assertEqual(self._extra(result, ["a"], ["a", "b", "c"]), ["b", "c"])

    def test_a_filter_operand_makes_its_mask_inputs_needed(self):
        result = TableQueryAuthResult(
            filter=[_leaf(1, "b", "GREATER_THAN", [5])],
            column_masking={"b": json.dumps(_field_ref(2, "c"))})
        self.assertEqual(self._extra(result, ["a"], ["a", "b", "c"]), ["b", "c"])

    def test_a_name_absent_from_the_table_is_skipped(self):
        result = TableQueryAuthResult(
            filter=[_leaf(9, "gone", "GREATER_THAN", [5])], column_masking=None)
        self.assertEqual(self._extra(result, ["id"], ["id"]), [])

    def test_no_restrictions_widens_nothing(self):
        self.assertEqual(
            self._extra(TableQueryAuthResult(filter=None, column_masking=None),
                        ["id"], ["id", "score"]), [])


class TestWidenedReadsDoNotCrash(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(cls.tempdir, 'wh')})
        cls.catalog.create_database('db', True)

        cls.flat_schema = pa.schema([
            ('id', pa.int32()), ('secret', pa.string()), ('salt', pa.string()),
            ('score', pa.int32())])
        cls._create('flat', cls.flat_schema, {
            'id': [1, 2], 'secret': ['a', 'b'], 'salt': ['x', 'y'],
            'score': [1, 9]})

        cls.nested_schema = pa.schema([
            ('id', pa.int32()),
            ('s', pa.struct([('a', pa.string()), ('b', pa.string())])),
            ('score', pa.int32())])
        cls._create('nested', cls.nested_schema, {
            'id': [1, 2],
            's': [{'a': 'a1', 'b': 'b1'}, {'a': 'a2', 'b': 'b2'}],
            'score': [1, 9]})

    @classmethod
    def _create(cls, name, pa_schema, rows):
        cls.catalog.create_table(
            'db.' + name, Schema.from_pyarrow_schema(pa_schema), False)
        table = cls.catalog.get_table('db.' + name)
        writer = table.new_batch_write_builder().new_write()
        writer.write_arrow(pa.Table.from_pydict(rows, schema=pa_schema))
        table.new_batch_write_builder().new_commit().commit(
            writer.prepare_commit())
        writer.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _read(self, name, projection, auth_result=None):
        table = self.catalog.get_table('db.' + name)
        if auth_result is not None:
            table.catalog_environment.table_query_auth = (
                lambda options, identifier: (lambda select: auth_result))
        builder = table.new_read_builder()
        if projection is not None:
            builder = builder.with_projection(projection)
        splits = builder.new_scan().plan().splits()
        return builder.new_read().to_arrow(splits).to_pydict()

    def test_mask_input_outside_the_projection(self):
        out = self._read('flat', ['id', 'secret'], TableQueryAuthResult(
            filter=None,
            column_masking={'secret': json.dumps(_field_ref(2, 'salt'))}))
        self.assertEqual(out, {'id': [1, 2], 'secret': ['x', 'y']})

    def test_a_mask_reading_a_masked_column_is_still_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._read('flat', ['id', 'secret'], TableQueryAuthResult(
                filter=None,
                column_masking={'secret': json.dumps(_field_ref(2, 'salt')),
                                'salt': _null_mask()}))
        self.assertIn("which is masked too", str(ctx.exception))

    def test_filter_operand_outside_the_projection(self):
        out = self._read('flat', ['id', 'secret'], TableQueryAuthResult(
            filter=[_leaf(3, 'score', 'GREATER_THAN', [5])],
            column_masking=None))
        self.assertEqual(out, {'id': [2], 'secret': ['b']})

    def test_nested_projection_with_filter_on_an_unprojected_column(self):
        out = self._read('nested', ['id', 's.a'], TableQueryAuthResult(
            filter=[_leaf(2, 'score', 'GREATER_THAN', [5])],
            column_masking=None))
        self.assertEqual(out, {'id': [2], 's_a': ['a2']})

    def test_nested_projection_with_a_mask_input_outside_the_projection(self):
        out = self._read('nested', ['id', 's.a'], TableQueryAuthResult(
            filter=None,
            column_masking={'id': json.dumps(_field_ref(2, 'score', 'INT'))}))
        self.assertEqual(out, {'id': [1, 9], 's_a': ['a1', 'a2']})

    def test_nested_projection_without_authorization_is_unchanged(self):
        self.assertEqual(self._read('nested', ['id', 's.a']),
                         {'id': [1, 2], 's_a': ['a1', 'a2']})


if __name__ == '__main__':
    unittest.main()
