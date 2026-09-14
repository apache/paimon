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
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange


class MapSelectedKeyProjectionTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.catalog = CatalogFactory.create({
            'warehouse': os.path.join(self.tmp, 'warehouse'),
        })
        self.catalog.create_database('default', False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_projects_literal_map_keys(self):
        table = self._write_table('normal', {})

        result = self._read(table, [
            'id', "attributes['first']",
            "attributes['key.with.dots']",
            "attributes['missing']",
        ])

        self.assertEqual(
            ['id', 'attributes_first',
             'attributes_key_with_dots', 'attributes_missing'],
            result.column_names,
        )
        self.assertEqual({
            'id': [1, 2, 3],
            'attributes_first': [10, None, None],
            'attributes_key_with_dots': [20, None, None],
            'attributes_missing': [None, None, None],
        }, result.to_pydict())

    def test_projects_map_key_from_data_evolution_table(self):
        table = self._write_table('data_evolution', {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })

        result = self._read(table, ['id', "attributes['first']"])

        self.assertEqual({
            'id': [1, 2, 3],
            'attributes_first': [10, None, None],
        }, result.to_pydict())

    def test_row_format_uses_full_map_fallback(self):
        table = self._write_table('row_format', {
            'file.format': 'row',
        })

        result = self._read(table, ["attributes['first']"])

        self.assertEqual(
            [10, None, None],
            result.column('attributes_first').to_pylist(),
        )

    def test_row_format_map_key_with_schema_evolution(self):
        for evolution in ('rename', 'value_type'):
            with self.subTest(evolution=evolution):
                name = 'row_format_' + evolution
                self._write_table(
                    name,
                    {'file.format': 'row'},
                    value_type=pa.int32(),
                )
                identifier = 'default.' + name
                if evolution == 'rename':
                    change = SchemaChange.rename_column(
                        'attributes', 'renamed_attributes')
                    projection = "renamed_attributes['first']"
                else:
                    change = SchemaChange.update_column_type(
                        ['attributes', 'value'], AtomicType('BIGINT'))
                    projection = "attributes['first']"
                self.catalog.alter_table(identifier, [change], False)

                result = self._read(
                    self.catalog.get_table(identifier), [projection])

                self.assertEqual(
                    [10, None, None], result.column(0).to_pylist())
                if evolution == 'value_type':
                    self.assertEqual(pa.int64(), result.schema.field(0).type)

    def test_projects_map_key_with_row_tracking_fields(self):
        table = self._write_table('row_tracking_fields', {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })

        for field in ['_ROW_ID', '_SEQUENCE_NUMBER']:
            with self.subTest(field=field):
                result = self._read(
                    table, ["attributes['first']", field])

                self.assertEqual(
                    ['attributes_first', field], result.column_names)
                self.assertEqual(
                    [10, None, None],
                    result.column('attributes_first').to_pylist(),
                )
                self.assertEqual(
                    [0, 1, 2] if field == '_ROW_ID' else [1, 1, 1],
                    result.column(field).to_pylist(),
                )

    def test_rejects_filter_on_projected_map_key(self):
        table = self._write_table('map_key_filter', {})
        builder = table.new_read_builder().with_projection(
            ['id', "attributes['first']"])

        with self.assertRaisesRegex(
                NotImplementedError, 'Filtering projected MAP keys'):
            builder.new_predicate_builder().equal('attributes_first', 10)

    def test_rejects_map_key_filter_after_projection_changes(self):
        table = self._write_table('changed_projection_filter', {})
        builder = table.new_read_builder().with_projection(
            ["attributes['first']"])
        predicate_builder = builder.new_predicate_builder()
        builder.with_projection(['id'])

        with self.assertRaisesRegex(
                NotImplementedError, 'Filtering projected MAP keys'):
            predicate_builder.equal('attributes_first', 10)

    def test_filters_physical_column_with_map_key_projection(self):
        table = self._write_table('physical_filter', {})
        builder = table.new_read_builder()
        predicate = builder.new_predicate_builder().equal('id', 1)
        builder.with_projection(
            ["attributes['first']"]).with_filter(predicate)

        result = builder.new_read().to_arrow(
            builder.new_scan().plan().splits())

        self.assertEqual([10], result.column(0).to_pylist())

    def test_data_evolution_filters_unprojected_physical_column(self):
        table = self._write_table('data_evolution_physical_filter', {
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        builder = table.new_read_builder()
        predicate = builder.new_predicate_builder().equal('id', 1)
        builder.with_projection(
            ["attributes['first']"]).with_filter(predicate)

        result = builder.new_read().to_arrow(
            builder.new_scan().plan().splits())

        self.assertEqual([10], result.column(0).to_pylist())

    def test_physical_filter_with_conflicting_map_key_alias(self):
        table = self._write_alias_collision_table('alias_filter')
        builder = table.new_read_builder()
        predicate = builder.new_predicate_builder().equal(
            'attributes_first', 100)
        builder.with_projection(
            ["attributes['first']"]).with_filter(predicate)

        result = builder.new_read().to_arrow(
            builder.new_scan().plan().splits())

        self.assertEqual(['attributes_first__0'], result.column_names)
        self.assertEqual([10], result.column(0).to_pylist())

    def test_projects_physical_column_with_conflicting_map_key_alias(self):
        table = self._write_alias_collision_table('alias_projection')
        builder = table.new_read_builder()
        predicate = builder.new_predicate_builder().equal(
            'attributes_first', 100)
        builder.with_projection([
            "attributes['first']",
            'attributes_first',
        ]).with_filter(predicate)

        result = builder.new_read().to_arrow(
            builder.new_scan().plan().splits())

        self.assertEqual(
            ['attributes_first__0', 'attributes_first'],
            result.column_names,
        )
        self.assertEqual({
            'attributes_first__0': [10],
            'attributes_first': [100],
        }, result.to_pydict())

    def test_rejects_conflicting_derived_map_key_filter(self):
        table = self._write_alias_collision_table('alias_derived_filter')
        builder = table.new_read_builder().with_projection(
            ["attributes['first']"])

        with self.assertRaisesRegex(
                NotImplementedError, 'Filtering projected MAP keys'):
            builder.new_predicate_builder().equal('attributes_first__0', 10)

    def test_rejects_query_auth_for_projected_map_key(self):
        table = self._write_table('map_key_auth', {})
        seen = []
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={'attributes': '{"name":"NULL"}'},
        )
        table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: (
                seen.append(select) or auth))
        builder = table.new_read_builder().with_projection(
            ['id', "attributes['first']"])
        splits = builder.new_scan().plan().splits()

        self.assertEqual([['id', 'attributes']], seen)
        with self.assertRaisesRegex(
                NotImplementedError, 'with query authorization'):
            builder.new_read().to_arrow(splits)

    def test_unencodable_map_key_uses_full_map_fallback(self):
        table = self._write_table('special_key', {})

        result = self._read(table, ["attributes['a;b']"])

        self.assertEqual(
            [30, None, None],
            result.column('attributes_a;b').to_pylist(),
        )

    def test_projects_key_from_dotted_map_column(self):
        pa_schema = pa.schema([
            ('a.b', pa.map_(pa.string(), pa.int64())),
        ])
        self.catalog.create_table(
            'default.dotted_map_column',
            Schema.from_pyarrow_schema(
                pa_schema,
                options={'bucket': '-1', 'file.format': 'parquet'},
            ),
            False,
        )
        table = self.catalog.get_table('default.dotted_map_column')
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.Table.from_arrays([
            pa.array([[('c', 10)]], type=pa_schema.field('a.b').type),
        ], schema=pa_schema))
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

        result = self._read(table, ["a.b['c']"])

        self.assertEqual([10], result.column('a.b_c').to_pylist())

    def test_full_map_and_selected_key_use_full_map_fallback(self):
        table = self._write_table('full_and_selected', {})

        result = self._read(
            table, ['attributes', "attributes['first']"])

        self.assertEqual(
            ['attributes', 'attributes_first'], result.column_names)
        self.assertEqual(
            [10, None, None],
            result.column('attributes_first').to_pylist(),
        )
        self.assertEqual(
            [[('first', 10), ('key.with.dots', 20), ('a;b', 30)], [], None],
            result.column('attributes').to_pylist(),
        )

    def test_projects_map_blob_keys(self):
        map_type = pa.map_(pa.string(), pa.large_binary())
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', map_type),
        ])
        self.catalog.create_table(
            'default.map_blob',
            Schema.from_pyarrow_schema(
                pa_schema,
                options={
                    'row-tracking.enabled': 'true',
                    'data-evolution.enabled': 'true',
                },
            ),
            False,
        )
        table = self.catalog.get_table('default.map_blob')
        data = pa.Table.from_arrays([
            pa.array([1, 2, 3, 4], type=pa.int32()),
            pa.array([
                [('k', b'hello'), ('v', b'world')],
                [('k', None)],
                [],
                None,
            ], type=map_type),
        ], schema=pa_schema)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

        full = self._read(table, ['payload']).column('payload').to_pylist()
        selected = self._read(
            table, ["payload['k']", "payload['missing']"])

        self.assertEqual(
            [b'hello', None, None, None],
            selected.column('payload_k').to_pylist(),
        )
        self.assertEqual(
            [None, None, None, None],
            selected.column('payload_missing').to_pylist(),
        )
        self.assertEqual(
            [None if row is None else dict(row).get('k') for row in full],
            selected.column('payload_k').to_pylist(),
        )

        row_ids = self._read(table, ['id', '_ROW_ID']).to_pylist()
        update_builder = table.new_batch_write_builder()
        update = update_builder.new_update().with_update_type(['payload'])
        messages = update.update_by_arrow_with_row_id(pa.Table.from_pydict({
            '_ROW_ID': pa.array([row_ids[0]['_ROW_ID']], type=pa.int64()),
            'payload': pa.array([[('k', b'updated')]], type=map_type),
        }))
        update_builder.new_commit().commit(messages)

        full = self._read(table, ['payload']).column('payload').to_pylist()
        selected = self._read(table, ["payload['k']"])
        self.assertEqual(
            [b'updated', None, None, None],
            selected.column('payload_k').to_pylist(),
        )
        self.assertEqual(
            [None if row is None else dict(row).get('k') for row in full],
            selected.column('payload_k').to_pylist(),
        )

        from pypaimon.table.row.blob import BlobDescriptor
        descriptors = self._read(
            table.copy({'blob-as-descriptor': 'true'}),
            ["payload['k']"],
        ).column('payload_k').to_pylist()
        self.assertIsInstance(
            BlobDescriptor.deserialize(descriptors[0]), BlobDescriptor)

    def test_projects_map_key_after_column_rename(self):
        self._write_table('renamed', {})
        self.catalog.alter_table(
            'default.renamed',
            [SchemaChange.rename_column('attributes', 'renamed_attributes')],
            False,
        )

        result = self._read(
            self.catalog.get_table('default.renamed'),
            ['id', "renamed_attributes['first']"],
        )

        self.assertEqual({
            'id': [1, 2, 3],
            'renamed_attributes_first': [10, None, None],
        }, result.to_pydict())

    def test_projects_map_key_across_value_type_evolution(self):
        value_type = pa.struct([('nested_value', pa.int32())])
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('attributes', pa.map_(pa.string(), value_type)),
        ])
        self.catalog.create_table(
            'default.evolved',
            Schema.from_pyarrow_schema(
                pa_schema,
                options={'bucket': '-1', 'file.format': 'parquet'},
            ),
            False,
        )
        table = self.catalog.get_table('default.evolved')
        data = pa.Table.from_arrays([
            pa.array([1], type=pa.int32()),
            pa.array([
                [('first', {'nested_value': 10})],
            ], type=pa.map_(pa.string(), value_type)),
        ], schema=pa_schema)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()
        self.catalog.alter_table(
            'default.evolved',
            [SchemaChange.update_column_type(
                ['attributes', 'value', 'nested_value'],
                AtomicType('BIGINT'),
            )],
            False,
        )

        result = self._read(
            self.catalog.get_table('default.evolved'),
            ["attributes['first']"],
        )

        self.assertEqual(
            pa.struct([('nested_value', pa.int64())]),
            result.schema.field(0).type,
        )
        self.assertEqual(
            [{'nested_value': 10}], result.column(0).to_pylist())

    def _write_table(self, name, extra_options, value_type=pa.int64()):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('attributes', pa.map_(pa.string(), value_type)),
        ])
        options = {'bucket': '-1', 'file.format': 'parquet'}
        options.update(extra_options)
        schema = Schema.from_pyarrow_schema(pa_schema, options=options)
        identifier = 'default.{}'.format(name)
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        data = pa.Table.from_arrays([
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([
                [('first', 10), ('key.with.dots', 20), ('a;b', 30)],
                [],
                None,
            ], type=pa.map_(pa.string(), value_type)),
        ], schema=pa_schema)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()
        return self.catalog.get_table(identifier)

    def _write_alias_collision_table(self, name):
        pa_schema = pa.schema([
            ('attributes', pa.map_(pa.string(), pa.int64())),
            ('attributes_first', pa.int64()),
        ])
        self.catalog.create_table(
            'default.' + name,
            Schema.from_pyarrow_schema(
                pa_schema,
                options={'bucket': '-1', 'file.format': 'parquet'},
            ),
            False,
        )
        table = self.catalog.get_table('default.' + name)
        data = pa.Table.from_arrays([
            pa.array([
                [('first', 10)],
                [('first', 20)],
            ], type=pa_schema.field('attributes').type),
            pa.array([100, 200], type=pa.int64()),
        ], schema=pa_schema)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()
        return self.catalog.get_table('default.' + name)

    @staticmethod
    def _read(table, projection):
        builder = table.new_read_builder().with_projection(projection)
        return builder.new_read().to_arrow(builder.new_scan().plan().splits())


if __name__ == '__main__':
    unittest.main()
