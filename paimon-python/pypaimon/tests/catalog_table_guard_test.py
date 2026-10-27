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
# limitations under the License.
#################################################################################

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.rest.property_change import PropertyChange
from pypaimon.common.identifier import Identifier
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange


def assert_table_guards(test, catalog, database, branch_reads=True):
    """Table operations reject branch and system tables, as in Java, and leave the table untouched."""
    pa_schema = pa.schema([('id', pa.int32())])
    schema = Schema.from_pyarrow_schema(pa_schema)
    catalog.create_database(database, True)
    name = database + '.t'
    catalog.create_table(name, schema, False)
    table = catalog.get_table(name)
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    table_write.write_arrow(pa.Table.from_pydict({'id': [1, 2]}, schema=pa_schema))
    table_commit.commit(table_write.prepare_commit())
    table_write.close()
    table_commit.close()
    table.branch_manager().create_branch('dev')

    add_column = SchemaChange.add_column('x', AtomicType('INT'))
    for target in (name + '$snapshots', name + '$branch_dev', name + '$branch_main'):
        operations = {
            'create_table': lambda: catalog.create_table(target, schema, True),
            'drop_table': lambda: catalog.drop_table(target, True),
            'rename_table': lambda: catalog.rename_table(target, database + '.renamed'),
        }
        if target.endswith('$snapshots'):
            operations['alter_table'] = lambda: catalog.alter_table(target, [add_column])
        for method, operation in operations.items():
            with test.subTest(target=target, method=method):
                with test.assertRaises(ValueError) as context:
                    operation()
                test.assertIn("Cannot '{}'".format(method), str(context.exception))
        with test.subTest(target=target, method='rename_table to'):
            with test.assertRaises(ValueError):
                catalog.rename_table(name, target)

    test.assertEqual(catalog.list_tables(database), ['t'])
    table = catalog.get_table(name)
    test.assertEqual([field.name for field in table.table_schema.fields], ['id'])
    read_builder = table.new_read_builder()
    result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
    test.assertEqual(sorted(result.column('id').to_pylist()), [1, 2])
    if branch_reads:
        test.assertIsNotNone(catalog.get_table(name + '$branch_dev'))

    # Identifier(db, t, branch='main') is encoded as and equal to db.t, so it is accepted.
    catalog.create_table(Identifier(database, 'm', branch='main'), schema, False)
    catalog.rename_table(Identifier(database, 'm', branch='main'), Identifier(database, 'm2', branch='MAIN'))
    test.assertEqual(sorted(catalog.list_tables(database)), ['m2', 't'])
    catalog.drop_table(Identifier(database, 'm2', branch='main'), False)
    test.assertEqual(catalog.list_tables(database), ['t'])

    # As in Java, the sys database and the tables in it cannot be created, dropped or changed.
    operations = [
        lambda: catalog.create_database('sys', True),
        lambda: catalog.drop_database('sys', True),
        lambda: catalog.create_table('sys.t', schema, True),
        lambda: catalog.drop_table('sys.t', True),
        lambda: catalog.rename_table(name, 'sys.t'),
        lambda: catalog.alter_table('sys.t', [add_column]),
    ]
    if type(catalog).alter_database is not Catalog.alter_database:
        operations.append(lambda: catalog.alter_database('sys', [PropertyChange.set_property('k', 'v')]))
    for operation in operations:
        with test.assertRaises(ValueError):
            operation()
    with test.assertRaisesRegex(ValueError, "Can't do operation on system database."):
        catalog.create_database('sys', False)
    test.assertNotIn('sys', catalog.list_databases())
    test.assertEqual(catalog.list_tables(database), ['t'])


class CatalogTableGuardTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.warehouse = os.path.join(self.temp_dir, "warehouse")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_filesystem_catalog(self):
        assert_table_guards(self, CatalogFactory.create({"warehouse": self.warehouse}), 'db')

    def test_jdbc_catalog(self):
        catalog = CatalogFactory.create({
            "metastore": "jdbc",
            "warehouse": self.warehouse,
            "uri": "jdbc:sqlite:" + os.path.join(self.temp_dir, "catalog.db"),
            "catalog-key": "test-jdbc-catalog",
        })
        assert_table_guards(self, catalog, 'db', branch_reads=False)
        add_column = SchemaChange.add_column('x', AtomicType('INT'))
        catalog.get_table('db.t').branch_manager().create_branch('MAIN')
        for target in ('db.t$branch_dev', 'db.t$branch_MAIN'):
            with self.subTest(target=target):
                with self.assertRaises(NotImplementedError):
                    catalog.alter_table(target, [add_column])
        self.assertEqual([field.name for field in catalog.get_table('db.t').table_schema.fields], ['id'])
        catalog.alter_table(Identifier('db', 't', branch='MAIN'), [add_column])
        self.assertEqual([field.name for field in catalog.get_table('db.t').table_schema.fields], ['id', 'x'])


if __name__ == '__main__':
    unittest.main()
