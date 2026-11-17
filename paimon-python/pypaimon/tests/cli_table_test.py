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

import json
import os
import shutil
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.cli.cli import main
from pypaimon.common.json_util import JSON
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.row.generic_row import GenericRow


class CliTableTest(unittest.TestCase):
    """Integration tests for CLI with real catalog and table operations."""

    @classmethod
    def setUpClass(cls):
        """Set up test catalog, database, and table with sample data."""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        
        # Create catalog
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('test_db', True)
        
        # Create test table with sample data
        cls._create_test_table()
        
        # Create catalog config file
        cls.config_file = os.path.join(cls.tempdir, 'paimon.yaml')
        with open(cls.config_file, 'w') as f:
            f.write(f"metastore: filesystem\nwarehouse: {cls.warehouse}\n")

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directory."""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @classmethod
    def _create_test_table(cls):
        """Create a test table and insert sample data."""
        # Define schema
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('city', pa.string())
        ])
        
        schema = Schema.from_pyarrow_schema(pa_schema)
        cls.catalog.create_table('test_db.users', schema, False)
        
        # Get table and write data
        table = cls.catalog.get_table('test_db.users')
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        
        # Create sample data
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'city': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou']
        }
        
        table_data = pa.Table.from_pydict(data, schema=pa_schema)
        table_write.write_arrow(table_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @staticmethod
    def _snapshot(snapshot_id, **overrides):
        values = dict(version=3, id=snapshot_id, schema_id=0,
                      base_manifest_list='base-{}'.format(snapshot_id),
                      delta_manifest_list='delta-{}'.format(snapshot_id),
                      total_record_count=3, delta_record_count=3,
                      commit_user='user-{}'.format(snapshot_id), commit_identifier=100,
                      commit_kind='APPEND', time_millis=1700000000123, uuid=None)
        values.update(overrides)
        return Snapshot(**values)

    def _create_snapshot_table(self, snapshots=None, primary_keys=None, options=None, suffix='snapshots'):
        """Create isolated metadata in the shared catalog without manifest or data files."""
        identifier = 'test_db.{}_{}'.format(self._testMethodName, suffix)
        fields = [DataField.from_dict({'id': 0, 'name': 'id', 'type': 'INT NOT NULL'})]
        self.catalog.create_table(identifier, Schema(fields=fields, primary_keys=primary_keys, options=options), False)
        table = self.catalog.get_table(identifier)
        manager = table.snapshot_manager()
        if snapshots is None:
            # Deliberately unordered, non-contiguous IDs distinguish numeric ordering
            # from lexical ordering. Referenced manifests/data files do not exist.
            snapshots = [
                self._snapshot(10, schema_id=2, total_record_count=9007199254740993,
                               next_row_id=9007199254740993, properties={'job': 'test'},
                               writer_version='test-writer', watermark=0),
                self._snapshot(2, schema_id=1, commit_kind='COMPACT', delta_record_count=0,
                               changelog_record_count=0, changelog_manifest_list='changelog-2'),
                self._snapshot(1, total_record_count=0, delta_record_count=0),
            ]
        if snapshots:
            table.file_io.mkdirs(manager.snapshot_dir)
            for snapshot in snapshots:
                table.file_io.try_to_write_atomic(manager.get_snapshot_path(snapshot.id), JSON.to_json(snapshot))
            table.file_io.try_to_write_atomic(manager.latest_file, str(max(s.id for s in snapshots)))
        return identifier, table

    def _create_schema_table(self):
        """Create schema history whose snapshots reference different field definitions."""
        identifier, table = self._create_snapshot_table(
            snapshots=[self._snapshot(4, schema_id=0), self._snapshot(5, schema_id=2),
                       self._snapshot(6, schema_id=10)], suffix='schemas')
        definitions = [
            {'id': 0, 'name': 'id', 'type': 'BIGINT NOT NULL', 'description': 'Identifier'},
            {'id': 1, 'name': 'region', 'type': 'STRING'},
            {'id': 2, 'name': 'profile', 'type': {'type': 'ROW', 'fields': [
                {'id': 3, 'name': 'city', 'type': 'STRING', 'description': '城市'}]}},
            {'id': 4, 'name': 'age', 'type': 'INT'},
        ]
        for schema_id, field_count in ((10, 4), (0, 2), (2, 3)):
            schema = TableSchema.from_schema(schema_id, Schema(
                fields=[DataField.from_dict(f) for f in definitions[:field_count]],
                partition_keys=['region'], options={'bucket': '-1', 'custom': '值'},
                comment='Schema {}'.format(schema_id)))
            schema.time_millis = 1700000000123 + schema_id
            with open(table.schema_manager._to_schema_path(schema_id), 'w') as f:
                f.write(JSON.to_json(schema))
        return identifier, table

    def _create_manifest_table(self, primary_keys=None, options=None, suffix='manifests'):
        """Write real Avro metadata without creating the referenced data files."""
        identifier, table = self._create_snapshot_table(
            snapshots=[self._snapshot(1), self._snapshot(2, changelog_manifest_list='change-2')],
            primary_keys=primary_keys, options=options, suffix=suffix)
        empty = GenericRow([], [])
        key = GenericRow([7] if primary_keys else [], table.trimmed_primary_keys_fields)
        stats = SimpleStats(GenericRow([7], table.fields), GenericRow([8], table.fields), [0])
        entry = ManifestEntry(0, empty, 0, table.total_buckets, DataFileMeta(
            file_name='data-test.parquet', file_size=1024, row_count=2,
            min_key=key, max_key=key, key_stats=SimpleStats(key, key, [0] if primary_keys else []),
            value_stats=stats, min_sequence_number=9007199254740993, max_sequence_number=9007199254740994,
            schema_id=0, level=0, extra_files=[], creation_time=Timestamp.from_epoch_millis(1700000000123),
            file_source=0))
        file_manager = ManifestFileManager(table)
        table.file_io.mkdirs(file_manager.manifest_path)
        metas = []
        for name in ('manifest-base', 'manifest-delta', 'manifest-change'):
            file_manager.write(name, [entry])
            metas.append(ManifestFileMeta(
                name, os.path.getsize(os.path.join(file_manager.manifest_path, name)), 1, 0,
                SimpleStats(empty, empty, []), 0))
        manager = ManifestListManager(table)
        for name, files in (('base-1', [metas[0]]), ('delta-1', []),
                            ('base-2', [metas[0]]), ('delta-2', [metas[1]]), ('change-2', [metas[2]])):
            manager.write(name, files)
        return identifier, table, entry

    def _run_table_command(self, *argv, config=None):
        """Run a table command and capture stdout, stderr and the exit code."""
        out, err = StringIO(), StringIO()
        code = 0
        with patch('sys.argv', ['paimon', '-c', config or self.config_file, 'table'] + list(argv)):
            with patch('sys.stdout', out), patch('sys.stderr', err):
                try:
                    main()
                except SystemExit as e:
                    code = e.code
        return out.getvalue(), err.getvalue(), code

    def _run_table_command_success(self, *argv):
        out, err, code = self._run_table_command(*argv)
        self.assertEqual(0, code, err)
        self.assertEqual('', err)
        return out

    def test_cli_table_read_basic(self):
        """Test basic table read via CLI."""
        # Simulate CLI command: paimon -c <config> table read test_db.users
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'read', 'test_db.users']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                
                # Verify output contains data
                self.assertIn('Alice', output)
                self.assertIn('Bob', output)
                self.assertIn('Beijing', output)
                self.assertIn('Shanghai', output)
                # Verify header
                self.assertIn('id', output.lower())
                self.assertIn('name', output.lower())

    def test_cli_table_read_with_limit(self):
        """Test table read with max results limit via CLI."""
        # Simulate CLI command: paimon table read test_db.users -n 2
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'read', 'test_db.users', '-l', '2']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                
                # Verify output contains limited data (only first 2 rows)
                lines = [line for line in output.split('\n') if line.strip()]
                # Should have header + 2 data rows
                self.assertLessEqual(len(lines), 4)  # header + 2 data rows + possible empty lines

    def test_cli_table_read_with_select(self):
        """Test table read with column selection via CLI."""
        # Simulate CLI command: paimon table read test_db.users --select id,name
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users', '--select', 'id,name']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                
                # Verify output contains selected columns
                self.assertIn('id', output.lower())
                self.assertIn('name', output.lower())
                # Verify selected data is present
                self.assertIn('Alice', output)
                self.assertIn('Bob', output)

    def test_cli_table_read_with_invalid_select(self):
        """Test table read with invalid column selection via CLI."""
        # Simulate CLI command: paimon table read test_db.users --select invalid_column
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users', '--select', 'invalid_col1,invalid_col2']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                try:
                    main()
                except SystemExit:
                    pass
                
                error_output = mock_stderr.getvalue()
                
                # Verify error message contains information about invalid column
                self.assertIn("Column(s) ['invalid_col1', 'invalid_col2'] do not exist in table", error_output)

    def test_cli_with_custom_config_path(self):
        """Test CLI with custom configuration file path."""
        # Create a different config file
        custom_config = os.path.join(self.tempdir, 'custom_catalog.yaml')
        with open(custom_config, 'w') as f:
            f.write(f"metastore: filesystem\nwarehouse: {self.warehouse}\n")
        
        with patch('sys.argv', ['paimon', '-c', custom_config, 'table', 'read', 'test_db.users']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('Alice', output)

    def test_cli_table_get_basic(self):
        """Test basic table get via CLI."""
        # Simulate CLI command: paimon -c <config> table get test_db.users
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'get', 'test_db.users']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                
                # Verify output is valid JSON
                import json
                schema_json = json.loads(output)
                
                # Verify schema structure
                self.assertIn('fields', schema_json)
                self.assertIsInstance(schema_json['fields'], list)
                
                # Verify field names are present
                field_names = [field['name'] for field in schema_json['fields']]
                self.assertIn('id', field_names)
                self.assertIn('name', field_names)
                self.assertIn('age', field_names)
                self.assertIn('city', field_names)

                self.assertEqual({'fields', 'partitionKeys', 'primaryKeys', 'options', 'comment'}, set(schema_json))
                expected = JSON.to_json(self.catalog.get_table('test_db.users').table_schema.to_schema(), indent=2)
                self.assertEqual(expected + '\n', output)

    def test_cli_table_get_schema_versions(self):
        identifier, _ = self._create_schema_table()
        cases = [((), ['id', 'region', 'profile', 'age']),
                 (('--schema-id', '0'), ['id', 'region']),
                 (('--schema-id', '2', '-f', 'json'), ['id', 'region', 'profile']),
                 (('--snapshot-id', '4'), ['id', 'region']),
                 (('--snapshot-id', '5'), ['id', 'region', 'profile'])]
        with patch.object(SnapshotManager, 'get_latest_snapshot', side_effect=AssertionError('latest snapshot lookup')):
            for options, field_names in cases:
                with self.subTest(options=options):
                    output = self._run_table_command_success('get', identifier, *options)
                    schema = json.loads(output)
                    self.assertEqual({'fields', 'partitionKeys', 'primaryKeys', 'options', 'comment'}, set(schema))
                    self.assertEqual(field_names, [f['name'] for f in schema['fields']])
                    self.assertEqual(field_names, [f.name for f in JSON.from_json(output, Schema).fields])

    def test_cli_table_list_schemas_json(self):
        identifier, _ = self._create_schema_table()
        output = self._run_table_command_success('list-schemas', identifier)
        schemas = json.loads(output)
        self.assertEqual([0, 2, 10], [s['id'] for s in schemas])
        self.assertEqual([2, 3, 4], [len(s['fields']) for s in schemas])
        self.assertEqual([1, 3, 4], [s['highestFieldId'] for s in schemas])
        self.assertEqual(3, schemas[1]['version'])
        self.assertEqual(1700000000125, schemas[1]['timeMillis'])
        self.assertEqual(['region'], schemas[1]['partitionKeys'])
        self.assertEqual([], schemas[1]['primaryKeys'])
        self.assertEqual({'bucket': '-1', 'custom': '值'}, schemas[1]['options'])
        self.assertEqual('Schema 2', schemas[1]['comment'])
        self.assertEqual({'id': 0, 'name': 'id', 'type': 'BIGINT NOT NULL', 'description': 'Identifier'},
                         schemas[1]['fields'][0])
        profile = schemas[1]['fields'][2]
        self.assertEqual(2, profile['id'])
        self.assertTrue(profile['type']['nullable'])
        self.assertEqual([{'id': 3, 'name': 'city', 'type': 'STRING', 'description': '城市'}],
                         profile['type']['fields'])
        self.assertIn('城市', output)
        self.assertEqual(schemas, json.loads(self._run_table_command_success('list-schemas', identifier, '-f', 'json')))

    def test_cli_table_schemas_format_table(self):
        identifier, _ = self._create_schema_table()
        for command in ('get', 'list-schemas'):
            with self.subTest(command=command):
                output = self._run_table_command_success(command, identifier, '--format', 'table')
                for value in ('id', 'region', 'profile', 'city', 'BIGINT NOT NULL', 'Identifier', 'Schema 10', '值'):
                    self.assertIn(value, output)
                if command == 'list-schemas':
                    self.assertEqual(['schema_id', 'fields', 'partition_keys', 'primary_keys',
                                      'options', 'comment', 'update_time'], output.splitlines()[0].split())
                    self.assertIn('2023-11-14T22:13:20.125Z', output)
                    self.assertIn('{"bucket":"-1","custom":"值"}', output)
                    self.assertIn('{"id":3,"name":"city","type":"STRING","description":"城市"}', output)
        output = self._run_table_command_success('list-schemas', 'test_db.users', '--format', 'table')
        self.assertRegex(output, r'-\s+\d{4}-\d{2}-\d{2}')

    def test_cli_table_list_schemas_limit(self):
        identifier, table = self._create_schema_table()
        schemas = json.loads(self._run_table_command_success('list-schemas', identifier, '-l', '2'))
        self.assertEqual([0, 2], [s['id'] for s in schemas])
        # The manager must stop before reading an unselected, corrupt schema.
        with open(table.schema_manager._to_schema_path(5), 'w') as f:
            f.write('{invalid json')
        schemas = json.loads(self._run_table_command_success('list-schemas', identifier, '-l', '2'))
        self.assertEqual([0, 2], [s['id'] for s in schemas])

    def test_cli_table_list_schemas_skips_missing(self):
        identifier, _ = self._create_schema_table()
        original = SchemaManager.get_schema

        def disappearing_read(manager, schema_id):
            return None if schema_id == 0 else original(manager, schema_id)

        with patch.object(SchemaManager, 'get_schema', autospec=True, side_effect=disappearing_read):
            schemas = json.loads(self._run_table_command_success('list-schemas', identifier, '--limit', '1'))
        self.assertEqual([2], [s['id'] for s in schemas])

    def test_cli_table_schemas_without_snapshots(self):
        identifier, _ = self._create_snapshot_table(snapshots=[], suffix='schemas')
        with patch.object(SnapshotManager, 'get_latest_snapshot', side_effect=AssertionError('snapshot lookup')):
            self.assertEqual([0], [s['id'] for s in json.loads(
                self._run_table_command_success('list-schemas', identifier))])
            schema = json.loads(self._run_table_command_success('get', identifier, '--schema-id', '0'))
        self.assertEqual(['id'], [f['name'] for f in schema['fields']])

    def test_cli_table_list_schemas_empty(self):
        table = self.catalog.get_table('test_db.users')
        with patch('pypaimon.cli.cli_table._open_schema_table', return_value=table):
            with patch.object(table.schema_manager, '_list_versioned_files', return_value=[]):
                self.assertEqual([], json.loads(self._run_table_command_success('list-schemas', 'test_db.users')))
                self.assertIn('No schemas', self._run_table_command_success(
                    'list-schemas', 'test_db.users', '-f', 'table'))

    def test_cli_table_get_missing_schema_or_snapshot(self):
        identifier, table = self._create_schema_table()
        os.remove(table.schema_manager._to_schema_path(2))
        for option, value, target in (('--schema-id', '7', '7'), ('--snapshot-id', '99', '99'),
                                      ('--snapshot-id', '5', '2')):
            with self.subTest(option=option, value=value):
                out, err, code = self._run_table_command('get', identifier, option, value)
                self.assertEqual(('', 1), (out, code))
                self.assertIn(identifier, err)
                self.assertIn(target, err)

    def test_cli_table_schema_invalid_selectors(self):
        for command, option, values in (('get', '--schema-id', ('-1', 'bad')),
                                        ('get', '--snapshot-id', ('0', '-1', 'bad')),
                                        ('list-schemas', '--limit', ('0', '-1', 'bad'))):
            for value in values:
                with self.subTest(command=command, option=option, value=value):
                    out, err, code = self._run_table_command(command, 'test_db.users', option, value)
                    self.assertEqual(('', 2), (out, code))
                    self.assertIn(option, err)
        out, err, code = self._run_table_command('get', 'test_db.users', '--schema-id', '0', '--snapshot-id', '1')
        self.assertEqual(('', 2), (out, code))
        self.assertIn('not allowed with argument', err)

    def test_cli_table_get_snapshot_without_schema_id(self):
        identifier, table = self._create_schema_table()
        snapshot = json.loads(JSON.to_json(self._snapshot(5, schema_id=None)))
        for missing in (False, True):
            with self.subTest(missing=missing):
                if missing:
                    del snapshot['schemaId']
                with open(table.snapshot_manager().get_snapshot_path(5), 'w') as f:
                    json.dump(snapshot, f)
                out, err, code = self._run_table_command('get', identifier, '--snapshot-id', '5')
                self.assertEqual(('', 1), (out, code))
                self.assertIn('5', err)
                self.assertIn('schema', err.lower())

    def test_cli_table_schemas_invalid_target(self):
        for command in ('get', 'list-schemas'):
            identifiers = ('db', '.t', 'db.', 'db.t.extra', 'test_db.missing_schema_table', 'test_db.users$schemas')
            for identifier in identifiers:
                with self.subTest(command=command, identifier=identifier):
                    out, err, code = self._run_table_command(command, identifier)
                    self.assertEqual(('', 1), (out, code))
                    self.assertIn(identifier, err)
            out, err, code = self._run_table_command(
                command, 'test_db.users', config=os.path.join(self.tempdir, 'missing.yaml'))
            self.assertEqual(('', 1), (out, code))
            self.assertIn('missing.yaml', err)

    def test_cli_table_schemas_corrupt_metadata(self):
        identifier, table = self._create_schema_table()
        cases = [(table.schema_manager._to_schema_path(2),
                  [('get', '--schema-id', '2'), ('get', '--snapshot-id', '5'), ('list-schemas',)]),
                 (table.snapshot_manager().get_snapshot_path(5), [('get', '--snapshot-id', '5')])]
        for path, commands in cases:
            with open(path) as f:
                original = f.read()
            with open(path, 'w') as f:
                f.write('{invalid json')
            for command in commands:
                with self.subTest(path=path, command=command):
                    out, err, code = self._run_table_command(command[0], identifier, *command[1:])
                    self.assertEqual(('', 1), (out, code))
                    self.assertIn(identifier, err)
            with open(path, 'w') as f:
                f.write(original)

    def test_cli_table_schemas_permission_error(self):
        identifier, table = self._create_schema_table()
        file_io = table.file_io
        original = type(file_io).read_file_utf8

        def denied_read(io, path):
            if path.endswith('/schema-2'):
                raise PermissionError('access denied')
            return original(io, path)

        with patch.object(type(file_io), 'read_file_utf8', autospec=True, side_effect=denied_read):
            for command in (('get', '--schema-id', '2'), ('get', '--snapshot-id', '5'), ('list-schemas',)):
                with self.subTest(command=command):
                    out, err, code = self._run_table_command(command[0], identifier, *command[1:])
                    self.assertEqual(('', 1), (out, code))
                    self.assertIn(identifier, err)

    def test_cli_table_schemas_table_modes(self):
        modes = [('append', [], {'bucket': '-1'}),
                 ('primary_key', ['id'], {'bucket': '1'}),
                 ('data_evolution', [], {'bucket': '-1', 'row-tracking.enabled': 'true',
                                         'data-evolution.enabled': 'true'})]
        for name, primary_keys, options in modes:
            with self.subTest(mode=name):
                identifier, _ = self._create_snapshot_table(
                    snapshots=[], primary_keys=primary_keys, options=options, suffix=name)
                schemas = json.loads(self._run_table_command_success('list-schemas', identifier))
                self.assertEqual([0], [s['id'] for s in schemas])
                self.assertEqual(primary_keys, schemas[0]['primaryKeys'])
                self.assertEqual(options, schemas[0]['options'])
                schema = json.loads(self._run_table_command_success('get', identifier, '--schema-id', '0'))
                self.assertEqual(primary_keys, schema['primaryKeys'])
                self.assertEqual(options, schema['options'])

    def test_cli_table_snapshot_basic(self):
        """Test basic table snapshot via CLI."""
        # Simulate CLI command: paimon -c <config> table snapshot test_db.users
        manager = self.catalog.get_table('test_db.users').snapshot_manager()
        # A second lookup could observe a different concurrent commit.
        with patch.object(SnapshotManager, 'get_latest_snapshot', wraps=manager.get_latest_snapshot) as latest:
            output = self._run_table_command_success('snapshot', 'test_db.users')
        latest.assert_called_once_with()
        
        # Verify output is valid JSON
        snapshot_json = json.loads(output)
        
        # Verify snapshot structure
        self.assertIn('id', snapshot_json)
        self.assertIn('schemaId', snapshot_json)
        self.assertIn('commitKind', snapshot_json)
        self.assertIn('timeMillis', snapshot_json)
        self.assertIn('totalRecordCount', snapshot_json)
        self.assertIn('deltaRecordCount', snapshot_json)
        
        # Verify snapshot has valid data
        self.assertGreater(snapshot_json['id'], 0)
        self.assertGreater(snapshot_json['totalRecordCount'], 0)

    def test_cli_table_list_snapshots_json(self):
        identifier, _ = self._create_snapshot_table()
        snapshots = json.loads(self._run_table_command_success('list-snapshots', identifier, '-f', 'json'))
        self.assertEqual([1, 2, 10], [s['id'] for s in snapshots])
        self.assertEqual([0, 1, 2], [s['schemaId'] for s in snapshots])
        self.assertEqual(0, snapshots[0]['totalRecordCount'])
        self.assertNotIn('changelogRecordCount', snapshots[0])
        self.assertEqual(0, snapshots[1]['changelogRecordCount'])
        self.assertEqual('COMPACT', snapshots[1]['commitKind'])
        self.assertEqual('changelog-2', snapshots[1]['changelogManifestList'])
        self.assertEqual(9007199254740993, snapshots[2]['totalRecordCount'])
        self.assertIsInstance(snapshots[2]['totalRecordCount'], int)
        self.assertEqual({'job': 'test'}, snapshots[2]['properties'])
        self.assertEqual(1700000000123, snapshots[2]['timeMillis'])

    def test_cli_table_list_snapshots_format_table(self):
        identifier, _ = self._create_snapshot_table()
        out = self._run_table_command_success('list-snapshots', identifier)
        for text in ('Schema', 'Commit', 'Total', 'Delta', 'Changelog',
                     'user-2', 'COMPACT', '2023-11-14T22:13:20.123Z', '9007199254740993'):
            self.assertIn(text, out)
        rows = out.strip().splitlines()[1:]
        self.assertEqual(['1', '2', '10'], [row.split()[0] for row in rows])
        self.assertEqual(['0', '0', '-'], rows[0].split()[-3:])
        self.assertEqual(['3', '0', '0'], rows[1].split()[-3:])

    def test_cli_table_list_snapshots_limit(self):
        identifier, _ = self._create_snapshot_table()
        original = SnapshotManager.get_snapshot_by_id
        
        def bounded_read(manager, snapshot_id):
            self.assertIn(snapshot_id, (1, 2), 'read beyond the requested limit')
            return original(manager, snapshot_id)
        
        with patch.object(SnapshotManager, 'get_snapshot_by_id', autospec=True, side_effect=bounded_read):
            snapshots = json.loads(self._run_table_command_success(
                'list-snapshots', identifier, '-l', '2', '-f', 'json'))
        self.assertEqual([1, 2], [s['id'] for s in snapshots])

    def test_cli_table_list_snapshots_skips_expired(self):
        identifier, _ = self._create_snapshot_table()
        original = SnapshotManager.get_snapshot_by_id
        
        def disappearing_read(manager, snapshot_id):
            if snapshot_id == 1:
                return None  # Expired before the existence check.
            if snapshot_id == 2:
                raise FileNotFoundError('expired during read')
            return original(manager, snapshot_id)
        
        with patch.object(SnapshotManager, 'get_snapshot_by_id', autospec=True, side_effect=disappearing_read):
            snapshots = json.loads(self._run_table_command_success(
                'list-snapshots', identifier, '--limit', '1', '-f', 'json'))
        self.assertEqual([10], [s['id'] for s in snapshots])

    def test_cli_table_snapshot_by_id(self):
        identifier, _ = self._create_snapshot_table()
        with patch.object(SnapshotManager, 'get_latest_snapshot', side_effect=AssertionError('latest lookup')):
            snapshot = json.loads(self._run_table_command_success(
                'snapshot', identifier, '--snapshot-id', '2', '--format', 'json'))
        self.assertEqual(2, snapshot['id'])
        self.assertEqual(1, snapshot['schemaId'])
        self.assertEqual(3, snapshot['totalRecordCount'])
        self.assertEqual(0, snapshot['deltaRecordCount'])
        self.assertEqual('base-2', snapshot['baseManifestList'])
        self.assertEqual('delta-2', snapshot['deltaManifestList'])

    def test_cli_table_snapshot_format_table(self):
        identifier, _ = self._create_snapshot_table()
        out = self._run_table_command_success('snapshot', identifier, '-f', 'table')
        for text in ('schemaId', 'baseManifestList', 'base-10', 'deltaManifestList', 'delta-10',
                     'nextRowId', '9007199254740993', 'properties', '{"job": "test"}',
                     'writerVersion', 'test-writer', '2023-11-14T22:13:20.123Z'):
            self.assertIn(text, out)
        self.assertNotIn('changelogManifestList', out)

    def test_cli_table_snapshots_empty(self):
        identifier, _ = self._create_snapshot_table(snapshots=[])
        self.assertEqual([], json.loads(self._run_table_command_success('list-snapshots', identifier, '-f', 'json')))
        self.assertIn('No snapshots', self._run_table_command_success('list-snapshots', identifier))
        out, err, code = self._run_table_command('snapshot', identifier)
        self.assertEqual(('', 1), (out, code))
        self.assertIn('No snapshot', err)

    def test_cli_table_snapshot_missing_id(self):
        identifier, _ = self._create_snapshot_table()
        with patch.object(SnapshotManager, 'get_latest_snapshot', side_effect=AssertionError('latest lookup')):
            out, err, code = self._run_table_command('snapshot', identifier, '--snapshot-id', '9')
        self.assertEqual(('', 1), (out, code))
        self.assertIn('9', err)
        self.assertIn(identifier, err)

    def test_cli_table_snapshot_invalid_numbers(self):
        for command, option in (('list-snapshots', '--limit'), ('snapshot', '--snapshot-id')):
            for value in ('0', '-1', 'bad'):
                with self.subTest(command=command, value=value):
                    out, err, code = self._run_table_command(command, 'test_db.users', option, value)
                    self.assertEqual(('', 2), (out, code))
                    self.assertIn(option, err)

    def test_cli_table_snapshot_invalid_identifiers(self):
        for command in ('snapshot', 'list-snapshots'):
            for table in ('db', '.t', 'db.', 'db.t.extra'):
                with self.subTest(command=command, table=table):
                    out, err, code = self._run_table_command(command, table)
                    self.assertEqual(('', 1), (out, code))
                    self.assertIn('Invalid table identifier', err)

    def test_cli_table_snapshot_invalid_table(self):
        for command in ('snapshot', 'list-snapshots'):
            for table in ('test_db.missing_snapshot_table', 'test_db.users$snapshots'):
                with self.subTest(command=command, table=table):
                    out, err, code = self._run_table_command(command, table)
                    self.assertEqual(('', 1), (out, code))
                    self.assertIn(table, err)

    def test_cli_table_snapshot_config_failure(self):
        for command in ('snapshot', 'list-snapshots'):
            with self.subTest(command=command):
                out, err, code = self._run_table_command(
                    command, 'test_db.users', config=os.path.join(self.tempdir, 'missing.yaml'))
                self.assertEqual(('', 1), (out, code))
                self.assertIn('missing.yaml', err)

    def test_cli_table_snapshot_corrupt_metadata(self):
        identifier, table = self._create_snapshot_table()
        with open(table.snapshot_manager().get_snapshot_path(2), 'w') as f:
            f.write('{invalid json')
        for args in (('snapshot', identifier, '--snapshot-id', '2'), ('list-snapshots', identifier, '-f', 'json')):
            with self.subTest(args=args):
                out, err, code = self._run_table_command(*args)
                self.assertEqual(('', 1), (out, code))
                self.assertIn('snapshot', err.lower())

    def test_cli_table_snapshot_permission_error(self):
        with patch.object(SnapshotManager, 'get_snapshot_by_id', side_effect=PermissionError('access denied')):
            for args in (('snapshot', 'test_db.users', '--snapshot-id', '2'),
                         ('list-snapshots', 'test_db.users', '-f', 'json')):
                with self.subTest(args=args):
                    out, err, code = self._run_table_command(*args)
                    self.assertEqual(('', 1), (out, code))
                    self.assertIn('access denied', err)

    def test_cli_table_snapshots_table_modes(self):
        for name, primary_keys, options in (
                ('append', [], {'bucket': '-1'}),
                ('pk', ['id'], {'bucket': '1'}),
                ('evolution', [], {'bucket': '-1', 'row-tracking.enabled': 'true',
                                   'data-evolution.enabled': 'true'})):
            with self.subTest(mode=name):
                identifier, _ = self._create_snapshot_table(
                    snapshots=[self._snapshot(1, next_row_id=3 if name == 'evolution' else None)],
                    primary_keys=primary_keys, options=options, suffix=name)
                snapshot = json.loads(self._run_table_command_success('snapshot', identifier, '--snapshot-id', '1'))
                self.assertEqual(1, snapshot['id'])
                if name == 'evolution':
                    self.assertEqual(3, snapshot['nextRowId'])
                listed = json.loads(self._run_table_command_success('list-snapshots', identifier, '-f', 'json'))
                self.assertEqual([1], [s['id'] for s in listed])

    def test_cli_table_list_manifests(self):
        identifier, table, _ = self._create_manifest_table()
        manager = ManifestListManager(table)
        meta = manager.read('base-2')[0]
        manager.write('base-2', [replace(meta, min_row_id=9007199254740993, max_row_id=9007199254740994),
                                 replace(meta, file_name='manifest-null')])
        latest = table.snapshot_manager().get_latest_snapshot()
        with patch.object(SnapshotManager, 'get_latest_snapshot',
                          side_effect=[latest, AssertionError('resolved twice')]), \
                patch.object(ManifestFileManager, 'read', side_effect=AssertionError('manifest entries read')):
            result = json.loads(self._run_table_command_success('list-manifests', identifier, '-f', 'json'))
        self.assertEqual((identifier, 2), (result['table'], result['snapshot_id']))
        self.assertEqual([('base', 'base-2'), ('base', 'base-2'), ('delta', 'delta-2'), ('changelog', 'change-2')],
                         [(m['type'], m['manifest_list']) for m in result['manifests']])
        self.assertEqual([9007199254740993, None, None, None], [m['min_row_id'] for m in result['manifests']])
        for options, snapshot_id, names in (
                (('--type', 'delta'), 2, ['manifest-delta']),
                (('--snapshot-id', '1'), 1, ['manifest-base']),
                (('--snapshot-id', '1', '--type', 'changelog'), 1, []),
                (('-l', '1'), 2, ['manifest-base'])):
            with self.subTest(options=options):
                result = json.loads(self._run_table_command_success(
                    'list-manifests', identifier, *options, '-f', 'json'))
                self.assertEqual(snapshot_id, result['snapshot_id'])
                self.assertEqual(names, [m['file_name'] for m in result['manifests']])
        output = self._run_table_command_success('list-manifests', identifier, '--limit', '3')
        header = next(line.split() for line in output.splitlines() if 'num_added_files' in line)
        self.assertEqual(['file_name', 'file_size', 'num_added_files', 'num_deleted_files', 'schema_id',
                          'min_partition_stats', 'max_partition_stats', 'min_row_id', 'max_row_id'], header)
        rows = [line.split() for line in output.splitlines() if line.lstrip().startswith('manifest-')]
        self.assertEqual(['manifest-base', 'manifest-null', 'manifest-delta'], [row[0] for row in rows])
        self.assertEqual(['{}', '{}', '9007199254740993', '9007199254740994'], rows[0][-4:])
        self.assertEqual(['-', '-'], rows[1][-2:])

    def test_cli_table_list_manifests_empty_and_missing_snapshot(self):
        identifier, _ = self._create_snapshot_table(snapshots=[])
        result = json.loads(self._run_table_command_success('list-manifests', identifier, '-f', 'json'))
        self.assertEqual({'table': identifier, 'snapshot_id': None, 'manifests': []}, result)
        self.assertIn('No manifests', self._run_table_command_success('list-manifests', identifier))
        out, err, code = self._run_table_command('list-manifests', identifier, '--snapshot-id', '99')
        self.assertEqual(('', 1), (out, code))
        self.assertIn('Snapshot 99 not found', err)

    def test_cli_table_manifest_preserves_raw_entries_and_formats(self):
        identifier, table, entry = self._create_manifest_table()
        extra = dict(first_row_id=9007199254740993, write_cols=['id'], write_cols_sequences=[9007199254740994],
                     extra_files=['file.index'], embedded_index=b'\x00\xff', delete_row_count=1, file_source=1,
                     external_path='s3://warehouse/external/data-test.parquet')
        # This file is absent from all snapshot manifest lists; its data file does not exist.
        deleted = replace(entry, kind=1, file=replace(entry.file, **extra))
        ManifestFileManager(table).write('manifest-retained', [entry, deleted])
        root = Path(table.table_path)
        before = {path: path.read_bytes() for path in root.rglob('*') if path.is_file()}
        args = ('manifest', identifier, '--file', 'manifest-retained')
        with patch.object(SnapshotManager, 'get_latest_snapshot', side_effect=AssertionError('snapshot read')):
            result = json.loads(self._run_table_command_success(*args))
        self.assertEqual((identifier, 'manifest-retained', table.table_path + '/manifest/manifest-retained'),
                         (result['table'], result['manifest_file'], result['manifest_path']))
        self.assertEqual([('ADD', {}, 0), ('DELETE', {}, 0)],
                         [(e['kind'], e['partition'], e['bucket']) for e in result['entries']])
        file = result['entries'][0]['file']
        expected = dict(file_name='data-test.parquet', file_format='parquet', file_size=1024, row_count=2,
                        schema_id=0, min_sequence_number=9007199254740993, first_row_id=None, file_source='APPEND',
                        creation_time='2023-11-14T22:13:20.123000',
                        file_path=table.table_path + '/bucket-0/data-test.parquet')
        self.assertEqual(expected, {key: file[key] for key in expected})
        file = result['entries'][1]['file']
        expected = dict(extra, file_source='COMPACT', embedded_index={'encoding': 'base64', 'data': 'AP8='},
                        file_path=extra['external_path'])
        self.assertEqual(expected, {key: file[key] for key in expected})
        output = self._run_table_command_success(*args, '-f', 'table')
        rows = dict(line.split(None, 1) for line in output.splitlines()[1:])
        self.assertEqual(['table', 'manifest_file', 'manifest_path', 'entries'], list(rows))
        self.assertEqual(result, dict(rows, entries=json.loads(rows['entries'])))
        self.assertEqual(before, {path: path.read_bytes() for path in root.rglob('*') if path.is_file()})

    def test_cli_table_manifest_historical_schema(self):
        identifier, table, entry = self._create_manifest_table(primary_keys=['id'], options={'bucket': '1'})
        renamed = [DataField.from_dict({'id': 0, 'name': 'renamed_id', 'type': 'INT NOT NULL'}),
                   DataField.from_dict({'id': 1, 'name': 'new_column', 'type': 'STRING'})]
        schema = TableSchema.from_schema(1, Schema(
            fields=renamed, primary_keys=['renamed_id'], options={'bucket': '1'}))
        with open(table.schema_manager._to_schema_path(1), 'w') as f:
            f.write(JSON.to_json(schema))
        meta = replace(entry.file, value_stats=replace(entry.file.value_stats, null_counts=None),
                       value_stats_cols=['id'])
        ManifestFileManager(table).write('manifest-old', [replace(entry, file=meta)])
        result = json.loads(self._run_table_command_success('manifest', identifier, '--file', 'manifest-old'))
        file = result['entries'][0]['file']
        self.assertEqual((0, ['id'], {'id': 7}, {'id': 7}),
                         (file['schema_id'], file['value_stats_cols'], file['min_key'], file['max_key']))
        self.assertEqual({'min_values': {'id': 7}, 'max_values': {'id': 7}, 'null_counts': {'id': 0}},
                         file['key_stats'])
        self.assertEqual({'min_values': {'id': 7}, 'max_values': {'id': 8}, 'null_counts': None}, file['value_stats'])

    def test_cli_table_manifest_typed_stats_and_partition(self):
        identifier = 'test_db.typed_manifest'
        definitions = [('pt', 'DATE'), ('amount', 'DECIMAL(30, 9)'), ('payload', 'BYTES'), ('at', 'TIMESTAMP(6)'),
                       ('nan', 'DOUBLE'), ('positive', 'DOUBLE'), ('negative', 'DOUBLE')]
        fields = [DataField.from_dict({'id': i, 'name': name, 'type': type_name})
                  for i, (name, type_name) in enumerate(definitions)]
        self.catalog.create_table(identifier, Schema(fields=fields, partition_keys=['pt']), False)
        table = self.catalog.get_table(identifier)
        empty = GenericRow([], [])
        values = [date(2026, 11, 13), Decimal('123456789012345678.123456789'), b'\x00\xff',
                  datetime(2026, 11, 13, 1, 2, 3, 123456), float('nan'), float('inf'), float('-inf')]
        stats = SimpleStats(GenericRow(values, fields), GenericRow(values, fields), [0, None, 0, 0])
        entry = ManifestEntry(0, GenericRow(values[:1], table.partition_keys_fields), 0, -1, DataFileMeta(
            'typed.parquet', 100, 1, empty, empty, SimpleStats(empty, empty, []), stats, 0, 0, 0, 0, []))
        manager = ManifestFileManager(table)
        table.file_io.mkdirs(manager.manifest_path)
        manager.write('manifest-typed', [entry])
        result = json.loads(self._run_table_command_success('manifest', identifier, '--file', 'manifest-typed'))
        self.assertEqual({'pt': '2026-11-13'}, result['entries'][0]['partition'])
        stats = result['entries'][0]['file']['value_stats']
        expected = {'pt': '2026-11-13', 'amount': '123456789012345678.123456789',
                    'payload': {'encoding': 'base64', 'data': 'AP8='}, 'at': '2026-11-13T01:02:03.123456',
                    'nan': 'NaN', 'positive': 'Infinity', 'negative': '-Infinity'}
        self.assertEqual(expected, stats['min_values'])
        self.assertEqual(expected, stats['max_values'])
        self.assertEqual(
            {'pt': 0, 'amount': None, 'payload': 0, 'at': 0, 'nan': None, 'positive': None, 'negative': None},
            stats['null_counts'])

    def test_cli_table_manifest_empty_entries_and_stats(self):
        identifier, table, entry = self._create_manifest_table()
        manager = ManifestFileManager(table)
        manager.write('manifest-empty', [])
        # Dedicated vector files can store an empty row while retaining schema field definitions.
        meta = replace(entry.file, value_stats=SimpleStats.empty_stats(), value_stats_cols=None)
        manager.write('manifest-empty-stats', [replace(entry, file=meta)])
        for name, count in (('manifest-empty', 0), ('manifest-empty-stats', 1)):
            with self.subTest(file=name):
                args = ('manifest', identifier, '--file', name)
                result = json.loads(self._run_table_command_success(*args, '--format', 'json'))
                self.assertEqual(count, len(result['entries']))
                if count:
                    self.assertEqual({'min_values': {}, 'max_values': {}, 'null_counts': {'id': None}},
                                     result['entries'][0]['file']['value_stats'])
                output = self._run_table_command_success(*args, '--format', 'table')
                rows = dict(line.split(None, 1) for line in output.splitlines()[1:])
                self.assertEqual(result['entries'], json.loads(rows['entries']))

    def test_cli_table_manifest_invalid_arguments(self):
        cases = [('manifest', 'test_db.users'), ('manifest', 'test_db.users', '--file', 'manifest-test', '-f', 'csv')]
        cases.extend(('list-manifests', 'test_db.users', option, value)
                     for option, value in (('--snapshot-id', '0'), ('--limit', '-1'), ('--type', 'invalid')))
        for args in cases:
            with self.subTest(args=args):
                out, err, code = self._run_table_command(*args)
                self.assertEqual(('', 2), (out, code))
                self.assertIn('error:', err)
        for name in ('../manifest-test', 'dir\\manifest-test', '..', '', '\x00'):
            with self.subTest(file=name):
                out, err, code = self._run_table_command('manifest', 'test_db.users', '--file', name)
                self.assertEqual(('', 1), (out, code))
                self.assertIn('file name', err.lower())

    def test_cli_table_manifest_metadata_errors(self):
        identifier, table, _ = self._create_manifest_table()
        os.remove(os.path.join(table.table_path, 'manifest', 'base-2'))
        with open(os.path.join(table.table_path, 'manifest', 'manifest-corrupt'), 'wb') as f:
            f.write(b'not avro')
        for args, message in (
                (('list-manifests', identifier), 'Failed to list manifests'),
                (('manifest', identifier, '--file', 'manifest-missing'), 'Failed to read manifest'),
                (('manifest', identifier, '--file', 'manifest-corrupt'), 'Failed to read manifest'),
                (('list-manifests', identifier + '$snapshots'), 'expected a FileStoreTable'),
                (('manifest', identifier + '$snapshots', '--file', 'manifest-base'), 'expected a FileStoreTable')):
            with self.subTest(args=args):
                out, err, code = self._run_table_command(*args)
                self.assertEqual(('', 1), (out, code))
                self.assertIn(message, err)

    def test_cli_table_create_with_json_schema(self):
        """Test table create with JSON schema file."""
        import json
        
        schema_file = os.path.join(self.tempdir, 'test_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'order_id', 'type': 'BIGINT'},
                {'id': 1, 'name': 'customer_id', 'type': 'INT'},
                {'id': 2, 'name': 'amount', 'type': 'DOUBLE'}
            ],
            'partitionKeys': ['customer_id'],
            'options': {'bucket': '3'}
        }
        
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)
        
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'create', 'test_db.orders', '-s', schema_file]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('created successfully', output)
        
        # Verify table was created with correct schema
        table = self.catalog.get_table('test_db.orders')
        schema = table.table_schema
        self.assertEqual(len(schema.fields), 3)
        self.assertIn('customer_id', schema.partition_keys)

    def test_cli_table_import_csv(self):
        """Test table import from CSV file via CLI."""
        import pandas as pd
        
        # Create a CSV file with test data
        csv_file = os.path.join(self.tempdir, 'test_import.csv')
        df = pd.DataFrame({
            'id': [10, 11, 12],
            'name': ['Frank', 'Grace', 'Henry'],
            'age': [40, 45, 50],
            'city': ['Tokyo', 'Seoul', 'Singapore']
        })
        df.to_csv(csv_file, index=False)
        
        # Simulate CLI command: paimon table import test_db.users --input test_import.csv
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'import', 'test_db.users', '-i', csv_file]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('Successfully imported', output)
                self.assertIn('3 rows', output)
        
        # Verify data was imported by reading the table
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'read', 'test_db.users']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                # Check that new data is present
                self.assertIn('Frank', output)
                self.assertIn('Grace', output)
                self.assertIn('Henry', output)

    def test_cli_table_import_json(self):
        """Test table import from JSON file via CLI."""
        import pandas as pd
        
        # Create a JSON file with test data
        json_file = os.path.join(self.tempdir, 'test_import.json')
        df = pd.DataFrame({
            'id': [20, 21],
            'name': ['Ivy', 'Jack'],
            'age': [55, 60],
            'city': ['Sydney', 'Melbourne']
        })
        df.to_json(json_file, orient='records')
        
        # Simulate CLI command: paimon table import test_db.users --input test_import.json
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'import', 'test_db.users', '-i', json_file]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('Successfully imported', output)
                self.assertIn('2 rows', output)
        
        # Verify data was imported by reading the table
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'read', 'test_db.users']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                # Check that new data is present
                self.assertIn('Ivy', output)
                self.assertIn('Jack', output)

    def test_cli_table_drop_basic(self):
        """Test basic table drop via CLI."""
        import json
        
        # Create a table to drop
        schema_file = os.path.join(self.tempdir, 'drop_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'name', 'type': 'STRING'}
            ],
            'primaryKeys': ['id']
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)
        
        # Create the table first
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'create', 'test_db.table_to_drop', '-s', schema_file]):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass
        
        # Verify table exists
        table = self.catalog.get_table('test_db.table_to_drop')
        self.assertIsNotNone(table)
        
        # Drop the table
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'drop', 'test_db.table_to_drop']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('dropped successfully', output)
        
        # Verify table no longer exists
        with self.assertRaises(Exception):
            self.catalog.get_table('test_db.table_to_drop')

    def test_cli_table_alter_set_option(self):
        """Test table alter set-option via CLI."""
        # Create test table
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_set_opt',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_set_opt',
                    'set-option', '-k', 'snapshot.num-retained-max', '-v', '10']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)
        
        # Verify option was set
        table = self.catalog.get_table('test_db.alter_set_opt')
        self.assertEqual(table.table_schema.options.get('snapshot.num-retained-max'), '10')

    def test_cli_table_alter_remove_option(self):
        """Test table alter remove-option via CLI."""
        # Create test table
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_rm_opt',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        # First set an option
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_rm_opt',
                    'set-option', '-k', 'test.option', '-v', 'test_value']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass
        
        # Then remove it
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_rm_opt',
                    'remove-option', '-k', 'test.option']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)
        
        # Verify option was removed
        table = self.catalog.get_table('test_db.alter_rm_opt')
        self.assertNotIn('test.option', table.table_schema.options)

    def test_cli_table_alter_add_column(self):
        """Test table alter add-column via CLI."""
        # Create test table
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_add_col',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_add_col',
                    'add-column', '-n', 'email', '-t', 'STRING']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)
        
        # Verify column was added
        table = self.catalog.get_table('test_db.alter_add_col')
        field_names = [f.name for f in table.table_schema.fields]
        self.assertIn('email', field_names)

    def test_cli_table_alter_drop_column(self):
        """Test table alter drop-column via CLI."""
        # Create test table
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_drop_col',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        # First add a column to drop
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_drop_col',
                    'add-column', '-n', 'temp_col', '-t', 'INT']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass
        
        # Drop the column
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_drop_col',
                    'drop-column', '-n', 'temp_col']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)
        
        # Verify column was dropped
        table = self.catalog.get_table('test_db.alter_drop_col')
        field_names = [f.name for f in table.table_schema.fields]
        self.assertNotIn('temp_col', field_names)

    def test_cli_table_alter_rename_column(self):
        """Test table alter rename-column via CLI."""
        # Create test table
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_rename_col',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        # First add a column to rename
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_rename_col',
                    'add-column', '-n', 'rename_me', '-t', 'STRING']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass
        
        # Rename the column
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_rename_col',
                    'rename-column', '-n', 'rename_me', '-m', 'renamed_col']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)
        
        # Verify column was renamed
        table = self.catalog.get_table('test_db.alter_rename_col')
        field_names = [f.name for f in table.table_schema.fields]
        self.assertNotIn('rename_me', field_names)
        self.assertIn('renamed_col', field_names)

    def test_cli_table_alter_column_type(self):
        """Test table alter alter-column to change column type via CLI."""
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'score', 'type': 'INT'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_col_type',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_col_type',
                    'alter-column', '-n', 'score', '-t', 'BIGINT']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_col_type')
        score_field = [f for f in table.table_schema.fields if f.name == 'score'][0]
        self.assertEqual(score_field.type.type, 'BIGINT')

    def test_cli_table_alter_column_comment(self):
        """Test table alter alter-column to change column comment via CLI."""
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'name', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_col_comment',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_col_comment',
                    'alter-column', '-n', 'name', '-c', 'User full name']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_col_comment')
        name_field = [f for f in table.table_schema.fields if f.name == 'name'][0]
        self.assertEqual(name_field.description, 'User full name')

    def test_cli_table_alter_column_position(self):
        """Test table alter alter-column to change column position via CLI."""
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'name', 'type': 'STRING'},
                {'id': 2, 'name': 'age', 'type': 'INT'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_col_pos',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        # Move 'age' to first position
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_col_pos',
                    'alter-column', '-n', 'age', '--first']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_col_pos')
        field_names = [f.name for f in table.table_schema.fields]
        self.assertEqual(field_names[0], 'age')

        # Move 'age' after 'name'
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_col_pos',
                    'alter-column', '-n', 'age', '--after', 'name']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_col_pos')
        field_names = [f.name for f in table.table_schema.fields]
        name_idx = field_names.index('name')
        age_idx = field_names.index('age')
        self.assertEqual(age_idx, name_idx + 1)

    def test_cli_table_alter_update_comment(self):
        """Test table alter update-comment via CLI."""
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ],
            'comment': 'original comment'
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_comment',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_comment',
                    'update-comment', '-c', 'Updated table comment']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_comment')
        self.assertEqual(table.table_schema.comment, 'Updated table comment')

    def test_cli_table_alter_add_column_with_position(self):
        """Test table alter add-column with position options via CLI."""
        import json
        schema_file = os.path.join(self.tempdir, 'alter_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'name', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.alter_add_pos',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        # Add column as first
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_add_pos',
                    'add-column', '-n', 'first_col', '-t', 'INT', '--first']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_add_pos')
        field_names = [f.name for f in table.table_schema.fields]
        self.assertEqual(field_names[0], 'first_col')

        # Add column after 'id'
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'alter', 'test_db.alter_add_pos',
                    'add-column', '-n', 'after_id_col', '-t', 'STRING', '--after', 'id']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('altered successfully', output)

        table = self.catalog.get_table('test_db.alter_add_pos')
        field_names = [f.name for f in table.table_schema.fields]
        id_idx = field_names.index('id')
        after_id_idx = field_names.index('after_id_col')
        self.assertEqual(after_id_idx, id_idx + 1)

    def test_cli_table_read_with_where_equal(self):
        """Test table read with --where equal filter via CLI."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users', '--where', "name = 'Alice'"]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('Alice', output)
                self.assertNotIn('Bob', output)
                self.assertNotIn('Charlie', output)

    def test_cli_table_read_with_where_greater_than(self):
        """Test table read with --where greater-than filter via CLI."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users', '-w', 'age > 30']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                # age > 30: Charlie(35), Eve(32)
                self.assertIn('Charlie', output)
                self.assertIn('Eve', output)
                self.assertNotIn('Alice', output)
                self.assertNotIn('Bob', output)

    def test_cli_table_read_with_where_and(self):
        """Test table read with --where AND condition via CLI."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users', '--where', 'age >= 28 AND age <= 32']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                # age >= 28 AND age <= 32: Bob(30), David(28), Eve(32)
                self.assertIn('Bob', output)
                self.assertIn('David', output)
                self.assertIn('Eve', output)
                self.assertNotIn('Alice', output)
                self.assertNotIn('Charlie', output)

    def test_cli_table_read_with_where_in(self):
        """Test table read with --where IN filter via CLI."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users',
                    '--where', "city IN ('Beijing', 'Shanghai')"]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('Alice', output)
                self.assertIn('Bob', output)
                self.assertNotIn('Charlie', output)

    def test_cli_table_read_with_where_and_select(self):
        """Test table read with both --where and --select via CLI."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users',
                    '--select', 'name,age',
                    '--where', 'age > 30']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('Charlie', output)
                self.assertIn('Eve', output)
                self.assertNotIn('Alice', output)

    def test_cli_table_read_where_field_not_in_select(self):
        """Test that where filter works even when the filtered field is not in select."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users',
                    '--select', 'name,city',
                    '--where', 'age > 30']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                # age > 30: Charlie(35,Guangzhou), Eve(32,Hangzhou)
                # Filter should work even though 'age' is not in select
                self.assertIn('Charlie', output)
                self.assertIn('Eve', output)
                self.assertIn('Guangzhou', output)
                self.assertIn('Hangzhou', output)
                # Excluded rows should not appear
                self.assertNotIn('Alice', output)
                self.assertNotIn('Bob', output)
                self.assertNotIn('David', output)
                # The 'age' column should NOT appear in output (it was only needed for filtering)
                self.assertNotIn(' 35', output)
                self.assertNotIn(' 32', output)
                self.assertNotIn(' 25', output)

    def test_cli_table_read_with_where_and_limit(self):
        """Test that where + limit returns correct filtered results without limit push-down.

        Writes data in two batches to produce multiple splits, so that limit
        push-down would actually take effect and potentially miss matching rows
        in later splits.
        """

        # Create a dedicated table for this test with two batches of data
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('val', pa.string()),
            ('score', pa.int32()),
        ])
        # Important: multiple splits are required for the limit to take effect
        schema = Schema.from_pyarrow_schema(pa_schema, options={'source.split.target-size': '1b'})
        self.catalog.create_table('test_db.limit_test', schema, True)
        table = self.catalog.get_table('test_db.limit_test')

        def write_batch():
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            batch = pa.Table.from_pydict({
                'id': [1, 2, 3],
                'val': ['a', 'b', 'c'],
                'score': [10, 20, 30],
            }, schema=pa_schema)
            table_write.write_arrow(batch)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        write_batch()
        write_batch()
        write_batch()

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.limit_test',
                    '--where', 'score = 20',
                    '--limit', '2']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                lines = [line for line in output.strip().split('\n') if line.strip()]
                self.assertEqual(len(lines), 3)
                self.assertNotIn(' a ', output)
                self.assertNotIn(' c ', output)

    def test_cli_table_read_with_invalid_where(self):
        """Test table read with invalid --where clause via CLI."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users',
                    '--where', 'age INVALID 30']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                try:
                    main()
                except SystemExit:
                    pass

                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_table_rename_basic(self):
        """Test basic table rename via CLI."""
        import json

        # Create a table to rename
        schema_file = os.path.join(self.tempdir, 'rename_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'create',
                    'test_db.rename_source', '-s', schema_file, '-i']):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass

        # Verify source table exists
        table = self.catalog.get_table('test_db.rename_source')
        self.assertIsNotNone(table)

        # Rename the table
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'rename',
                    'test_db.rename_source', 'test_db.rename_target']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('renamed', output)
                self.assertIn('successfully', output)

        # Verify target table exists
        table = self.catalog.get_table('test_db.rename_target')
        self.assertIsNotNone(table)

        # Verify source table no longer exists
        with self.assertRaises(Exception):
            self.catalog.get_table('test_db.rename_source')

    @classmethod
    def _create_partitioned_table(cls):
        """Create a partitioned test table and insert sample data."""
        pa_schema = pa.schema([
            ('dt', pa.string()),
            ('region', pa.string()),
            ('id', pa.int32()),
            ('value', pa.string())
        ])

        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt', 'region'])
        cls.catalog.create_table('test_db.partitioned', schema, True)

        table = cls.catalog.get_table('test_db.partitioned')

        # Write data for partition dt=2024-01-01, region=us
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'dt': ['2024-01-01', '2024-01-01'],
            'region': ['us', 'us'],
            'id': [1, 2],
            'value': ['a', 'b'],
        }, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Write data for partition dt=2024-01-02, region=eu
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'dt': ['2024-01-02', '2024-01-02', '2024-01-02'],
            'region': ['eu', 'eu', 'eu'],
            'id': [3, 4, 5],
            'value': ['c', 'd', 'e'],
        }, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def test_cli_table_list_partitions(self):
        """Test list-partitions command with real partitioned table."""
        self._create_partitioned_table()

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'list-partitions', 'test_db.partitioned']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                # Verify header columns
                self.assertIn('Partition', output)
                self.assertIn('RecordCount', output)
                self.assertIn('FileSizeInBytes', output)
                self.assertIn('FileCount', output)
                # Verify partition specs
                self.assertIn('dt=2024-01-01,region=us', output)
                self.assertIn('dt=2024-01-02,region=eu', output)

    def test_cli_table_list_partitions_with_pattern(self):
        """Test list-partitions command with --pattern filter."""
        self._create_partitioned_table()

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'list-partitions', 'test_db.partitioned',
                    '--pattern', 'dt=2024-01-01*']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('dt=2024-01-01,region=us', output)
                self.assertNotIn('dt=2024-01-02', output)

    def test_cli_table_read_format_json(self):
        """Test table read with --format json output."""
        import json
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users', '--format', 'json']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                rows = json.loads(output)
                self.assertIsInstance(rows, list)
                self.assertGreaterEqual(len(rows), 5)
                names = {r['name'] for r in rows}
                self.assertIn('Alice', names)
                self.assertIn('Bob', names)
                for r in rows:
                    self.assertIn('id', r)
                    self.assertIn('name', r)
                    self.assertIn('age', r)
                    self.assertIn('city', r)

    def test_cli_table_read_format_json_with_select(self):
        """Test table read with --format json and --select."""
        import json
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'read', 'test_db.users',
                    '--select', 'name,age', '--format', 'json']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                rows = json.loads(output)
                self.assertIsInstance(rows, list)
                for r in rows:
                    self.assertIn('name', r)
                    self.assertIn('age', r)
                    self.assertNotIn('id', r)
                    self.assertNotIn('city', r)

    def test_cli_table_list_partitions_format_json(self):
        """Test list-partitions with --format json output."""
        import json
        self._create_partitioned_table()

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'list-partitions', 'test_db.partitioned',
                    '--format', 'json']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                rows = json.loads(output)
                self.assertIsInstance(rows, list)
                self.assertEqual(len(rows), 2)
                for r in rows:
                    self.assertIn('Partition', r)
                    self.assertIn('RecordCount', r)
                    self.assertIn('FileSizeInBytes', r)
                    self.assertIn('FileCount', r)
                partitions = {r['Partition'] for r in rows}
                self.assertIn('dt=2024-01-01,region=us', partitions)
                self.assertIn('dt=2024-01-02,region=eu', partitions)

    def test_cli_table_list_partitions_empty(self):
        """Test list-partitions on non-partitioned table (no snapshot = empty)."""
        # Create a table with no data
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('val', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('test_db.empty_part', schema, True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'list-partitions', 'test_db.empty_part']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('No partitions found', output)

    def test_cli_table_drop_partition(self):
        """Test drop-partition command drops a partition from a table."""
        # Create a partitioned table for drop-partition test
        pa_schema = pa.schema([
            ('dt', pa.string()),
            ('id', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('test_db.drop_part_cli', schema, True)
        table = self.catalog.get_table('test_db.drop_part_cli')

        # Write data for two partitions
        for dt_val in ['2024-01-01', '2024-01-02', '2024-01-03']:
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'dt': [dt_val],
                'id': [1],
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Drop one partition via CLI
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'drop-partition', 'test_db.drop_part_cli',
                    '--partition', 'dt=2024-01-02']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('Successfully dropped', output)

        # Verify partition was dropped
        result = self.catalog.list_partitions_paged('test_db.drop_part_cli')
        specs = sorted(p.spec['dt'] for p in result.elements)
        self.assertEqual(specs, ['2024-01-01', '2024-01-03'])

    def test_cli_table_drop_partition_multiple(self):
        """Test drop-partition command with multiple --partition flags."""
        # Create a partitioned table
        pa_schema = pa.schema([
            ('dt', pa.string()),
            ('id', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
        self.catalog.create_table('test_db.drop_multi_cli', schema, True)
        table = self.catalog.get_table('test_db.drop_multi_cli')

        for dt_val in ['2024-01-01', '2024-01-02', '2024-01-03']:
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({
                'dt': [dt_val],
                'id': [1],
            }, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Drop multiple partitions via CLI
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'drop-partition', 'test_db.drop_multi_cli',
                    '--partition', 'dt=2024-01-01',
                    '--partition', 'dt=2024-01-03']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('Successfully dropped', output)

        # Verify only the middle partition remains
        result = self.catalog.list_partitions_paged('test_db.drop_multi_cli')
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].spec['dt'], '2024-01-02')

    def test_cli_table_explain_basic(self):
        """Basic `table explain` prints the render anchors and no data."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.users']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()

                # render_explain anchors
                self.assertIn('== PyPaimon Scan Plan ==', output)
                self.assertIn('Table:', output)
                self.assertIn('Snapshot:', output)
                self.assertIn('Splits:', output)
                self.assertIn('Files:', output)
                # No data rows: row data ('Alice'/'Bob') should not appear
                self.assertNotIn('Alice', output)
                self.assertNotIn('Bob', output)

    def test_cli_table_explain_with_select_and_limit(self):
        """`--select` and `--limit` are reflected in the Projection / Limit lines."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.users',
                    '--select', 'id,name',
                    '--limit', '3']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()

                self.assertIn('Projection:', output)
                self.assertIn('[id, name]', output)
                self.assertIn('Limit:', output)
                self.assertIn('3', output)

    def test_cli_table_explain_verbose_lists_splits(self):
        """`--verbose` triggers a Splits[] section listing each split."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.users',
                    '--verbose']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('Splits[]', output)
                # The per-split bullet uses "[0] partition=" as a prefix
                self.assertIn('[0] partition=', output)

    @pytest.mark.python_plan
    def test_cli_table_explain_where_partition_pruning(self):
        """A partition predicate fires the partition-pruning funnel."""
        self._create_partitioned_table()

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.partitioned',
                    '--where', "dt = '2024-01-01' AND region = 'us'"]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()

                # Predicate is rendered
                self.assertIn('Predicate:', output)
                self.assertIn('dt', output)
                # Partition pruning funnel shows before -> after  (pruned N>0)
                self.assertIn('Partition pruning:', output)
                self.assertRegex(output, r'Partition pruning:\s+\d+ -> \d+\s+\(pruned [1-9]\d*\)')

    def test_cli_table_explain_format_json(self):
        """`--format json` is valid JSON with stringified level_histogram keys."""
        import json
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.users',
                    '--format', 'json']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                payload = json.loads(output)

                # Top-level identity / snapshot / split aggregates
                self.assertEqual(payload['table_identifier'], 'test_db.users')
                self.assertIn('snapshot_id', payload)
                self.assertIn('split_count', payload)
                self.assertIn('level_histogram', payload)
                self.assertIn('partition_pruning', payload)

                # level_histogram keys must be strings (json-safe). When the
                # table has data, at least one level entry exists.
                for key in payload['level_histogram'].keys():
                    self.assertIsInstance(key, str)

                # Non-verbose => splits is null
                self.assertIsNone(payload['splits'])

    def test_cli_table_explain_invalid_table(self):
        """Unknown table identifier produces a clean error on stderr."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.does_not_exist']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as ctx:
                    main()
                self.assertEqual(ctx.exception.code, 1)
                self.assertIn("Failed to get table", mock_stderr.getvalue())

    def test_cli_table_explain_invalid_where(self):
        """Malformed WHERE produces a clean error on stderr."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file,
                    'table', 'explain', 'test_db.users',
                    '--where', 'this is not a valid clause']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as ctx:
                    main()
                self.assertEqual(ctx.exception.code, 1)
                self.assertIn("Invalid WHERE clause", mock_stderr.getvalue())

    def test_cli_table_rollback_to_snapshot(self):
        """table rollback --snapshot restores an earlier snapshot."""
        pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('test_db.rollback_users', schema, True)

        def _write(row_id, name):
            table = self.catalog.get_table('test_db.rollback_users')
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            c = wb.new_commit()
            w.write_arrow(pa.Table.from_pydict(
                {'id': [row_id], 'name': [name]}, schema=pa_schema))
            c.commit(w.prepare_commit())
            w.close()
            c.close()

        _write(1, 'a')   # snapshot 1
        _write(2, 'b')   # snapshot 2

        def _latest_id():
            fresh = CatalogFactory.create({'warehouse': self.warehouse})
            return fresh.get_table(
                'test_db.rollback_users').snapshot_manager().get_latest_snapshot().id

        self.assertEqual(_latest_id(), 2)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'rollback',
                    'test_db.rollback_users', '--snapshot', '1']):
            with patch('sys.stdout', new_callable=StringIO) as out:
                try:
                    main()
                except SystemExit:
                    pass
        self.assertIn('Rolled back', out.getvalue())

        # The latest snapshot is back to 1 (snapshot 2 was rolled back).
        self.assertEqual(_latest_id(), 1)

    def test_cli_table_rollback_requires_a_target(self):
        """table rollback without any target selector exits with an error."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'rollback',
                    'test_db.users']):
            with patch('sys.stderr', new_callable=StringIO):
                with self.assertRaises(SystemExit) as ctx:
                    main()
                # argparse rejects the missing mutually-exclusive group.
                self.assertNotEqual(ctx.exception.code, 0)


if __name__ == '__main__':
    unittest.main()
