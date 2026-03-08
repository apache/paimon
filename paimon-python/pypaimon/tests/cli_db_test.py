#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import json
import os
import shutil
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.cli.cli import main


class CliDbTest(unittest.TestCase):
    """Integration tests for CLI database commands with real catalog operations."""

    @classmethod
    def setUpClass(cls):
        """Set up test catalog and configuration."""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')

        # Create catalog
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })

        # Create catalog config file
        cls.config_file = os.path.join(cls.tempdir, 'paimon.yaml')
        with open(cls.config_file, 'w') as f:
            f.write(f"metastore: filesystem\nwarehouse: {cls.warehouse}\n")

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directory."""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_cli_db_create_basic(self):
        """Test basic database create via CLI."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'db', 'create', 'new_db']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('created successfully', output)

        # Verify database was created
        database = self.catalog.get_database('new_db')
        self.assertEqual(database.name, 'new_db')

    def test_cli_db_create_ignore_if_exists(self):
        """Test database create with ignore-if-exists flag."""
        # Create database first
        self.catalog.create_database('existing_db', True)

        # Try to create again with ignore-if-exists flag
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'create', 'existing_db', '-i']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('created successfully', output)

    def test_cli_db_create_already_exists_error(self):
        """Test database create raises error when database already exists."""
        self.catalog.create_database('dup_db', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'create', 'dup_db']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_db_get_basic(self):
        """Test basic database get via CLI."""
        self.catalog.create_database('get_db', True)

        with patch('sys.argv', ['paimon', '-c', self.config_file, 'db', 'get', 'get_db']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()

                # Verify output is valid JSON
                result = json.loads(output)
                self.assertEqual(result['name'], 'get_db')
                self.assertIn('options', result)

    def test_cli_db_get_nonexistent(self):
        """Test CLI error handling for nonexistent database."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'get', 'nonexistent_db']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_db_drop_basic(self):
        """Test basic database drop via CLI."""
        self.catalog.create_database('drop_db', True)

        with patch('sys.argv', ['paimon', '-c', self.config_file, 'db', 'drop', 'drop_db']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('dropped successfully', output)

        # Verify database was dropped
        from pypaimon.catalog.catalog_exception import DatabaseNotExistException
        with self.assertRaises(DatabaseNotExistException):
            self.catalog.get_database('drop_db')

    def test_cli_db_drop_nonexistent_error(self):
        """Test CLI error handling for dropping nonexistent database."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'drop', 'nonexistent_drop_db']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_db_drop_ignore_if_not_exists(self):
        """Test database drop with ignore-if-not-exists flag."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'drop',
                    'nonexistent_ignore_db', '-i']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('dropped successfully', output)

    def test_cli_db_drop_cascade(self):
        """Test database drop with cascade flag."""
        # Create database with a table
        self.catalog.create_database('cascade_db', True)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('cascade_db.test_table', schema, False)

        # Drop with cascade
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'drop', 'cascade_db', '--cascade']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('dropped successfully', output)

        # Verify database was dropped
        from pypaimon.catalog.catalog_exception import DatabaseNotExistException
        with self.assertRaises(DatabaseNotExistException):
            self.catalog.get_database('cascade_db')

    def test_cli_db_list_tables_basic(self):
        """Test basic list-tables via CLI."""
        # Create database with tables
        self.catalog.create_database('list_db', True)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table('list_db.table_a', schema, True)
        self.catalog.create_table('list_db.table_b', schema, True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'list-tables', 'list_db']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('table_a', output)
                self.assertIn('table_b', output)

    def test_cli_db_list_tables_empty(self):
        """Test list-tables for empty database."""
        self.catalog.create_database('empty_list_db', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'list-tables', 'empty_list_db']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('No tables found', output)

    def test_cli_db_list_tables_nonexistent_db(self):
        """Test list-tables for nonexistent database."""
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'list-tables', 'nonexistent_list_db']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_db_alter_not_supported(self):
        """Test db alter on filesystem catalog raises not supported error."""
        self.catalog.create_database('alter_db', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'alter', 'alter_db',
                    '--set', '{"key1": "value1"}']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('not supported', error_output)

    def test_cli_db_alter_no_changes(self):
        """Test db alter with no changes specified."""
        self.catalog.create_database('alter_no_change_db', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'alter', 'alter_no_change_db']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('No changes specified', error_output)

    def test_cli_db_alter_invalid_json(self):
        """Test db alter with invalid JSON for --set."""
        self.catalog.create_database('alter_invalid_db', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'alter', 'alter_invalid_db',
                    '--set', 'not_valid_json']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()

                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Invalid JSON', error_output)

    def test_cli_db_list_basic(self):
        """Test basic list databases via CLI."""
        self.catalog.create_database('list_db_a', True)
        self.catalog.create_database('list_db_b', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'list']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('list_db_a', output)
                self.assertIn('list_db_b', output)

    def test_cli_db_list_excludes_nonexistent(self):
        """Test list databases does not include non-existent databases."""
        self.catalog.create_database('list_exists_db', True)

        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'db', 'list']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass

                output = mock_stdout.getvalue()
                self.assertIn('list_exists_db', output)
                self.assertNotIn('never_created_db', output)


if __name__ == '__main__':
    unittest.main()
