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

import os
import shutil
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.cli.cli import main


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

if __name__ == '__main__':
    unittest.main()
