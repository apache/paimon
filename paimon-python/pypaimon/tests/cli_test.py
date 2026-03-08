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


class CliTest(unittest.TestCase):
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

    def test_cli_table_read_nonexistent_database(self):
        """Test CLI error handling for nonexistent database."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'read', 'nonexistent.table']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()
                
                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_table_read_invalid_table_identifier(self):
        """Test CLI error handling for invalid table identifier format."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'read', 'invalid_format']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()
                
                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Invalid table identifier', error_output)

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

    def test_cli_table_get_nonexistent_table(self):
        """Test CLI error handling for nonexistent table in table get."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'get', 'nonexistent.table']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()
                
                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Error', error_output)

    def test_cli_table_get_invalid_table_identifier(self):
        """Test CLI error handling for invalid table identifier format in table get."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'get', 'invalid_format']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()
                
                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Invalid table identifier', error_output)

    def test_cli_table_create_basic(self):
        """Test basic table create via CLI."""
        # Create schema file in JSON format (CLI only supports JSON)
        import json
        schema_file = os.path.join(self.tempdir, 'test_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'product_id', 'type': 'BIGINT'},
                {'id': 1, 'name': 'product_name', 'type': 'STRING'},
                {'id': 2, 'name': 'price', 'type': 'DOUBLE'},
                {'id': 3, 'name': 'category', 'type': 'STRING'}
            ],
            'primaryKeys': ['product_id'],
            'options': {'bucket': '2'},
            'comment': 'Test products table'
        }
        
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)
        
        # Simulate CLI command: paimon -c <config> table create test_db.products -s schema.json
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'create', 'test_db.products', '-s', schema_file]):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                output = mock_stdout.getvalue()
                
                # Verify success message
                self.assertIn('created successfully', output)
                
        # Verify table was created
        table = self.catalog.get_table('test_db.products')
        self.assertIsNotNone(table)

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

    def test_cli_table_create_ignore_if_exists(self):
        """Test table create with ignore-if-exists flag."""
        import json
        schema_file = os.path.join(self.tempdir, 'test_schema2.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'},
                {'id': 1, 'name': 'value', 'type': 'STRING'}
            ]
        }
        
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)
        
        # Create table first time
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'create', 'test_db.temp_table', '-s', schema_file]):
            with patch('sys.stdout', new_callable=StringIO):
                try:
                    main()
                except SystemExit:
                    pass
        
        # Try to create again with ignore-if-exists flag
        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.temp_table',
                    '-s',
                    schema_file,
                    '-i']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
                
                # Should succeed without error
                output = mock_stdout.getvalue()
                self.assertIn('created successfully', output)

    def test_cli_table_create_invalid_table_identifier(self):
        """Test CLI error handling for invalid table identifier format in table create."""
        import json
        schema_file = os.path.join(self.tempdir, 'dummy_schema.json')
        schema_data = {
            'fields': [
                {'id': 0, 'name': 'id', 'type': 'INT'}
            ]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)
        
        with patch('sys.argv',
                   ['paimon', '-c', self.config_file, 'table', 'create', 'invalid_format', '-s', schema_file]):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()
                
                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Invalid table identifier', error_output)

    def test_cli_table_create_missing_schema_file(self):
        """Test CLI error handling for missing schema file."""
        with patch('sys.argv',
                   ['paimon',
                    '-c',
                    self.config_file,
                    'table',
                    'create',
                    'test_db.missing_table',
                    '-s',
                    '/nonexistent/path/schema.json']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as context:
                    main()
                
                self.assertEqual(context.exception.code, 1)
                error_output = mock_stderr.getvalue()
                self.assertIn('Schema file not found', error_output)

if __name__ == '__main__':
    unittest.main()
