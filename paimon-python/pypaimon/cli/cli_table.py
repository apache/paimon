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

"""
Table commands for Paimon CLI.

This module provides table-related commands for the CLI.
"""

import sys


def cmd_table_read(args):
    """
    Execute the 'table read' command.
    
    Reads data from a Paimon table and displays it.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Get table
    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)
    
    # Build read pipeline
    read_builder = table.new_read_builder()
    
    # Apply limit if specified
    limit = args.limit
    if limit:
        read_builder = read_builder.with_limit(limit)
    
    # Scan and read
    scan = read_builder.new_scan()
    plan = scan.plan()
    splits = plan.splits()
    
    read = read_builder.new_read()

    # Use pandas to display as a nice table
    df = read.to_pandas(splits)
    if limit and len(df) > limit:
        df = df.head(limit)
    print(df.to_string(index=False))


def cmd_table_get(args):
    """
    Execute the 'table get' command.
    
    Gets and displays table schema information.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Get table
    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)
    
    # Get table schema
    schema = table.table_schema
    
    # Display table information
    print("=" * 80)
    print(f"Table: {database_name}.{table_name}")
    print("=" * 80)
    
    # Display schema ID
    print(f"\nSchema ID: {schema.id}")
    
    # Display comment if exists
    if schema.comment:
        print(f"\nComment: {schema.comment}")
    
    # Display fields
    print(f"\n{'='*80}")
    print("Fields:")
    print(f"{'='*80}")
    print(f"{'ID':<5} {'Name':<18} {'Type':<20} {'Nullable':<9} {'Description'}")
    print(f"{'-'*5} {'-'*18} {'-'*20} {'-'*9} {'-'*26}")
    
    for field in schema.fields:
        nullable = "YES" if field.type.nullable else "NO"
        description = field.description or ""
        print(f"{field.id:<5} {field.name:<18} {str(field.type):<20} {nullable:<9} {description}")
    
    # Display partition keys
    if schema.partition_keys:
        print(f"\n{'='*80}")
        print(f"Partition Keys: {', '.join(schema.partition_keys)}")
    
    # Display primary keys
    if schema.primary_keys:
        print(f"\n{'='*80}")
        print(f"Primary Keys: {', '.join(schema.primary_keys)}")
    
    # Display options
    if schema.options:
        print(f"\n{'='*80}")
        print("Table Options:")
        print(f"{'='*80}")
        for key, value in sorted(schema.options.items()):
            print(f"  {key:<40} = {value}")
    
    print(f"\n{'='*80}\n")


def cmd_table_create(args):
    """
    Execute the 'table create' command.
    
    Creates a new Paimon table with the specified schema.
    
    Args:
        args: Parsed command line arguments.
    """
    import json
    import yaml
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon import Schema
    
    # Load catalog configuration
    config_path = args.config
    config = load_catalog_config(config_path)
    
    # Create catalog
    catalog = create_catalog(config)
    
    # Parse table identifier
    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)
    
    database_name, table_name = parts
    
    # Load schema from file
    schema_file = args.schema_file
    if not schema_file:
        print("Error: Schema file is required. Use --schema-file option.", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            if schema_file.endswith('.json'):
                schema_def = json.load(f)
            elif schema_file.endswith('.yaml') or schema_file.endswith('.yml'):
                schema_def = yaml.safe_load(f)
            else:
                print("Error: Unsupported schema file format. Use .json, .yaml, or .yml", file=sys.stderr)
                sys.exit(1)
    except FileNotFoundError:
        print(f"Error: Schema file not found: {schema_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read schema file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Parse schema definition
    try:
        # Validate required fields
        if 'fields' not in schema_def:
            print("Error: Schema must contain 'fields' section", file=sys.stderr)
            sys.exit(1)
        
        # Build PyArrow schema from definition
        import pyarrow as pa
        pa_fields = []
        for field in schema_def['fields']:
            field_name = field.get('name')
            field_type = field.get('type')
            
            if not field_name or not field_type:
                print("Error: Each field must have 'name' and 'type'", file=sys.stderr)
                sys.exit(1)
            
            # Convert type string to PyArrow type
            pa_type = _parse_field_type(field_type)
            pa_fields.append(pa.field(field_name, pa_type))
        
        pa_schema = pa.schema(pa_fields)
        
        # Extract optional parameters
        partition_keys = schema_def.get('partition_keys', [])
        primary_keys = schema_def.get('primary_keys', [])
        options = schema_def.get('options', {})
        comment = schema_def.get('comment')
        
        # Create Paimon schema
        paimon_schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=partition_keys if partition_keys else None,
            primary_keys=primary_keys if primary_keys else None,
            options=options if options else None,
            comment=comment
        )
        
        # Create table
        ignore_if_exists = args.ignore_if_exists
        catalog.create_table(f"{database_name}.{table_name}", paimon_schema, ignore_if_exists)
        
        print(f"Table '{database_name}.{table_name}' created successfully.")
        
    except Exception as e:
        print(f"Error: Failed to create table: {e}", file=sys.stderr)
        sys.exit(1)


def _parse_field_type(type_str: str):
    """
    Parse a type string to PyArrow data type.
    
    Args:
        type_str: Type string (e.g., 'INT', 'STRING', 'BIGINT')
    
    Returns:
        PyArrow data type.
    """
    import pyarrow as pa
    
    type_upper = type_str.upper().strip()
    
    # Map common type names to PyArrow types
    type_mapping = {
        'INT': pa.int32(),
        'INTEGER': pa.int32(),
        'BIGINT': pa.int64(),
        'LONG': pa.int64(),
        'SMALLINT': pa.int16(),
        'TINYINT': pa.int8(),
        'FLOAT': pa.float32(),
        'DOUBLE': pa.float64(),
        'BOOLEAN': pa.bool_(),
        'BOOL': pa.bool_(),
        'STRING': pa.string(),
        'VARCHAR': pa.string(),
        'CHAR': pa.string(),
        'BINARY': pa.binary(),
        'VARBINARY': pa.binary(),
        'DATE': pa.date32(),
        'TIMESTAMP': pa.timestamp('us'),
        'TIMESTAMP(6)': pa.timestamp('us'),
        'TIMESTAMP(3)': pa.timestamp('ms'),
    }
    
    if type_upper in type_mapping:
        return type_mapping[type_upper]
    
    # Handle DECIMAL(precision, scale)
    if type_upper.startswith('DECIMAL'):
        import re
        match = re.match(r'DECIMAL\((\d+),\s*(\d+)\)', type_upper)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return pa.decimal128(precision, scale)
    
    # Default to string if unknown
    print(f"Warning: Unknown type '{type_str}', defaulting to STRING", file=sys.stderr)
    return pa.string()


def add_table_subcommands(table_parser):
    """
    Add table subcommands to the parser.
    
    Args:
        table_parser: The table subparser to add commands to.
    """
    table_subparsers = table_parser.add_subparsers(dest='table_command', help='Table commands')
    
    # table read command
    read_parser = table_subparsers.add_parser('read', help='Read data from a table')
    read_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    read_parser.add_argument(
        '--limit', '-l',
        type=int,
        default=100,
        help='Maximum number of results to display (default: 100)'
    )
    read_parser.set_defaults(func=cmd_table_read)
    
    # table get command
    get_parser = table_subparsers.add_parser('get', help='Get table schema information')
    get_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    get_parser.set_defaults(func=cmd_table_get)
    
    # table create command
    create_parser = table_subparsers.add_parser('create', help='Create a new table')
    create_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    create_parser.add_argument(
        '--schema-file', '-s',
        required=True,
        help='Path to schema definition file (JSON or YAML)'
    )
    create_parser.add_argument(
        '--ignore-if-exists', '-i',
        action='store_true',
        help='Do not raise error if table already exists'
    )
    create_parser.set_defaults(func=cmd_table_create)
