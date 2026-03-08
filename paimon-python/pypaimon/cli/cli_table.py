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
from pypaimon.common.json_util import JSON


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
    
    Gets and displays table schema information in JSON format.
    
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
    
    # Get table schema and convert to Schema, then output as JSON
    schema = table.table_schema.to_schema()
    print(JSON.to_json(schema, indent=2))


def cmd_table_create(args):
    """
    Execute the 'table create' command.
    
    Creates a new Paimon table with the specified schema.
    
    Args:
        args: Parsed command line arguments.
    """
    import json
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
    
    # Load schema from JSON file
    schema_file = args.schema
    if not schema_file:
        print("Error: Schema is required. Use --schema option.", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_json = f.read()
        paimon_schema = JSON.from_json(schema_json, Schema)
        
    except FileNotFoundError:
        print(f"Error: Schema file not found: {schema_file}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in schema file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to parse schema: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Create table
    try:
        ignore_if_exists = args.ignore_if_exists
        catalog.create_table(f"{database_name}.{table_name}", paimon_schema, ignore_if_exists)
        
        print(f"Table '{database_name}.{table_name}' created successfully.")
        
    except Exception as e:
        print(f"Error: Failed to create table: {e}", file=sys.stderr)
        sys.exit(1)


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
        '--schema', '-s',
        required=True,
        help='Path to schema JSON file'
    )
    create_parser.add_argument(
        '--ignore-if-exists', '-i',
        action='store_true',
        help='Do not raise error if table already exists'
    )
    create_parser.set_defaults(func=cmd_table_create)
