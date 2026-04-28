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


def cmd_table_stream(args):
    """Execute the 'table stream' command."""
    from pypaimon.cli.cli_table_stream import cmd_table_stream as _cmd
    _cmd(args)


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
    
    available_fields = set(field.name for field in table.table_schema.fields)

    # Parse select and where options
    select_columns = args.select
    where_clause = args.where
    user_columns = None
    extra_where_columns = []

    if select_columns:
        # Parse column names (comma-separated)
        user_columns = [col.strip() for col in select_columns.split(',')]

        # Validate that all columns exist in the table schema
        invalid_columns = [col for col in user_columns if col not in available_fields]
        if invalid_columns:
            print(f"Error: Column(s) {invalid_columns} do not exist in table '{table_identifier}'.", file=sys.stderr)
            sys.exit(1)

    # When both select and where are specified, ensure where-referenced fields
    # are included in the projection so the filter can work correctly.
    if user_columns and where_clause:
        from pypaimon.cli.where_parser import extract_fields_from_where
        where_fields = extract_fields_from_where(where_clause, available_fields)
        user_column_set = set(user_columns)
        extra_where_columns = [f for f in where_fields if f not in user_column_set]
        projection_columns = user_columns + extra_where_columns
        read_builder = read_builder.with_projection(projection_columns)
    elif user_columns:
        read_builder = read_builder.with_projection(user_columns)

    # Apply where filter if specified
    if where_clause:
        from pypaimon.cli.where_parser import parse_where_clause
        try:
            predicate = parse_where_clause(where_clause, table.table_schema.fields)
            if predicate:
                read_builder = read_builder.with_filter(predicate)
        except ValueError as e:
            print(f"Error: Invalid WHERE clause: {e}", file=sys.stderr)
            sys.exit(1)

    # Apply limit: only push down when there is no where clause,
    # because limit push-down may stop reading before enough rows
    # pass the filter, leading to fewer results than expected.
    limit = args.limit
    if limit and not where_clause:
        read_builder = read_builder.with_limit(limit)
    
    # Scan and read
    scan = read_builder.new_scan()
    plan = scan.plan()
    splits = plan.splits()
    
    read = read_builder.new_read()

    # Read splits incrementally, stopping early when limit is reached
    if limit:
        import pandas as pd
        collected_rows = 0
        table_list = []
        for split in splits:
            if collected_rows >= limit:
                break
            partial_df = read.to_pandas([split])
            collected_rows += len(partial_df)
            table_list.append(partial_df)
        df = pd.concat(table_list, ignore_index=True) if table_list else read.to_pandas([])
        if len(df) > limit:
            df = df.head(limit)
    else:
        df = read.to_pandas(splits)

    # Drop extra columns that were added only for where-clause filtering
    if extra_where_columns:
        df = df.drop(columns=extra_where_columns, errors='ignore')

    output_format = getattr(args, 'format', 'table')
    if output_format == 'json':
        import json
        print(json.dumps(df.to_dict(orient='records'), ensure_ascii=False))
    else:
        print(df.to_string(index=False))


def cmd_table_full_text_search(args):
    """
    Execute the 'table full-text-search' command.

    Performs full-text search on a Paimon table and displays matching rows.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config_path = args.config
    config = load_catalog_config(config_path)
    catalog = create_catalog(config)

    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(f"Error: Invalid table identifier '{table_identifier}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    database_name, table_name = parts

    try:
        table = catalog.get_table(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    # Build full-text search
    text_column = args.column
    query_text = args.query
    limit = args.limit

    try:
        builder = table.new_full_text_search_builder()
        builder.with_text_column(text_column)
        builder.with_query_text(query_text)
        builder.with_limit(limit)
        result = builder.execute_local()
    except Exception as e:
        print(f"Error: Full-text search failed: {e}", file=sys.stderr)
        sys.exit(1)

    if result.is_empty():
        print("No matching rows found.")
        return

    # Read matching rows using global index result
    read_builder = table.new_read_builder()

    select_columns = args.select
    if select_columns:
        projection = [col.strip() for col in select_columns.split(',')]
        available_fields = set(field.name for field in table.table_schema.fields)
        invalid_columns = [col for col in projection if col not in available_fields]
        if invalid_columns:
            print(f"Error: Column(s) {invalid_columns} do not exist in table '{table_identifier}'.",
                  file=sys.stderr)
            sys.exit(1)
        read_builder = read_builder.with_projection(projection)

    scan = read_builder.new_scan().with_global_index_result(result)
    plan = scan.plan()
    splits = plan.splits()
    read = read_builder.new_read()
    df = read.to_pandas(splits)

    output_format = getattr(args, 'format', 'table')
    if output_format == 'json':
        import json
        print(json.dumps(df.to_dict(orient='records'), ensure_ascii=False))
    else:
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


def cmd_table_snapshot(args):
    """
    Execute the 'table snapshot' command.
    
    Gets and displays the latest snapshot of a Paimon table in JSON format.
    
    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.table.file_store_table import FileStoreTable
    
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
    
    # Check if table is FileStoreTable
    if not isinstance(table, FileStoreTable):
        print(f"Error: Table '{table_identifier}' is not a FileStoreTable. "
              f"Snapshot operation is not supported for this table type.", file=sys.stderr)
        sys.exit(1)
    
    # Get latest snapshot
    try:
        snapshot_manager = table.snapshot_manager()
        snapshot = snapshot_manager.get_latest_snapshot()
        
        if snapshot is None:
            print(f"Error: No snapshot found for table '{table_identifier}'.", file=sys.stderr)
            sys.exit(1)
        
        # Output snapshot as JSON
        print(JSON.to_json(snapshot, indent=2))
        
    except Exception as e:
        print(f"Error: Failed to get snapshot: {e}", file=sys.stderr)
        sys.exit(1)


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


def cmd_table_import(args):
    """
    Execute the 'table import' command.
    
    Imports data from a CSV or JSON file into a Paimon table.
    
    Args:
        args: Parsed command line arguments.
    """
    import pandas as pd
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
    
    # Get input file path
    input_file = args.input
    if not input_file:
        print("Error: Input file is required. Use --input option.", file=sys.stderr)
        sys.exit(1)
    
    # Read data from file
    try:
        file_lower = input_file.lower()
        if file_lower.endswith('.csv'):
            # Read CSV file
            df = pd.read_csv(input_file)
        elif file_lower.endswith('.json'):
            # Read JSON file
            df = pd.read_json(input_file)
        else:
            print("Error: Unsupported file format. Only CSV and JSON files are supported.", file=sys.stderr)
            sys.exit(1)
        
        if df.empty:
            print("Warning: No data found in file '{input_file}'.", file=sys.stderr)
            return
        
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read input file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Write data to table
    table_write = None
    table_commit = None
    try:
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        
        # Get table schema and convert DataFrame to match it
        import pyarrow as pa
        from pypaimon.schema.data_types import PyarrowFieldParser
        pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        
        # Convert DataFrame to PyArrow Table with the correct schema
        table_data = pa.Table.from_pandas(df, schema=pa_schema)
        
        # Write data
        table_write.write_arrow(table_data)
        
        # Commit write
        table_commit.commit(table_write.prepare_commit())
        
        print(f"Successfully imported {len(df)} rows into '{database_name}.{table_name}'.")
        
    except Exception as e:
        print(f"Error: Failed to import data: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if table_write is not None:
            table_write.close()
        if table_commit is not None:
            table_commit.close()


def cmd_table_drop(args):
    """
    Execute the 'table drop' command.
    
    Drops a Paimon table.
    
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
    
    # Drop table
    try:
        ignore_if_not_exists = args.ignore_if_not_exists
        catalog.drop_table(f"{database_name}.{table_name}", ignore_if_not_exists)
        
        print(f"Table '{database_name}.{table_name}' dropped successfully.")
        
    except Exception as e:
        print(f"Error: Failed to drop table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_rename(args):
    """
    Execute the 'table rename' command.

    Renames a Paimon table.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config_path = args.config
    config = load_catalog_config(config_path)
    catalog = create_catalog(config)

    source_table = args.table
    target_table = args.target

    source_parts = source_table.split('.')
    if len(source_parts) != 2:
        print(f"Error: Invalid source table identifier '{source_table}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    target_parts = target_table.split('.')
    if len(target_parts) != 2:
        print(f"Error: Invalid target table identifier '{target_table}'. "
              f"Expected format: 'database.table'", file=sys.stderr)
        sys.exit(1)

    try:
        catalog.rename_table(source_table, target_table)
        print(f"Table '{source_table}' renamed to '{target_table}' successfully.")
    except NotImplementedError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to rename table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_alter(args):
    """
    Execute the 'table alter' command.

    Alters a Paimon table with the specified schema changes.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.schema.schema_change import SchemaChange
    from pypaimon.schema.data_types import DataTypeParser

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

    # Build schema change based on alter subcommand
    alter_command = args.alter_command
    changes = []

    if alter_command == 'set-option':
        changes.append(SchemaChange.set_option(args.key, args.value))
    elif alter_command == 'remove-option':
        changes.append(SchemaChange.remove_option(args.key))
    elif alter_command == 'add-column':
        from pypaimon.schema.schema_change import Move
        move = None
        if getattr(args, 'first', False):
            move = Move.first(args.name)
        elif getattr(args, 'after', None):
            move = Move.after(args.name, args.after)
        data_type = DataTypeParser.parse_atomic_type_sql_string(args.type)
        changes.append(SchemaChange.add_column(args.name, data_type, comment=args.comment, move=move))
    elif alter_command == 'drop-column':
        changes.append(SchemaChange.drop_column(args.name))
    elif alter_command == 'rename-column':
        changes.append(SchemaChange.rename_column(args.name, args.new_name))
    elif alter_command == 'update-comment':
        changes.append(SchemaChange.update_comment(args.comment))
    elif alter_command == 'alter-column':
        from pypaimon.schema.schema_change import Move
        column_name = args.name
        has_action = False
        if getattr(args, 'type', None):
            new_type = DataTypeParser.parse_atomic_type_sql_string(args.type)
            changes.append(SchemaChange.update_column_type(column_name, new_type))
            has_action = True
        if getattr(args, 'comment', None) is not None:
            changes.append(SchemaChange.update_column_comment(column_name, args.comment))
            has_action = True
        if getattr(args, 'first', False):
            changes.append(SchemaChange.update_column_position(Move.first(column_name)))
            has_action = True
        elif getattr(args, 'after', None):
            changes.append(SchemaChange.update_column_position(Move.after(column_name, args.after)))
            has_action = True
        if not has_action:
            print("Error: At least one of --type, --comment, --first, or --after must be specified.",
                  file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: Unknown alter command '{alter_command}'.", file=sys.stderr)
        sys.exit(1)

    # Apply schema changes
    try:
        ignore_if_not_exists = args.ignore_if_not_exists
        catalog.alter_table(f"{database_name}.{table_name}", changes, ignore_if_not_exists)
        print(f"Table '{database_name}.{table_name}' altered successfully.")
    except Exception as e:
        print(f"Error: Failed to alter table: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_table_list_partitions(args):
    """
    Execute the 'table list-partitions' command.

    Lists partitions of a Paimon table with optional pattern filtering.

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

    # List partitions with pagination
    pattern = getattr(args, 'pattern', None)
    try:
        paged_list = catalog.list_partitions_paged(
            table_identifier,
            partition_name_pattern=pattern,
        )
        import pandas as pd

        partitions = paged_list.elements
        if not partitions:
            print("No partitions found.")
            return

        data = []
        for p in partitions:
            spec_str = ",".join(f"{k}={v}" for k, v in p.spec.items())
            data.append({
                'Partition': spec_str,
                'RecordCount': p.record_count,
                'FileSizeInBytes': p.file_size_in_bytes,
                'FileCount': p.file_count,
                'LastFileCreationTime': p.last_file_creation_time,
                'UpdatedAt': p.updated_at,
                'UpdatedBy': p.updated_by or '',
            })

        output_format = getattr(args, 'format', 'table')
        if output_format == 'json':
            import json
            print(json.dumps(data, ensure_ascii=False))
        else:
            df = pd.DataFrame(data)
            print(df.to_string(index=False))

    except NotImplementedError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to list partitions: {e}", file=sys.stderr)
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
        '--select', '-s',
        type=str,
        default=None,
        help='Select specific columns to read (comma-separated, e.g., "id,name,age")'
    )
    read_parser.add_argument(
        '--where', '-w',
        type=str,
        default=None,
        help=('Filter condition in SQL-like syntax '
              '(e.g., "age > 18", "name = \'Alice\' AND status IN (\'active\', \'pending\')")')
    )
    read_parser.add_argument(
        '--limit', '-l',
        type=int,
        default=100,
        help='Maximum number of results to display (default: 100)'
    )
    read_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    read_parser.set_defaults(func=cmd_table_read)
    
    # table get command
    get_parser = table_subparsers.add_parser('get', help='Get table schema information')
    get_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    get_parser.set_defaults(func=cmd_table_get)
    
    # table snapshot command
    snapshot_parser = table_subparsers.add_parser('snapshot', help='Get the latest snapshot of a table')
    snapshot_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    snapshot_parser.set_defaults(func=cmd_table_snapshot)
    
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
    
    # table drop command
    drop_parser = table_subparsers.add_parser('drop', help='Drop a table')
    drop_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    drop_parser.add_argument(
        '--ignore-if-not-exists', '-i',
        action='store_true',
        help='Do not raise error if table does not exist'
    )
    drop_parser.set_defaults(func=cmd_table_drop)
    
    # table import command
    import_parser = table_subparsers.add_parser('import', help='Import data from CSV or JSON file')
    import_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    import_parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to input file (CSV or JSON format)'
    )
    import_parser.set_defaults(func=cmd_table_import)
    
    # table list-partitions command
    list_partitions_parser = table_subparsers.add_parser('list-partitions', help='List partitions of a table')
    list_partitions_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    list_partitions_parser.add_argument(
        '--pattern', '-p',
        type=str,
        default=None,
        help='Partition name pattern to filter partitions (e.g., "dt=2024*")'
    )
    list_partitions_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    list_partitions_parser.set_defaults(func=cmd_table_list_partitions)

    # table rename command
    rename_parser = table_subparsers.add_parser('rename', help='Rename a table')

    # table full-text-search command
    fts_parser = table_subparsers.add_parser('full-text-search', help='Full-text search on a table')
    fts_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    fts_parser.add_argument(
        '--column', '-c',
        required=True,
        help='Text column to search on'
    )
    fts_parser.add_argument(
        '--query', '-q',
        required=True,
        help='Query text to search for'
    )
    fts_parser.add_argument(
        '--limit', '-l',
        type=int,
        default=10,
        help='Maximum number of results to return (default: 10)'
    )
    fts_parser.add_argument(
        '--select', '-s',
        type=str,
        default=None,
        help='Select specific columns to display (comma-separated, e.g., "id,name,content")'
    )
    fts_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    fts_parser.set_defaults(func=cmd_table_full_text_search)
    rename_parser.add_argument(
        'table',
        help='Source table identifier in format: database.table'
    )
    rename_parser.add_argument(
        'target',
        help='Target table identifier in format: database.table'
    )
    rename_parser.set_defaults(func=cmd_table_rename)

    # table stream command
    stream_parser = table_subparsers.add_parser(
        'stream',
        help='Continuously stream new rows from a table until interrupted (Ctrl+C)'
    )
    stream_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    stream_parser.add_argument(
        '--select', '-s',
        type=str,
        default=None,
        help='Select specific columns (comma-separated, e.g., "id,name")'
    )
    stream_parser.add_argument(
        '--where', '-w',
        type=str,
        default=None,
        help='Filter condition in SQL-like syntax (e.g., "age > 18")'
    )
    stream_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    stream_parser.add_argument(
        '--from',
        dest='from_position',
        type=str,
        default='latest',
        help=(
            'Starting position: "latest" (default), "earliest", a numeric snapshot ID, '
            'or a timestamp (e.g., "2025-01-15", "2025-01-15T10:30:00Z")'
        )
    )
    stream_parser.add_argument(
        '--poll-interval-ms',
        type=int,
        default=1000,
        help='Milliseconds between snapshot polls (default: 1000)'
    )
    stream_parser.add_argument(
        '--include-row-kind',
        action='store_true',
        default=False,
        help='Include _row_kind column (+I, -U, +U, -D) in output'
    )
    stream_parser.add_argument(
        '--consumer-id',
        type=str,
        default=None,
        help='Consumer ID for persisting scan progress across invocations'
    )
    stream_parser.set_defaults(func=cmd_table_stream)

    # table alter command
    alter_parser = table_subparsers.add_parser('alter', help='Alter a table with schema changes')
    alter_parser.add_argument(
        'table',
        help='Table identifier in format: database.table'
    )
    alter_parser.add_argument(
        '--ignore-if-not-exists', '-i',
        action='store_true',
        help='Do not raise error if table does not exist'
    )
    alter_subparsers = alter_parser.add_subparsers(dest='alter_command', help='Alter commands')
    
    # alter set-option
    set_option_parser = alter_subparsers.add_parser('set-option', help='Set a table option')
    set_option_parser.add_argument('--key', '-k', required=True, help='Option key')
    set_option_parser.add_argument('--value', '-v', required=True, help='Option value')
    set_option_parser.set_defaults(func=cmd_table_alter)
    
    # alter remove-option
    remove_option_parser = alter_subparsers.add_parser('remove-option', help='Remove a table option')
    remove_option_parser.add_argument('--key', '-k', required=True, help='Option key to remove')
    remove_option_parser.set_defaults(func=cmd_table_alter)
    
    # alter add-column
    add_column_parser = alter_subparsers.add_parser('add-column', help='Add a column to the table')
    add_column_parser.add_argument('--name', '-n', required=True, help='Column name')
    add_column_parser.add_argument('--type', '-t', required=True, help='Column data type (e.g. INT, STRING, BIGINT)')
    add_column_parser.add_argument('--comment', '-c', default=None, help='Column comment')
    add_column_position = add_column_parser.add_mutually_exclusive_group()
    add_column_position.add_argument('--first', action='store_true', help='Add column as the first column')
    add_column_position.add_argument('--after', metavar='COLUMN', help='Add column after the specified column')
    add_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter drop-column
    drop_column_parser = alter_subparsers.add_parser('drop-column', help='Drop a column from the table')
    drop_column_parser.add_argument('--name', '-n', required=True, help='Column name to drop')
    drop_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter rename-column
    rename_column_parser = alter_subparsers.add_parser('rename-column', help='Rename a column')
    rename_column_parser.add_argument('--name', '-n', required=True, help='Current column name')
    rename_column_parser.add_argument('--new-name', '-m', required=True, help='New column name')
    rename_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter alter-column (change column type, comment, or position)
    alter_column_parser = alter_subparsers.add_parser('alter-column', help='Alter column type, comment, or position')
    alter_column_parser.add_argument('--name', '-n', required=True, help='Column name to alter')
    alter_column_parser.add_argument('--type', '-t', default=None, help='New column data type (e.g. DOUBLE, BIGINT)')
    alter_column_parser.add_argument('--comment', '-c', default=None, help='New column comment')
    alter_column_position = alter_column_parser.add_mutually_exclusive_group()
    alter_column_position.add_argument('--first', action='store_true', help='Move column to the first position')
    alter_column_position.add_argument('--after', metavar='COLUMN', help='Move column after the specified column')
    alter_column_parser.set_defaults(func=cmd_table_alter)
    
    # alter update-comment
    update_comment_parser = alter_subparsers.add_parser('update-comment', help='Update table comment')
    update_comment_parser.add_argument('--comment', '-c', required=True, help='New table comment')
    update_comment_parser.set_defaults(func=cmd_table_alter)
