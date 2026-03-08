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
Database commands for Paimon CLI.

This module provides database-related commands for the CLI.
"""

import json
import sys


def cmd_db_get(args):
    """
    Execute the 'db get' command.

    Gets and displays database information in JSON format.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    database_name = args.database

    try:
        database = catalog.get_database(database_name)
    except Exception as e:
        print(f"Error: Failed to get database '{database_name}': {e}", file=sys.stderr)
        sys.exit(1)

    result = {
        "name": database.name,
        "options": database.options if database.options else {},
    }
    if database.comment is not None:
        result["comment"] = database.comment

    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_db_create(args):
    """
    Execute the 'db create' command.

    Creates a new database.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    database_name = args.database
    ignore_if_exists = args.ignore_if_exists

    # Parse properties if provided
    properties = None
    if args.properties:
        try:
            properties = json.loads(args.properties)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format for properties: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        catalog.create_database(database_name, ignore_if_exists, properties)
        print(f"Database '{database_name}' created successfully.")
    except Exception as e:
        print(f"Error: Failed to create database '{database_name}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_db_drop(args):
    """
    Execute the 'db drop' command.

    Drops an existing database.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    database_name = args.database
    ignore_if_not_exists = args.ignore_if_not_exists
    cascade = args.cascade

    try:
        catalog.drop_database(database_name, ignore_if_not_exists, cascade)
        print(f"Database '{database_name}' dropped successfully.")
    except Exception as e:
        print(f"Error: Failed to drop database '{database_name}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_db_alter(args):
    """
    Execute the 'db alter' command.

    Alters database properties by setting or removing properties.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.catalog.rest.property_change import PropertyChange

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    database_name = args.database

    changes = []

    # Parse set properties
    if args.set:
        try:
            set_properties = json.loads(args.set)
            if not isinstance(set_properties, dict):
                print("Error: --set value must be a JSON object.", file=sys.stderr)
                sys.exit(1)
            for key, value in set_properties.items():
                changes.append(PropertyChange.set_property(key, str(value)))
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format for --set: {e}", file=sys.stderr)
            sys.exit(1)

    # Parse remove properties
    if args.remove:
        for key in args.remove:
            changes.append(PropertyChange.remove_property(key))

    if not changes:
        print("Error: No changes specified. Use --set or --remove.", file=sys.stderr)
        sys.exit(1)

    try:
        catalog.alter_database(database_name, changes)
        print(f"Database '{database_name}' altered successfully.")
    except NotImplementedError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to alter database '{database_name}': {e}", file=sys.stderr)
        sys.exit(1)


def cmd_db_list_tables(args):
    """
    Execute the 'db list-tables' command.

    Lists all tables in a database.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    database_name = args.database

    try:
        tables = catalog.list_tables(database_name)
    except Exception as e:
        print(f"Error: Failed to list tables in database '{database_name}': {e}", file=sys.stderr)
        sys.exit(1)

    if not tables:
        print(f"No tables found in database '{database_name}'.")
    else:
        for table_name in tables:
            print(table_name)


def add_db_subcommands(db_parser):
    """
    Add database subcommands to the parser.

    Args:
        db_parser: The db subparser to add commands to.
    """
    db_subparsers = db_parser.add_subparsers(dest='db_command', help='Database commands')

    # db get command
    get_parser = db_subparsers.add_parser('get', help='Get database information')
    get_parser.add_argument(
        'database',
        help='Database name'
    )
    get_parser.set_defaults(func=cmd_db_get)

    # db create command
    create_parser = db_subparsers.add_parser('create', help='Create a new database')
    create_parser.add_argument(
        'database',
        help='Database name'
    )
    create_parser.add_argument(
        '--properties', '-p',
        default=None,
        help='Database properties as JSON string (e.g., \'{"key1": "value1"}\')'
    )
    create_parser.add_argument(
        '--ignore-if-exists', '-i',
        action='store_true',
        help='Do not raise error if database already exists'
    )
    create_parser.set_defaults(func=cmd_db_create)

    # db drop command
    drop_parser = db_subparsers.add_parser('drop', help='Drop a database')
    drop_parser.add_argument(
        'database',
        help='Database name'
    )
    drop_parser.add_argument(
        '--ignore-if-not-exists',
        action='store_true',
        help='Do not raise error if database does not exist'
    )
    drop_parser.add_argument(
        '--cascade',
        action='store_true',
        help='Drop all tables in the database before dropping it'
    )
    drop_parser.set_defaults(func=cmd_db_drop)

    # db alter command
    alter_parser = db_subparsers.add_parser('alter', help='Alter database properties')
    alter_parser.add_argument(
        'database',
        help='Database name'
    )
    alter_parser.add_argument(
        '--set', '-s',
        default=None,
        help='Properties to set as JSON string (e.g., \'{"key1": "value1"}\')'
    )
    alter_parser.add_argument(
        '--remove', '-r',
        nargs='+',
        default=None,
        help='Property keys to remove'
    )
    alter_parser.set_defaults(func=cmd_db_alter)

    # db list-tables command
    list_tables_parser = db_subparsers.add_parser('list-tables', help='List all tables in a database')
    list_tables_parser.add_argument(
        'database',
        help='Database name'
    )
    list_tables_parser.set_defaults(func=cmd_db_list_tables)
