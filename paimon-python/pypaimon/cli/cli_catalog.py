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
Catalog commands for Paimon CLI.

This module provides catalog-related commands for the CLI.
"""

import sys


def cmd_catalog_list_dbs(args):
    """
    Execute the 'catalog list-dbs' command.

    Lists all databases in the catalog.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    try:
        databases = catalog.list_databases()
    except Exception as e:
        print(f"Error: Failed to list databases: {e}", file=sys.stderr)
        sys.exit(1)

    if not databases:
        print("No databases found.")
    else:
        for database_name in databases:
            print(database_name)


def add_catalog_subcommands(catalog_parser):
    """
    Add catalog subcommands to the parser.

    Args:
        catalog_parser: The catalog subparser to add commands to.
    """
    catalog_subparsers = catalog_parser.add_subparsers(dest='catalog_command', help='Catalog commands')

    # catalog list-dbs command
    list_dbs_parser = catalog_subparsers.add_parser('list-dbs', help='List all databases')
    list_dbs_parser.set_defaults(func=cmd_catalog_list_dbs)
