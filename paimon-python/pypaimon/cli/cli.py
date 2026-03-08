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
Paimon CLI - Command Line Interface for Apache Paimon.

This module provides a CLI for interacting with Paimon catalogs and tables.
"""

import argparse
import os
import sys
from typing import Dict

import yaml


def load_catalog_config(config_path: str = 'paimon.yaml') -> Dict:
    """
    Load catalog configuration from a YAML file.
    
    Args:
        config_path: Path to the catalog configuration file.
                     Defaults to 'paimon.yaml' in the current directory.
    
    Returns:
        Dictionary containing catalog configuration options.
    
    Raises:
        FileNotFoundError: If the configuration file does not exist.
        ValueError: If the configuration file is invalid.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Catalog configuration file not found: {config_path}\n"
            f"Please create a paimon.yaml file in the current directory.\n"
            f"Example paimon.yaml:\n"
            f"  metastore: filesystem\n"
            f"  warehouse: /path/to/warehouse\n"
        )
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    if not config:
        raise ValueError(f"Empty configuration file: {config_path}")
    
    # Validate required fields
    if 'metastore' not in config:
        config['metastore'] = 'filesystem'
    
    if config['metastore'] == 'filesystem' and 'warehouse' not in config:
        raise ValueError(
            "Missing required 'warehouse' configuration for filesystem catalog.\n"
            "Please add 'warehouse: /path/to/warehouse' to your paimon.yaml"
        )
    
    return config


def create_catalog(config: Dict):
    """
    Create a catalog instance from configuration.
    
    Args:
        config: Dictionary containing catalog configuration options.
    
    Returns:
        Catalog instance.
    """
    from pypaimon import CatalogFactory
    return CatalogFactory.create(config)


def main():
    """Main entry point for the Paimon CLI."""
    parser = argparse.ArgumentParser(
        prog='paimon',
        description='Apache Paimon Command Line Interface'
    )
    parser.add_argument(
        '--config', '-c',
        default='paimon.yaml',
        help='Path to catalog configuration file (default: paimon.yaml)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Table commands
    table_parser = subparsers.add_parser('table', help='Table operations')
    
    # Import and add table subcommands
    from pypaimon.cli.cli_table import add_table_subcommands
    add_table_subcommands(table_parser)

    # Database commands
    db_parser = subparsers.add_parser('db', help='Database operations')

    # Import and add database subcommands
    from pypaimon.cli.cli_db import add_db_subcommands
    add_db_subcommands(db_parser)

    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(0)
    
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
