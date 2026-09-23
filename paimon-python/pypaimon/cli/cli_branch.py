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

"""Branch commands for the Paimon CLI.

Adds the top-level ``branch {create,list,delete,rename,fast-forward}``
command. All operations go through the Catalog layer so they get typed
exceptions and work for both filesystem and REST catalogs. Command and
argument names mirror the Java branch procedures (``create_branch`` /
``delete_branch`` / ``rename_branch`` / ``fast_forward``).
"""

import json
import sys

from pypaimon.catalog.catalog_exception import (BranchAlreadyExistException,
                                                BranchNotExistException,
                                                TableNotExistException,
                                                TagNotExistException)


def _open_catalog(args):
    """Load config, build the catalog and validate the ``database.table`` id.

    Returns ``(catalog, identifier)``. On any failure, prints to stderr and
    exits with a non-zero status (matching the other CLI commands).
    """
    from pypaimon.cli.cli import create_catalog, load_catalog_config

    identifier = args.table
    if len(identifier.split('.')) != 2:
        print("Error: Invalid table identifier '{}'. Expected format: "
              "'database.table'".format(identifier), file=sys.stderr)
        sys.exit(1)
    try:
        catalog = create_catalog(load_catalog_config(args.config))
    except Exception as e:
        print("Error: {}".format(e), file=sys.stderr)
        sys.exit(1)
    return catalog, identifier


def cmd_branch_create(args):
    """Execute ``branch create``."""
    catalog, identifier = _open_catalog(args)
    try:
        catalog.create_branch(identifier, args.branch_name, tag_name=args.tag)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except BranchAlreadyExistException:
        print("Error: Branch '{}' already exists.".format(args.branch_name),
              file=sys.stderr)
        sys.exit(1)
    except TagNotExistException:
        print("Error: Tag '{}' does not exist.".format(args.tag),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to create branch: {}".format(e), file=sys.stderr)
        sys.exit(1)
    if args.tag is not None:
        print("Branch '{}' created from tag '{}' on table '{}'.".format(
            args.branch_name, args.tag, identifier))
    else:
        print("Branch '{}' created on table '{}'.".format(
            args.branch_name, identifier))


def cmd_branch_delete(args):
    """Execute ``branch delete``."""
    catalog, identifier = _open_catalog(args)
    try:
        catalog.drop_branch(identifier, args.branch_name)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except BranchNotExistException:
        print("Error: Branch '{}' does not exist.".format(args.branch_name),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to delete branch: {}".format(e), file=sys.stderr)
        sys.exit(1)
    print("Branch '{}' deleted from table '{}'.".format(
        args.branch_name, identifier))


def cmd_branch_list(args):
    """Execute ``branch list``."""
    catalog, identifier = _open_catalog(args)
    try:
        branches = catalog.list_branches(identifier)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to list branches: {}".format(e), file=sys.stderr)
        sys.exit(1)

    if args.format == 'json':
        print(json.dumps(branches, ensure_ascii=False))
    elif not branches:
        print("No branches found.")
    else:
        for branch in branches:
            print(branch)


def cmd_branch_rename(args):
    """Execute ``branch rename``."""
    catalog, identifier = _open_catalog(args)
    try:
        catalog.rename_branch(identifier, args.from_branch, args.to_branch)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except BranchNotExistException:
        print("Error: Branch '{}' does not exist.".format(args.from_branch),
              file=sys.stderr)
        sys.exit(1)
    except BranchAlreadyExistException:
        print("Error: Branch '{}' already exists.".format(args.to_branch),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to rename branch: {}".format(e), file=sys.stderr)
        sys.exit(1)
    print("Branch '{}' renamed to '{}' on table '{}'.".format(
        args.from_branch, args.to_branch, identifier))


def cmd_branch_fast_forward(args):
    """Execute ``branch fast-forward``."""
    catalog, identifier = _open_catalog(args)
    try:
        catalog.fast_forward(identifier, args.branch_name)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except BranchNotExistException:
        print("Error: Branch '{}' does not exist.".format(args.branch_name),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to fast-forward: {}".format(e), file=sys.stderr)
        sys.exit(1)
    print("Fast-forwarded table '{}' to branch '{}'.".format(
        identifier, args.branch_name))


def add_branch_subcommands(subparsers):
    """Register the top-level ``branch <command>`` command.

    Args:
        subparsers: The subparsers object from the main argument parser.
    """
    branch_parser = subparsers.add_parser('branch', help='Branch operations')
    branch_subparsers = branch_parser.add_subparsers(
        dest='branch_command', help='Branch commands')

    # branch create
    create_parser = branch_subparsers.add_parser(
        'create', help='Create a branch on a table')
    create_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    create_parser.add_argument(
        'branch_name', help='Name of the branch to create')
    create_parser.add_argument(
        '--tag', '-t', default=None,
        help='Create the branch from this tag (default: current state)')
    create_parser.set_defaults(func=cmd_branch_create)

    # branch list
    list_parser = branch_subparsers.add_parser(
        'list', help='List branches of a table')
    list_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    list_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='table',
        help='Output format: table (default) or json')
    list_parser.set_defaults(func=cmd_branch_list)

    # branch delete
    delete_parser = branch_subparsers.add_parser(
        'delete', help='Delete a branch from a table')
    delete_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    delete_parser.add_argument(
        'branch_name', help='Name of the branch to delete')
    delete_parser.set_defaults(func=cmd_branch_delete)

    # branch rename
    rename_parser = branch_subparsers.add_parser(
        'rename', help='Rename a branch')
    rename_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    rename_parser.add_argument('from_branch', help='Current branch name')
    rename_parser.add_argument('to_branch', help='New branch name')
    rename_parser.set_defaults(func=cmd_branch_rename)

    # branch fast-forward
    ff_parser = branch_subparsers.add_parser(
        'fast-forward', help='Fast-forward main to a branch')
    ff_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    ff_parser.add_argument(
        'branch_name', help='Name of the branch to fast-forward to')
    ff_parser.set_defaults(func=cmd_branch_fast_forward)
