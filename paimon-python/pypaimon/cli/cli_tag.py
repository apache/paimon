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

"""Tag commands for the Paimon CLI.

Adds the top-level ``tag {create,list,delete,get}`` subcommands (alongside
``table`` / ``db`` / ``catalog``). All operations go through the Catalog
layer so they get typed exceptions and work for both filesystem and REST
catalogs.
"""

import json
import sys

from pypaimon.catalog.catalog_exception import (TableNotExistException,
                                                TagAlreadyExistException,
                                                TagNotExistException)
from pypaimon.common.json_util import JSON


def _open_catalog(args):
    """Load config, build the catalog and validate the ``database.table`` id.

    Returns ``(catalog, identifier)``. On any failure, prints to stderr and
    exits with a non-zero status (matching the other table commands).
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


def cmd_tag_create(args):
    """Execute ``tag create``."""
    catalog, identifier = _open_catalog(args)
    try:
        catalog.create_tag(
            identifier,
            args.tag_name,
            snapshot_id=args.snapshot_id,
            ignore_if_exists=args.ignore_if_exists,
        )
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except TagAlreadyExistException:
        print("Error: Tag '{}' already exists.".format(args.tag_name),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to create tag: {}".format(e), file=sys.stderr)
        sys.exit(1)
    print("Tag '{}' created on table '{}'.".format(args.tag_name, identifier))


def cmd_tag_delete(args):
    """Execute ``tag delete``."""
    catalog, identifier = _open_catalog(args)
    try:
        catalog.delete_tag(identifier, args.tag_name)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except TagNotExistException:
        print("Error: Tag '{}' does not exist.".format(args.tag_name),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to delete tag: {}".format(e), file=sys.stderr)
        sys.exit(1)
    print("Tag '{}' deleted from table '{}'.".format(args.tag_name, identifier))


def cmd_tag_list(args):
    """Execute ``tag list``."""
    catalog, identifier = _open_catalog(args)
    try:
        paged = catalog.list_tags_paged(identifier, tag_name_prefix=args.prefix)
        tags = paged.elements
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to list tags: {}".format(e), file=sys.stderr)
        sys.exit(1)

    # The CLI does not expose paging; warn (rather than silently truncate) if
    # the catalog returned a partial page.
    if paged.next_page_token is not None:
        print("Note: tag list may be truncated; more tags are available.",
              file=sys.stderr)

    if args.format == 'json':
        print(json.dumps(tags, ensure_ascii=False))
    elif not tags:
        print("No tags found.")
    else:
        for tag in tags:
            print(tag)


def cmd_tag_get(args):
    """Execute ``tag get``."""
    catalog, identifier = _open_catalog(args)
    try:
        response = catalog.get_tag(identifier, args.tag_name)
    except TableNotExistException:
        print("Error: Table '{}' does not exist.".format(identifier),
              file=sys.stderr)
        sys.exit(1)
    except TagNotExistException:
        print("Error: Tag '{}' does not exist.".format(args.tag_name),
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: Failed to get tag: {}".format(e), file=sys.stderr)
        sys.exit(1)

    if args.format == 'json':
        print(JSON.to_json(response, indent=2))
        return

    print("Tag: {}".format(response.tag_name))
    snapshot = response.snapshot
    if snapshot is not None:
        print("  Snapshot ID: {}".format(snapshot.id))
        print("  Schema ID: {}".format(snapshot.schema_id))
        print("  Record Count: {}".format(snapshot.total_record_count))
    # create_time / time_retained are surfaced only when present (populated for
    # tags created with a retention).
    if response.tag_create_time is not None:
        print("  Create Time: {}".format(response.tag_create_time))
    if response.tag_time_retained is not None:
        print("  Time Retained: {}".format(response.tag_time_retained))


def add_tag_subcommands(subparsers):
    """Register the top-level ``tag <command>`` subcommands."""
    tag_parser = subparsers.add_parser(
        'tag', help='Tag operations on a table')
    tag_subparsers = tag_parser.add_subparsers(
        dest='tag_command', help='Tag commands')

    # tag create
    create_parser = tag_subparsers.add_parser(
        'create', help='Create a tag on a table')
    create_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    create_parser.add_argument('tag_name', help='Name of the tag to create')
    create_parser.add_argument(
        '--snapshot-id', '-s', type=int, default=None,
        help='Snapshot id to tag (default: the latest snapshot)')
    create_parser.add_argument(
        '--ignore-if-exists', '-i', action='store_true',
        help='Do not error if the tag already exists')
    create_parser.set_defaults(func=cmd_tag_create)

    # tag list
    list_parser = tag_subparsers.add_parser(
        'list', help='List tags of a table')
    list_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    list_parser.add_argument(
        '--prefix', '-p', default=None,
        help='Only list tags whose name starts with this prefix')
    list_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='table',
        help='Output format: table (default) or json')
    list_parser.set_defaults(func=cmd_tag_list)

    # tag get
    get_parser = tag_subparsers.add_parser(
        'get', help='Show details of a tag')
    get_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    get_parser.add_argument('tag_name', help='Name of the tag')
    get_parser.add_argument(
        '--format', '-f', choices=['table', 'json'], default='table',
        help='Output format: table (default) or json')
    get_parser.set_defaults(func=cmd_tag_get)

    # tag delete
    delete_parser = tag_subparsers.add_parser(
        'delete', help='Delete a tag from a table')
    delete_parser.add_argument(
        'table', help='Table identifier in format: database.table')
    delete_parser.add_argument('tag_name', help='Name of the tag to delete')
    delete_parser.set_defaults(func=cmd_tag_delete)
