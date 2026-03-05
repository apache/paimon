#!/usr/bin/env python3
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""
Apache Paimon CLI - Main entry point.

Usage:
    paimon <command> [options]

Commands:
    tail    Tail a Paimon table (stream new data)
"""

import argparse
import sys


def setup_tail_parser(parser: argparse.ArgumentParser) -> None:
    """Configure the tail subcommand parser."""
    parser.add_argument(
        'warehouse',
        help='Warehouse path (e.g., s3://bucket/warehouse)'
    )
    parser.add_argument(
        'table',
        help='Table identifier (e.g., database.table or `db.name`.table)'
    )
    parser.add_argument(
        '--from', '-s',
        dest='from_pos',
        default=None,
        help=(
            'Start position: earliest, latest, snapshot ID, timestamp, or relative time '
            '(e.g., 12345, -1h, -30m, -7d, 2024-01-15, 2024-01-15T10:30:00). '
            'Default: earliest (or latest if --follow is set)'
        )
    )
    parser.add_argument(
        '--to', '-e',
        dest='to_pos',
        default=None,
        help=(
            'End position: latest, snapshot ID, timestamp, or relative time '
            '(e.g., 12345, -1h, 2024-01-15). Default: latest. '
            'Mutually exclusive with --follow'
        )
    )
    parser.add_argument(
        '--output', '-o',
        choices=['jsonl', 'json', 'csv', 'table'],
        default='jsonl',
        help='Output format (default: jsonl)'
    )
    parser.add_argument(
        '--filter', '-f',
        action='append',
        dest='filters',
        metavar='EXPR',
        help='Filter expression (repeatable): col=val, col>val, col~prefix'
    )
    parser.add_argument(
        '--columns', '-c',
        help='Columns to output (comma-separated)'
    )
    parser.add_argument(
        '--limit', '-n',
        type=int,
        help='Exit after N records'
    )
    parser.add_argument(
        '--follow', '-F',
        action='store_true',
        help='Keep waiting for new data (like tail -f). Mutually exclusive with --to'
    )
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=1000,
        help='Poll interval in milliseconds (default: 1000)'
    )
    parser.add_argument(
        '--consumer-id',
        help='Consumer ID for checkpointing'
    )
    parser.add_argument(
        '--include-row-kind',
        action='store_true',
        help='Include _row_kind column (+I, -D, etc.)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print status messages to stderr'
    )
    parser.add_argument(
        '--profile',
        action='store_true',
        help='Enable profiling output'
    )


def main() -> int:
    """Main entry point for the Paimon CLI."""
    parser = argparse.ArgumentParser(
        prog='paimon',
        description='Apache Paimon CLI - interact with Paimon tables'
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # tail subcommand
    tail_parser = subparsers.add_parser(
        'tail',
        help='Tail a Paimon table (stream new data)',
        description=(
            'Stream data from a Paimon table, similar to kafka-console-consumer. '
            'Supports various start positions, filtering, and output formats.'
        )
    )
    setup_tail_parser(tail_parser)

    args = parser.parse_args()

    if args.command == 'tail':
        # Validate mutual exclusion of --follow and --to
        if args.follow and args.to_pos:
            parser.error('--follow and --to are mutually exclusive')

        # Apply defaults for --from based on --follow
        if args.from_pos is None:
            args.from_pos = 'latest' if args.follow else 'earliest'

        # Apply default for --to
        if args.to_pos is None:
            args.to_pos = 'latest'

        from pypaimon.cli.tail import run_tail
        return run_tail(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
