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
SQL commands for Paimon CLI.

This module provides SQL query capability via pypaimon-rust + DataFusion.
"""

import os
import re
import sys
import time

_PAIMON_BANNER = r"""
    ____        _
   / __ \____ _(_)___ ___  ____  ____
  / /_/ / __ `/ / __ `__ \/ __ \/ __ \
 / ____/ /_/ / / / / / / / /_/ / / / /
/_/    \__,_/_/_/ /_/ /_/\____/_/ /_/

  Powered by pypaimon-rust + DataFusion
  Type 'help' for usage, 'exit' to quit.
"""

_USE_PATTERN = re.compile(
    r"^\s*use\s+(\w+)\s*;?\s*$",
    re.IGNORECASE,
)

_HISTORY_FILE = os.path.expanduser("~/.paimon_history")
_HISTORY_MAX_LENGTH = 1000

_PROMPT = "paimon> "
_CONTINUATION_PROMPT = "      > "


def _get_readline():
    """Get the best available readline module.

    Prefers gnureadline (full GNU readline) over the built-in readline
    (which is libedit on macOS and may have limited features).
    """
    try:
        import gnureadline as readline
        return readline
    except ImportError:
        pass
    try:
        import readline
        return readline
    except ImportError:
        return None


def _is_libedit(rl):
    """Check if the readline module is backed by libedit (macOS default)."""
    return hasattr(rl, '__doc__') and rl.__doc__ and 'libedit' in rl.__doc__


def _setup_readline():
    """Enable readline for arrow key support and persistent command history."""
    rl = _get_readline()
    if rl is None:
        return
    rl.set_history_length(_HISTORY_MAX_LENGTH)
    if not os.path.exists(_HISTORY_FILE):
        return
    if _is_libedit(rl):
        # libedit escapes spaces as \040 in history files, so we load manually.
        with open(_HISTORY_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.rstrip('\n')
                if line:
                    rl.add_history(line)
    else:
        rl.read_history_file(_HISTORY_FILE)


def _save_history():
    """Save readline history to file."""
    rl = _get_readline()
    if rl is None:
        return
    try:
        if _is_libedit(rl):
            # Write history manually to avoid libedit's \040 escaping.
            length = rl.get_current_history_length()
            lines = []
            for i in range(1, length + 1):
                item = rl.get_history_item(i)
                if item is not None:
                    lines.append(item)
            # Keep only the last N entries
            lines = lines[-_HISTORY_MAX_LENGTH:]
            with open(_HISTORY_FILE, 'w', encoding='utf-8') as f:
                for line in lines:
                    f.write(line + '\n')
        else:
            rl.write_history_file(_HISTORY_FILE)
    except OSError:
        pass


def cmd_sql(args):
    """
    Execute the 'sql' command.

    Runs a SQL query against Paimon tables, or starts an interactive SQL REPL.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config

    config_path = args.config
    config = load_catalog_config(config_path)

    try:
        from pypaimon.sql.sql_context import SQLContext
        catalog_options = {str(k): str(v) for k, v in config.items()}
        ctx = SQLContext()
        ctx.register_catalog("paimon", catalog_options)
        ctx.set_current_catalog("paimon")
        ctx.set_current_database("default")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    query = args.query
    if query:
        _execute_query(ctx, query, getattr(args, 'format', 'table'))
    else:
        _interactive_repl(ctx, getattr(args, 'format', 'table'))


def _execute_query(ctx, query, output_format):
    """Execute a single SQL query and print the result."""
    try:
        table = ctx.sql(query)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    _print_table(table, output_format)


def _print_table(table, output_format, elapsed=None):
    """Print a PyArrow Table in the requested format."""
    df = table.to_pandas()
    if output_format == 'json':
        import json
        print(json.dumps(df.to_dict(orient='records'), ensure_ascii=False))
    else:
        print(df.to_string(index=False))

    if elapsed is not None:
        row_count = len(df)
        print(f"({row_count} {'row' if row_count == 1 else 'rows'} in {elapsed:.2f}s)")


def _read_multiline_query():
    """Read a potentially multi-line SQL query, terminated by ';'.

    Returns the complete query string, or None on EOF/interrupt.
    """
    lines = []
    prompt = _PROMPT
    while True:
        try:
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            if lines:
                # Cancel current multi-line input
                print()
                return ""
            return None

        lines.append(line)
        joined = "\n".join(lines).strip()

        if not joined:
            lines.clear()
            prompt = _PROMPT
            continue

        # Single-word commands that don't need ';'
        lower = joined.lower().rstrip(';').strip()
        if lower in ('exit', 'quit', 'help'):
            return joined

        # USE command doesn't strictly need ';'
        if _USE_PATTERN.match(joined):
            return joined

        # For SQL statements, wait for ';'
        if joined.endswith(';'):
            return joined

        prompt = _CONTINUATION_PROMPT


def _handle_use(ctx, match):
    """Handle USE <database> command."""
    database = match.group(1)
    try:
        ctx.set_current_database(database)
        print(f"Using database '{database}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)


def _interactive_repl(ctx, output_format):
    """Run an interactive SQL REPL."""
    _setup_readline()
    print(_PAIMON_BANNER)

    try:
        while True:
            query = _read_multiline_query()
            if query is None:
                print("\nBye!")
                break

            if not query:
                continue

            lower = query.lower().rstrip(';').strip()
            if lower in ('exit', 'quit'):
                print("Bye!")
                break
            if lower == 'help':
                _print_help()
                continue

            # Handle USE <database>
            use_match = _USE_PATTERN.match(query)
            if use_match:
                _handle_use(ctx, use_match)
                continue

            try:
                start = time.time()
                table = ctx.sql(query)
                elapsed = time.time() - start
                _print_table(table, output_format, elapsed)
                print()
            except Exception as e:
                print(f"Error: {e}\n", file=sys.stderr)
    finally:
        _save_history()


def _print_help():
    """Print REPL help information."""
    print("""
Commands:
  USE <database>;              Switch the default database
  SHOW DATABASES;              List all databases
  SHOW TABLES;                 List tables in the current database
  SELECT ... FROM <table>;     Execute a SQL query
  exit / quit                  Exit the REPL

Table reference:
  <table>                      Table in the current default database
  <database>.<table>           Table in a specific database

Tips:
  - SQL statements end with ';' and can span multiple lines
  - Arrow keys are supported for line editing and command history
  - Command history is saved across sessions (~/.paimon_history)
""")


def add_sql_subcommand(subparsers):
    """
    Add the sql subcommand to the main parser.

    Args:
        subparsers: The subparsers object from the main argument parser.
    """
    sql_parser = subparsers.add_parser(
        'sql',
        help='Execute SQL queries on Paimon tables (requires pypaimon-rust)'
    )
    sql_parser.add_argument(
        'query',
        nargs='?',
        default=None,
        help='SQL query to execute. If omitted, starts interactive REPL.'
    )
    sql_parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['table', 'json'],
        default='table',
        help='Output format: table (default) or json'
    )
    sql_parser.set_defaults(func=cmd_sql)
