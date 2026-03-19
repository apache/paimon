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
#  limitations under the License.
################################################################################
"""
Table stream command for Paimon CLI.

Continuously polls a table for new snapshots and prints rows as they arrive.
"""

import json
import sys
from typing import Union

from pypaimon.snapshot.snapshot_manager import SnapshotManager


def _parse_timestamp(value: str) -> int:
    """Parse a human-readable datetime string to epoch milliseconds.

    Accepts ISO 8601, space-separated datetimes, date-only strings, and named
    timezones (e.g. "2025-01-15T10:30:00 EST", "2025-01-15T10:30:00 Europe/London").
    Naive datetimes (no timezone) are treated as local machine time.
    Returns epoch ms as an integer.
    Raises ValueError on unrecognised format.
    """
    from dateutil import parser as dateutil_parser
    from dateutil.parser import ParserError

    try:
        dt = dateutil_parser.parse(value)
    except (ParserError, OverflowError):
        raise ValueError(
            f"Unrecognised timestamp format: '{value}'. "
            "Expected formats: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, "
            "YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SSZ, YYYY-MM-DDTHH:MM:SS+HH:MM, "
            "or a datetime with a named timezone (e.g. '2025-01-15T10:30:00 EST')"
        )

    if dt.tzinfo is None:
        # Treat naive datetime as local time
        dt = dt.astimezone()

    return int(dt.timestamp() * 1000)


def parse_from_position(value: str, snapshot_manager: SnapshotManager) -> Union[str, int]:
    """Resolve a --from value to a keyword or snapshot ID integer.

    Args:
        value: The raw --from argument (keyword, integer string, or timestamp).
        snapshot_manager: Used to resolve timestamps to snapshot IDs.

    Returns:
        "latest", "earliest", or an integer snapshot ID.

    Raises:
        ValueError: If the value is unrecognised or the timestamp precedes all snapshots.
    """
    if value in ("latest", "earliest"):
        return value

    # Pure integer → snapshot ID
    if value.strip().isdigit():
        return int(value.strip())

    # Anything containing '-', 'T', ':', or '/' is treated as a timestamp
    if any(c in value for c in ("-", "T", ":", "/")):
        epoch_ms = _parse_timestamp(value)
        snapshot = snapshot_manager.earlier_or_equal_time_mills(epoch_ms)
        if snapshot is None:
            raise ValueError(
                f"No snapshot found at or before '{value}'. "
                "The timestamp may predate all stored snapshots."
            )
        return snapshot.id

    raise ValueError(
        f"Unrecognised --from value: '{value}'. "
        "Use 'latest', 'earliest', a snapshot ID, or a timestamp."
    )


def cmd_table_stream(args):
    """Execute the 'table stream' command.

    Continuously reads new rows from a Paimon table and prints them to stdout
    until interrupted with Ctrl+C.

    Args:
        args: Parsed command line arguments.
    """
    from pypaimon.cli.cli import load_catalog_config, create_catalog
    from pypaimon.table.file_store_table import FileStoreTable

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(
            f"Error: Invalid table identifier '{table_identifier}'. "
            "Expected format: 'database.table'",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        table = catalog.get_table(table_identifier)
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    available_fields = set(field.name for field in table.table_schema.fields)

    # --- Validate and build projection ---
    user_columns = None
    if args.select:
        user_columns = [col.strip() for col in args.select.split(',')]
        invalid_columns = [col for col in user_columns if col not in available_fields]
        if invalid_columns:
            print(
                f"Error: Column(s) {invalid_columns} do not exist in table '{table_identifier}'.",
                file=sys.stderr,
            )
            sys.exit(1)

    # --- Parse WHERE predicate ---
    predicate = None
    if args.where:
        from pypaimon.cli.where_parser import parse_where_clause
        try:
            predicate = parse_where_clause(args.where, table.table_schema.fields)
        except ValueError as e:
            print(f"Error: Invalid WHERE clause: {e}", file=sys.stderr)
            sys.exit(1)

    # --- Resolve --from position ---
    from_position = args.from_position
    if from_position not in ("latest", "earliest") and not from_position.strip().isdigit():
        # Likely a timestamp — need snapshot_manager to resolve it
        if not isinstance(table, FileStoreTable):
            print(
                "Error: Timestamp-based --from requires a FileStoreTable.",
                file=sys.stderr,
            )
            sys.exit(1)
        snapshot_manager = table.snapshot_manager()
        try:
            from_position = parse_from_position(from_position, snapshot_manager)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    elif from_position.strip().isdigit():
        from_position = int(from_position.strip())

    # --- Build StreamReadBuilder ---
    builder = table.new_stream_read_builder()

    if user_columns:
        builder = builder.with_projection(user_columns)
    if predicate:
        builder = builder.with_filter(predicate)
    if args.poll_interval_ms is not None:
        builder = builder.with_poll_interval_ms(args.poll_interval_ms)
    if args.include_row_kind:
        builder = builder.with_include_row_kind(True)
    if args.consumer_id:
        builder = builder.with_consumer_id(args.consumer_id)

    builder = builder.with_scan_from(from_position)

    scan = builder.new_streaming_scan()
    read = builder.new_read()
    output_format = getattr(args, 'format', 'table')

    first_batch = True
    try:
        for plan in scan.stream_sync():
            splits = plan.splits()
            if not splits:
                continue

            df = read.to_pandas(splits)
            if df.empty:
                continue

            if output_format == 'json':
                for record in df.to_dict(orient='records'):
                    print(json.dumps(record, ensure_ascii=False, default=str))
            else:
                if not first_batch:
                    print("---")
                if first_batch:
                    print(df.to_string(index=False))
                    first_batch = False
                else:
                    # Print rows only (no header) for subsequent batches
                    print(df.to_string(index=False, header=False))

    except KeyboardInterrupt:
        sys.exit(0)
