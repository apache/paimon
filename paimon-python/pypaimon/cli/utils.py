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
Utility functions for the Paimon CLI.

This module provides:
- Output formatters (JSON Lines, JSON array, table, CSV)
- Filter expression parser
- Timestamp parser for start positions
"""

import csv
import json
import re
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TextIO, Tuple

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder


# -----------------------------------------------------------------------------
# Filter Parser
# -----------------------------------------------------------------------------


def parse_filter_expr(expr: str) -> Tuple[str, str, str]:
    """
    Parse a filter expression into (column, operator, value).

    Supported operators:
        =   : equal
        !=  : not equal
        >   : greater than
        >=  : greater or equal
        <   : less than
        <=  : less or equal
        ~   : starts with (prefix match)

    Args:
        expr: Filter expression like 'col=value' or 'col>=10'

    Returns:
        Tuple of (column_name, operator, value)

    Raises:
        ValueError: If expression format is invalid
    """
    match = re.match(r'^(\w+)(>=|<=|!=|>|<|=|~)(.+)$', expr)
    if not match:
        raise ValueError(f"Invalid filter expression: {expr}")
    return match.groups()


def build_predicate_from_filter(
    column: str,
    operator: str,
    value: str,
    predicate_builder: PredicateBuilder
) -> Predicate:
    """
    Build a Predicate from a parsed filter expression.

    Args:
        column: Column name
        operator: Comparison operator
        value: Value to compare against
        predicate_builder: PredicateBuilder instance

    Returns:
        Predicate object

    Raises:
        ValueError: If operator is not supported
    """
    # Try to convert value to appropriate type
    typed_value: Any = value
    try:
        typed_value = int(value)
    except ValueError:
        try:
            typed_value = float(value)
        except ValueError:
            pass  # Keep as string

    if operator == '=':
        return predicate_builder.equal(column, typed_value)
    elif operator == '!=':
        return predicate_builder.not_equal(column, typed_value)
    elif operator == '>':
        return predicate_builder.greater_than(column, typed_value)
    elif operator == '>=':
        return predicate_builder.greater_or_equal(column, typed_value)
    elif operator == '<':
        return predicate_builder.less_than(column, typed_value)
    elif operator == '<=':
        return predicate_builder.less_or_equal(column, typed_value)
    elif operator == '~':
        return predicate_builder.startswith(column, value)
    else:
        raise ValueError(f"Unsupported operator: {operator}")


def parse_filters(
    filter_exprs: List[str],
    predicate_builder: PredicateBuilder
) -> Optional[Predicate]:
    """
    Parse multiple filter expressions and combine with AND.

    Args:
        filter_exprs: List of filter expressions
        predicate_builder: PredicateBuilder instance

    Returns:
        Combined predicate or None if no filters
    """
    if not filter_exprs:
        return None

    predicates = []
    for expr in filter_exprs:
        column, operator, value = parse_filter_expr(expr)
        pred = build_predicate_from_filter(column, operator, value, predicate_builder)
        predicates.append(pred)

    return PredicateBuilder.and_predicates(predicates)


# -----------------------------------------------------------------------------
# Timestamp Parser
# -----------------------------------------------------------------------------

def parse_timestamp(ts_str: str) -> int:
    """
    Parse a timestamp string to milliseconds since epoch.

    Supported formats:
        -1h         : 1 hour ago
        -30m        : 30 minutes ago
        -7d         : 7 days ago
        2024-01-15T10:30:00  : ISO datetime
        2024-01-15  : Date only (start of day)

    Args:
        ts_str: Timestamp string

    Returns:
        Milliseconds since epoch

    Raises:
        ValueError: If format is not recognized
    """
    # Relative time: -1h, -30m, -7d
    if ts_str.startswith('-'):
        match = re.match(r'^-(\d+)([hdm])$', ts_str)
        if match:
            val, unit = int(match.group(1)), match.group(2)
            delta = {
                'h': timedelta(hours=val),
                'm': timedelta(minutes=val),
                'd': timedelta(days=val)
            }[unit]
            return int((datetime.now() - delta).timestamp() * 1000)

    # ISO format
    for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
        try:
            dt = datetime.strptime(ts_str, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    raise ValueError(f"Invalid timestamp: {ts_str}")


def parse_position(pos: str, snapshot_manager) -> Optional[int]:
    """
    Parse a position string into snapshot ID.

    Supported positions:
        earliest        : First available snapshot
        latest          : Latest snapshot (returns None)
        12345           : Specific snapshot ID (numeric)
        snapshot:12345  : Specific snapshot ID (explicit prefix)
        -1h, -30m, -7d  : Relative time (hours, minutes, days ago)
        2024-01-15      : ISO date
        2024-01-15T10:30:00 : ISO datetime
        time:<timestamp>: Timestamp with explicit prefix

    Args:
        pos: Position string
        snapshot_manager: SnapshotManager instance

    Returns:
        Snapshot ID, or None for latest

    Raises:
        ValueError: If format is invalid
    """
    if pos == 'earliest':
        snap = snapshot_manager.try_get_earliest_snapshot()
        return snap.id if snap else 1

    elif pos == 'latest':
        return None

    # Explicit snapshot: prefix
    elif pos.startswith('snapshot:'):
        return int(pos.split(':')[1])

    # Explicit time: prefix
    elif pos.startswith('time:'):
        ts = parse_timestamp(pos.split(':', 1)[1])
        snap = snapshot_manager.earlier_or_equal_time_mills(ts)
        if snap:
            return snap.id
        # No snapshot found at or before timestamp - use earliest
        earliest = snapshot_manager.try_get_earliest_snapshot()
        return earliest.id if earliest else 1

    # Numeric snapshot ID
    elif pos.isdigit():
        return int(pos)

    # Relative time (-1h, -30m, -7d) or ISO date/datetime
    else:
        try:
            ts = parse_timestamp(pos)
            snap = snapshot_manager.earlier_or_equal_time_mills(ts)
            if snap:
                return snap.id
            # No snapshot found at or before timestamp - use earliest
            earliest = snapshot_manager.try_get_earliest_snapshot()
            return earliest.id if earliest else 1
        except ValueError:
            raise ValueError(
                f"Invalid position: {pos}. Expected: earliest, latest, snapshot ID, "
                f"relative time (-1h, -30m, -7d), or ISO date/datetime"
            )


def parse_start_position(pos: str, snapshot_manager) -> Optional[int]:
    """
    Parse start position string into snapshot ID.

    See parse_position() for supported formats.

    Args:
        pos: Start position string
        snapshot_manager: SnapshotManager instance

    Returns:
        Snapshot ID to start from, or None for latest

    Raises:
        ValueError: If format is invalid
    """
    return parse_position(pos, snapshot_manager)


def parse_end_position(pos: str, snapshot_manager) -> Optional[int]:
    """
    Parse end position string into snapshot ID.

    See parse_position() for supported formats.

    Args:
        pos: End position string
        snapshot_manager: SnapshotManager instance

    Returns:
        Snapshot ID to end at (inclusive), or None for latest

    Raises:
        ValueError: If format is invalid
    """
    return parse_position(pos, snapshot_manager)


# -----------------------------------------------------------------------------
# Output Formatters
# -----------------------------------------------------------------------------

class OutputFormatter(ABC):
    """Base class for output formatters."""

    @abstractmethod
    def write(self, row: Dict[str, Any]) -> None:
        """Write a single row."""

    def close(self) -> None:
        """Finalize output (e.g., close JSON array)."""


class JsonLinesFormatter(OutputFormatter):
    """Output formatter for JSON Lines format (one JSON object per line)."""

    def __init__(self, output: TextIO = None):
        self.output = output or sys.stdout

    def write(self, row: Dict[str, Any]) -> None:
        print(json.dumps(row, default=str), file=self.output)


class JsonArrayFormatter(OutputFormatter):
    """Output formatter for JSON array format."""

    def __init__(self, output: TextIO = None):
        self.output = output or sys.stdout
        self.first = True

    def write(self, row: Dict[str, Any]) -> None:
        if self.first:
            print('[', file=self.output)
            self.first = False
        else:
            print(',', file=self.output)
        print(f'  {json.dumps(row, default=str)}', end='', file=self.output)

    def close(self) -> None:
        if self.first:
            print('[]', file=self.output)
        else:
            print('\n]', file=self.output)


class CsvFormatter(OutputFormatter):
    """Output formatter for CSV format."""

    def __init__(self, output: TextIO = None):
        self.output = output or sys.stdout
        self.writer: Optional[csv.DictWriter] = None
        self._fieldnames: Optional[List[str]] = None

    def write(self, row: Dict[str, Any]) -> None:
        if self.writer is None:
            self._fieldnames = list(row.keys())
            self.writer = csv.DictWriter(
                self.output,
                fieldnames=self._fieldnames,
                extrasaction='ignore'
            )
            self.writer.writeheader()
        self.writer.writerow({k: str(v) if v is not None else '' for k, v in row.items()})


class TableFormatter(OutputFormatter):
    """Output formatter for ASCII table format.

    Warning: This formatter buffers all rows in memory before rendering.
    For large result sets, consider using --output jsonl or --output csv instead,
    or use --limit to cap the number of rows.
    """

    # Warn user when buffered rows exceed this threshold
    MEMORY_WARNING_THRESHOLD = 10000

    def __init__(self, output: TextIO = None, max_col_width: int = 30):
        self.output = output or sys.stdout
        self.max_col_width = max_col_width
        self.rows: List[Dict[str, Any]] = []
        self.columns: Optional[List[str]] = None
        self._memory_warning_shown = False

    def write(self, row: Dict[str, Any]) -> None:
        if self.columns is None:
            self.columns = list(row.keys())
        self.rows.append(row)

        # Warn once when buffer gets large
        if not self._memory_warning_shown and len(self.rows) == self.MEMORY_WARNING_THRESHOLD:
            self._memory_warning_shown = True
            print(
                f"Warning: Table output has buffered {self.MEMORY_WARNING_THRESHOLD} rows in memory. "
                "Consider using --output jsonl or --output csv for large result sets, "
                "or use --limit to cap output.",
                file=sys.stderr
            )

    def close(self) -> None:
        if not self.rows or not self.columns:
            return

        # Calculate column widths
        widths = {}
        for col in self.columns:
            col_width = len(col)
            for row in self.rows:
                val = str(row.get(col, ''))
                col_width = max(col_width, min(len(val), self.max_col_width))
            widths[col] = col_width

        # Print header
        separator = '+' + '+'.join('-' * (w + 2) for w in widths.values()) + '+'
        header = '|' + '|'.join(f' {col:{widths[col]}} ' for col in self.columns) + '|'

        print(separator, file=self.output)
        print(header, file=self.output)
        print(separator, file=self.output)

        # Print rows
        for row in self.rows:
            values = []
            for col in self.columns:
                val = str(row.get(col, ''))
                if len(val) > self.max_col_width:
                    val = val[:self.max_col_width - 3] + '...'
                values.append(f' {val:{widths[col]}} ')
            print('|' + '|'.join(values) + '|', file=self.output)

        print(separator, file=self.output)


def get_formatter(format_name: str, output: TextIO = None) -> OutputFormatter:
    """
    Get an output formatter by name.

    Args:
        format_name: One of 'jsonl', 'json', 'csv', 'table'
        output: Output stream (defaults to stdout)

    Returns:
        OutputFormatter instance

    Raises:
        ValueError: If format is not recognized
    """
    formatters = {
        'jsonl': JsonLinesFormatter,
        'json': JsonArrayFormatter,
        'csv': CsvFormatter,
        'table': TableFormatter,
    }

    if format_name not in formatters:
        raise ValueError(
            f"Unknown output format: {format_name}. "
            f"Supported formats: {', '.join(formatters.keys())}"
        )

    return formatters[format_name](output)
