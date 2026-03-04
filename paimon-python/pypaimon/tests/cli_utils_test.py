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
"""Tests for CLI utility functions."""

import unittest
from datetime import datetime, timedelta

from pypaimon.cli.utils import (parse_end_position, parse_filter_expr,
                                parse_position, parse_start_position,
                                parse_timestamp)


class MockSnapshot:
    """Mock snapshot for testing."""

    def __init__(self, snapshot_id: int):
        self.id = snapshot_id


class MockSnapshotManager:
    """Mock snapshot manager for testing position parsing."""

    def __init__(self, earliest_id: int = 1, latest_id: int = 100):
        self.earliest_id = earliest_id
        self.latest_id = latest_id
        self._snapshots_by_time = {}

    def try_get_earliest_snapshot(self):
        return MockSnapshot(self.earliest_id)

    def get_latest_snapshot(self):
        return MockSnapshot(self.latest_id)

    def earlier_or_equal_time_mills(self, ts_millis: int):
        # Return a snapshot based on timestamp
        # For testing, assume snapshot ID = ts_millis // 1000 (simplified)
        snap_id = min(ts_millis // 1000, self.latest_id)
        snap_id = max(snap_id, self.earliest_id)
        return MockSnapshot(snap_id)


class ParseTimestampTest(unittest.TestCase):
    """Tests for parse_timestamp function."""

    def test_relative_hours(self):
        """Test parsing relative hours like -1h."""
        ts = parse_timestamp('-1h')
        expected = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)
        # Allow 1 second tolerance for test execution time
        self.assertAlmostEqual(ts, expected, delta=1000)

    def test_relative_minutes(self):
        """Test parsing relative minutes like -30m."""
        ts = parse_timestamp('-30m')
        expected = int((datetime.now() - timedelta(minutes=30)).timestamp() * 1000)
        self.assertAlmostEqual(ts, expected, delta=1000)

    def test_relative_days(self):
        """Test parsing relative days like -7d."""
        ts = parse_timestamp('-7d')
        expected = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
        self.assertAlmostEqual(ts, expected, delta=1000)

    def test_iso_date(self):
        """Test parsing ISO date like 2024-01-15."""
        ts = parse_timestamp('2024-01-15')
        expected = int(datetime(2024, 1, 15).timestamp() * 1000)
        self.assertEqual(ts, expected)

    def test_iso_datetime(self):
        """Test parsing ISO datetime like 2024-01-15T10:30:00."""
        ts = parse_timestamp('2024-01-15T10:30:00')
        expected = int(datetime(2024, 1, 15, 10, 30, 0).timestamp() * 1000)
        self.assertEqual(ts, expected)

    def test_invalid_timestamp(self):
        """Test that invalid timestamps raise ValueError."""
        with self.assertRaises(ValueError):
            parse_timestamp('invalid')
        with self.assertRaises(ValueError):
            parse_timestamp('abc123')


class ParsePositionTest(unittest.TestCase):
    """Tests for parse_position function."""

    def setUp(self):
        self.snapshot_mgr = MockSnapshotManager(earliest_id=1, latest_id=100)

    def test_earliest(self):
        """Test 'earliest' position."""
        result = parse_position('earliest', self.snapshot_mgr)
        self.assertEqual(result, 1)

    def test_latest(self):
        """Test 'latest' position returns None."""
        result = parse_position('latest', self.snapshot_mgr)
        self.assertIsNone(result)

    def test_numeric_snapshot_id(self):
        """Test numeric snapshot ID like '50'."""
        result = parse_position('50', self.snapshot_mgr)
        self.assertEqual(result, 50)

    def test_explicit_snapshot_prefix(self):
        """Test explicit snapshot: prefix like 'snapshot:42'."""
        result = parse_position('snapshot:42', self.snapshot_mgr)
        self.assertEqual(result, 42)

    def test_explicit_time_prefix(self):
        """Test explicit time: prefix like 'time:-1h'."""
        result = parse_position('time:-1h', self.snapshot_mgr)
        # Result depends on mock's earlier_or_equal_time_mills implementation
        self.assertIsInstance(result, int)

    def test_relative_time_hours(self):
        """Test relative time like '-1h' without prefix."""
        result = parse_position('-1h', self.snapshot_mgr)
        self.assertIsInstance(result, int)

    def test_relative_time_minutes(self):
        """Test relative time like '-30m' without prefix."""
        result = parse_position('-30m', self.snapshot_mgr)
        self.assertIsInstance(result, int)

    def test_relative_time_days(self):
        """Test relative time like '-7d' without prefix."""
        result = parse_position('-7d', self.snapshot_mgr)
        self.assertIsInstance(result, int)

    def test_iso_date(self):
        """Test ISO date like '2024-01-15' without prefix."""
        result = parse_position('2024-01-15', self.snapshot_mgr)
        self.assertIsInstance(result, int)

    def test_iso_datetime(self):
        """Test ISO datetime like '2024-01-15T10:30:00' without prefix."""
        result = parse_position('2024-01-15T10:30:00', self.snapshot_mgr)
        self.assertIsInstance(result, int)

    def test_invalid_position(self):
        """Test that invalid positions raise ValueError."""
        with self.assertRaises(ValueError):
            parse_position('invalid', self.snapshot_mgr)
        with self.assertRaises(ValueError):
            parse_position('abc123', self.snapshot_mgr)


class ParseStartEndPositionTest(unittest.TestCase):
    """Tests for parse_start_position and parse_end_position functions."""

    def setUp(self):
        self.snapshot_mgr = MockSnapshotManager(earliest_id=1, latest_id=100)

    def test_start_position_earliest(self):
        """Test parse_start_position with 'earliest'."""
        result = parse_start_position('earliest', self.snapshot_mgr)
        self.assertEqual(result, 1)

    def test_start_position_latest(self):
        """Test parse_start_position with 'latest' returns None."""
        result = parse_start_position('latest', self.snapshot_mgr)
        self.assertIsNone(result)

    def test_end_position_latest(self):
        """Test parse_end_position with 'latest' returns None."""
        result = parse_end_position('latest', self.snapshot_mgr)
        self.assertIsNone(result)

    def test_end_position_snapshot_id(self):
        """Test parse_end_position with numeric snapshot ID."""
        result = parse_end_position('75', self.snapshot_mgr)
        self.assertEqual(result, 75)

    def test_end_position_relative_time(self):
        """Test parse_end_position with relative time."""
        result = parse_end_position('-1h', self.snapshot_mgr)
        self.assertIsInstance(result, int)


class ParseFilterExprTest(unittest.TestCase):
    """Tests for parse_filter_expr function."""

    def test_equal(self):
        """Test parsing equal filter."""
        col, op, val = parse_filter_expr('name=John')
        self.assertEqual(col, 'name')
        self.assertEqual(op, '=')
        self.assertEqual(val, 'John')

    def test_not_equal(self):
        """Test parsing not equal filter."""
        col, op, val = parse_filter_expr('status!=active')
        self.assertEqual(col, 'status')
        self.assertEqual(op, '!=')
        self.assertEqual(val, 'active')

    def test_greater_than(self):
        """Test parsing greater than filter."""
        col, op, val = parse_filter_expr('age>25')
        self.assertEqual(col, 'age')
        self.assertEqual(op, '>')
        self.assertEqual(val, '25')

    def test_greater_or_equal(self):
        """Test parsing greater or equal filter."""
        col, op, val = parse_filter_expr('score>=90')
        self.assertEqual(col, 'score')
        self.assertEqual(op, '>=')
        self.assertEqual(val, '90')

    def test_less_than(self):
        """Test parsing less than filter."""
        col, op, val = parse_filter_expr('price<100')
        self.assertEqual(col, 'price')
        self.assertEqual(op, '<')
        self.assertEqual(val, '100')

    def test_less_or_equal(self):
        """Test parsing less or equal filter."""
        col, op, val = parse_filter_expr('count<=10')
        self.assertEqual(col, 'count')
        self.assertEqual(op, '<=')
        self.assertEqual(val, '10')

    def test_startswith(self):
        """Test parsing startswith filter."""
        col, op, val = parse_filter_expr('prefix~abc')
        self.assertEqual(col, 'prefix')
        self.assertEqual(op, '~')
        self.assertEqual(val, 'abc')

    def test_invalid_filter(self):
        """Test that invalid filters raise ValueError."""
        with self.assertRaises(ValueError):
            parse_filter_expr('invalid')
        with self.assertRaises(ValueError):
            parse_filter_expr('no operator here')


class CliArgumentParsingTest(unittest.TestCase):
    """Tests for CLI argument parsing and validation."""

    def test_follow_and_to_mutual_exclusion(self):
        """Test that --follow and --to are mutually exclusive."""
        import sys
        from io import StringIO

        from pypaimon.cli.main import main

        # Capture stderr to suppress error output during test
        old_stderr = sys.stderr
        sys.stderr = StringIO()
        old_argv = sys.argv

        try:
            sys.argv = ['paimon', 'tail', '/warehouse', 'db.table', '--follow', '--to', '100']
            with self.assertRaises(SystemExit) as cm:
                main()
            # argparse exits with code 2 for errors
            self.assertEqual(cm.exception.code, 2)
        finally:
            sys.stderr = old_stderr
            sys.argv = old_argv

    def test_default_from_without_follow(self):
        """Test that --from defaults to 'earliest' without --follow."""
        import argparse

        from pypaimon.cli.main import setup_tail_parser

        parser = argparse.ArgumentParser()
        setup_tail_parser(parser)
        args = parser.parse_args(['/warehouse', 'db.table'])

        # Before validation logic, default is None
        self.assertIsNone(args.from_pos)

    def test_default_to(self):
        """Test that --to defaults to None (will become 'latest' after validation)."""
        import argparse

        from pypaimon.cli.main import setup_tail_parser

        parser = argparse.ArgumentParser()
        setup_tail_parser(parser)
        args = parser.parse_args(['/warehouse', 'db.table'])

        self.assertIsNone(args.to_pos)

    def test_explicit_from_value(self):
        """Test that explicit --from value is preserved."""
        import argparse

        from pypaimon.cli.main import setup_tail_parser

        parser = argparse.ArgumentParser()
        setup_tail_parser(parser)
        # Use = syntax for values starting with - to avoid argparse treating them as options
        args = parser.parse_args(['/warehouse', 'db.table', '--from=-1h'])

        self.assertEqual(args.from_pos, '-1h')

    def test_explicit_to_value(self):
        """Test that explicit --to value is preserved."""
        import argparse

        from pypaimon.cli.main import setup_tail_parser

        parser = argparse.ArgumentParser()
        setup_tail_parser(parser)
        args = parser.parse_args(['/warehouse', 'db.table', '--to', '2024-01-15'])

        self.assertEqual(args.to_pos, '2024-01-15')

    def test_short_options(self):
        """Test that short options -s and -e work."""
        import argparse

        from pypaimon.cli.main import setup_tail_parser

        parser = argparse.ArgumentParser()
        setup_tail_parser(parser)
        args = parser.parse_args(['/warehouse', 'db.table', '-s', '100', '-e', '200'])

        self.assertEqual(args.from_pos, '100')
        self.assertEqual(args.to_pos, '200')


if __name__ == '__main__':
    unittest.main()
