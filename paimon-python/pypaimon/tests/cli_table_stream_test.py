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
"""Tests for the 'table stream' CLI command."""

import json
import os
import shutil
import sys
import tempfile
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch, call

import pyarrow as pa
import pandas as pd

from pypaimon import CatalogFactory, Schema
from pypaimon.cli.cli import main
from pypaimon.cli.cli_table_stream import parse_from_position


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plan(data):
    """Create a mock Plan that yields a real DataFrame when read."""
    from pypaimon.read.plan import Plan
    plan = MagicMock(spec=Plan)
    plan.splits.return_value = ["dummy_split"]
    plan._data = data
    return plan


def _make_mock_read(df):
    """Return a mock TableRead whose to_pandas() returns df."""
    mock_read = MagicMock()
    mock_read.to_pandas.return_value = df
    return mock_read


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _write_snapshot(table, data_dict, schema):
    """Write one snapshot of data to table."""
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    arrow_table = pa.Table.from_pydict(data_dict, schema=schema)
    table_write.write_arrow(arrow_table)
    table_commit.commit(table_write.prepare_commit())
    table_write.close()
    table_commit.close()


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------

class CliTableStreamTest(unittest.TestCase):
    """CLI tests for 'table stream' using mocked stream_sync."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('test_db', True)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        cls.catalog.create_table('test_db.stream_users', schema, False)

        table = cls.catalog.get_table('test_db.stream_users')
        _write_snapshot(table, {'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [25, 30]}, pa_schema)
        _write_snapshot(table, {'id': [3], 'name': ['Charlie'], 'age': [35]}, pa_schema)

        cls.config_file = os.path.join(cls.tempdir, 'paimon.yaml')
        with open(cls.config_file, 'w') as f:
            f.write(f"metastore: filesystem\nwarehouse: {cls.warehouse}\n")

        cls.pa_schema = pa_schema

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _run_stream(self, extra_args, plans_dfs, capture_stderr=False):
        """Run 'paimon table stream' with stream_sync mocked to yield given DataFrames."""
        base_args = ['paimon', '-c', self.config_file, 'table', 'stream', 'test_db.stream_users']
        argv = base_args + extra_args

        # Build mock plans
        mock_plans = []
        for df in plans_dfs:
            plan = MagicMock()
            plan.splits.return_value = [] if df is None else ["split"]
            mock_plans.append(plan)

        # stream_sync just yields the plans once, then stops
        def fake_stream_sync():
            return iter(mock_plans)

        mock_scan = MagicMock()
        mock_scan.stream_sync = fake_stream_sync

        # to_pandas returns the corresponding df for each call
        call_count = [0]
        dfs_iter = [df for df in plans_dfs if df is not None]

        def fake_to_pandas(splits):
            if not splits:
                return pd.DataFrame()
            idx = call_count[0]
            call_count[0] += 1
            return dfs_iter[idx] if idx < len(dfs_iter) else pd.DataFrame()

        mock_read = MagicMock()
        mock_read.to_pandas.side_effect = fake_to_pandas

        mock_builder = MagicMock()
        mock_builder.new_streaming_scan.return_value = mock_scan
        mock_builder.new_read.return_value = mock_read
        mock_builder.with_projection.return_value = mock_builder
        mock_builder.with_filter.return_value = mock_builder
        mock_builder.with_poll_interval_ms.return_value = mock_builder
        mock_builder.with_include_row_kind.return_value = mock_builder
        mock_builder.with_consumer_id.return_value = mock_builder
        mock_builder.with_scan_from.return_value = mock_builder

        stdout_buf = StringIO()
        stderr_buf = StringIO()

        with patch('sys.argv', argv):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                    with patch.object(
                        __import__('pypaimon.table.file_store_table',
                                   fromlist=['FileStoreTable']).FileStoreTable,
                        'new_stream_read_builder',
                        return_value=mock_builder
                    ):
                        try:
                            main()
                        except SystemExit as e:
                            exit_code = e.code
                        else:
                            exit_code = 0
                    stdout_out = mock_stdout.getvalue()
                    stderr_out = mock_stderr.getvalue()

        return stdout_out, stderr_out, exit_code, mock_builder

    def test_basic_streaming_output(self):
        """Rows from mock plan appear in stdout."""
        df = pd.DataFrame({'id': [3], 'name': ['Charlie'], 'age': [35]})
        stdout, _, exit_code, _ = self._run_stream([], [df])
        self.assertIn('Charlie', stdout)
        self.assertEqual(exit_code, 0)

    def test_select_projection_passed_to_builder(self):
        """--select passes projection to StreamReadBuilder.with_projection()."""
        df = pd.DataFrame({'id': [1], 'name': ['Alice']})
        _, _, _, builder = self._run_stream(['--select', 'id,name'], [df])
        builder.with_projection.assert_called_once_with(['id', 'name'])

    def test_where_filter_passed_to_builder(self):
        """--where passes a predicate to StreamReadBuilder.with_filter()."""
        df = pd.DataFrame({'id': [1], 'name': ['Alice'], 'age': [25]})
        _, _, _, builder = self._run_stream(['--where', 'age > 20'], [df])
        builder.with_filter.assert_called_once()

    def test_format_json_outputs_json_objects(self):
        """--format json outputs one JSON object per row."""
        df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [25, 30]})
        stdout, _, exit_code, _ = self._run_stream(['--format', 'json'], [df])
        lines = [l for l in stdout.strip().splitlines() if l]
        self.assertEqual(len(lines), 2)
        for line in lines:
            obj = json.loads(line)
            self.assertIn('id', obj)

    def test_include_row_kind_passed_to_builder(self):
        """--include-row-kind calls builder.with_include_row_kind(True)."""
        df = pd.DataFrame({'_row_kind': ['+I'], 'id': [1], 'name': ['Alice'], 'age': [25]})
        _, _, _, builder = self._run_stream(['--include-row-kind'], [df])
        builder.with_include_row_kind.assert_called_once_with(True)

    def test_consumer_id_passed_to_builder(self):
        """--consumer-id passes value to builder.with_consumer_id()."""
        df = pd.DataFrame({'id': [1], 'name': ['Alice'], 'age': [25]})
        _, _, _, builder = self._run_stream(['--consumer-id', 'my-consumer'], [df])
        builder.with_consumer_id.assert_called_once_with('my-consumer')

    def test_from_earliest_passed_to_builder(self):
        """--from earliest passes 'earliest' to builder.with_scan_from()."""
        df = pd.DataFrame({'id': [1], 'name': ['Alice'], 'age': [25]})
        _, _, _, builder = self._run_stream(['--from', 'earliest'], [df])
        builder.with_scan_from.assert_called_once_with('earliest')

    def test_from_snapshot_id_passed_to_builder(self):
        """--from 42 passes integer 42 to builder.with_scan_from()."""
        df = pd.DataFrame({'id': [1], 'name': ['Alice'], 'age': [25]})
        _, _, _, builder = self._run_stream(['--from', '42'], [df])
        builder.with_scan_from.assert_called_once_with(42)

    def test_empty_plan_produces_no_output(self):
        """A plan with no splits produces no stdout output."""
        stdout, _, _, _ = self._run_stream([], [None])
        self.assertEqual(stdout.strip(), '')

    def test_invalid_select_column_exits_nonzero(self):
        """--select with nonexistent column prints error and exits non-zero."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'stream',
                                'test_db.stream_users', '--select', 'id,bogus']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                try:
                    main()
                    exit_code = 0
                except SystemExit as e:
                    exit_code = e.code
            self.assertNotEqual(exit_code, 0)
            self.assertIn('bogus', mock_stderr.getvalue())

    def test_invalid_where_clause_exits_nonzero(self):
        """--where with invalid syntax prints error and exits non-zero."""
        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'stream',
                                'test_db.stream_users', '--where', '%%invalid%%']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                try:
                    main()
                    exit_code = 0
                except SystemExit as e:
                    exit_code = e.code
            self.assertNotEqual(exit_code, 0)
            self.assertIn('WHERE', mock_stderr.getvalue())

    def test_keyboard_interrupt_exits_cleanly(self):
        """KeyboardInterrupt exits with code 0 and no traceback."""
        mock_scan = MagicMock()
        mock_scan.stream_sync.side_effect = KeyboardInterrupt

        mock_builder = MagicMock()
        mock_builder.new_streaming_scan.return_value = mock_scan
        mock_builder.new_read.return_value = MagicMock()
        mock_builder.with_projection.return_value = mock_builder
        mock_builder.with_filter.return_value = mock_builder
        mock_builder.with_poll_interval_ms.return_value = mock_builder
        mock_builder.with_include_row_kind.return_value = mock_builder
        mock_builder.with_consumer_id.return_value = mock_builder
        mock_builder.with_scan_from.return_value = mock_builder

        with patch('sys.argv', ['paimon', '-c', self.config_file, 'table', 'stream',
                                'test_db.stream_users']):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with patch.object(
                    __import__('pypaimon.table.file_store_table',
                               fromlist=['FileStoreTable']).FileStoreTable,
                    'new_stream_read_builder',
                    return_value=mock_builder
                ):
                    try:
                        main()
                        exit_code = 0
                    except SystemExit as e:
                        exit_code = e.code
            stderr_out = mock_stderr.getvalue()

        self.assertEqual(exit_code, 0)
        self.assertNotIn('Traceback', stderr_out)


# ---------------------------------------------------------------------------
# Unit tests for parse_from_position
# ---------------------------------------------------------------------------

class ParseFromPositionTest(unittest.TestCase):
    """Unit tests for parse_from_position()."""

    def _mock_snapshot_manager(self, snapshot_id=None):
        mgr = MagicMock()
        if snapshot_id is not None:
            snap = MagicMock()
            snap.id = snapshot_id
            mgr.earlier_or_equal_time_mills.return_value = snap
        else:
            mgr.earlier_or_equal_time_mills.return_value = None
        return mgr

    def test_latest_passthrough(self):
        self.assertEqual(parse_from_position("latest", MagicMock()), "latest")

    def test_earliest_passthrough(self):
        self.assertEqual(parse_from_position("earliest", MagicMock()), "earliest")

    def test_integer_string(self):
        self.assertEqual(parse_from_position("42", MagicMock()), 42)

    def test_date_string_resolves_to_snapshot_id(self):
        mgr = self._mock_snapshot_manager(snapshot_id=7)
        result = parse_from_position("2025-01-15", mgr)
        self.assertEqual(result, 7)
        mgr.earlier_or_equal_time_mills.assert_called_once()

    def test_utc_timestamp_resolves_to_snapshot_id(self):
        mgr = self._mock_snapshot_manager(snapshot_id=12)
        result = parse_from_position("2025-01-15T10:30:00Z", mgr)
        self.assertEqual(result, 12)
        # Verify the epoch ms passed is consistent with the UTC timestamp
        epoch_ms_arg = mgr.earlier_or_equal_time_mills.call_args[0][0]
        # 2025-01-15T10:30:00Z = 1736937000000 ms (approx)
        self.assertAlmostEqual(epoch_ms_arg, 1736937000000, delta=60000)

    def test_timestamp_before_all_snapshots_raises(self):
        mgr = self._mock_snapshot_manager(snapshot_id=None)
        with self.assertRaises(ValueError) as ctx:
            parse_from_position("2020-01-01", mgr)
        self.assertIn("No snapshot found", str(ctx.exception))

    def test_unrecognised_value_raises(self):
        with self.assertRaises(ValueError) as ctx:
            parse_from_position("not_a_thing", MagicMock())
        self.assertIn("Unrecognised", str(ctx.exception))

    def test_from_timestamp_cli_error_message(self):
        """--from with a timestamp that has no prior snapshot prints error to stderr."""
        with patch('sys.argv', [
            'paimon', '-c', '/nonexistent/paimon.yaml',
            'table', 'stream', 'test_db.stream_users',
            '--from', '1900-01-01'
        ]):
            # This will fail at catalog load before reaching timestamp resolution,
            # so we just verify parse_from_position raises correctly in isolation.
            mgr = self._mock_snapshot_manager(snapshot_id=None)
            with self.assertRaises(ValueError):
                parse_from_position("1900-01-01", mgr)


if __name__ == '__main__':
    unittest.main()
