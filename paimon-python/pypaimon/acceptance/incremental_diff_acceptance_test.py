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
Acceptance tests for IncrementalDiffScanner.

These tests verify that the diff approach (reading 2 base_manifest_lists)
returns the same data as the delta approach (reading N delta_manifest_lists).

Uses real file I/O with local temp filesystem.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.scanner.append_table_split_generator import \
    AppendTableSplitGenerator
from pypaimon.read.scanner.incremental_diff_scanner import \
    IncrementalDiffScanner
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class IncrementalDiffAcceptanceTest(unittest.TestCase):
    """Acceptance tests for diff vs delta equivalence with real data."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.string()),
            ('partition_col', pa.string())
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table_with_snapshots(self, name, num_snapshots=5, partition_keys=None):
        """Create a table and write num_snapshots of data.

        Returns:
            Tuple of (table, expected_data_per_snapshot)
        """
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=partition_keys)
        self.catalog.create_table(f'default.{name}', schema, False)
        table = self.catalog.get_table(f'default.{name}')

        all_data = []
        for snap_id in range(1, num_snapshots + 1):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            data = {
                'id': [snap_id * 10 + i for i in range(5)],
                'value': [f'snap{snap_id}_row{i}' for i in range(5)],
                'partition_col': ['p1' if i % 2 == 0 else 'p2' for i in range(5)]
            }
            all_data.append(data)

            pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        return table, all_data

    def _read_via_diff(self, table, start_snap_id, end_snap_id):
        """Read data using IncrementalDiffScanner between two snapshots."""
        snapshot_manager = SnapshotManager(table)
        start_snapshot = snapshot_manager.get_snapshot_by_id(start_snap_id)
        end_snapshot = snapshot_manager.get_snapshot_by_id(end_snap_id)

        scanner = IncrementalDiffScanner(table)
        plan = scanner.scan(start_snapshot, end_snapshot)

        splits = plan.splits()
        if not splits:
            # Return empty table with correct schema
            return pa.Table.from_pydict({
                'id': [],
                'value': [],
                'partition_col': []
            }, schema=self.pa_schema)

        table_read = table.new_read_builder().new_read()
        return table_read.to_arrow(splits)

    def _read_via_delta(self, table, start_snap_id, end_snap_id):
        """Read data by iterating delta_manifest_lists between two snapshots."""
        snapshot_manager = SnapshotManager(table)
        manifest_list_manager = ManifestListManager(table)
        manifest_file_manager = ManifestFileManager(table)

        all_entries = []
        for snap_id in range(start_snap_id + 1, end_snap_id + 1):
            snapshot = snapshot_manager.get_snapshot_by_id(snap_id)
            if snapshot and snapshot.commit_kind == 'APPEND':
                manifest_files = manifest_list_manager.read_delta(snapshot)
                if manifest_files:
                    entries = manifest_file_manager.read_entries_parallel(manifest_files)
                    all_entries.extend(entries)

        if not all_entries:
            return pa.Table.from_pydict({
                'id': [],
                'value': [],
                'partition_col': []
            }, schema=self.pa_schema)

        # Create splits from entries
        options = table.options
        split_generator = AppendTableSplitGenerator(
            table,
            options.source_split_target_size(),
            options.source_split_open_file_cost(),
            {}
        )
        splits = split_generator.create_splits(all_entries)

        table_read = table.new_read_builder().new_read()
        return table_read.to_arrow(splits)

    def _rows_to_set(self, arrow_table):
        """Convert arrow table to set of (id, value, partition_col) tuples."""
        rows = set()
        for i in range(arrow_table.num_rows):
            row = (
                arrow_table.column('id')[i].as_py(),
                arrow_table.column('value')[i].as_py(),
                arrow_table.column('partition_col')[i].as_py()
            )
            rows.add(row)
        return rows

    def test_diff_returns_same_rows_as_delta_simple(self):
        """
        Basic case: 5 snapshots, verify row-level equivalence.

        Creates a table with 5 snapshots, then reads data from snapshot 1 to 5
        using both diff and delta approaches, verifying they return the same rows.
        """
        table, all_data = self._create_table_with_snapshots(
            'test_diff_delta_simple',
            num_snapshots=5
        )

        # Read using both approaches (from snapshot 1 to 5, so we get snapshots 2-5)
        diff_result = self._read_via_diff(table, 1, 5)
        delta_result = self._read_via_delta(table, 1, 5)

        # Convert to sets for order-independent comparison
        diff_rows = self._rows_to_set(diff_result)
        delta_rows = self._rows_to_set(delta_result)

        self.assertEqual(diff_rows, delta_rows)

        # Verify we got the expected number of rows (snapshots 2-5, 5 rows each = 20)
        self.assertEqual(len(diff_rows), 20)

        # Verify specific IDs are present (from snapshots 2-5)
        expected_ids = set()
        for snap_id in range(2, 6):  # snapshots 2, 3, 4, 5
            for i in range(5):
                expected_ids.add(snap_id * 10 + i)

        actual_ids = {row[0] for row in diff_rows}
        self.assertEqual(actual_ids, expected_ids)

    def test_diff_returns_same_rows_as_delta_many_snapshots(self):
        """
        Stress test: 20 snapshots, verify row-level equivalence.

        This tests the catch-up scenario where there are many snapshots
        between start and end.
        """
        table, all_data = self._create_table_with_snapshots(
            'test_diff_delta_many',
            num_snapshots=20
        )

        # Read using both approaches (from snapshot 1 to 20)
        diff_result = self._read_via_diff(table, 1, 20)
        delta_result = self._read_via_delta(table, 1, 20)

        # Convert to sets for order-independent comparison
        diff_rows = self._rows_to_set(diff_result)
        delta_rows = self._rows_to_set(delta_result)

        self.assertEqual(diff_rows, delta_rows)

        # Verify we got the expected number of rows (snapshots 2-20, 5 rows each = 95)
        self.assertEqual(len(diff_rows), 95)

    def test_diff_returns_same_rows_with_mixed_partitions(self):
        """
        Partitioned table: Verify diff handles multiple partitions correctly.

        Creates a partitioned table and verifies diff and delta return
        the same rows across all partitions.
        """
        table, all_data = self._create_table_with_snapshots(
            'test_diff_delta_partitioned',
            num_snapshots=5,
            partition_keys=['partition_col']
        )

        # Read using both approaches
        diff_result = self._read_via_diff(table, 1, 5)
        delta_result = self._read_via_delta(table, 1, 5)

        # Convert to sets for order-independent comparison
        diff_rows = self._rows_to_set(diff_result)
        delta_rows = self._rows_to_set(delta_result)

        self.assertEqual(diff_rows, delta_rows)

        # Verify both partitions have data
        p1_rows = {r for r in diff_rows if r[2] == 'p1'}
        p2_rows = {r for r in diff_rows if r[2] == 'p2'}

        self.assertGreater(len(p1_rows), 0, "Should have rows in partition p1")
        self.assertGreater(len(p2_rows), 0, "Should have rows in partition p2")


if __name__ == '__main__':
    unittest.main()
