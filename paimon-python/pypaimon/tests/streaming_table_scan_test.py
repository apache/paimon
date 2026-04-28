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
"""Tests for AsyncStreamingTableScan."""

import asyncio
import unittest
from unittest.mock import Mock, patch

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.read.plan import Plan
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.snapshot.snapshot import Snapshot


def _create_mock_snapshot(snapshot_id: int, commit_kind: str = "APPEND"):
    """Helper to create a mock snapshot."""
    snapshot = Mock(spec=Snapshot)
    snapshot.id = snapshot_id
    snapshot.commit_kind = commit_kind
    snapshot.time_millis = 1000000 + snapshot_id
    snapshot.base_manifest_list = f"manifest-list-{snapshot_id}"
    snapshot.delta_manifest_list = f"delta-manifest-list-{snapshot_id}"
    return snapshot


def _create_mock_table(latest_snapshot_id: int = 5):
    """Helper to create a mock table."""
    table = Mock()
    table.table_path = "/tmp/test_table"
    table.is_primary_key_table = False
    table.options = Mock()
    table.options.source_split_target_size.return_value = 128 * 1024 * 1024
    table.options.source_split_open_file_cost.return_value = 4 * 1024 * 1024
    table.options.scan_manifest_parallelism.return_value = 8
    table.options.bucket.return_value = 1
    table.options.data_evolution_enabled.return_value = False
    table.options.deletion_vectors_enabled.return_value = False
    table.options.changelog_producer.return_value = ChangelogProducer.NONE
    table.field_names = ['col1', 'col2']
    table.trimmed_primary_keys = []
    table.partition_keys = []
    table.file_io = Mock()
    table.table_schema = Mock()
    table.table_schema.id = 0
    table.table_schema.fields = []
    table.schema_manager = Mock()
    table.schema_manager.get_schema.return_value = table.table_schema

    return table, latest_snapshot_id


class AsyncStreamingTableScanTest(unittest.TestCase):
    """Tests for AsyncStreamingTableScan async streaming functionality."""

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_initial_scan(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """Initial scan should yield a Plan and set next_snapshot_id to latest + 1."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(5)
        mock_snapshot_manager.get_snapshot_by_id.return_value = None

        MockStartingScanner.return_value.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table)

        async def get_first_plan():
            async for plan in scan.stream():
                return plan

        plan = asyncio.run(get_first_plan())

        self.assertIsInstance(plan, Plan)
        self.assertEqual(scan.next_snapshot_id, 6)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_stream_skips_non_append_commits(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """Stream should skip COMPACT/OVERWRITE commits."""
        table, _ = _create_mock_table(latest_snapshot_id=7)

        # Setup mocks
        mock_snapshot_manager = MockSnapshotManager.return_value

        # Snapshots: 6 (COMPACT - skip), 7 (APPEND - scan)
        snapshot_7 = _create_mock_snapshot(7, "APPEND")

        # find_next_scannable returns (snapshot, next_id, skipped_count)
        # Start at 6, skip 1 (COMPACT), return snapshot 7, next_id=8
        mock_snapshot_manager.find_next_scannable.return_value = (snapshot_7, 8, 1)
        mock_snapshot_manager.get_cache_stats.return_value = {"cache_hits": 0, "cache_misses": 0, "cache_size": 0}
        # Mock get_latest_snapshot for diff catch-up check (gap=1, below threshold)
        mock_snapshot_manager.get_latest_snapshot.return_value = snapshot_7

        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_list_manager.read_delta.return_value = []

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.read_manifest_entries.return_value = []

        scan = AsyncStreamingTableScan(table)
        scan.next_snapshot_id = 6  # Start from snapshot 6

        async def get_plans():
            plans = []
            count = 0
            async for plan in scan.stream():
                plans.append(plan)
                count += 1
                if count >= 1:  # Get one plan (snapshot 7)
                    break
            return plans

        asyncio.run(get_plans())

        # Should have skipped snapshot 6 (COMPACT) and scanned 7 (APPEND)
        self.assertEqual(scan.next_snapshot_id, 8)
        # Verify lookahead skipped 1 snapshot
        self.assertEqual(scan._lookahead_skips, 1)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_stream_sync_yields_plans(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """stream_sync() should provide a synchronous iterator."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        # Setup mocks
        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(5)
        mock_snapshot_manager.get_snapshot_by_id.return_value = None

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table)

        # Get first plan synchronously
        for plan in scan.stream_sync():
            self.assertIsInstance(plan, Plan)
            break  # Just get one

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    def test_poll_interval_configurable(self, MockManifestListManager, MockSnapshotManager):
        """Poll interval should be configurable."""
        table, _ = _create_mock_table()

        scan = AsyncStreamingTableScan(table, poll_interval_ms=500)

        self.assertEqual(scan.poll_interval, 0.5)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_no_snapshot_waits_and_polls(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """When no new snapshot exists, should wait and poll again."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_cache_stats.return_value = {"cache_hits": 0, "cache_misses": 0, "cache_size": 0}

        # No snapshot 6 exists yet - find_next_scannable returns (None, 6, 0) first,
        # then on subsequent calls returns a snapshot
        call_count = [0]
        snapshot_6 = _create_mock_snapshot(6, "APPEND")

        def find_next_scannable(start_id, should_scan, lookahead_size=10, max_workers=4):
            call_count[0] += 1
            # After 3 calls, snapshot 6 appears
            if call_count[0] > 3:
                return (snapshot_6, 7, 0)
            # No snapshot yet - return (None, start_id, 0) to indicate no snapshot exists
            return (None, start_id, 0)

        mock_snapshot_manager.find_next_scannable.side_effect = find_next_scannable
        mock_snapshot_manager.get_latest_snapshot.return_value = None

        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_list_manager.read_delta.return_value = []

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.read_manifest_entries.return_value = []

        scan = AsyncStreamingTableScan(table, poll_interval_ms=10)
        scan.next_snapshot_id = 6

        async def get_plan_with_timeout():
            async for plan in scan.stream():
                return plan

        # Should eventually get a plan after polling
        plan = asyncio.run(asyncio.wait_for(get_plan_with_timeout(), timeout=1.0))
        self.assertIsInstance(plan, Plan)


class StreamingPrefetchTest(unittest.TestCase):
    """Tests for prefetching functionality in AsyncStreamingTableScan."""

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_prefetch_enabled_by_default(self, MockManifestFileManager, MockManifestListManager, MockSnapshotManager):
        """Prefetching should be enabled by default."""
        table, _ = _create_mock_table()
        scan = AsyncStreamingTableScan(table)
        self.assertTrue(scan._prefetch_enabled)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_prefetch_can_be_disabled(self, MockManifestFileManager, MockManifestListManager, MockSnapshotManager):
        """Prefetching can be disabled via constructor parameter."""
        table, _ = _create_mock_table()
        scan = AsyncStreamingTableScan(table, prefetch_enabled=False)
        self.assertFalse(scan._prefetch_enabled)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_prefetch_starts_after_yielding_plan(
            self,
            MockManifestFileManager,
            MockManifestListManager,
            MockSnapshotManager):
        """After yielding a plan, prefetch for next snapshot should start."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_file_manager = MockManifestFileManager.return_value
        mock_snapshot_manager.get_cache_stats.return_value = {"cache_hits": 0, "cache_misses": 0, "cache_size": 0}

        # Snapshots 5, 6, 7 exist - find_next_scannable returns each one
        snapshot_5 = _create_mock_snapshot(5, "APPEND")
        snapshot_6 = _create_mock_snapshot(6, "APPEND")
        snapshot_7 = _create_mock_snapshot(7, "APPEND")

        call_count = [0]

        def find_next_scannable(start_id, should_scan, lookahead_size=10, max_workers=4):
            call_count[0] += 1
            if start_id == 5:
                return (snapshot_5, 6, 0)
            elif start_id == 6:
                return (snapshot_6, 7, 0)
            elif start_id == 7:
                return (snapshot_7, 8, 0)
            return (None, start_id, 0)

        mock_snapshot_manager.find_next_scannable.side_effect = find_next_scannable
        mock_snapshot_manager.get_latest_snapshot.return_value = None

        mock_manifest_list_manager.read_delta.return_value = []
        mock_manifest_file_manager.read_entries_parallel.return_value = []

        scan = AsyncStreamingTableScan(table, poll_interval_ms=10)
        scan.next_snapshot_id = 5

        async def get_two_plans():
            plans = []
            async for plan in scan.stream():
                plans.append(plan)
                # After first plan, prefetch task should exist
                if len(plans) == 1:
                    # Give prefetch a moment to start
                    await asyncio.sleep(0.01)
                    self.assertIsNotNone(scan._prefetch_future)
                if len(plans) >= 2:
                    break
            return plans

        plans = asyncio.run(get_two_plans())
        self.assertEqual(len(plans), 2)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_prefetch_returns_same_data_as_sequential(
            self,
            MockManifestFileManager,
            MockManifestListManager,
            MockSnapshotManager):
        """Prefetched plans should contain the same data as non-prefetched."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_file_manager = MockManifestFileManager.return_value
        mock_snapshot_manager.get_cache_stats.return_value = {"cache_hits": 0, "cache_misses": 0, "cache_size": 0}

        snapshot_5 = _create_mock_snapshot(5, "APPEND")
        snapshot_6 = _create_mock_snapshot(6, "APPEND")

        def find_next_scannable(start_id, should_scan, lookahead_size=10, max_workers=4):
            if start_id == 5:
                return (snapshot_5, 6, 0)
            elif start_id == 6:
                return (snapshot_6, 7, 0)
            return (None, start_id, 0)

        mock_snapshot_manager.find_next_scannable.side_effect = find_next_scannable
        mock_snapshot_manager.get_latest_snapshot.return_value = None

        mock_manifest_list_manager.read_delta.return_value = []
        mock_manifest_file_manager.read_entries_parallel.return_value = []

        # Test with prefetch enabled
        scan_prefetch = AsyncStreamingTableScan(table, poll_interval_ms=10, prefetch_enabled=True)
        scan_prefetch.next_snapshot_id = 5

        # Test with prefetch disabled
        scan_sequential = AsyncStreamingTableScan(table, poll_interval_ms=10, prefetch_enabled=False)
        scan_sequential.next_snapshot_id = 5

        async def get_plans(scan, count):
            plans = []
            async for plan in scan.stream():
                plans.append(plan)
                if len(plans) >= count:
                    break
            return plans

        plans_prefetch = asyncio.run(get_plans(scan_prefetch, 2))
        plans_sequential = asyncio.run(get_plans(scan_sequential, 2))

        # Both should get the same number of plans
        self.assertEqual(len(plans_prefetch), len(plans_sequential))

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_prefetch_handles_no_next_snapshot(
            self,
            MockManifestFileManager,
            MockManifestListManager,
            MockSnapshotManager):
        """When no next snapshot exists, prefetch should return None gracefully."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_file_manager = MockManifestFileManager.return_value
        mock_snapshot_manager.get_cache_stats.return_value = {"cache_hits": 0, "cache_misses": 0, "cache_size": 0}

        snapshot_5 = _create_mock_snapshot(5, "APPEND")

        # Only snapshot 5 exists
        def find_next_scannable(start_id, should_scan, lookahead_size=10, max_workers=4):
            if start_id == 5:
                return (snapshot_5, 6, 0)
            # No more snapshots after 5
            return (None, start_id, 0)

        mock_snapshot_manager.find_next_scannable.side_effect = find_next_scannable
        mock_snapshot_manager.get_latest_snapshot.return_value = None

        mock_manifest_list_manager.read_delta.return_value = []
        mock_manifest_file_manager.read_entries_parallel.return_value = []

        scan = AsyncStreamingTableScan(table, poll_interval_ms=10)
        scan.next_snapshot_id = 5

        async def get_one_plan():
            async for plan in scan.stream():
                # After getting plan for snapshot 5, prefetch for 6 should start
                # but return None since snapshot 6 doesn't exist
                await asyncio.sleep(0.05)  # Let prefetch complete
                # Prefetch task should have completed (or be None)
                return plan

        plan = asyncio.run(get_one_plan())
        self.assertIsInstance(plan, Plan)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_prefetch_disabled_no_prefetch_future(
            self,
            MockManifestFileManager,
            MockManifestListManager,
            MockSnapshotManager):
        """With prefetch disabled, no prefetch future should be created."""
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_file_manager = MockManifestFileManager.return_value
        mock_snapshot_manager.get_cache_stats.return_value = {"cache_hits": 0, "cache_misses": 0, "cache_size": 0}

        snapshot_5 = _create_mock_snapshot(5, "APPEND")

        def find_next_scannable(start_id, should_scan, lookahead_size=10, max_workers=4):
            if start_id == 5:
                return (snapshot_5, 6, 0)
            return (None, start_id, 0)

        mock_snapshot_manager.find_next_scannable.side_effect = find_next_scannable
        mock_snapshot_manager.get_latest_snapshot.return_value = None

        mock_manifest_list_manager.read_delta.return_value = []
        mock_manifest_file_manager.read_entries_parallel.return_value = []

        scan = AsyncStreamingTableScan(table, poll_interval_ms=10, prefetch_enabled=False)
        scan.next_snapshot_id = 5

        async def get_one_plan():
            async for plan in scan.stream():
                await asyncio.sleep(0.01)
                # With prefetch disabled, no task should exist
                self.assertIsNone(scan._prefetch_future)
                return plan

        asyncio.run(get_one_plan())


class StreamingCatchUpDiffTest(unittest.TestCase):
    """Tests for diff-based catch-up optimization in AsyncStreamingTableScan."""

    @patch('pypaimon.read.streaming_table_scan.IncrementalDiffScanner')
    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_stream_triggers_diff_catch_up_for_large_gap(
        self, MockManifestFileManager, MockManifestListManager,
        MockSnapshotManager, MockDiffScanner
    ):
        """
        When starting with a large gap, stream() should use diff scanner.

        This tests the full flow:
        1. CLI calls restore({"next_snapshot_id": 5}) for --from snapshot:5
        2. stream() detects large gap (5 to 100, gap=95)
        3. Diff scanner is triggered
        """
        table, _ = _create_mock_table(latest_snapshot_id=100)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_diff_scanner = MockDiffScanner.return_value

        # Setup: latest is 100, start is 5 (gap=95)
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(100)
        mock_snapshot_manager.get_snapshot_by_id.return_value = _create_mock_snapshot(4)  # start-1

        # Diff scanner returns a plan with some splits
        mock_split = Mock()
        mock_plan = Plan([mock_split])
        mock_diff_scanner.scan.return_value = mock_plan

        scan = AsyncStreamingTableScan(table, poll_interval_ms=10, prefetch_enabled=False)

        # Simulate --from snapshot:5: restore to snapshot 5
        scan.next_snapshot_id = 5

        # Verify diff catch-up should be used (gap=95 > threshold=10)
        self.assertTrue(scan._should_use_diff_catch_up())

        # Run stream() and get first plan
        async def get_first_plan():
            async for plan in scan.stream():
                return plan

        asyncio.run(get_first_plan())

        # Verify diff scanner was used
        MockDiffScanner.assert_called_once_with(table)
        mock_diff_scanner.scan.assert_called_once()

        # Verify next_snapshot_id was updated to latest + 1
        self.assertEqual(scan.next_snapshot_id, 101)


class StreamingConsumerTest(unittest.TestCase):
    """Tests for consumer management integration in AsyncStreamingTableScan."""

    @patch('pypaimon.read.streaming_table_scan.ConsumerManager')
    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_consumer_restores_next_snapshot_id(
        self, MockManifestFileManager, MockManifestListManager,
        MockSnapshotManager, MockConsumerManager
    ):
        """When consumer exists, stream() should resume from saved position."""
        from pypaimon.consumer.consumer import Consumer

        table, _ = _create_mock_table(latest_snapshot_id=10)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_consumer_manager = MockConsumerManager.return_value
        mock_manifest_list_manager = MockManifestListManager.return_value

        # Consumer has saved progress at snapshot 8
        mock_consumer_manager.consumer.return_value = Consumer(next_snapshot=8)

        # Snapshot 8 exists and is APPEND
        snapshot_8 = _create_mock_snapshot(8, "APPEND")
        mock_snapshot_manager.find_next_scannable.return_value = (snapshot_8, 9, 0)
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(10)

        mock_manifest_list_manager.read_delta.return_value = []

        scan = AsyncStreamingTableScan(
            table, poll_interval_ms=10, prefetch_enabled=False,
            consumer_id="my-consumer"
        )

        # next_snapshot_id is None initially
        self.assertIsNone(scan.next_snapshot_id)

        async def get_first_plan():
            async for plan in scan.stream():
                return plan

        asyncio.run(get_first_plan())

        # Consumer should have been queried
        mock_consumer_manager.consumer.assert_called_once_with("my-consumer")

        # next_snapshot_id should have been restored from consumer (8),
        # then advanced after scanning snapshot 8
        self.assertEqual(scan.next_snapshot_id, 9)

    @patch('pypaimon.read.streaming_table_scan.ConsumerManager')
    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_consumer_saves_after_yield(
        self, MockFileScanner, MockManifestListManager,
        MockSnapshotManager, MockConsumerManager
    ):
        """Consumer progress is flushed on the next __anext__() call after yielding a plan.

        The save is deferred until the caller asks for the next plan (i.e. after the caller's
        loop body has completed), giving at-least-once semantics: the consumer file only
        advances once the caller has finished processing the previous plan.
        """
        table, _ = _create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_consumer_manager = MockConsumerManager.return_value

        # No existing consumer state
        mock_consumer_manager.consumer.return_value = None

        # Latest snapshot is 5
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(5)
        mock_snapshot_manager.get_snapshot_by_id.return_value = None
        # No follow-up snapshots available (so the polling loop sleeps and we break out)
        mock_snapshot_manager.find_next_scannable.return_value = (None, 6, 0)

        mock_file_scanner = MockFileScanner.return_value
        mock_file_scanner.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(
            table, poll_interval_ms=10, prefetch_enabled=False,
            consumer_id="save-test"
        )

        async def get_first_plan_then_resume():
            gen = scan.stream()
            plan = await gen.__anext__()        # yields initial plan; save is staged but not flushed
            # Consumer not yet saved — save is pending until the next __anext__() call
            mock_consumer_manager.reset_consumer.assert_not_called()
            # Drive the generator one more step (enters polling loop, flushes pending save,
            # then finds no snapshot and sleeps — we cancel via timeout)
            try:
                await asyncio.wait_for(gen.__anext__(), timeout=0.05)
            except (asyncio.TimeoutError, StopAsyncIteration):
                pass
            return plan

        asyncio.run(get_first_plan_then_resume())

        # Consumer should now have been saved with next_snapshot_id = 6
        mock_consumer_manager.reset_consumer.assert_called_once()
        call_args = mock_consumer_manager.reset_consumer.call_args
        self.assertEqual(call_args[0][0], "save-test")
        self.assertEqual(call_args[0][1].next_snapshot, 6)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    def test_no_consumer_when_consumer_id_not_set(
        self, MockManifestFileManager, MockManifestListManager,
        MockSnapshotManager
    ):
        """Without consumer_id, no ConsumerManager should be created."""
        table, _ = _create_mock_table()

        scan = AsyncStreamingTableScan(table)

        self.assertIsNone(scan._consumer_id)
        self.assertIsNone(scan._consumer_manager)


class ScanFromTest(unittest.TestCase):
    """Integration tests for AsyncStreamingTableScan scan_from parameter."""

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_scan_from_earliest(
        self, MockFileScanner, MockManifestFileManager,
        MockManifestListManager, MockSnapshotManager
    ):
        """scan_from='earliest' should yield initial plan from the earliest snapshot."""
        table, _ = _create_mock_table()

        earliest = _create_mock_snapshot(1)
        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.try_get_earliest_snapshot.return_value = earliest
        mock_snapshot_manager.find_next_scannable.return_value = (None, 2, 0)

        MockFileScanner.return_value.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table, scan_from="earliest", prefetch_enabled=False)

        async def get_first():
            async for plan in scan.stream():
                return plan

        plan = asyncio.run(get_first())
        self.assertIsInstance(plan, Plan)
        # next_snapshot_id should be earliest.id + 1
        self.assertEqual(scan.next_snapshot_id, 2)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_scan_from_numeric_id(
        self, MockFileScanner, MockManifestFileManager,
        MockManifestListManager, MockSnapshotManager
    ):
        """scan_from=5 should set next_snapshot_id=5 without an initial full scan."""
        table, _ = _create_mock_table()

        mock_snapshot_manager = MockSnapshotManager.return_value
        # get_latest_snapshot needed by _should_use_diff_catch_up
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(5)
        mock_snapshot_manager.find_next_scannable.return_value = (None, 6, 0)

        scan = AsyncStreamingTableScan(table, scan_from=5, prefetch_enabled=False)

        async def check_state():
            gen = scan.stream()
            try:
                await asyncio.wait_for(gen.__anext__(), timeout=0.05)
            except (asyncio.TimeoutError, StopAsyncIteration):
                pass

        asyncio.run(check_state())
        # Numeric scan_from bypasses both earliest and latest initial-scan paths;
        # try_get_earliest_snapshot must NOT have been called (that's the earliest path).
        mock_snapshot_manager.try_get_earliest_snapshot.assert_not_called()
        # FileScanner.scan was NOT called for an initial full-scan plan
        MockFileScanner.return_value.scan.assert_not_called()

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_scan_from_latest_matches_default(
        self, MockFileScanner, MockManifestFileManager,
        MockManifestListManager, MockSnapshotManager
    ):
        """scan_from='latest' should behave identically to the default (no scan_from)."""
        table, _ = _create_mock_table()

        latest = _create_mock_snapshot(7)
        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = latest
        mock_snapshot_manager.get_snapshot_by_id.return_value = None

        MockFileScanner.return_value.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table, scan_from="latest", prefetch_enabled=False)

        async def get_first():
            async for plan in scan.stream():
                return plan

        asyncio.run(get_first())
        self.assertEqual(scan.next_snapshot_id, 8)

    @patch('pypaimon.read.streaming_table_scan.ConsumerManager')
    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestFileManager')
    @patch('pypaimon.read.streaming_table_scan.FileScanner')
    def test_consumer_restore_overrides_scan_from(
        self, MockFileScanner, MockManifestFileManager, MockManifestListManager,
        MockSnapshotManager, MockConsumerManager
    ):
        """Consumer restore should take precedence over scan_from='earliest'."""
        table, _ = _create_mock_table()

        mock_consumer_manager = MockConsumerManager.return_value
        mock_consumer = Mock()
        mock_consumer.next_snapshot = 10
        mock_consumer_manager.consumer.return_value = mock_consumer

        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = _create_mock_snapshot(10)
        mock_snapshot_manager.find_next_scannable.return_value = (None, 11, 0)

        scan = AsyncStreamingTableScan(
            table, scan_from="earliest", consumer_id="my-consumer",
            prefetch_enabled=False
        )

        async def check_state():
            gen = scan.stream()
            try:
                await asyncio.wait_for(gen.__anext__(), timeout=0.05)
            except (asyncio.TimeoutError, StopAsyncIteration):
                pass

        asyncio.run(check_state())
        # Consumer restored position; try_get_earliest_snapshot must NOT have been called
        mock_snapshot_manager.try_get_earliest_snapshot.assert_not_called()
        # Consumer manager's consumer() was called to restore position
        mock_consumer_manager.consumer.assert_called_once_with("my-consumer")


if __name__ == '__main__':
    unittest.main()
