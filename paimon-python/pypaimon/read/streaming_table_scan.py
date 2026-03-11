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
AsyncStreamingTableScan for continuous streaming reads from Paimon tables.

This module provides async-based streaming reads that continuously poll for
new snapshots and yield Plans as new data arrives. It is the Python equivalent
of Java's DataTableStreamScan.
"""

import asyncio
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import AsyncIterator, Callable, Iterator, List, Optional

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.common.predicate import Predicate
from pypaimon.consumer.consumer import Consumer
from pypaimon.consumer.consumer_manager import ConsumerManager
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.plan import Plan
from pypaimon.read.scanner.append_table_split_generator import \
    AppendTableSplitGenerator
from pypaimon.read.scanner.changelog_follow_up_scanner import \
    ChangelogFollowUpScanner
from pypaimon.read.scanner.delta_follow_up_scanner import DeltaFollowUpScanner
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.read.scanner.follow_up_scanner import FollowUpScanner
from pypaimon.read.scanner.incremental_diff_scanner import \
    IncrementalDiffScanner
from pypaimon.read.scanner.primary_key_table_split_generator import \
    PrimaryKeyTableSplitGenerator
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class AsyncStreamingTableScan:
    """
    Async streaming table scan for continuous reads from Paimon tables.

    This class provides an async iterator that continuously polls for new
    snapshots and yields Plans containing splits for new data.

    Usage:
        scan = AsyncStreamingTableScan(table)

        async for plan in scan.stream():
            for split in plan.splits():
                # Process the data
                pass

    For synchronous usage:
        for plan in scan.stream_sync():
            process(plan)
    """

    def __init__(
        self,
        table,
        predicate: Optional[Predicate] = None,
        poll_interval_ms: int = 1000,
        follow_up_scanner: Optional[FollowUpScanner] = None,
        bucket_filter: Optional[Callable[[int], bool]] = None,
        prefetch_enabled: bool = True,
        diff_threshold: int = 10,
        consumer_id: Optional[str] = None
    ):
        """Initialize the streaming table scan."""
        self.table = table
        self.predicate = predicate
        self.poll_interval = poll_interval_ms / 1000.0

        # Bucket filter for parallel consumption
        self._bucket_filter = bucket_filter

        # Diff-based catch-up configuration
        self._diff_threshold = diff_threshold
        self._catch_up_in_progress = False

        # Prefetching configuration
        self._prefetch_enabled = prefetch_enabled
        self._prefetch_future: Optional[Future] = None
        self._prefetch_snapshot_id: Optional[int] = None
        self._prefetch_hits = 0
        self._prefetch_misses = 0
        self._lookahead_skips = 0  # Track how many snapshots were skipped via lookahead
        self._prefetch_executor = ThreadPoolExecutor(max_workers=1) if prefetch_enabled else None
        self._lookahead_size = 10  # How many snapshots to look ahead
        self._diff_catch_up_used = False  # Track if diff-based catch-up was used

        # Initialize managers
        self._snapshot_manager = SnapshotManager(table)
        self._manifest_list_manager = ManifestListManager(table)
        self._manifest_file_manager = ManifestFileManager(table)

        # Consumer management for persisting streaming progress
        self._consumer_id = consumer_id
        self._consumer_manager = (
            ConsumerManager(table.file_io, table.table_path)
            if consumer_id else None
        )

        # Scanner for determining which snapshots to read
        # Auto-select based on changelog-producer if not explicitly provided
        self.follow_up_scanner = follow_up_scanner or self._create_follow_up_scanner()

        # State tracking
        self.next_snapshot_id: Optional[int] = None
        self._initialized = False

    async def stream(self) -> AsyncIterator[Plan]:
        """Yield Plans as new snapshots appear.

        On first call, performs an initial full scan of the latest snapshot.
        Subsequent iterations poll for new snapshots and yield delta Plans.

        Yields:
            Plan objects containing splits for reading
        """
        # Restore from consumer if available
        if self.next_snapshot_id is None and self._consumer_manager:
            consumer = self._consumer_manager.consumer(self._consumer_id)
            if consumer:
                self.next_snapshot_id = consumer.next_snapshot

        # Initial scan
        if self.next_snapshot_id is None:
            latest_snapshot = self._snapshot_manager.get_latest_snapshot()
            if latest_snapshot:
                self.next_snapshot_id = latest_snapshot.id + 1
                self._save_consumer()
                yield self._create_initial_plan(latest_snapshot)
                self._initialized = True

        # Check for catch-up scenario: starting from earlier snapshot with large gap
        # This handles --from earliest or --from snapshot:X with many snapshots to process
        if self._should_use_diff_catch_up():
            self._catch_up_in_progress = True
            self._diff_catch_up_used = True
            try:
                latest_snapshot = self._snapshot_manager.get_latest_snapshot()
                if latest_snapshot and self.next_snapshot_id:
                    catch_up_plan = self._create_catch_up_plan(
                        self.next_snapshot_id,
                        latest_snapshot
                    )
                    self.next_snapshot_id = latest_snapshot.id + 1
                    self._initialized = True
                    self._save_consumer()
                    if catch_up_plan.splits():
                        yield catch_up_plan
            finally:
                self._catch_up_in_progress = False

        # Follow-up polling loop with lookahead and optional prefetching
        while True:
            plan = None
            snapshot_processed = False  # Track if we processed (or skipped) a snapshot

            # Check if we have a prefetched result ready
            prefetch_used = False
            if self._prefetch_future is not None:
                try:
                    # Wait for the prefetch thread to complete
                    # Returns (plan, next_id, skipped_count) tuple
                    prefetch_result = self._prefetch_future.result(timeout=30)
                    prefetch_used = True

                    if prefetch_result is not None:
                        prefetch_plan, next_id, skipped_count = prefetch_result
                        self._lookahead_skips += skipped_count
                        self.next_snapshot_id = next_id
                        snapshot_processed = skipped_count > 0 or prefetch_plan is not None

                        if prefetch_plan is not None:
                            plan = prefetch_plan
                            self._prefetch_hits += 1
                except Exception:
                    # Prefetch failed, fall back to synchronous
                    prefetch_used = False
                finally:
                    self._prefetch_future = None
                    self._prefetch_snapshot_id = None

            # If prefetch wasn't available or failed, use lookahead to find next scannable
            if not prefetch_used:
                self._prefetch_misses += 1
                # Use batch lookahead to find the next scannable snapshot
                snapshot, next_id, skipped_count = self._snapshot_manager.find_next_scannable(
                    self.next_snapshot_id,
                    self.follow_up_scanner.should_scan,
                    lookahead_size=self._lookahead_size
                )
                self._lookahead_skips += skipped_count
                self.next_snapshot_id = next_id

                # Check if we found a scannable snapshot or skipped some
                snapshot_processed = skipped_count > 0 or snapshot is not None

                if snapshot is not None:
                    plan = self._create_follow_up_plan(snapshot)

            if plan is not None:
                # Start prefetching next scannable snapshot before yielding
                if self._prefetch_enabled:
                    self._start_prefetch(self.next_snapshot_id)
                self._save_consumer()
                yield plan
            elif not snapshot_processed:
                # No snapshot available yet, wait and poll again
                await asyncio.sleep(self.poll_interval)
            # If snapshots were processed but plan is None (all skipped), continue loop immediately

    def stream_sync(self) -> Iterator[Plan]:
        """
        Synchronous wrapper for stream().

        Provides a blocking iterator for use in non-async code.

        Yields:
            Plan objects containing splits for reading
        """
        loop = asyncio.new_event_loop()
        try:
            async_gen = self.stream()
            while True:
                try:
                    plan = loop.run_until_complete(async_gen.__anext__())
                    yield plan
                except StopAsyncIteration:
                    break
        finally:
            loop.close()

    def _save_consumer(self) -> None:
        """Persist next_snapshot_id to consumer file."""
        if self._consumer_manager and self._consumer_id and self.next_snapshot_id is not None:
            self._consumer_manager.reset_consumer(
                self._consumer_id,
                Consumer(next_snapshot=self.next_snapshot_id)
            )

    def _start_prefetch(self, snapshot_id: int) -> None:
        """Start prefetching the next scannable snapshot in a background thread."""
        if self._prefetch_future is not None or self._prefetch_executor is None:
            return  # Already prefetching or executor not available

        self._prefetch_snapshot_id = snapshot_id
        # Submit to thread pool - this starts immediately, not when event loop runs
        self._prefetch_future = self._prefetch_executor.submit(
            self._fetch_plan_with_lookahead,
            snapshot_id
        )

    def _fetch_plan_with_lookahead(self, start_id: int) -> Optional[tuple]:
        """Find next scannable snapshot via lookahead and create a plan. Runs in thread pool."""
        try:
            snapshot, next_id, skipped_count = self._snapshot_manager.find_next_scannable(
                start_id,
                self.follow_up_scanner.should_scan,
                lookahead_size=self._lookahead_size
            )

            if snapshot is None:
                return (None, next_id, skipped_count)

            plan = self._create_follow_up_plan(snapshot)
            return (plan, next_id, skipped_count)
        except Exception:
            return None

    def _create_follow_up_plan(self, snapshot: Snapshot) -> Plan:
        """Route to changelog or delta plan based on scanner type."""
        if isinstance(self.follow_up_scanner, ChangelogFollowUpScanner):
            return self._create_changelog_plan(snapshot)
        else:
            return self._create_delta_plan(snapshot)

    def _create_follow_up_scanner(self) -> FollowUpScanner:
        """Create the appropriate follow-up scanner based on changelog-producer option."""
        changelog_producer = self.table.options.changelog_producer()
        if changelog_producer == ChangelogProducer.NONE:
            return DeltaFollowUpScanner()
        else:
            # INPUT, FULL_COMPACTION, LOOKUP all use changelog scanner
            return ChangelogFollowUpScanner()

    def _filter_entries_for_shard(self, entries: List) -> List:
        """Filter manifest entries by bucket filter, if set."""
        if self._bucket_filter is not None:
            return [e for e in entries if self._bucket_filter(e.bucket)]
        return entries

    def _create_initial_plan(self, snapshot: Snapshot) -> Plan:
        """Create a Plan for the initial full scan of the latest snapshot."""
        def all_manifests():
            return self._manifest_list_manager.read_all(snapshot)

        starting_scanner = FileScanner(
            self.table,
            all_manifests,
            predicate=self.predicate,
            limit=None
        )
        return starting_scanner.scan()

    def _create_delta_plan(self, snapshot: Snapshot) -> Plan:
        """Read new files from delta_manifest_list (changelog-producer=none)."""
        manifest_files = self._manifest_list_manager.read_delta(snapshot)
        return self._create_plan_from_manifests(manifest_files)

    def _create_changelog_plan(self, snapshot: Snapshot) -> Plan:
        """Read from changelog_manifest_list (changelog-producer=input/full-compaction/lookup)."""
        manifest_files = self._manifest_list_manager.read_changelog(snapshot)
        return self._create_plan_from_manifests(manifest_files)

    def _create_plan_from_manifests(self, manifest_files: List) -> Plan:
        """Create splits from manifest files, applying shard filtering."""
        if not manifest_files:
            return Plan([])

        # Use configurable parallelism from table options
        max_workers = max(8, self.table.options.scan_manifest_parallelism(os.cpu_count() or 8))

        # Read manifest entries from manifest files
        entries = self._manifest_file_manager.read_entries_parallel(
            manifest_files,
            manifest_entry_filter=None,
            max_workers=max_workers
        )

        # Apply shard/bucket filtering for parallel consumption
        entries = self._filter_entries_for_shard(entries) if entries else []
        if not entries:
            return Plan([])

        # Get split options from table
        options = self.table.options
        target_split_size = options.source_split_target_size()
        open_file_cost = options.source_split_open_file_cost()

        # Create appropriate split generator based on table type
        if self.table.is_primary_key_table:
            split_generator = PrimaryKeyTableSplitGenerator(
                self.table,
                target_split_size,
                open_file_cost,
                deletion_files_map={}
            )
        else:
            split_generator = AppendTableSplitGenerator(
                self.table,
                target_split_size,
                open_file_cost,
                deletion_files_map={}
            )

        splits = split_generator.create_splits(entries)
        return Plan(splits)

    def _should_use_diff_catch_up(self) -> bool:
        """Check if diff-based catch-up should be used (large gap to latest)."""
        if self._catch_up_in_progress:
            return False

        if self.next_snapshot_id is None:
            return False

        latest = self._snapshot_manager.get_latest_snapshot()
        if latest is None:
            return False

        gap = latest.id - self.next_snapshot_id
        return gap > self._diff_threshold

    def _create_catch_up_plan(self, start_id: int, end_snapshot: Snapshot) -> Plan:
        """Create a catch-up plan using diff-based scanning between start and end snapshots."""
        # Get start snapshot (one before where we want to start reading)
        # If start_id is 0 or 1, use None to indicate "from beginning"
        start_snapshot = None
        if start_id > 1:
            start_snapshot = self._snapshot_manager.get_snapshot_by_id(start_id - 1)

        # Create diff scanner
        diff_scanner = IncrementalDiffScanner(self.table)

        if start_snapshot is None:
            # No start snapshot - return all files from end snapshot
            # This is equivalent to a full scan of end snapshot
            def end_snapshot_manifests():
                return self._manifest_list_manager.read_all(end_snapshot)

            starting_scanner = FileScanner(
                self.table,
                end_snapshot_manifests,
                predicate=self.predicate,
                limit=None
            )
            return starting_scanner.scan()
        else:
            # Use diff scanner for efficient catch-up
            return diff_scanner.scan(start_snapshot, end_snapshot)
