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

"""
AsyncStreamingTableScan for continuous streaming reads from Paimon tables.

This module provides async-based streaming reads that continuously poll for
new snapshots and yield Plans as new data arrives. It is the Python equivalent
of Java's DataTableStreamScan.
"""

import asyncio
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import AsyncGenerator, Callable, Iterator, List, Optional

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.common.predicate import Predicate
from pypaimon.consumer.consumer import Consumer
from pypaimon.consumer.consumer_manager import ConsumerManager
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.native_plan import _raise_if_native_fork_safety_error
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import wrap_plan_with_auth
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
from pypaimon.table.bucket_mode import BucketMode


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
        self._lookahead_skips = 0
        self._prefetch_executor = ThreadPoolExecutor(max_workers=1) if prefetch_enabled else None
        self._lookahead_size = 10  # How many snapshots to look ahead

        # Initialize managers
        self._snapshot_manager = table.snapshot_manager()
        self._manifest_list_manager = ManifestListManager(table)
        self._manifest_file_manager = ManifestFileManager(table)

        # Consumer management for persisting streaming progress
        self._consumer_id = consumer_id
        self._read_type = None
        self._query_auth_fn = self.table.catalog_environment.table_query_auth(
            self.table.options, self.table.identifier)
        self._consumer_manager = (
            ConsumerManager(table.file_io, table.table_path)
            if consumer_id else None
        )

        # Scanner for determining which snapshots to read
        # Auto-select based on changelog-producer if not explicitly provided
        self.follow_up_scanner = follow_up_scanner or self._create_follow_up_scanner()

        # State tracking
        self.next_snapshot_id: Optional[int] = None
        self._pending_consumer_snapshot: Optional[int] = None

    async def stream(self) -> AsyncGenerator[Plan, None]:
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
                plan = self._create_initial_plan(latest_snapshot)
                self.next_snapshot_id = latest_snapshot.id + 1
                self._stage_consumer()
                yield plan
                # Resumes here when caller calls __anext__() — after caller processed the plan.
                self._flush_pending_consumer()

        # Check for catch-up scenario: starting from earlier snapshot with large gap.
        # This block only executes once per stream() call (before the while True loop).
        # Handles --from snapshot:X with many snapshots to process.
        if self._should_use_diff_catch_up():
            self._catch_up_in_progress = True
            try:
                latest_snapshot = self._snapshot_manager.get_latest_snapshot()
                if latest_snapshot and self.next_snapshot_id:
                    catch_up_plan = self._create_catch_up_plan(
                        self.next_snapshot_id,
                        latest_snapshot
                    )
                    self.next_snapshot_id = latest_snapshot.id + 1
                    self._stage_consumer()
                    yield catch_up_plan
                    # Resumes here when caller calls __anext__().
                    self._flush_pending_consumer()
            finally:
                self._catch_up_in_progress = False

        # Follow-up polling loop with lookahead and optional prefetching
        while True:
            # Flush any consumer position staged by the previous yield before doing more work.
            self._flush_pending_consumer()
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
                except Exception as error:
                    _raise_if_native_fork_safety_error(error)
                    # Prefetch failed, fall back to synchronous
                    prefetch_used = False
                finally:
                    self._prefetch_future = None
                    self._prefetch_snapshot_id = None

            # If prefetch wasn't available or failed, use lookahead to find next scannable
            if not prefetch_used:
                # Use batch lookahead to find the next scannable snapshot
                snapshot, next_id, skipped_count = self._snapshot_manager.find_next_scannable(
                    self.next_snapshot_id,
                    self.follow_up_scanner.should_scan,
                    lookahead_size=self._lookahead_size
                )
                # Check if we found a scannable snapshot or skipped some
                snapshot_processed = skipped_count > 0 or snapshot is not None

                if snapshot is not None:
                    plan = self._create_follow_up_plan(snapshot)
                # Advance only after constructing the selected frame. A failed
                # callback/read must leave that snapshot available for retry.
                self._lookahead_skips += skipped_count
                self.next_snapshot_id = next_id

            if plan is not None:
                # Start prefetching next scannable snapshot before yielding
                if self._prefetch_enabled:
                    self._start_prefetch(self.next_snapshot_id)
                self._stage_consumer()
                yield plan
                # _flush_pending_consumer() is called at the top of the next iteration.
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

    def _stage_consumer(self) -> None:
        """Stage next_snapshot_id to be written to disk on the next generator resume."""
        if self._consumer_manager and self._consumer_id and self.next_snapshot_id is not None:
            self._pending_consumer_snapshot = self.next_snapshot_id

    def _flush_pending_consumer(self) -> None:
        """Flush the staged consumer position to disk.

        Called at the resume point after each yield — i.e. when the caller calls
        __anext__() to request the next plan, which happens after the caller's loop
        body (to_arrow + sink write) has completed. This gives at-least-once semantics:
        the consumer file is only advanced after the caller has processed the prior plan.
        """
        if self._consumer_manager and self._consumer_id and self._pending_consumer_snapshot is not None:
            self._consumer_manager.reset_consumer(
                self._consumer_id,
                Consumer(next_snapshot=self._pending_consumer_snapshot)
            )
            self._pending_consumer_snapshot = None

    def __apply_auth(self, plan) -> Plan:
        return wrap_plan_with_auth(self.__auth_query(), plan)

    def __auth_query(self):
        from pypaimon.read.table_scan import authorize
        return authorize(self.table, self._query_auth_fn, self._read_type)

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
        except Exception as error:
            _raise_if_native_fork_safety_error(error)
            logging.exception("Prefetch failed for snapshot_id=%d; falling back to synchronous", start_id)
            return None

    def _create_follow_up_plan(self, snapshot: Snapshot) -> Plan:
        """Route to changelog or delta plan based on scanner type."""
        if isinstance(self.follow_up_scanner, ChangelogFollowUpScanner):
            plan = self._create_changelog_plan(snapshot)
        else:
            plan = self._create_delta_plan(snapshot)
        return self.__apply_auth(plan)

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

    def _only_read_real_buckets(self) -> bool:
        # Java DataTableStreamScan leaves postpone pending data visible only
        # when there is no changelog producer to emit it after assignment.
        return (self.table.options.bucket() == BucketMode.POSTPONE_BUCKET.value
                and self.table.options.changelog_producer() != ChangelogProducer.NONE)

    def __create_initial_plan_raw(self, snapshot, auth_result=None):
        def all_manifests():
            return self._manifest_list_manager.read_all(snapshot), snapshot

        starting_scanner = FileScanner(
            self.table,
            all_manifests,
            predicate=self.predicate,
            limit=None
        )
        starting_scanner.only_read_real_buckets = self._only_read_real_buckets()
        if auth_result is not None:
            from pypaimon.read.table_scan import prune_scanner_by_auth
            prune_scanner_by_auth(self.table, starting_scanner, auth_result)
        plan = starting_scanner.scan()
        if self._bucket_filter is not None:
            plan = Plan([split for split in plan.splits() if self._bucket_filter(split.bucket)],
                        snapshot_id=plan.snapshot_id)
        return plan

    def _create_initial_plan(self, snapshot: Snapshot) -> Plan:
        """Create a Plan for the initial full scan of the latest snapshot."""
        auth_result = self.__auth_query()
        plan = self.__create_initial_plan_raw(snapshot, auth_result)
        return wrap_plan_with_auth(auth_result, plan)

    def _create_delta_plan(self, snapshot: Snapshot) -> Plan:
        """Read new files from delta_manifest_list (changelog-producer=none)."""
        manifest_files = self._manifest_list_manager.read_delta(snapshot)
        return self._create_plan_from_manifests(manifest_files, snapshot.id)

    def _create_changelog_plan(self, snapshot: Snapshot) -> Plan:
        """Java ChangelogFollowUpScanner consumes any snapshot with a changelog."""
        manifest_files = self._manifest_list_manager.read_changelog(snapshot)
        return self._create_plan_from_manifests(manifest_files, snapshot.id)

    def _create_plan_from_manifests(self, manifest_files: List, snapshot_id=None) -> Plan:
        """Create splits from manifest files, applying shard filtering."""
        if not manifest_files:
            return Plan([], snapshot_id=snapshot_id)

        # Use configurable parallelism from table options
        max_workers = max(8, self.table.options.scan_manifest_parallelism(os.cpu_count() or 8))

        def require_add(entry):
            if entry.kind != 0:
                raise ValueError("Incremental manifests must contain only ADD entries")
            return True

        # Validate before the manifest reader reconciles ADD/DELETE entries.
        entries = self._manifest_file_manager.read_entries_parallel(
            manifest_files,
            manifest_entry_filter=require_add,
            max_workers=max_workers
        )

        # Apply shard/bucket filtering for parallel consumption
        if self._only_read_real_buckets():
            entries = [entry for entry in entries if entry.bucket >= 0]
        entries = self._filter_entries_for_shard(entries) if entries else []
        if not entries:
            return Plan([], snapshot_id=snapshot_id)

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
                deletion_files_map={},
                snapshot_id=snapshot_id,
            )
        else:
            split_generator = AppendTableSplitGenerator(
                self.table,
                target_split_size,
                open_file_cost,
                deletion_files_map={},
                snapshot_id=snapshot_id,
            )

        splits = split_generator.create_splits(entries)
        for split in splits:
            split.is_streaming = True
        return Plan(splits, snapshot_id=snapshot_id)

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
        start_snapshot = None
        if start_id > 1:
            start_snapshot = self._snapshot_manager.get_snapshot_by_id(start_id - 1)

        auth_result = self.__auth_query()
        if start_snapshot is None:
            plan = self.__create_initial_plan_raw(end_snapshot, auth_result)
        else:
            plan = IncrementalDiffScanner(self.table).scan(start_snapshot, end_snapshot)
        return wrap_plan_with_auth(auth_result, plan)


class StreamTableScan:
    """Python iterator adapter for Rust's stateful StreamTableScan.

    Rust owns snapshot selection, split planning, checkpoints and consumer
    persistence. Python only polls, converts splits and acknowledges a yielded
    plan when the caller resumes the iterator after processing it.
    """

    def __init__(self, table, predicate=None, read_type=None, poll_interval_ms=1000,
                 bucket_filter=None, consumer_id=None):
        from pypaimon.read.native_plan import native_stream_scan
        self.table = table
        self.poll_interval = poll_interval_ms / 1000.0
        self._scan = native_stream_scan(
            table, predicate=predicate, read_type=read_type,
            bucket_filter=bucket_filter, consumer_id=consumer_id)
        self._pending_consumer_snapshot = None

    @property
    def next_snapshot_id(self):
        return self.checkpoint()

    @next_snapshot_id.setter
    def next_snapshot_id(self, next_snapshot_id):
        self.restore(next_snapshot_id)

    def checkpoint(self):
        return self._scan.checkpoint()

    def watermark(self):
        return self._scan.watermark()

    def restore(self, next_snapshot_id=None):
        self._scan.restore(next_snapshot_id)
        self._pending_consumer_snapshot = None

    def notify_checkpoint_complete(self, next_snapshot):
        self._scan.notify_checkpoint_complete(next_snapshot)

    def plan(self):
        from pypaimon.read.native_plan import _from_native_plan
        # Decode before returning or acknowledging progress. A failed conversion
        # must leave the selected frame available for retry.
        checkpoint = self.checkpoint()
        native_plan = self._scan.plan()
        if native_plan is None:
            return None
        try:
            return _from_native_plan(self.table, native_plan)
        except Exception:
            self._scan.restore(checkpoint)
            raise

    def _flush_pending_consumer(self):
        if self._pending_consumer_snapshot is not None:
            self.notify_checkpoint_complete(self._pending_consumer_snapshot)
            self._pending_consumer_snapshot = None

    async def stream(self):
        while True:
            self._flush_pending_consumer()
            checkpoint = self.checkpoint()
            plan = self.plan()
            if plan is None:
                next_snapshot = self.checkpoint()
                if next_snapshot is not None and next_snapshot != checkpoint:
                    # Skipped frames have no rows awaiting caller processing.
                    self._pending_consumer_snapshot = next_snapshot
                    self._flush_pending_consumer()
                await asyncio.sleep(self.poll_interval)
                continue
            self._pending_consumer_snapshot = self.checkpoint()
            yield plan

    def stream_sync(self):
        loop = asyncio.new_event_loop()
        iterator = self.stream()
        try:
            while True:
                yield loop.run_until_complete(iterator.__anext__())
        finally:
            loop.run_until_complete(iterator.aclose())
            loop.close()
