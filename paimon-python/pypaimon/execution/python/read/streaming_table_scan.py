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

import asyncio
import os
import operator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import AsyncGenerator, Callable, Iterator, List, Optional

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.common.predicate import Predicate
from pypaimon.consumer.consumer import Consumer
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import wrap_plan_with_auth
from pypaimon.execution.python.read.scanner.append_table_split_generator import \
    AppendTableSplitGenerator
from pypaimon.execution.python.read.scanner.changelog_follow_up_scanner import \
    ChangelogFollowUpScanner
from pypaimon.execution.python.read.scanner.delta_follow_up_scanner import DeltaFollowUpScanner
from pypaimon.execution.python.read.scanner.file_scanner import FileScanner
from pypaimon.execution.python.read.scanner.follow_up_scanner import FollowUpScanner
from pypaimon.execution.python.read.scanner.primary_key_table_split_generator import \
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

        # Retain the constructor argument without replacing physical stream
        # events with a net snapshot diff when a consumer falls behind.
        self._diff_threshold = diff_threshold

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
            table.consumer_manager()
            if consumer_id else None
        )

        # Scanner for determining which snapshots to read
        # Auto-select based on changelog-producer if not explicitly provided
        self.follow_up_scanner = follow_up_scanner or self._create_follow_up_scanner()

        # State tracking
        self._next_snapshot_id: Optional[int] = None
        self._pending_consumer_snapshot: Optional[int] = None
        self._pending_consumer_owner = None
        self._consumer_restored = False
        self._watermark: Optional[int] = None
        self._closed = False
        self._generation = 0
        self._missing_snapshot_polls = 0

    @property
    def next_snapshot_id(self):
        return self.checkpoint()

    @next_snapshot_id.setter
    def next_snapshot_id(self, next_snapshot_id):
        self.restore(next_snapshot_id)

    def checkpoint(self):
        """Return the next snapshot to consume, without acknowledging it."""
        return self._next_snapshot_id

    def watermark(self):
        """Return the watermark of the last successfully planned frame."""
        return self._watermark

    def restore(self, next_snapshot_id=None):
        """Restore a checkpoint, or reset to the initial/consumer position."""
        self._require_open()
        next_snapshot_id = self._snapshot_id(next_snapshot_id)
        if next_snapshot_id is not None and next_snapshot_id < 1:
            raise ValueError("next snapshot id must be positive")
        self._generation += 1
        self._discard_prefetch()
        self._next_snapshot_id = next_snapshot_id
        self._consumer_restored = next_snapshot_id is not None
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None
        self._lookahead_skips = 0
        self._missing_snapshot_polls = 0

    def notify_checkpoint_complete(self, next_snapshot):
        """Persist progress only after the caller has processed its plan."""
        self._require_open()
        next_snapshot = self._snapshot_id(next_snapshot)
        if self._consumer_manager and self._consumer_id and next_snapshot is not None:
            if next_snapshot < 1:
                raise ValueError("consumer next snapshot must be positive")
            self._consumer_manager.reset_consumer(
                self._consumer_id, Consumer(next_snapshot=next_snapshot))

    def plan(self):
        """Plan one available frame; return None instead of waiting for data.

        Initial state may be empty. Empty follow-up frames and irrelevant
        commits advance the checkpoint without emitting a frame, as in Java
        and Rust. A planning failure leaves the selected snapshot retryable.
        """
        self._require_open()
        if self._next_snapshot_id is None and not self._consumer_restored:
            if self._consumer_manager:
                consumer = self._consumer_manager.consumer(self._consumer_id)
                if consumer is not None:
                    self.restore(consumer.next_snapshot)
            self._consumer_restored = True
        if self._next_snapshot_id is None:
            generation = self._generation
            latest = self._snapshot_manager.get_latest_snapshot()
            if latest is None:
                self.__auth_query()
                return None
            plan = self._create_initial_plan(latest)
            self._check_generation(generation)
            self._next_snapshot_id = latest.id + 1
            self._watermark = latest.watermark
            return plan

        while True:
            generation = self._generation
            if self._prefetch_future is not None:
                future = self._prefetch_future
                self._prefetch_future = None
                self._prefetch_snapshot_id = None
                result = future.result()
            else:
                result = self._fetch_plan_with_lookahead(self._next_snapshot_id)
            self._check_generation(generation)
            plan, next_id, skipped, watermark = result
            self._lookahead_skips += skipped
            self._next_snapshot_id = next_id
            if plan is not None:
                self._missing_snapshot_polls = 0
                self._watermark = watermark
                if plan.splits():
                    if self._prefetch_enabled:
                        self._start_prefetch(next_id)
                    return plan
            elif skipped == 0:
                self.__auth_query()
                self._check_missing_snapshot()
                return None
            else:
                self._missing_snapshot_polls = 0

    async def stream(self) -> AsyncGenerator[Plan, None]:
        """Poll plan(), acknowledging each yielded frame on iterator resume."""
        owner = object()
        try:
            while True:
                self._flush_pending_consumer()
                checkpoint = self.checkpoint()
                plan = self.plan()
                if plan is None:
                    next_snapshot = self.checkpoint()
                    if next_snapshot is not None and next_snapshot != checkpoint:
                        self._stage_consumer(owner)
                        self._flush_pending_consumer()
                    await asyncio.sleep(self.poll_interval)
                    continue
                self._stage_consumer(owner)
                yield plan
        except (GeneratorExit, asyncio.CancelledError):
            if self._pending_consumer_owner is owner:
                self._pending_consumer_snapshot = None
                self._pending_consumer_owner = None
                self._discard_prefetch()
            raise

    def stream_sync(self) -> Iterator[Plan]:
        """Synchronously poll the same scan, closing its async iterator."""
        loop = asyncio.new_event_loop()
        iterator = self.stream()
        try:
            while True:
                yield loop.run_until_complete(iterator.__anext__())
        finally:
            try:
                loop.run_until_complete(iterator.aclose())
            finally:
                loop.close()

    def _require_open(self):
        if self._closed:
            raise RuntimeError("stream scan is closed")

    @staticmethod
    def _snapshot_id(value):
        if value is None:
            return None
        value = operator.index(value)
        if not -(1 << 63) <= value < (1 << 63):
            raise OverflowError("snapshot id is outside the signed 64-bit range")
        return value

    def _check_generation(self, generation):
        self._require_open()
        if generation != self._generation:
            raise RuntimeError("stream scan was restored during planning")

    def _check_missing_snapshot(self):
        # Java NextSnapshotFetcher and Rust periodically distinguish delayed
        # publication from an expired checkpoint or a recreated table.
        self._missing_snapshot_polls += 1
        if self._missing_snapshot_polls % 16:
            return
        latest = self._snapshot_manager.get_latest_snapshot()
        earliest = (self._snapshot_manager.try_get_earliest_snapshot(latest.id)
                    if latest is not None else None)
        checkpoint = self._next_snapshot_id
        if ((earliest is not None and earliest.id > checkpoint)
                or (latest is not None and checkpoint > latest.id + 1)
                or (latest is None and checkpoint > 1)):
            raise ValueError("stream checkpoint %s is outside the available snapshots %s..%s" % (
                checkpoint, earliest.id if earliest is not None else None,
                latest.id if latest is not None else None))

    def _discard_prefetch(self):
        future = self._prefetch_future
        self._prefetch_future = None
        self._prefetch_snapshot_id = None
        if future is not None:
            future.cancel()

    def cancel_prefetch(self):
        """Discard speculative work when a public polling iterator stops."""
        self._discard_prefetch()

    def close(self):
        """Discard speculative work without acknowledging pending rows.

        Running Python I/O cannot be interrupted. Its detached result cannot
        advance the checkpoint or be reused after close/restore.
        """
        if self._closed:
            return
        self._closed = True
        self._generation += 1
        self._discard_prefetch()
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None
        if self._prefetch_executor is not None:
            self._prefetch_executor.shutdown(wait=False)

    def _stage_consumer(self, owner) -> None:
        """Stage next_snapshot_id to be written to disk on the next generator resume."""
        self._pending_consumer_owner = owner
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
            self.notify_checkpoint_complete(self._pending_consumer_snapshot)
            self._pending_consumer_snapshot = None
            self._pending_consumer_owner = None

    def __apply_auth(self, plan) -> Plan:
        return wrap_plan_with_auth(self.__auth_query(), plan)

    def __auth_query(self):
        from pypaimon.read.auth_support import authorize
        return authorize(self.table, self._query_auth_fn, self._read_type, self.predicate)

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

    def _fetch_plan_with_lookahead(self, start_id: int) -> tuple:
        """Plan without mutating progress; the caller owns checkpoint updates."""
        snapshot, next_id, skipped = self._snapshot_manager.find_next_scannable(
            start_id, self.follow_up_scanner.should_scan,
            lookahead_size=self._lookahead_size)
        if snapshot is None:
            return None, next_id, skipped, None
        plan = self._create_follow_up_plan(snapshot)
        return plan, next_id, skipped, snapshot.watermark

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
        producer = self.table.options.changelog_producer()
        if producer == ChangelogProducer.LOOKUP:
            # L0 will produce its changelog after lookup compaction, so the
            # initial state must contain only the already compacted levels.
            starting_scanner.level_filter = lambda level: level > 0
        elif producer == ChangelogProducer.FULL_COMPACTION:
            options = self.table.options.options.to_map()
            levels = options.get('num-levels')
            if levels is None:
                # Java CoreOptions.numLevels uses incrementSafely(trigger).
                trigger = int(options.get('num-sorted-run.compaction-trigger', 5))
                levels = min(trigger + 1, (1 << 31) - 1)
            last_level = int(levels) - 1
            starting_scanner.level_filter = lambda level: level == last_level
        if auth_result is not None:
            from pypaimon.execution.python.read.auth_support import prune_scanner_by_auth
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
