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
Paimon tail command implementation.

Streams data from a Paimon table, similar to kafka-console-consumer.
"""

import asyncio
import signal
import sys
import time
from argparse import Namespace
from datetime import datetime
from typing import Optional

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.cli.utils import (OutputFormatter, get_formatter,
                                parse_end_position, parse_filters,
                                parse_start_position)
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class Timer:
    """Simple timer for profiling."""

    def __init__(self):
        self.timings = {}
        self._start = None
        self._current_phase = None

    def start(self, phase: str):
        self._current_phase = phase
        self._start = time.perf_counter()

    def stop(self):
        if self._start and self._current_phase:
            elapsed = time.perf_counter() - self._start
            if self._current_phase not in self.timings:
                self.timings[self._current_phase] = {"total": 0, "count": 0}
            self.timings[self._current_phase]["total"] += elapsed
            self.timings[self._current_phase]["count"] += 1
        self._start = None
        self._current_phase = None

    def report(self) -> str:
        lines = ["", "=== Timing Report ==="]
        for phase, data in sorted(self.timings.items(), key=lambda x: -x[1]["total"]):
            avg = data["total"] / data["count"] if data["count"] > 0 else 0
            lines.append(f"  {phase}: {data['total']:.3f}s total, "
                         f"{data['count']} calls, {avg*1000:.1f}ms avg")
        return "\n".join(lines)


def print_banner(args: Namespace, verbose: bool = False) -> None:
    """Print startup banner."""
    if not verbose:
        return

    print("=" * 70, file=sys.stderr)
    print("  Paimon Tail", file=sys.stderr)
    print(f"  Warehouse: {args.warehouse}", file=sys.stderr)
    print(f"  Table: {args.table}", file=sys.stderr)
    if args.consumer_id:
        print(f"  Consumer ID: {args.consumer_id}", file=sys.stderr)
    print(f"  From: {args.from_pos}", file=sys.stderr)
    print(f"  To: {args.to_pos}", file=sys.stderr)
    print(f"  Output: {args.output}", file=sys.stderr)
    if args.filters:
        print(f"  Filters: {args.filters}", file=sys.stderr)
    if args.columns:
        print(f"  Columns: {args.columns}", file=sys.stderr)
    if args.limit:
        print(f"  Limit: {args.limit}", file=sys.stderr)
    if args.follow:
        print("  Follow: enabled", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(file=sys.stderr)


def log(msg: str, verbose: bool = False) -> None:
    """Print a timestamped log message to stderr."""
    if not verbose:
        return
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", file=sys.stderr)


async def tail_async(args: Namespace) -> int:
    """
    Async implementation of the tail command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success)
    """
    verbose = args.verbose
    profile = getattr(args, 'profile', False)
    timer = Timer() if profile else None

    print_banner(args, verbose)

    # Create catalog
    log("Connecting to catalog...", verbose)
    if timer:
        timer.start("catalog_create")
    catalog = CatalogFactory.create({
        "warehouse": args.warehouse,
        "metastore": "filesystem",
    })
    if timer:
        timer.stop()

    # Get table
    log(f"Getting table: {args.table}", verbose)
    if timer:
        timer.start("get_table")
    table = catalog.get_table(args.table)
    if timer:
        timer.stop()

    log(f"Schema: {[f.name for f in table.fields]}", verbose)

    # Create snapshot manager
    snapshot_mgr = SnapshotManager(table)

    # Create streaming read builder
    stream_builder = table.new_stream_read_builder()
    stream_builder.with_poll_interval_ms(args.poll_interval)

    if args.consumer_id:
        stream_builder.with_consumer_id(args.consumer_id)

    if args.include_row_kind:
        stream_builder.with_include_row_kind(True)

    # Apply column projection
    if args.columns:
        columns = [c.strip() for c in args.columns.split(',')]
        stream_builder.with_projection(columns)

    # Apply filters
    if args.filters:
        predicate_builder = stream_builder.new_predicate_builder()
        predicate = parse_filters(args.filters, predicate_builder)
        if predicate:
            stream_builder.with_filter(predicate)

    # Create scan and reader
    scan = stream_builder.new_streaming_scan()
    table_read = stream_builder.new_read()

    # Determine start position
    start_snapshot_id: Optional[int] = None
    if args.from_pos != 'latest':
        start_snapshot_id = parse_start_position(args.from_pos, snapshot_mgr)
        if start_snapshot_id:
            scan.next_snapshot_id = start_snapshot_id
            log(f"Starting from snapshot {start_snapshot_id}", verbose)
    else:
        # For 'latest', start from latest+1 (skip existing data)
        latest = snapshot_mgr.get_latest_snapshot()
        if latest:
            scan.next_snapshot_id = latest.id + 1
            log(f"Starting from latest (snapshot {latest.id + 1})", verbose)

    # Determine end position (None means "latest" - keep going until caught up)
    end_snapshot_id: Optional[int] = parse_end_position(args.to_pos, snapshot_mgr)
    if end_snapshot_id:
        log(f"Ending at snapshot {end_snapshot_id}", verbose)
    else:
        log("Ending at latest snapshot", verbose)

    log("Streaming started (Ctrl+C to stop)...", verbose)
    if verbose:
        print("-" * 70, file=sys.stderr)

    # Create output formatter
    formatter: OutputFormatter = get_formatter(args.output)
    total_rows = 0
    limit = args.limit
    snapshots_processed = 0

    try:
        stream_iter = scan.stream().__aiter__()
        while True:
            # Time the plan creation (includes snapshot read + manifest reads)
            if timer:
                timer.start("scan_next_plan")
            try:
                plan = await stream_iter.__anext__()
            except StopAsyncIteration:
                if timer:
                    timer.stop()
                break
            if timer:
                timer.stop()

            if timer:
                timer.start("get_splits")
            splits = plan.splits()
            if timer:
                timer.stop()

            if not splits:
                if not args.follow:
                    # Check if we've reached end position or caught up
                    if end_snapshot_id:
                        if scan.next_snapshot_id and scan.next_snapshot_id > end_snapshot_id:
                            break
                    else:
                        # No more data and not following - exit
                        break
                continue

            # Read data from splits
            if timer:
                timer.start("read_to_arrow")
            arrow_table = table_read.to_arrow(splits)
            if timer:
                timer.stop()

            num_rows = arrow_table.num_rows

            if num_rows == 0:
                continue

            # Convert to list of dicts and output
            if timer:
                timer.start("to_pylist")
            rows = arrow_table.to_pylist()
            if timer:
                timer.stop()

            if timer:
                timer.start("format_output")
            for row in rows:
                formatter.write(row)
                total_rows += 1

                if limit and total_rows >= limit:
                    if timer:
                        timer.stop()
                    return 0
            if timer:
                timer.stop()

            # Flush stdout for real-time output
            sys.stdout.flush()

            snapshots_processed += 1

            # Checkpoint if consumer_id is set (best effort)
            if args.consumer_id:
                try:
                    scan.notify_checkpoint_complete(scan.next_snapshot_id)
                except OSError:
                    pass  # Ignore checkpoint failures

            if not args.follow:
                # Check if we've reached the end position or caught up
                if end_snapshot_id:
                    # Stop if we've passed the end snapshot
                    if scan.next_snapshot_id and scan.next_snapshot_id > end_snapshot_id:
                        break
                else:
                    # No end position specified - stop when caught up to latest
                    latest = snapshot_mgr.get_latest_snapshot()
                    if latest and scan.next_snapshot_id and scan.next_snapshot_id > latest.id:
                        break

    except KeyboardInterrupt:
        if verbose:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Interrupted", file=sys.stderr)
    finally:
        formatter.close()
        if verbose:
            print("-" * 70, file=sys.stderr)
            print(f"Total: {total_rows} rows", file=sys.stderr)

        if profile:
            print(timer.report(), file=sys.stderr)
            print("\n=== Cache Stats ===", file=sys.stderr)
            # Get cache stats from the managers
            mfm = scan._manifest_file_manager
            mlm = scan._manifest_list_manager
            sm = scan._snapshot_manager
            sm_stats = sm.get_cache_stats()
            print(f"  Snapshot:     {sm_stats['cache_hits']} hits, {sm_stats['cache_misses']} misses", file=sys.stderr)
            print(f"  ManifestFile: {mfm._cache_hits} hits, {mfm._cache_misses} misses", file=sys.stderr)
            print(f"  ManifestList: {mlm._cache_hits} hits, {mlm._cache_misses} misses", file=sys.stderr)
            print("\n=== Prefetch & Lookahead Stats ===", file=sys.stderr)
            print(f"  Prefetch: {scan._prefetch_hits} hits, {scan._prefetch_misses} misses", file=sys.stderr)
            print(f"  Lookahead skips: {scan._lookahead_skips} (non-scannable snapshots skipped)", file=sys.stderr)
            print(f"  Diff-based catch-up: {'used' if scan._diff_catch_up_used else 'not used'}", file=sys.stderr)
            print(f"\n  Snapshots processed: {snapshots_processed}", file=sys.stderr)

    return 0


def run_tail(args: Namespace) -> int:
    """
    Run the tail command.

    This is the main entry point called from main.py.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Run the async tail function
    return asyncio.run(tail_async(args))
