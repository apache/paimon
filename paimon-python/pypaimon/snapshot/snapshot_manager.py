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
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

from cachetools import LRUCache

from pypaimon.common.file_io import FileIO

logger = logging.getLogger(__name__)
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot


class SnapshotManager:
    """Manager for snapshot files using unified FileIO."""

    # Default cache size for snapshots (matching Java's typical cache size)
    DEFAULT_CACHE_SIZE = 100

    def __init__(self, table, cache_size: int = DEFAULT_CACHE_SIZE):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.file_io: FileIO = self.table.file_io
        snapshot_path = self.table.table_path.rstrip('/')
        self.snapshot_dir = f"{snapshot_path}/snapshot"
        self.latest_file = f"{self.snapshot_dir}/LATEST"

        # LRU cache for snapshots (like Java's Cache<Path, Snapshot>)
        self._cache: LRUCache = LRUCache(maxsize=cache_size)
        self._cache_hits = 0
        self._cache_misses = 0

    def get_latest_snapshot(self) -> Optional[Snapshot]:
        snapshot_json = self.get_latest_snapshot_json()
        if snapshot_json is None:
            return None
        return JSON.from_json(snapshot_json, Snapshot)

    def get_latest_snapshot_json(self) -> Optional[str]:
        if not self.file_io.exists(self.latest_file):
            return None

        latest_content = self.read_latest_file()
        latest_snapshot_id = int(latest_content.strip())

        snapshot_file = f"{self.snapshot_dir}/snapshot-{latest_snapshot_id}"
        if not self.file_io.exists(snapshot_file):
            return None

        return self.file_io.read_file_utf8(snapshot_file)

    def read_latest_file(self, max_retries: int = 5):
        """
        Read the latest snapshot ID from LATEST file with retry mechanism.
        If file doesn't exist or is empty after retries, scan snapshot directory for max ID.
        """
        import re
        import time

        # Try to read LATEST file with retries
        for retry_count in range(max_retries):
            try:
                if self.file_io.exists(self.latest_file):
                    content = self.file_io.read_file_utf8(self.latest_file)
                    if content and content.strip():
                        return content.strip()

                # File doesn't exist or is empty, wait a bit before retry
                if retry_count < max_retries - 1:
                    time.sleep(0.001)

            except Exception:
                # On exception, wait and retry
                if retry_count < max_retries - 1:
                    time.sleep(0.001)

        # List all files in snapshot directory
        file_infos = self.file_io.list_status(self.snapshot_dir)

        max_snapshot_id = None
        snapshot_pattern = re.compile(r'^snapshot-(\d+)$')

        for file_info in file_infos:
            # Get filename from path
            filename = file_info.path.split('/')[-1]
            match = snapshot_pattern.match(filename)
            if match:
                snapshot_id = int(match.group(1))
                if max_snapshot_id is None or snapshot_id > max_snapshot_id:
                    max_snapshot_id = snapshot_id

        if not max_snapshot_id:
            raise RuntimeError(f"No snapshot content found in {self.snapshot_dir}")

        return str(max_snapshot_id)

    def get_snapshot_path(self, snapshot_id: int) -> str:
        """
        Get the path for a snapshot file.

        Args:
            snapshot_id: The snapshot ID

        Returns:
            Path to the snapshot file
        """
        return f"{self.snapshot_dir}/snapshot-{snapshot_id}"

    def try_get_earliest_snapshot(self) -> Optional[Snapshot]:
        earliest_file = f"{self.snapshot_dir}/EARLIEST"
        if self.file_io.exists(earliest_file):
            earliest_content = self.file_io.read_file_utf8(earliest_file)
            earliest_snapshot_id = int(earliest_content.strip())
            snapshot = self.get_snapshot_by_id(earliest_snapshot_id)
            if snapshot is None:
                logger.warning(
                    "The earliest snapshot or changelog was once identified but disappeared. "
                    "It might have been expired by other jobs operating on this table."
                )
            return snapshot
        return self.get_snapshot_by_id(1)

    def earlier_or_equal_time_mills(self, timestamp: int) -> Optional[Snapshot]:
        """
        Find the latest snapshot with time_millis <= the given timestamp.

        Args:
            timestamp: The timestamp to compare against

        Returns:
            The latest snapshot with time_millis <= timestamp, or None if no such snapshot exists
        """
        earliest_snap = self.try_get_earliest_snapshot()
        latest_snap = self.get_latest_snapshot()

        if earliest_snap is None or latest_snap is None:
            return None

        earliest = earliest_snap.id
        latest = latest_snap.id
        final_snapshot = None

        while earliest <= latest:
            mid = earliest + (latest - earliest) // 2
            snapshot = self.get_snapshot_by_id(mid)

            # Handle gaps in snapshot sequence (expired snapshots)
            if snapshot is None:
                # Search forward to find next existing snapshot
                found = False
                for i in range(mid + 1, latest + 1):
                    snapshot = self.get_snapshot_by_id(i)
                    if snapshot is not None:
                        mid = i
                        found = True
                        break
                if not found:
                    # No snapshots from mid to latest, search lower half
                    latest = mid - 1
                    continue

            commit_time = snapshot.time_millis

            if commit_time > timestamp:
                latest = mid - 1
            elif commit_time < timestamp:
                earliest = mid + 1
                final_snapshot = snapshot
            else:
                final_snapshot = snapshot
                break

        return final_snapshot

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """
        Get a snapshot by its ID, using cache if available.

        Args:
            snapshot_id: The snapshot ID

        Returns:
            The snapshot with the specified ID, or None if not found
        """
        # Check cache first
        if snapshot_id in self._cache:
            self._cache_hits += 1
            return self._cache[snapshot_id]

        self._cache_misses += 1
        snapshot_file = self.get_snapshot_path(snapshot_id)
        if not self.file_io.exists(snapshot_file):
            return None

        snapshot_content = self.file_io.read_file_utf8(snapshot_file)
        snapshot = JSON.from_json(snapshot_content, Snapshot)

        # Cache the result
        if snapshot is not None:
            self._cache[snapshot_id] = snapshot

        return snapshot

    def get_snapshots_batch(
        self, snapshot_ids: List[int], max_workers: int = 4
    ) -> Dict[int, Optional[Snapshot]]:
        """
        Fetch multiple snapshots in parallel.

        This is more efficient than sequential fetches when reading from S3
        or other high-latency storage systems.

        Args:
            snapshot_ids: List of snapshot IDs to fetch
            max_workers: Maximum number of parallel workers

        Returns:
            Dictionary mapping snapshot ID to Snapshot (or None if not found)
        """
        if not snapshot_ids:
            return {}

        # First, batch check which snapshot files exist
        paths = [self.get_snapshot_path(sid) for sid in snapshot_ids]
        existence = self.file_io.exists_batch(paths)

        # Filter to only existing snapshots
        existing_ids = [
            sid for sid, path in zip(snapshot_ids, paths)
            if existence.get(path, False)
        ]

        if not existing_ids:
            return {sid: None for sid in snapshot_ids}

        # Fetch existing snapshots in parallel
        def fetch_one(sid: int) -> tuple:
            try:
                return (sid, self.get_snapshot_by_id(sid))
            except Exception:
                return (sid, None)

        results = {sid: None for sid in snapshot_ids}

        with ThreadPoolExecutor(max_workers=min(len(existing_ids), max_workers)) as executor:
            for sid, snapshot in executor.map(fetch_one, existing_ids):
                results[sid] = snapshot

        return results

    def find_next_scannable(
        self,
        start_id: int,
        should_scan: Callable[[Snapshot], bool],
        lookahead_size: int = 10,
        max_workers: int = 4
    ) -> tuple:
        """
        Find the next scannable snapshot using batch lookahead.

        This method batch-fetches multiple snapshots ahead and finds the first
        one that passes the should_scan filter. This is more efficient than
        sequential fetching when many snapshots may be skipped (e.g., COMPACT
        commits when only APPEND is desired).

        Args:
            start_id: Starting snapshot ID to search from
            should_scan: Function that returns True if a snapshot should be scanned
            lookahead_size: Number of snapshots to fetch ahead (default 10)
            max_workers: Maximum parallel workers for batch fetch

        Returns:
            Tuple of (snapshot, next_id, skipped_count):
            - snapshot: The first scannable snapshot, or None if not found
            - next_id: The next snapshot ID to check (after found or after lookahead)
            - skipped_count: Number of non-scannable snapshots skipped
        """
        # Generate the range of snapshot IDs to check
        snapshot_ids = list(range(start_id, start_id + lookahead_size))

        # Batch fetch all snapshots
        snapshots = self.get_snapshots_batch(snapshot_ids, max_workers)

        # Find the first scannable snapshot in order
        skipped_count = 0
        for sid in snapshot_ids:
            snapshot = snapshots.get(sid)
            if snapshot is None:
                # No more snapshots exist at this ID
                # Return next_id = sid so caller knows where to wait
                return (None, sid, skipped_count)
            if should_scan(snapshot):
                # Found a scannable snapshot
                return (snapshot, sid + 1, skipped_count)
            skipped_count += 1

        # All fetched snapshots were skipped, but more may exist
        # Return next_id pointing past the batch
        return (None, start_id + lookahead_size, skipped_count)

    def get_cache_stats(self) -> Dict[str, int]:
        """Return cache statistics for monitoring."""
        return {
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_size": len(self._cache),
        }
