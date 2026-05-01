#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

from pypaimon.common.file_io import FileIO

logger = logging.getLogger(__name__)
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_loader import SnapshotLoader


class SnapshotManager:
    """Manager for snapshot files using unified FileIO."""

    def __init__(self, table, branch: Optional[str] = None):
        # Lazy imports to avoid a cycle: pypaimon.branch.__init__
        # eagerly loads FileSystemBranchManager, which imports
        # SnapshotManager.
        from pypaimon.branch.branch_manager import BranchManager
        from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.file_io: FileIO = self.table.file_io
        self.snapshot_loader: Optional[SnapshotLoader] = self.table.catalog_environment.snapshot_loader()

        if branch is None:
            branch = self.table.current_branch() or DEFAULT_MAIN_BRANCH
        self.branch = BranchManager.normalize_branch(branch)

        table_root = self.table.table_path.rstrip('/')
        branch_root = BranchManager.branch_path(table_root, self.branch)
        self.snapshot_dir = f"{branch_root}/snapshot"
        self.latest_file = f"{self.snapshot_dir}/LATEST"

    def copy_with_branch(self, branch_name: str) -> 'SnapshotManager':
        # Mirrors Java SnapshotManager.copyWithBranch for the local
        # filesystem path. SnapshotLoader rebranching (REST path) is
        # not implemented yet -- the new manager re-fetches a loader
        # from catalog_environment, which on the FileSystemCatalog
        # path is None and therefore inert.
        return SnapshotManager(self.table, branch_name)

    def get_latest_snapshot(self) -> Optional[Snapshot]:
        """
        Get the latest snapshot with loader priority.

        1. Try to load from snapshotLoader if available
        2. Fallback to filesystem if loader is not available or throws NotImplementedError`
        3. Return None if no snapshot found

        Returns:
            The latest snapshot JSON string, or None if not found
        """
        # Try to load from snapshotLoader if available
        if self.snapshot_loader is not None:
            try:
                snapshot = self.snapshot_loader.load()
            except NotImplementedError:
                # Loader not supported, fallback to filesystem
                snapshot = self._get_latest_snapshot_from_filesystem()
            except IOError as e:
                # IO error, re-raise with context
                raise RuntimeError(f"Failed to load snapshot from loader: {e}")
        else:
            # No loader, use filesystem directly
            snapshot = self._get_latest_snapshot_from_filesystem()
        return snapshot

    def _get_latest_snapshot_from_filesystem(self) -> Optional[Snapshot]:
        """
        Get the latest snapshot from filesystem by reading LATEST file.
        
        Returns:
            The latest snapshot, or None if not found
        """
        if not self.file_io.exists(self.latest_file):
            return None

        latest_content = self.read_latest_file()
        latest_snapshot_id = int(latest_content.strip())

        snapshot_file = f"{self.snapshot_dir}/snapshot-{latest_snapshot_id}"
        if not self.file_io.exists(snapshot_file):
            return None

        return JSON.from_json(self.file_io.read_file_utf8(snapshot_file), Snapshot)

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
        """Get the file path for the given snapshot ID."""
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
        Get a snapshot by its ID.

        Args:
            snapshot_id: The snapshot ID

        Returns:
            The snapshot with the specified ID, or None if not found
        """
        snapshot_file = self.get_snapshot_path(snapshot_id)
        if not self.file_io.exists(snapshot_file):
            return None
        snapshot_content = self.file_io.read_file_utf8(snapshot_file)
        return JSON.from_json(snapshot_content, Snapshot)

    def get_snapshots_batch(
        self, snapshot_ids: List[int], max_workers: int = 4
    ) -> Dict[int, Optional[Snapshot]]:
        """Fetch multiple snapshots in parallel, returning {id: Snapshot|None}."""
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
        """Find the next snapshot passing should_scan, using batch lookahead."""
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
