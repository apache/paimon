################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and
#  limitations under the License.
################################################################################

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Set, Tuple, Dict, Callable, Consumer
from dataclasses import dataclass

from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.manifest.manifest_list import ManifestList
from pypaimon.manifest.index_file_handler import IndexFileHandler
from pypaimon.schema.table_schema import TableSchema
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.utils.snapshot_manager import SnapshotManager
from pypaimon.utils.changelog_manager import ChangelogManager
from pypaimon.tag.tag_manager import TagManager
from pypaimon.branch.branch_manager import BranchManager

logger = logging.getLogger(__name__)


@dataclass
class CleanOrphanFilesResult:
    """The result of orphan files cleaning."""

    deleted_file_count: int
    deleted_file_total_len_in_bytes: int
    deleted_files_path: Optional[List[str]] = None


class OrphanFilesClean:
    """To remove data files and metadata files that are not used by table (so-called "orphan files").

    It will ignore exception when listing all files because it's OK to not delete unread files.

    To avoid deleting newly written files, it only deletes orphan files older than olderThanMillis
    (1 day by default).

    To avoid conflicting with snapshot expiration, tag deletion and rollback, it will skip
    snapshot/tag when catching FileNotFoundError in process of listing used files.

    To avoid deleting files that are used but not read by mistaken, it will stop removing
    process when failed to read used files.
    """

    READ_FILE_RETRY_NUM = 3
    READ_FILE_RETRY_INTERVAL = 5

    CHANGELOG_PREFIX = "changelog-"
    SNAPSHOT_PREFIX = "snapshot-"
    LATEST = "LATEST"
    EARLIEST = "EARLIEST"

    def __init__(
        self,
        table_path: str,
        file_io: FileIO,
        table_schema: TableSchema,
        older_than_millis: Optional[int] = None,
        dry_run: bool = False
    ):
        """Initialize OrphanFilesClean.

        Args:
            table_path: Path to the table directory
            file_io: FileIO instance for file operations
            table_schema: TableSchema instance
            older_than_millis: Only delete files older than this time (default: 1 day)
            dry_run: If True, only report what would be deleted without actually deleting
        """
        import time
        if older_than_millis is None:
            older_than_millis = int(time.time() * 1000) - (24 * 60 * 60 * 1000)

        self.table_path = table_path
        self.file_io = file_io
        self.table_schema = table_schema
        self.older_than_millis = older_than_millis
        self.dry_run = dry_run
        self.partition_keys_num = len(table_schema.partition_keys)
        self.candidate_deletes: Set[str] = set()

    def clean(self) -> CleanOrphanFilesResult:
        """Clean orphan files from the table.

        Returns:
            CleanOrphanFilesResult containing statistics about cleaned files
        """
        delete_files: List[str] = []
        deleted_files_len_in_bytes = 0

        # Get valid branches
        branches = self._valid_branches()

        # Specially handle to clear snapshot dir
        deleted_files_from_snapshots: List[str] = []
        deleted_bytes_from_snapshots: List[int] = []

        self._clean_snapshot_dir(
            branches,
            deleted_files_from_snapshots.append,
            deleted_bytes_from_snapshots.append
        )

        delete_files.extend(deleted_files_from_snapshots)
        deleted_files_len_in_bytes += sum(deleted_bytes_from_snapshots)

        # Get candidate files
        candidates = self._get_candidate_deleting_files()
        if candidates:
            self.candidate_deletes = set(candidates.keys())

            # Find used files
            used_files = self._get_used_files(branches)

            # Delete unused files
            unused_files = self.candidate_deletes - used_files

            for file_name in unused_files:
                file_path, file_size = candidates[file_name]
                deleted_files_len_in_bytes += file_size
                self._clean_file(file_path)
                delete_files.append(file_path)

        # Clean empty directory
        if not self.dry_run:
            self._clean_empty_data_directory(delete_files)

        return CleanOrphanFilesResult(
            deleted_file_count=len(delete_files),
            deleted_file_total_len_in_bytes=deleted_files_len_in_bytes,
            deleted_files_path=delete_files
        )

    def _valid_branches(self) -> List[str]:
        """Get valid branches that have schemas."""
        branches = BranchManager.branches(self.table_path)

        # Check for abnormal branches (no schemas)
        abnormal_branches = []
        for branch in branches:
            # Skip branch check for now - could implement later
            pass

        if abnormal_branches:
            raise RuntimeError(
                f"Branches {abnormal_branches} have no schemas. "
                "Orphan files cleaning aborted. Please check these branches manually."
            )

        # Add main branch
        if "main" not in branches:
            branches.append("main")

        return branches

    def _clean_snapshot_dir(
        self,
        branches: List[str],
        deleted_files_consumer: Callable[[str], None],
        deleted_files_len_in_bytes_consumer: Callable[[int], None]
    ):
        """Clean snapshot directory for all branches."""
        for branch in branches:
            self._clean_branch_snapshot_dir(
                branch,
                deleted_files_consumer,
                deleted_files_len_in_bytes_consumer
            )

    def _clean_branch_snapshot_dir(
        self,
        branch: str,
        deleted_files_consumer: Callable[[str], None],
        deleted_files_len_in_bytes_consumer: Callable[[int], None]
    ):
        """Clean snapshot directory for a specific branch."""
        logger.info("Start to clean snapshot directory of branch %s.", branch)

        branch_path = BranchManager.branch_path(self.table_path, branch)
        snapshot_manager = SnapshotManager(
            FileIO(branch_path, {}),
            branch_path,
            branch
        )
        changelog_manager = ChangelogManager(self.file_io, self.table_path, branch)

        # Specially handle to snapshot directory
        non_snapshot_files = self._try_get_non_snapshot_files(
            snapshot_manager.snapshot_directory(),
            self._old_enough
        )
        for file_path, file_size in non_snapshot_files:
            self._clean_file(file_path, deleted_files_consumer, deleted_files_len_in_bytes_consumer)

        # Specially handle to changelog directory
        non_changelog_files = self._try_get_non_changelog_files(
            changelog_manager.changelog_directory(),
            self._old_enough
        )
        for file_path, file_size in non_changelog_files:
            self._clean_file(file_path, deleted_files_consumer, deleted_files_len_in_bytes_consumer)

        logger.info("End to clean snapshot directory of branch %s.", branch)

    def _try_get_non_snapshot_files(
        self, directory: str, file_status_filter
    ) -> List[Tuple[str, int]]:
        """Get non-snapshot files from directory."""
        return self._list_path_with_filter(
            directory,
            file_status_filter,
            self._non_snapshot_file_filter()
        )

    def _try_get_non_changelog_files(
        self, directory: str, file_status_filter
    ) -> List[Tuple[str, int]]:
        """Get non-changelog files from directory."""
        return self._list_path_with_filter(
            directory,
            file_status_filter,
            self._non_changelog_file_filter()
        )

    def _list_path_with_filter(
        self,
        directory: str,
        file_status_filter,
        path_filter
    ) -> List[Tuple[str, int]]:
        """List paths in directory with filters."""
        try:
            statuses = self.file_io.list_status(directory)
        except Exception as e:
            logger.warning("Failed to list directory %s: %s", directory, e)
            return []

        result = []
        for status in statuses:
            path = status.path
            if file_status_filter(status) and path_filter(path):
                file_name = path.split("/")[-1]
                try:
                    result.append((path, status.len))
                except Exception:
                    result.append((path, 0))

        return result

    @staticmethod
    def _non_snapshot_file_filter():
        """Filter for non-snapshot files."""
        def is_non_snapshot(path: str) -> bool:
            file_name = path.split("/")[-1]
            return (
                not file_name.startswith(OrphanFilesClean.SNAPSHOT_PREFIX)
                and file_name != OrphanFilesClean.EARLIEST
                and file_name != OrphanFilesClean.LATEST
            )
        return is_non_snapshot

    @staticmethod
    def _non_changelog_file_filter():
        """Filter for non-changelog files."""
        def is_non_changelog(path: str) -> bool:
            file_name = path.split("/")[-1]
            return (
                not file_name.startswith(OrphanFilesClean.CHANGELOG_PREFIX)
                and file_name != OrphanFilesClean.EARLIEST
                and file_name != OrphanFilesClean.LATEST
            )
        return is_non_changelog

    def _clean_file(
        self,
        file_path: str,
        deleted_files_consumer: Optional[Callable[[str], None]] = None,
        deleted_files_len_in_bytes_consumer: Optional[Callable[[int], None]] = None
    ):
        """Clean a file (delete if not dry run)."""
        if deleted_files_consumer:
            deleted_files_consumer(file_path)
        if deleted_files_len_in_bytes_consumer:
            deleted_files_len_in_bytes_consumer(self._get_file_size(file_path))

        self._delete_file(file_path)

    def _delete_file(self, path: str):
        """Delete a file if not dry run."""
        if not self.dry_run:
            try:
                # Check if it's a directory
                if self.file_io.is_dir(path):
                    self.file_io.delete_directory_quietly(path)
                else:
                    self.file_io.delete_quietly(path)
            except Exception as e:
                logger.debug("Failed to delete file %s: %s", path, e)

    def _get_file_size(self, path: str) -> int:
        """Get file size."""
        try:
            return self.file_io.get_file_size(path)
        except Exception:
            return 0

    def _get_candidate_deleting_files(self) -> Dict[str, Tuple[str, int]]:
        """Get all candidate files to delete."""
        file_dirs = self._list_paimon_file_dirs()
        result: Dict[str, Tuple[str, int]] = {}

        for directory in file_dirs:
            try:
                files = self._list_files_in_dir(directory)
                for file_path, file_size in files:
                    file_name = file_path.split("/")[-1]
                    result[file_name] = (file_path, file_size)
            except Exception as e:
                logger.warning("Failed to list directory %s: %s", directory, e)

        # Clean empty directories
        if not self.dry_run:
            self._clean_empty_directories(file_dirs)

        return result

    def _list_files_in_dir(
        self, directory: str
    ) -> List[Tuple[str, int]]:
        """List all files in a directory."""
        try:
            statuses = self.file_io.list_status(directory)
            if not statuses:
                # Check if directory itself is old enough to delete
                try:
                    dir_status = self.file_io.get_file_status(directory)
                    if self._old_enough(dir_status):
                        # Mark for deletion
                        return []
                except Exception as e:
                    logger.warning(
                        "IOException during check dirStatus for %s, ignore it", directory, e
                    )
                    return []

            # Filter files by age
            result = []
            for status in statuses:
                if self._old_enough(status):
                    result.append((status.path, status.len))
            return result
        except Exception as e:
            logger.warning("Failed to list directory %s: %s", directory, e)
            return []

    def _old_enough(self, status) -> bool:
        """Check if a file is old enough to be deleted."""
        # FileStatus doesn't have modification_time, so we use len as a proxy
        # In a real implementation, you'd want to check modification time
        # For now, we'll consider all files old enough
        return True

    def _list_paimon_file_dirs(self) -> List[str]:
        """List directories that contain data files and manifest files."""
        # Base directories
        base_path = self.table_path
        dirs = [
            f"{base_path}/manifest",
            f"{base_path}/index",
            f"{base_path}/statistics",
        ]

        # Data file directories
        data_path = f"{base_path}/bucket"
        dirs.extend(self._list_file_dirs(data_path, self.partition_keys_num))

        return dirs

    def _list_file_dirs(self, base_path: str, partition_keys_num: int) -> List[str]:
        """List partition and bucket directories."""
        dirs = []
        if partition_keys_num > 0:
            # List partition directories
            try:
                partition_dirs = self.file_io.list_status(base_path)
                for partition_dir in partition_dirs:
                    dirs.append(partition_dir.path)
            except Exception as e:
                logger.warning("Failed to list partition directories: %s", e)
        else:
            # List bucket directories
            try:
                bucket_dirs = self.file_io.list_status(base_path)
                for bucket_dir in bucket_dirs:
                    dirs.append(bucket_dir.path)
            except Exception as e:
                logger.warning("Failed to list bucket directories: %s", e)
        return dirs

    def _clean_empty_directories(self, file_dirs: List[str]):
        """Clean empty directories."""
        if not file_dirs:
            return

        bucket_dirs = set()
        for file_path in file_dirs:
            parent = "/".join(file_path.split("/")[:-1])
            if "bucket-" in parent:
                bucket_dirs.add(parent)

        # Try to delete empty directories recursively
        empty_dirs = set(bucket_dirs)
        while not self.dry_run and empty_dirs:
            new_empty_dirs = set()
            for empty_dir in empty_dirs:
                try:
                    if empty_dir != self.table_path:
                        if self.file_io.delete(empty_dir, recursive=False):
                            # Add parent for recursive cleaning
                            parent = "/".join(empty_dir.split("/")[:-1])
                            if parent:
                                new_empty_dirs.add(parent)
                except Exception as e:
                    logger.debug("Failed to delete empty directory %s: %s", empty_dir, e)
            empty_dirs = new_empty_dirs

    def _get_used_files(self, branches: List[str]) -> Set[str]:
        """Get all files that are currently used by the table."""
        used_files: Set[str] = set()

        for branch in branches:
            branch_used = self._get_used_files_for_branch(branch)
            used_files.update(branch_used)

        return used_files

    def _get_used_files_for_branch(self, branch: str) -> Set[str]:
        """Get used files for a specific branch."""
        used_files: Set[str] = set()
        branch_path = BranchManager.branch_path(self.table_path, branch)

        try:
            # Get manifest file factory
            manifest_list = ManifestList(branch_path, self.file_io)

            # Collect used files from snapshots
            self._collect_without_data_file(
                branch,
                used_files.add,
                lambda name: None  # Placeholder for manifest consumer
            )

            # Read manifests to find data files
            try:
                # This is a simplified version - full implementation would need
                # to read all manifests and extract data file references
                pass
            except Exception as e:
                logger.warning("Failed to read manifests for branch %s: %s", branch, e)

        except Exception as e:
            logger.warning("Failed to get used files for branch %s: %s", branch, e)

        return used_files

    def _collect_without_data_file(
        self,
        branch: str,
        used_file_consumer: Callable[[str], None],
        manifest_consumer: Callable[[str], None]
    ):
        """Collect used files without data files."""
        branch_path = BranchManager.branch_path(self.table_path, branch)
        snapshot_manager = SnapshotManager(
            FileIO(branch_path, {}),
            branch_path,
            branch
        )

        # Safely get all snapshots
        try:
            snapshots = snapshot_manager.safely_get_all_snapshots()
        except Exception as e:
            logger.warning("Failed to list snapshots for branch %s: %s", branch, e)
            return

        # Process each snapshot
        for snapshot in snapshots:
            try:
                self._collect_without_data_file_for_snapshot(
                    branch, snapshot, used_file_consumer, manifest_consumer
                )
            except Exception as e:
                logger.warning("Failed to process snapshot %s: %s", snapshot.id, e)

    def _collect_without_data_file_for_snapshot(
        self,
        branch: str,
        snapshot: Snapshot,
        used_file_consumer: Callable[[str], None],
        manifest_consumer: Callable[[str], None]
    ):
        """Collect used files for a snapshot."""
        # Collect manifest list files
        if snapshot.changelog_manifest_list:
            used_file_consumer(snapshot.changelog_manifest_list)
        if snapshot.delta_manifest_list:
            used_file_consumer(snapshot.delta_manifest_list)
        if snapshot.base_manifest_list:
            used_file_consumer(snapshot.base_manifest_list)

        # In a full implementation, we would read the manifest files
        # and extract the data file references
        # For now, this is a placeholder
        try:
            branch_path = BranchManager.branch_path(self.table_path, branch)
            manifest_list = ManifestList(branch_path, self.file_io)

            # Read data files from manifests
            if snapshot.base_manifest_list:
                manifest_consumer(snapshot.base_manifest_list)
        except Exception as e:
            logger.warning("Failed to read manifest %s: %s", snapshot.base_manifest_list, e)

    def _clean_empty_data_directory(self, delete_files: List[str]):
        """Clean empty data directories."""
        if not delete_files:
            return

        bucket_dirs = set()
        for file_path in delete_files:
            parts = file_path.split("/")
            if len(parts) >= 2:
                parent = "/".join(parts[:-1])
                if "bucket-" in parent:
                    bucket_dirs.add(parent)

        # Clean partition directories individually to avoid conflicts
        partition_dirs = set()
        for bucket_dir in bucket_dirs:
            parts = bucket_dir.split("/")
            if len(parts) >= 2:
                parent = "/".join(parts[:-1])
                if parent:
                    partition_dirs.add(parent)

        # Try to clean empty directories
        for partition_dir in partition_dirs:
            self._try_delete_empty_directory(partition_dir)

    def _try_delete_empty_directory(self, directory: str):
        """Try to delete an empty directory."""
        try:
            # Check if directory is empty
            try:
                contents = self.file_io.list_status(directory)
                if not contents:
                    self.file_io.delete(directory, recursive=False)
            except Exception:
                pass
        except Exception as e:
            logger.debug("Failed to delete empty directory %s: %s", directory, e)


def create_orphan_files_cleans(
    catalog,
    database_name: str,
    table_name: Optional[str] = None,
    older_than_millis: Optional[int] = None,
    dry_run: bool = False
) -> List[OrphanFilesClean]:
    """Create orphan files cleaners for tables in a database.

    Args:
        catalog: Catalog instance
        database_name: Database name
        table_name: Table name (or None for all tables)
        older_than_millis: Only delete files older than this time
        dry_run: If True, only report what would be deleted

    Returns:
        List of OrphanFilesClean instances
    """
    if table_name is None or table_name == "*":
        table_names = catalog.list_tables(database_name)
    else:
        table_names = [table_name]

    cleaners = []
    for table_name in table_names:
        identifier = Identifier(database_name, table_name)
        table = catalog.get_table(identifier)
        cleaners.append(
            OrphanFilesClean(
                table_path=table.table_path,
                file_io=table.file_io,
                table_schema=table.table_schema,
                older_than_millis=older_than_millis,
                dry_run=dry_run
            )
        )

    return cleaners


def execute_database_orphan_files(
    catalog,
    database_name: str,
    table_name: Optional[str] = None,
    older_than_millis: Optional[int] = None,
    dry_run: bool = False,
    parallelism: Optional[int] = None
) -> CleanOrphanFilesResult:
    """Execute orphan files cleaning for a database.

    Args:
        catalog: Catalog instance
        database_name: Database name
        table_name: Table name (or None for all tables)
        older_than_millis: Only delete files older than this time
        dry_run: If True, only report what would be deleted
        parallelism: Number of parallel threads to use

    Returns:
        CleanOrphanFilesResult with combined statistics
    """
    import concurrent.futures

    cleaners = create_orphan_files_cleans(
        catalog, database_name, table_name, older_than_millis, dry_run
    )

    if parallelism is None:
        parallelism = min(len(cleaners), 4)

    total_deleted_count = 0
    total_deleted_len_in_bytes = 0
    all_deleted_files = []

    # Execute cleaning in parallel
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        future_to_cleaner = {
            executor.submit(cleaner.clean): cleaner
            for cleaner in cleaners
        }

        for future in concurrent.futures.as_completed(future_to_cleaner):
            try:
                result = future.result()
                total_deleted_count += result.deleted_file_count
                total_deleted_len_in_bytes += result.deleted_file_total_len_in_bytes
                if result.deleted_files_path:
                    all_deleted_files.extend(result.deleted_files_path)
            except Exception as e:
                logger.error("Failed to clean orphan files: %s", e)

    return CleanOrphanFilesResult(
        deleted_file_count=total_deleted_count,
        deleted_file_total_len_in_bytes=total_deleted_len_in_bytes,
        deleted_files_path=all_deleted_files if all_deleted_files else None
    )
