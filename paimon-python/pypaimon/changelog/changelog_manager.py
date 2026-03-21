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
from typing import Iterator, List, Optional

from pypaimon.branch.branch_manager import BranchManager
from pypaimon.changelog.changelog import Changelog
from pypaimon.common.file_io import FileIO

logger = logging.getLogger(__name__)


class ChangelogManager:
    """Manager for Changelog, providing utility methods related to paths and changelog hints."""

    CHANGELOG_PREFIX = "changelog-"

    def __init__(self, file_io: FileIO, table_path: str, branch: Optional[str] = None):
        """Initialize ChangelogManager.

        Args:
            file_io: FileIO instance for file operations
            table_path: Path to the table directory
            branch: Branch name (defaults to 'main' if None)
        """
        self.file_io = file_io
        self.table_path = table_path
        self.branch = BranchManager.normalize_branch(branch) if branch else 'main'

    def file_io(self) -> FileIO:
        """Get the FileIO instance."""
        return self.file_io

    def latest_long_lived_changelog_id(self) -> Optional[int]:
        """Get the latest long-lived changelog ID.

        Returns:
            The latest changelog ID, or None if not found
        """
        try:
            return self._find_latest_hint_id(self.changelog_directory())
        except Exception as e:
            raise RuntimeError("Failed to find latest changelog id") from e

    def earliest_long_lived_changelog_id(self) -> Optional[int]:
        """Get the earliest long-lived changelog ID.

        Returns:
            The earliest changelog ID, or None if not found
        """
        try:
            return self._find_earliest_hint_id(self.changelog_directory())
        except Exception as e:
            raise RuntimeError("Failed to find earliest changelog id") from e

    def long_lived_changelog_exists(self, snapshot_id: int) -> bool:
        """Check if a long-lived changelog exists for the given snapshot ID.

        Args:
            snapshot_id: Snapshot ID to check

        Returns:
            True if changelog exists, False otherwise
        """
        path = self.long_lived_changelog_path(snapshot_id)
        try:
            return self.file_io.exists(path)
        except Exception as e:
            raise RuntimeError(f"Failed to determine if changelog #{snapshot_id} exists in path {path}") from e

    def long_lived_changelog(self, snapshot_id: int) -> Changelog:
        """Get a long-lived changelog for the given snapshot ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            Changelog instance
        """
        return Changelog.from_path(self.file_io, self.long_lived_changelog_path(snapshot_id))

    def changelog(self, snapshot_id: int) -> Changelog:
        """Get a changelog for the given snapshot ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            Changelog instance
        """
        changelog_path = self.long_lived_changelog_path(snapshot_id)
        return Changelog.from_path(self.file_io, changelog_path)

    def long_lived_changelog_path(self, snapshot_id: int) -> str:
        """Get the path to a long-lived changelog for the given snapshot ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            Path string to the changelog file
        """
        return f"{self._branch_path()}/changelog/{self.CHANGELOG_PREFIX}{snapshot_id}"

    def changelog_directory(self) -> str:
        """Get the changelog directory path.

        Returns:
            Path string to the changelog directory
        """
        return f"{self._branch_path()}/changelog"

    def commit_changelog(self, changelog: Changelog, changelog_id: int) -> None:
        """Commit a changelog to storage.

        Args:
            changelog: Changelog instance to commit
            changelog_id: Changelog ID
        """
        path = self.long_lived_changelog_path(changelog_id)
        self.file_io.write_file(path, changelog.to_json(), True)

    def commit_long_lived_changelog_latest_hint(self, snapshot_id: int) -> None:
        """Commit the latest hint for long-lived changelog.

        Args:
            snapshot_id: Latest snapshot ID
        """
        self._commit_latest_hint(snapshot_id, self.changelog_directory())

    def commit_long_lived_changelog_earliest_hint(self, snapshot_id: int) -> None:
        """Commit the earliest hint for long-lived changelog.

        Args:
            snapshot_id: Earliest snapshot ID
        """
        self._commit_earliest_hint(snapshot_id, self.changelog_directory())

    def try_get_changelog(self, snapshot_id: int) -> Optional[Changelog]:
        """Try to get a changelog for the given snapshot ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            Changelog instance if found, None otherwise

        Raises:
            FileNotFoundError: If changelog file is explicitly not found
        """
        changelog_path = self.long_lived_changelog_path(snapshot_id)
        return Changelog.try_from_path(self.file_io, changelog_path)

    def changelogs(self) -> Iterator[Changelog]:
        """Get an iterator over all changelogs, sorted by ID.

        Yields:
            Changelog instances in ascending order of ID
        """
        for snapshot_id in self._list_changelog_ids():
            yield self.changelog(snapshot_id)

    def safely_get_all_changelogs(self) -> List[Changelog]:
        """Safely get all changelogs, handling potential errors gracefully.

        This method reads changelogs in parallel and handles cases where
        files might be missing or corrupted.

        Returns:
            List of Changelog instances (may include None for failed reads)
        """
        paths = [self.long_lived_changelog_path(sid) for sid in self._list_changelog_ids()]
        changelogs: List[Optional[Changelog]] = [None] * len(paths)

        def read_changelog(index: int, path: str):
            """Read changelog from path, handling errors gracefully."""
            try:
                changelog_str = self.file_io.read_file_utf8(path)
                if not changelog_str or not changelog_str.strip():
                    logger.warning(f"Changelog file is empty, path: {path}")
                    return
                changelogs[index] = Changelog.from_json(changelog_str)
            except FileNotFoundError:
                # File not found is expected in some cases, ignore
                pass
            except Exception as e:
                raise RuntimeError(f"Failed to read changelog from path {path}") from e

        # Read changelogs in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(read_changelog, i, path): (i, path)
                for i, path in enumerate(paths)
            }
            for future in as_completed(futures):
                future.result()

        # Filter out None values and sort by ID
        valid_changelogs = [c for c in changelogs if c is not None]
        valid_changelogs.sort(key=lambda c: c.id)
        return valid_changelogs

    def delete_latest_hint(self) -> None:
        """Delete the latest hint file."""
        self._delete_latest_hint(self.changelog_directory())

    def delete_earliest_hint(self) -> None:
        """Delete the earliest hint file."""
        self._delete_earliest_hint(self.changelog_directory())

    def _branch_path(self) -> str:
        """Get the branch-specific path.

        Returns:
            Path string for the current branch
        """
        return BranchManager.branch_path(self.table_path, self.branch)

    def _list_changelog_ids(self) -> List[int]:
        """List all changelog IDs in the changelog directory.

        Returns:
            Sorted list of changelog IDs
        """
        pattern = re.compile(r'^changelog-(\d+)$')
        changelog_ids = []

        try:
            file_infos = self.file_io.list_status(self.changelog_directory())
            for file_info in file_infos:
                filename = file_info.path.split('/')[-1]
                match = pattern.match(filename)
                if match:
                    changelog_id = int(match.group(1))
                    changelog_ids.append(changelog_id)
        except Exception as e:
            logger.warning(f"Failed to list changelog files: {e}")
            return []

        return sorted(changelog_ids)

    def _find_latest_hint_id(self, directory: str) -> Optional[int]:
        """Find the latest snapshot ID from LATEST hint file.

        Args:
            directory: Directory containing the hint file

        Returns:
            Latest snapshot ID, or None if not found
        """
        latest_file = f"{directory}/LATEST"
        if not self.file_io.exists(latest_file):
            return None

        try:
            content = self.file_io.read_file_utf8(latest_file)
            if content and content.strip():
                return int(content.strip())
        except Exception as e:
            logger.warning(f"Failed to read latest hint from {latest_file}: {e}")

        return None

    def _find_earliest_hint_id(self, directory: str) -> Optional[int]:
        """Find the earliest snapshot ID from EARLIEST hint file.

        Args:
            directory: Directory containing the hint file

        Returns:
            Earliest snapshot ID, or None if not found
        """
        earliest_file = f"{directory}/EARLIEST"
        if not self.file_io.exists(earliest_file):
            return None

        try:
            content = self.file_io.read_file_utf8(earliest_file)
            if content and content.strip():
                return int(content.strip())
        except Exception as e:
            logger.warning(f"Failed to read earliest hint from {earliest_file}: {e}")

        return None

    def _commit_latest_hint(self, snapshot_id: int, directory: str) -> None:
        """Commit the latest hint file.

        Args:
            snapshot_id: Latest snapshot ID
            directory: Directory to write hint file
        """
        latest_file = f"{directory}/LATEST"
        self.file_io.write_file(latest_file, str(snapshot_id), False)

    def _commit_earliest_hint(self, snapshot_id: int, directory: str) -> None:
        """Commit the earliest hint file.

        Args:
            snapshot_id: Earliest snapshot ID
            directory: Directory to write hint file
        """
        earliest_file = f"{directory}/EARLIEST"
        self.file_io.write_file(earliest_file, str(snapshot_id), False)

    def _delete_latest_hint(self, directory: str) -> None:
        """Delete the latest hint file.

        Args:
            directory: Directory containing the hint file
        """
        latest_file = f"{directory}/LATEST"
        try:
            self.file_io.delete(latest_file)
        except FileNotFoundError:
            # File doesn't exist, that's fine
            pass
        except Exception as e:
            logger.warning(f"Failed to delete latest hint {latest_file}: {e}")

    def _delete_earliest_hint(self, directory: str) -> None:
        """Delete the earliest hint file.

        Args:
            directory: Directory containing the hint file
        """
        earliest_file = f"{directory}/EARLIEST"
        try:
            self.file_io.delete(earliest_file)
        except FileNotFoundError:
            # File doesn't exist, that's fine
            pass
        except Exception as e:
            logger.warning(f"Failed to delete earliest hint {earliest_file}: {e}")
