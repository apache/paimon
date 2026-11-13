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

import logging
import os
import re
from typing import List, Optional

from pypaimon.branch.branch_manager import BranchManager, BRANCH_PREFIX
from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.filesystem.caching_file_io import CachingFileIO
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.tag.tag_manager import TagManager

logger = logging.getLogger(__name__)


class FileSystemBranchManager(BranchManager):
    """A BranchManager implementation to manage branches via file system."""

    def __init__(
        self,
        file_io: FileIO,
        table_path: str,
        snapshot_manager: SnapshotManager,
        tag_manager: TagManager,
        schema_manager: SchemaManager,
        current_branch: str = "main"
    ):
        """
        Initialize FileSystemBranchManager.
        
        Args:
            file_io: FileIO instance for file operations
            table_path: Path to the table root directory
            snapshot_manager: SnapshotManager instance
            tag_manager: TagManager instance
            schema_manager: SchemaManager instance
            current_branch: Current branch name
        """
        self.file_io = file_io
        self.table_path = table_path.rstrip('/')
        self.snapshot_manager = snapshot_manager
        self.tag_manager = tag_manager
        self.schema_manager = schema_manager
        self.current_branch = self._normalize_branch(current_branch)

    @staticmethod
    def _normalize_branch(branch: str) -> str:
        """Normalize branch name."""
        return BranchManager.normalize_branch(branch)

    def _branch_directory(self) -> str:
        """Return the root directory of branch."""
        return f"{self.table_path}/branch"

    def branch_path(self, branch_name: str) -> str:
        """
        Return the path of a branch.
        
        Args:
            branch_name: Name of the branch
        
        Returns:
            The path to the branch
        """
        return BranchManager.branch_path(self.table_path, branch_name)

    def _copy_with_branch(self, manager, branch: str):
        """
        Return a new manager whose path accessors land under
        ``{table_path}/branch/branch-{name}``.

        Equivalent to the per-manager rebranch calls inlined in Java
        ``FileSystemBranchManager.createBranch(name, tagName)`` (e.g.
        ``snapshotManager.copyWithBranch(branchName)``). ``TagManager``
        is rebuilt directly because the Python ``TagManager`` has no
        ``copy_with_branch`` factory yet.
        """
        if isinstance(manager, TagManager):
            return TagManager(self.file_io, self.table_path, branch)
        elif isinstance(manager, SchemaManager):
            return manager.copy_with_branch(branch)
        elif isinstance(manager, SnapshotManager):
            return manager.copy_with_branch(branch)
        else:
            return manager

    def _file_exists(self, path: str) -> bool:
        """
        Check if a file or directory exists.
        
        Args:
            path: The path to check
        
        Returns:
            True if path exists, False otherwise
        """
        return self.file_io.exists(path)

    def create_branch(
        self,
        branch_name: str,
        tag_name: Optional[str] = None,
        ignore_if_exists: bool = False
    ) -> None:
        """
        Create a branch from the current state or from a tag.

        A retained tag can be used even after its snapshot file expires.

        Args:
            branch_name: Name of the branch to create
            tag_name: Optional tag name to create branch from, None for current state
            ignore_if_exists: If true, do nothing when branch already exists
        """
        if ignore_if_exists and self.branch_exists(branch_name):
            return

        self._validate_branch(branch_name)

        if tag_name is None:
            # Create branch from current state
            try:
                latest_schema = self.schema_manager.latest()
                if latest_schema:
                    self._copy_schemas_to_branch(branch_name, latest_schema.id)
                else:
                    raise RuntimeError("No schema found for creating branch")
            except Exception as e:
                raise RuntimeError(
                    f"Exception occurs when create branch '{branch_name}' "
                    f"(directory in {self.branch_path(branch_name)})."
                ) from e
        else:
            # Create branch from tag
            tag = self.tag_manager.get_or_throw(tag_name)
            snapshot = tag.trim_to_snapshot()

            try:
                branch_tag_manager = TagManager(self.file_io, self.table_path, branch_name)
                branch_snapshot_manager = self._copy_with_branch(
                    self.snapshot_manager, branch_name
                )

                # Copy the corresponding tag, snapshot and schema files into the branch directory
                self.file_io.copy_file(
                    self.tag_manager.tag_path(tag_name),
                    branch_tag_manager.tag_path(tag_name),
                    overwrite=True
                )
                snapshot_path = self.snapshot_manager.get_snapshot_path(snapshot.id)
                branch_snapshot_path = branch_snapshot_manager.get_snapshot_path(snapshot.id)
                if self.file_io.exists(snapshot_path):
                    self.file_io.copy_file(snapshot_path, branch_snapshot_path, overwrite=True)
                else:
                    # The tag retains the snapshot metadata after the original file expires.
                    self.file_io.write_file(branch_snapshot_path, JSON.to_json(snapshot), overwrite=True)
                self._copy_schemas_to_branch(branch_name, snapshot.schema_id)
            except Exception as e:
                raise RuntimeError(
                    f"Exception occurs when create branch '{branch_name}' "
                    f"(directory in {self.branch_path(branch_name)})."
                ) from e

    def drop_branch(self, branch_name: str) -> None:
        """
        Drop a branch.

        Args:
            branch_name: Name of the branch to drop

        Raises:
            ValueError: If branch is main or doesn't exist
        """
        if BranchManager.is_main_branch(branch_name):
            raise ValueError("The main branch cannot be deleted.")
        if not self.branch_exists(branch_name):
            raise ValueError(f"Branch name '{branch_name}' doesn't exist.")

        try:
            # Delete branch directory recursively
            branch_path = self.branch_path(branch_name)
            self.file_io.delete(branch_path, recursive=True)
        except Exception as e:
            logger.warning(
                f"Deleting the branch failed due to an exception in deleting the directory "
                f"{self.branch_path(branch_name)}. Please try again."
            )
            raise RuntimeError(f"Failed to delete branch '{branch_name}'") from e

    def rename_branch(self, from_branch: str, to_branch: str) -> None:
        """
        Rename a branch.

        Args:
            from_branch: Current name of the branch
            to_branch: New name for the branch

        Raises:
            ValueError: If from_branch or to_branch is blank, from_branch doesn't exist,
                       to_branch already exists, or trying to rename the main branch
            RuntimeError: If the rename operation fails
        """
        # Check if from_branch is main branch
        if self.is_main_branch(from_branch):
            raise ValueError(f"Cannot rename the main branch '{from_branch}'.")

        # Check if from_branch is blank
        if not from_branch or from_branch.isspace():
            raise ValueError("Source branch name shouldn't be blank.")

        # Check if to_branch is blank
        if not to_branch or to_branch.isspace():
            raise ValueError("Target branch name shouldn't be blank.")

        # Validate the new branch name
        try:
            self.validate_branch(to_branch)
        except ValueError as e:
            raise ValueError(f"Invalid target branch name: {e}")

        # Check if from_branch exists
        if not self.branch_exists(from_branch):
            raise ValueError(f"Source branch '{from_branch}' doesn't exist.")

        # Check if to_branch already exists
        if self.branch_exists(to_branch):
            raise ValueError(f"Target branch '{to_branch}' already exists.")

        try:
            # Rename the branch directory
            from_path = self.branch_path(from_branch)
            to_path = self.branch_path(to_branch)
            self.file_io.rename(from_path, to_path)
            logger.info(f"Successfully renamed branch from '{from_branch}' to '{to_branch}'")
        except Exception as e:
            logger.warning(
                f"Renaming branch from '{from_branch}' to '{to_branch}' failed due to an exception. "
                f"Please try again."
            )
            raise RuntimeError(
                f"Failed to rename branch from '{from_branch}' to '{to_branch}'"
            ) from e

    def fast_forward(self, branch_name: str) -> None:
        """
        Fast forward the current branch to the specified branch.
        
        Args:
            branch_name: The branch to fast forward to
        
        Raises:
            ValueError: If fast-forward parameters are invalid
            RuntimeError: If fast-forward operation fails
        """
        BranchManager.fast_forward_validate(branch_name, self.current_branch)
        
        if not self.branch_exists(branch_name):
            raise ValueError(f"Branch name '{branch_name}' doesn't exist.")
        
        try:
            branch_snapshot_manager = self._copy_with_branch(
                self.snapshot_manager, branch_name
            )

            earliest_snapshot = branch_snapshot_manager.try_get_earliest_snapshot()
            if earliest_snapshot is None:
                raise RuntimeError(
                    f"Cannot fast forward branch {branch_name}, "
                    "because it does not have snapshot."
                )
            
            branch_schema_manager = self.schema_manager.copy_with_branch(branch_name)
            branch_tag_manager = self._copy_with_branch(self.tag_manager, branch_name)
            branch_snapshots = self._snapshot_paths(branch_snapshot_manager)
            main_snapshots = self._snapshot_paths(self.snapshot_manager)
            branch_schema_ids = set(branch_schema_manager._list_versioned_files())
            if (earliest_snapshot.id not in branch_snapshots
                    or earliest_snapshot.schema_id not in branch_schema_ids):
                raise RuntimeError(f"Incomplete snapshot or schema metadata in branch '{branch_name}'.")
            branch_tag_directory = branch_tag_manager.tag_directory()
            branch_tags = (set(branch_tag_manager.list_tags())
                           if self.file_io.exists(branch_tag_directory) else set())

            # Replace metadata from the branch's earliest retained snapshot onward.
            # Files with the same name are overwritten; only main-only files need deletion.
            # Inspect snapshot names, not their contents, so a partially copied snapshot
            # from a failed attempt can be overwritten on retry.
            delete_paths = [
                path for snapshot_id, path in main_snapshots.items()
                if snapshot_id >= earliest_snapshot.id and snapshot_id not in branch_snapshots
            ]
            delete_paths.extend(
                self.schema_manager._to_schema_path(schema_id)
                for schema_id in self.schema_manager._list_versioned_files()
                if schema_id >= earliest_snapshot.schema_id and schema_id not in branch_schema_ids
            )
            if self.file_io.exists(self.tag_manager.tag_directory()):
                delete_paths.extend(
                    self.tag_manager.tag_path(tag_name)
                    for tag_name in self.tag_manager.list_tags()
                    if tag_name not in branch_tags
                    and self.tag_manager.get_or_throw(tag_name).id >= earliest_snapshot.id
                )

            if isinstance(self.file_io, CachingFileIO):
                # Clear replaced and removed paths, including IDs a later commit may reuse.
                # Fail before changing metadata if persistent cache entries cannot be removed.
                invalidate_paths = set(delete_paths)
                invalidate_paths.update(self.schema_manager._to_schema_path(schema_id)
                                        for schema_id in branch_schema_ids)
                invalidate_paths.update(self.snapshot_manager.get_snapshot_path(snapshot_id)
                                        for snapshot_id in branch_snapshots)
                self.file_io.invalidate_cache(invalidate_paths)

            # Install schemas before snapshots which may reference them.
            # This operation is not a multi-file transaction.
            self.schema_manager.schema_cache.clear()
            self.file_io.copy_files(
                branch_schema_manager.schema_path,
                self.schema_manager.schema_path,
                overwrite=True
            )
            if branch_tags:
                self.file_io.copy_files(
                    branch_tag_directory, self.tag_manager.tag_directory(), overwrite=True)
            for snapshot_id, path in sorted(branch_snapshots.items()):
                self.file_io.copy_file(
                    path, self.snapshot_manager.get_snapshot_path(snapshot_id), overwrite=True)

            for path in delete_paths:
                if not self.file_io.delete(path) and self.file_io.exists(path):
                    raise IOError(f"Failed to delete metadata file {path}")

            # Branch hints do not describe the older history retained on main.
            retained_ids = [snapshot_id for snapshot_id in main_snapshots
                            if snapshot_id < earliest_snapshot.id]
            self.file_io.overwrite_file_utf8(
                f"{self.snapshot_manager.snapshot_dir}/EARLIEST",
                str(min(retained_ids + list(branch_snapshots))))
            self.file_io.overwrite_file_utf8(
                self.snapshot_manager.latest_file, str(max(branch_snapshots)))
            
            # Invalidate cache if applicable
            if hasattr(self.snapshot_manager, 'invalidate_cache'):
                self.snapshot_manager.invalidate_cache()
                
        except Exception as e:
            raise RuntimeError(
                f"Exception occurs when fast forward '{branch_name}' "
                f"(directory in {self.branch_path(branch_name)})."
            ) from e

    def _snapshot_paths(self, manager: SnapshotManager):
        paths = {}
        if self.file_io.exists(manager.snapshot_dir):
            for status in self.file_io.list_status(manager.snapshot_dir):
                match = re.fullmatch(r"snapshot-(\d+)", status.path.rsplit('/', 1)[-1])
                if match:
                    paths[int(match.group(1))] = status.path
        return paths

    def branch_exists(self, branch_name: str) -> bool:
        """
        Check if a branch exists.
        
        Args:
            branch_name: Name of the branch to check
        
        Returns:
            True if branch exists, False otherwise
        """
        branch_path = self.branch_path(branch_name)
        return self._file_exists(branch_path)

    def _validate_branch(self, branch_name: str) -> None:
        """
        Validate branch name and check if it already exists.
        
        Args:
            branch_name: The branch name to validate
        
        Raises:
            ValueError: If branch name is invalid or already exists
        """
        BranchManager.validate_branch(branch_name)
        if self.branch_exists(branch_name):
            raise ValueError(f"Branch name '{branch_name}' already exists.")

    def branches(self) -> List[str]:
        """
        List all branches.

        Returns:
            List of branch names
        """
        branch_dir = self._branch_directory()
        result = []

        try:
            if not self._file_exists(branch_dir):
                return result

            file_infos = self.file_io.list_status(branch_dir)
            if file_infos is None:
                return result

            for file_info in file_infos:
                # Get directory name
                dir_name = None
                if hasattr(file_info, 'base_name'):
                    dir_name = file_info.base_name
                else:
                    try:
                        dir_name = os.path.basename(file_info.path)
                    except (AttributeError, TypeError):
                        dir_name = None

                if dir_name and dir_name.startswith(BRANCH_PREFIX):
                    branch_name = dir_name[len(BRANCH_PREFIX):]
                    result.append(branch_name)

        except Exception as e:
            raise RuntimeError(f"Failed to list branches: {e}") from e

        return result

    def _copy_schemas_to_branch(self, branch_name: str, schema_id: int) -> None:
        """
        Copy schemas to the specified branch.

        Args:
            branch_name: The target branch name
            schema_id: The maximum schema id to copy
        """
        branch_schema_manager = SchemaManager(self.file_io, self.branch_path(branch_name))

        for i in range(schema_id + 1):
            schema = self.schema_manager.get_schema(i)
            if schema is not None:
                schema_path = self.schema_manager._to_schema_path(i)
                branch_schema_path = branch_schema_manager._to_schema_path(i)
                self.file_io.copy_file(schema_path, branch_schema_path, overwrite=True)
