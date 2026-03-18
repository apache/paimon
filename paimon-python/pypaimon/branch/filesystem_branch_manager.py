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
import os
from typing import List, Optional

from pypaimon.branch.branch_manager import BranchManager, BRANCH_PREFIX
from pypaimon.common.file_io import FileIO
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
        if not branch or branch == "main":
            return "main"
        return branch

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
        Create a copy of the manager for a different branch.

        Args:
            manager: The manager to copy (TagManager, SnapshotManager, etc.)
            branch: The target branch name

        Returns:
            A new manager instance for the specified branch
        """
        if isinstance(manager, TagManager):
            return TagManager(self.file_io, self.table_path, branch)
        elif isinstance(manager, SchemaManager):
            # SchemaManager doesn't support branch parameter, use branch path instead
            return SchemaManager(self.file_io, self.branch_path(branch))
        elif isinstance(manager, SnapshotManager):
            return SnapshotManager(self.snapshot_manager.table)
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
                self.file_io.copy_file(
                    self.snapshot_manager.get_snapshot_path(snapshot.id),
                    branch_snapshot_manager.get_snapshot_path(snapshot.id),
                    overwrite=True
                )
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
            ValueError: If branch doesn't exist
        """
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

            earliest_snapshot_id = branch_snapshot_manager.try_get_earliest_snapshot()
            if earliest_snapshot_id is None:
                raise RuntimeError(
                    f"Cannot fast forward branch {branch_name}, "
                    "because it does not have snapshot."
                )
            
            # Delete snapshot, schema, and tag from the main branch which occurs after
            # earliestSnapshotId
            # Note: This is a simplified version - full implementation would need
            # to properly handle these deletions
            
            # Copy files from branch to main
            self.file_io.copy_files(
                branch_snapshot_manager.snapshot_dir,
                self.snapshot_manager.snapshot_dir,
                overwrite=True
            )
            
            # Invalidate cache if applicable
            if hasattr(self.snapshot_manager, 'invalidate_cache'):
                self.snapshot_manager.invalidate_cache()
                
        except Exception as e:
            raise RuntimeError(
                f"Exception occurs when fast forward '{branch_name}' "
                f"(directory in {self.branch_path(branch_name)})."
            ) from e

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
