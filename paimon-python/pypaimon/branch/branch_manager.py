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
from typing import List, Optional

from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH

logger = logging.getLogger(__name__)

BRANCH_PREFIX = "branch-"


class BranchManager:
    """
    Manager for Branch.
    
    This is a base class for managing table branches in Paimon.
    Branches allow multiple lines of development on the same table.
    """

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
            ignore_if_exists: If true, do nothing when branch already exists;
                            if false, throw exception

        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement create_branch")

    def drop_branch(self, branch_name: str) -> None:
        """
        Drop a branch.
        
        Args:
            branch_name: Name of the branch to drop
        
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement drop_branch")

    def rename_branch(self, from_branch: str, to_branch: str) -> None:
        """
        Rename a branch.
        
        Args:
            from_branch: Current name of the branch
            to_branch: New name for the branch
        
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement rename_branch")

    def fast_forward(self, branch_name: str) -> None:
        """
        Fast forward the current branch to the specified branch.
        
        Args:
            branch_name: The branch to fast forward to
        
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement fast_forward")

    def branches(self) -> List[str]:
        """
        List all branches.
        
        Returns:
            List of branch names
        
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement branches")

    def branch_exists(self, branch_name: str) -> bool:
        """
        Check if a branch exists.
        
        Args:
            branch_name: Name of the branch to check
        
        Returns:
            True if branch exists, False otherwise
        """
        return branch_name in self.branches()

    @staticmethod
    def branch_path(table_path: str, branch: str) -> str:
        """
        Return the path string of a branch.
        
        Args:
            table_path: The table path
            branch: The branch name
        
        Returns:
            The path to the branch
        """
        if BranchManager.is_main_branch(branch):
            return table_path
        return f"{table_path}/branch/{BRANCH_PREFIX}{branch}"

    @staticmethod
    def normalize_branch(branch: str) -> str:
        """
        Normalize branch name.
        
        Args:
            branch: The branch name to normalize
        
        Returns:
            The normalized branch name
        """
        if not branch or not branch.strip():
            return DEFAULT_MAIN_BRANCH
        return branch.strip()

    @staticmethod
    def is_main_branch(branch: str) -> bool:
        """
        Check if the branch is the main branch.
        
        Args:
            branch: The branch name to check
        
        Returns:
            True if the branch is the main branch, False otherwise
        """
        return branch == DEFAULT_MAIN_BRANCH

    @staticmethod
    def validate_branch(branch_name: str) -> None:
        """
        Validate branch name.
        
        Args:
            branch_name: The branch name to validate
        
        Raises:
            ValueError: If branch name is invalid
        """
        if BranchManager.is_main_branch(branch_name):
            raise ValueError(
                f"Branch name '{branch_name}' is the default branch and cannot be used."
            )
        if not branch_name or not branch_name.strip():
            raise ValueError("Branch name is blank.")
        if branch_name.strip().isdigit():
            raise ValueError(
                f"Branch name cannot be pure numeric string but is '{branch_name}'."
            )

    @staticmethod
    def fast_forward_validate(branch_name: str, current_branch: str) -> None:
        """
        Validate fast-forward parameters.
        
        Args:
            branch_name: The branch to fast forward to
            current_branch: The current branch name
        
        Raises:
            ValueError: If parameters are invalid
        """
        if branch_name == DEFAULT_MAIN_BRANCH:
            raise ValueError(
                f"Branch name '{branch_name}' do not use in fast-forward."
            )
        if not branch_name or not branch_name.strip():
            raise ValueError("Branch name is blank.")
        if branch_name == current_branch:
            raise ValueError(
                f"Fast-forward from the current branch '{current_branch}' is not allowed."
            )
