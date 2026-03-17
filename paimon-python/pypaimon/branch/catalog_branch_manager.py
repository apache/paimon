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

from typing import List, Optional

from pypaimon.branch.branch_manager import BranchManager
from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_exception import (
    BranchAlreadyExistException,
    BranchNotExistException,
    TagNotExistException
)
from pypaimon.catalog.catalog_loader import CatalogLoader
from pypaimon.common.identifier import Identifier


class CatalogBranchManager(BranchManager):
    """
    Branch manager for managing branches via catalog.

    This implementation delegates branch operations to the underlying catalog,
    which handles branch storage and management at the catalog level.
    This is useful for catalog-based table access (e.g., REST catalog).
    """

    def __init__(self, catalog_loader: CatalogLoader, identifier: Identifier):
        """Initialize CatalogBranchManager.

        Args:
            catalog_loader: CatalogLoader instance to load catalog
            identifier: Identifier for the table
        """
        self.catalog_loader = catalog_loader
        self.identifier = identifier

    def _execute_post(self, func) -> None:
        """Execute a function that doesn't return a value with the catalog.

        Args:
            func: Function that takes a Catalog as parameter

        Raises:
            ValueError: If branch or tag operations fail
        """
        catalog = self.catalog_loader.load()
        try:
            func(catalog)
        except BranchNotExistException as e:
            raise ValueError(f"Branch name '{e.branch}' doesn't exist.")
        except TagNotExistException as e:
            raise ValueError(f"Tag '{e.tag}' doesn't exist.")
        except BranchAlreadyExistException as e:
            raise ValueError(f"Branch name '{e.branch}' already exists.")

    def _execute_get(self, func):
        """Execute a function that returns a value with the catalog.

        Args:
            func: Function that takes a Catalog as parameter and returns a value

        Returns:
            The value returned by the function
        """
        catalog = self.catalog_loader.load()
        return func(catalog)

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

        Raises:
            ValueError: If branch name is invalid or branch already exists (and ignore_if_exists=False)
        """
        def _create(catalog: Catalog):
            BranchManager.validate_branch(branch_name)
            existing_branches = catalog.list_branches(self.identifier)
            if branch_name in existing_branches:
                if not ignore_if_exists:
                    raise ValueError(f"Branch name '{branch_name}' already exists.")
                return
            catalog.create_branch(self.identifier, branch_name, tag_name)

        self._execute_post(_create)

    def drop_branch(self, branch_name: str) -> None:
        """
        Drop a branch.

        Args:
            branch_name: Name of the branch to drop

        Raises:
            ValueError: If branch doesn't exist
        """
        def _drop(catalog: Catalog):
            catalog.drop_branch(self.identifier, branch_name)

        self._execute_post(_drop)

    def fast_forward(self, branch_name: str) -> None:
        """
        Fast forward the current branch to the specified branch.

        Args:
            branch_name: The branch to fast forward to

        Raises:
            ValueError: If fast-forward parameters are invalid
        """
        def _fast_forward(catalog: Catalog):
            current_branch = self.identifier.get_branch_name_or_default()
            BranchManager.fast_forward_validate(branch_name, current_branch)
            catalog.fast_forward(self.identifier, branch_name)

        self._execute_post(_fast_forward)

    def branches(self) -> List[str]:
        """
        List all branches.

        Returns:
            List of branch names
        """
        def _list(catalog: Catalog):
            return catalog.list_branches(self.identifier)

        return self._execute_get(_list)
