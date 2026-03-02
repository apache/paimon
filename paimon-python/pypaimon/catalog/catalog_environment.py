"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Optional

from pypaimon.catalog.catalog_loader import CatalogLoader
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.catalog_snapshot_commit import CatalogSnapshotCommit
from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit
from pypaimon.snapshot.snapshot_commit import SnapshotCommit


class CatalogEnvironment:

    def __init__(
            self,
            identifier: Optional[Identifier] = None,
            uuid: Optional[str] = None,
            catalog_loader: Optional[CatalogLoader] = None,
            supports_version_management: bool = False
    ):
        self.identifier = identifier
        self.uuid = uuid
        self.catalog_loader = catalog_loader
        self.supports_version_management = supports_version_management

    def snapshot_commit(self, snapshot_manager) -> Optional[SnapshotCommit]:
        """
        Create a SnapshotCommit instance based on the catalog environment configuration.

        Args:
            snapshot_manager: The SnapshotManager instance

        Returns:
            SnapshotCommit instance or None
        """
        if self.catalog_loader is not None and self.supports_version_management:
            # Use catalog-based snapshot commit when catalog loader is available
            # and version management is supported
            catalog = self.catalog_loader.load()
            return CatalogSnapshotCommit(catalog, self.identifier, self.uuid)
        else:
            # Use file renaming-based snapshot commit
            # In a full implementation, this would use a proper lock factory
            # to create locks based on the catalog lock context
            return RenamingSnapshotCommit(snapshot_manager)

    def copy(self, identifier: Identifier) -> 'CatalogEnvironment':
        """
        Create a copy of this CatalogEnvironment with a different identifier.

        Args:
            identifier: The new identifier

        Returns:
            A new CatalogEnvironment instance
        """
        return CatalogEnvironment(
            identifier=identifier,
            uuid=self.uuid,
            catalog_loader=self.catalog_loader,
            supports_version_management=self.supports_version_management
        )

    @staticmethod
    def empty() -> 'CatalogEnvironment':
        """
        Create an empty CatalogEnvironment with default values.

        Returns:
            An empty CatalogEnvironment instance
        """
        return CatalogEnvironment(
            identifier=None,
            uuid=None,
            catalog_loader=None,
            supports_version_management=False
        )
