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
"""Snapshot loader for loading snapshots from a catalog."""

from typing import Optional


class SnapshotLoader:
    """Loader to load latest snapshot from a catalog.
    
    This loader uses a CatalogLoader to create catalog instances and
    load snapshots through the catalog's load_snapshot method.
    """

    def __init__(self, catalog_loader, identifier):
        """Initialize the loader with a catalog loader and table identifier.
        
        Args:
            catalog_loader: The CatalogLoader instance to create catalog
            identifier: The table identifier
        """
        self.catalog_loader = catalog_loader
        self.identifier = identifier

    def copy_with_branch(self, branch: str) -> 'SnapshotLoader':
        """Return a new loader whose identifier carries ``branch``.

        Mirrors Java ``SnapshotLoaderImpl.copyWithBranch``
        (tag/SnapshotLoaderImpl.java).
        """
        from pypaimon.common.identifier import Identifier
        rebranched = Identifier(
            database=self.identifier.get_database_name(),
            object=self.identifier.get_table_name(),
            branch=branch,
        )
        return SnapshotLoader(self.catalog_loader, rebranched)

    def load(self) -> Optional[str]:
        """Load the latest snapshot from the catalog.
        
        Returns:
            The latest snapshot JSON string, or None if not found
            
        Raises:
            RuntimeError: If there's an error loading the snapshot
        """
        try:
            catalog = self.catalog_loader.load()
            table_snapshot = catalog.load_snapshot(self.identifier)
            if table_snapshot is None:
                return None
            return table_snapshot.snapshot
        except RuntimeError as e:
            raise e
        except Exception as e:
            raise RuntimeError(f"Failed to load snapshot: {e}")
