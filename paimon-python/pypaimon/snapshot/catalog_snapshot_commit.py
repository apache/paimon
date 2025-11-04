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

from typing import List

from pypaimon.catalog.catalog import Catalog
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)


class CatalogSnapshotCommit(SnapshotCommit):
    """A SnapshotCommit using Catalog to commit."""

    def __init__(self, catalog: Catalog, identifier: Identifier, uuid: str):
        """
        Initialize CatalogSnapshotCommit.

        Args:
            catalog: The catalog instance to use for committing
            identifier: The table identifier
            uuid: Optional table UUID for verification
        """
        self.catalog = catalog
        self.identifier = identifier
        self.uuid = uuid

    def commit(self, snapshot: Snapshot, branch: str, statistics: List[PartitionStatistics]) -> bool:
        """
        Commit the snapshot using the catalog.

        Args:
            snapshot: The snapshot to commit
            branch: The branch name to commit to
            statistics: List of partition statistics

        Returns:
            True if commit was successful

        Raises:
            Exception: If commit fails
        """
        new_identifier = Identifier(
            database=self.identifier.get_database_name(),
            object=self.identifier.get_table_name(),
            branch=branch
        )

        # Call catalog's commit_snapshot method
        if hasattr(self.catalog, 'commit_snapshot'):
            return self.catalog.commit_snapshot(new_identifier, self.uuid, snapshot, statistics)
        else:
            # Fallback for catalogs that don't support snapshot commits
            raise NotImplementedError(
                "The catalog does not support snapshot commits. "
                "The commit_snapshot method needs to be implemented in the catalog interface."
            )

    def close(self):
        """Close the catalog and release resources."""
        if hasattr(self.catalog, 'close'):
            self.catalog.close()
