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

from abc import ABC, abstractmethod


class TableRollback(ABC):
    """Rollback table to instant from snapshot."""

    @abstractmethod
    def rollback_to(self, instant, from_snapshot=None):
        """Rollback table to the given instant.

        Args:
            instant: The Instant (SnapshotInstant or TagInstant) to rollback to.
            from_snapshot: Optional snapshot ID. Success only occurs when the
                latest snapshot is this snapshot.
        """


class CatalogTableRollback(TableRollback):
    """Internal TableRollback implementation that delegates to catalog.rollback_to."""

    def __init__(self, catalog, identifier):
        self._catalog = catalog
        self._identifier = identifier

    def rollback_to(self, instant, from_snapshot=None):
        """Rollback table to the given instant via catalog.

        Args:
            instant: The Instant (SnapshotInstant or TagInstant) to rollback to.
            from_snapshot: Optional snapshot ID. Success only occurs when the
                latest snapshot is this snapshot.

        Raises:
            RuntimeError: If the table does not exist in the catalog.
        """
        try:
            self._catalog.rollback_to(self._identifier, instant, from_snapshot)
        except Exception as e:
            raise RuntimeError(
                "Failed to rollback table {}: {}".format(
                    self._identifier, e)) from e
