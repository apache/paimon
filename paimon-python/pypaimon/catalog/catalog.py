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
#################################################################################

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics


class Catalog(ABC):
    """
    This interface is responsible for reading and writing
    metadata such as database/table from a paimon catalog.
    """
    DB_SUFFIX = ".db"
    DEFAULT_DATABASE = "default"
    SYSTEM_DATABASE_NAME = "sys"

    DB_LOCATION_PROP = "location"
    COMMENT_PROP = "comment"
    OWNER_PROP = "owner"

    @abstractmethod
    def get_database(self, name: str) -> 'Database':
        """Get paimon database identified by the given name."""

    @abstractmethod
    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        """Create a database with properties."""

    @abstractmethod
    def get_table(self, identifier: Union[str, Identifier]) -> 'Table':
        """Get paimon table identified by the given Identifier."""

    @abstractmethod
    def create_table(self, identifier: Union[str, Identifier], schema: Schema, ignore_if_exists: bool):
        """Create table with schema."""

    @abstractmethod
    def alter_table(
        self,
        identifier: Union[str, Identifier],
        changes: List[SchemaChange],
        ignore_if_not_exists: bool = False
    ):
        """Alter table with schema changes."""

    def supports_version_management(self) -> bool:
        """
        Whether this catalog supports version management for tables.

        Returns:
            True if the catalog supports version management, False otherwise
        """
        return False

    @abstractmethod
    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        """
        Commit the Snapshot for table identified by the given Identifier.

        Args:
            identifier: Path of the table
            table_uuid: UUID of the table to avoid wrong commit
            snapshot: Snapshot to be committed
            statistics: Statistics information of this change

        Returns:
            True if commit was successful, False otherwise

        """

    def drop_partitions(
        self,
        identifier: Union[str, Identifier],
        partitions: List[Dict[str, str]],
    ) -> None:
        raise NotImplementedError(
            "drop_partitions is not supported by this catalog. Use REST catalog for partition drop."
        )
