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
    def list_databases(self) -> List[str]:
        """List all database names in the catalog."""

    @abstractmethod
    def get_database(self, name: str) -> 'Database':
        """Get paimon database identified by the given name."""

    @abstractmethod
    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        """Create a database with properties."""

    @abstractmethod
    def drop_database(self, name: str, ignore_if_not_exists: bool = False, cascade: bool = False):
        """Drop a database.

        Args:
            name: Name of the database to drop.
            ignore_if_not_exists: If True, do not raise error if database does not exist.
            cascade: If True, drop all tables in the database before dropping it.
        """

    @abstractmethod
    def list_tables(self, database_name: str) -> List[str]:
        """List all table names in the given database.

        Args:
            database_name: Name of the database.

        Returns:
            List of table names.
        """

    def alter_database(self, name: str, changes: list):
        """Alter database properties.

        Args:
            name: Name of the database.
            changes: List of PropertyChange objects.

        Raises:
            NotImplementedError: If the catalog does not support alter database.
        """
        raise NotImplementedError(
            "alter_database is not supported by this catalog."
        )

    @abstractmethod
    def get_table(self, identifier: Union[str, Identifier]) -> 'Table':
        """Get paimon table identified by the given Identifier."""

    @abstractmethod
    def create_table(self, identifier: Union[str, Identifier], schema: Schema, ignore_if_exists: bool):
        """Create table with schema."""

    @abstractmethod
    def drop_table(self, identifier: Union[str, Identifier], ignore_if_not_exists: bool = False):
        """Drop a table from the catalog.

        Args:
            identifier: Table identifier (string or Identifier instance)
            ignore_if_not_exists: If True, do not raise error if table does not exist

        Raises:
            TableNotExistException: If table does not exist and ignore_if_not_exists is False
        """

    def rename_table(self, source_identifier: Union[str, Identifier], target_identifier: Union[str, Identifier]):
        """Rename a table.

        Args:
            source_identifier: Current table identifier.
            target_identifier: New table identifier.

        Raises:
            NotImplementedError: If the catalog does not support rename table.
        """
        raise NotImplementedError(
            "rename_table is not supported by this catalog."
        )

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
    def load_snapshot(self, identifier: Identifier):
        """Load the snapshot of table identified by the given Identifier.

        Args:
            identifier: Path of the table

        Returns:
            TableSnapshot instance

        Raises:
            NotImplementedError: If the catalog does not support version management
            TableNotExistException: If the table does not exist
        """

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

    def rollback_to(self, identifier, instant, from_snapshot=None):
        """Rollback table by the given identifier and instant.

        Args:
            identifier: Path of the table (Identifier instance).
            instant: The Instant (SnapshotInstant or TagInstant) to rollback to.
            from_snapshot: Optional snapshot ID. Success only occurs when the
                latest snapshot is this snapshot.

        Raises:
            TableNotExistException: If the table does not exist.
            UnsupportedOperationError: If the catalog does not support version management.
        """
        raise NotImplementedError(
            "rollback_to is not supported by this catalog."
        )

    def drop_partitions(
            self,
            identifier: Union[str, Identifier],
            partitions: List[Dict[str, str]],
    ) -> None:
        raise NotImplementedError(
            "drop_partitions is not supported by this catalog. Use REST catalog for partition drop."
        )

    def list_partitions_paged(
            self,
            identifier: Union[str, Identifier],
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            partition_name_pattern: Optional[str] = None,
    ):
        """List partitions of a table with pagination.

        Args:
            identifier: Path of the table.
            max_results: Maximum number of results to return per page.
            page_token: Token for pagination.
            partition_name_pattern: Optional pattern to filter partition names.

        Returns:
            PagedList of Partition objects.

        Raises:
            NotImplementedError: If the catalog does not support listing partitions.
        """
        raise NotImplementedError(
            "list_partitions_paged is not supported by this catalog."
        )

    def create_branch(
            self,
            identifier: Identifier,
            branch_name: str,
            tag_name: Optional[str] = None
    ) -> None:
        """
        Create a branch for the table.

        Args:
            identifier: Table identifier
            branch_name: Name of the branch to create
            tag_name: Optional tag name to create branch from, None for current state

        Raises:
            NotImplementedError: If the catalog does not support branch management
        """
        raise NotImplementedError(
            "create_branch is not supported by this catalog."
        )

    def drop_branch(self, identifier: Identifier, branch_name: str) -> None:
        """
        Drop a branch for the table.

        Args:
            identifier: Table identifier
            branch_name: Name of the branch to drop

        Raises:
            NotImplementedError: If the catalog does not support branch management
        """
        raise NotImplementedError(
            "drop_branch is not supported by this catalog."
        )

    def fast_forward(self, identifier: Identifier, branch_name: str) -> None:
        """
        Fast forward the current branch to the specified branch.

        Args:
            identifier: Table identifier
            branch_name: The branch to fast forward to

        Raises:
            NotImplementedError: If the catalog does not support branch management
        """
        raise NotImplementedError(
            "fast_forward is not supported by this catalog."
        )

    def list_branches(self, identifier: Identifier) -> List[str]:
        """
        List all branches for the table.

        Args:
            identifier: Table identifier

        Returns:
            List of branch names

        Raises:
            NotImplementedError: If the catalog does not support branch management
        """
        raise NotImplementedError(
            "list_branches is not supported by this catalog."
        )

    def rename_branch(
            self,
            identifier: Identifier,
            from_branch: str,
            to_branch: str
    ) -> None:
        """
        Rename a branch for the table.

        Args:
            identifier: Table identifier
            from_branch: Existing branch name to rename
            to_branch: New branch name

        Raises:
            NotImplementedError: If the catalog does not support branch management
        """
        raise NotImplementedError(
            "rename_branch is not supported by this catalog."
        )

    def create_tag(
            self,
            identifier: Union[str, Identifier],
            tag_name: str,
            snapshot_id: Optional[int] = None,
            time_retained: Optional[str] = None,
            ignore_if_exists: bool = False,
    ) -> None:
        """Create a tag on a table.

        Args:
            identifier: Table identifier (Identifier or string).
            tag_name: Tag name to create.
            snapshot_id: Optional snapshot id; if not set the latest snapshot is tagged.
            time_retained: Optional time retained string (e.g., ``"1d"``, ``"12h"``, ``"30m"``).
            ignore_if_exists: If True, do not raise when the tag already exists.

        Raises:
            NotImplementedError: If the catalog does not support tag management.
        """
        raise NotImplementedError(
            "create_tag is not supported by this catalog."
        )

    def delete_tag(
            self,
            identifier: Union[str, Identifier],
            tag_name: str,
    ) -> None:
        """Delete a tag from a table.

        Args:
            identifier: Table identifier (Identifier or string).
            tag_name: Tag name to delete.

        Raises:
            NotImplementedError: If the catalog does not support tag management.
        """
        raise NotImplementedError(
            "delete_tag is not supported by this catalog."
        )

    def get_tag(
            self,
            identifier: Union[str, Identifier],
            tag_name: str,
    ):
        """Get a tag of a table.

        Args:
            identifier: Table identifier (Identifier or string).
            tag_name: Tag name to look up.

        Returns:
            GetTagResponse describing the tag.

        Raises:
            NotImplementedError: If the catalog does not support tag management.
        """
        raise NotImplementedError(
            "get_tag is not supported by this catalog."
        )

    def list_tags_paged(
            self,
            identifier: Union[str, Identifier],
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            tag_name_prefix: Optional[str] = None,
    ):
        """List tags of a table with pagination.

        Args:
            identifier: Table identifier (Identifier or string).
            max_results: Maximum number of results to return per page.
            page_token: Token for pagination.
            tag_name_prefix: Optional prefix filter for tag names.

        Returns:
            PagedList of tag names.

        Raises:
            NotImplementedError: If the catalog does not support tag management.
        """
        raise NotImplementedError(
            "list_tags_paged is not supported by this catalog."
        )
