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
from typing import Optional, Union

from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema import Schema


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
