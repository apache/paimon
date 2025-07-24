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
from typing import Optional

from pypaimon.api.schema import Schema
from pypaimon.pynative.table.table import Table


class Catalog(ABC):
    """
    This interface is responsible for reading and writing
    metadata such as database/table from a paimon catalog.
    """
    SYSTEM_DATABASE_NAME = "sys"

    @abstractmethod
    def get_database(self, name: str) -> 'Database':
        """Get paimon database identified by the given name."""

    @abstractmethod
    def get_table(self, identifier: str) -> Table:
        """Get paimon table identified by the given Identifier."""

    @abstractmethod
    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        """Create a database with properties."""

    @abstractmethod
    def create_table(self, identifier: str, schema: Schema, ignore_if_exists: bool):
        """Create table."""
