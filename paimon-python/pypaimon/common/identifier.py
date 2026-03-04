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
from dataclasses import dataclass
from typing import Optional

from pypaimon.common.json_util import json_field

SYSTEM_TABLE_SPLITTER = '$'
SYSTEM_BRANCH_PREFIX = 'branch-'


@dataclass
class Identifier:

    database: str = json_field("database", default=None)
    object: str = json_field("object", default=None)
    branch: Optional[str] = json_field("branch", default=None)

    @classmethod
    def create(cls, database: str, object: str) -> "Identifier":
        return cls(database, object)

    @classmethod
    def from_string(cls, full_name: str) -> "Identifier":
        """
        Parse an identifier from a string.

        Supports two modes:
        1. Backtick-quoted (for names with periods):
           - `database.name`.table → database="database.name", object="table"
           - `db`.`table` → database="db", object="table"

        2. Simple split on first period (Java-compatible fallback):
           - database.table → database="database", object="table"

        For database names containing periods, use backticks or
        Identifier.create("database.name", "table") directly.

        Args:
            full_name: The full identifier string

        Returns:
            Identifier instance

        Raises:
            ValueError: If the format is invalid
        """
        if not full_name or not full_name.strip():
            raise ValueError("fullName cannot be null or empty")

        # Check if backticks are used - if so, parse with backtick support
        if '`' in full_name:
            return cls._parse_with_backticks(full_name)

        # Otherwise, use Java-compatible split on first period only
        parts = full_name.split(".", 1)

        if len(parts) != 2:
            raise ValueError(
                f"Cannot get splits from '{full_name}' to get database and object"
            )

        return cls(parts[0], parts[1])

    @classmethod
    def _parse_with_backticks(cls, full_name: str) -> "Identifier":
        """
        Parse identifier with backtick-quoted segments.

        Examples:
        - `db.name`.table → database="db.name", object="table"
        - `db`.`table` → database="db", object="table"
        """
        parts = []
        current = ""
        in_backticks = False

        for char in full_name:
            if char == '`':
                in_backticks = not in_backticks
            elif char == '.' and not in_backticks:
                parts.append(current)
                current = ""
            else:
                current += char

        if current:
            parts.append(current)

        if in_backticks:
            raise ValueError(f"Unclosed backtick in identifier: {full_name}")

        if len(parts) != 2:
            raise ValueError(f"Invalid identifier format: {full_name}")

        return cls(parts[0], parts[1])

    def get_full_name(self) -> str:
        if self.branch:
            return "{}.{}.{}".format(self.database, self.object, self.branch)
        return "{}.{}".format(self.database, self.object)

    def get_database_name(self) -> str:
        return self.database

    def get_table_name(self) -> str:
        return self.object

    def get_object_name(self) -> str:
        return self.object

    def get_branch_name(self) -> Optional[str]:
        return self.branch

    def is_system_table(self) -> bool:
        return self.object.startswith('$')
