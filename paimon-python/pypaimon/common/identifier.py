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

    database_name: str = json_field("database", default=None)
    object_name: str = json_field("object", default=None)
    branch_name: Optional[str] = json_field("branch", default=None)

    @classmethod
    def create(cls, database_name: str, object_name: str) -> "Identifier":
        return cls(database_name, object_name)

    @classmethod
    def from_string(cls, full_name: str) -> "Identifier":
        parts = full_name.split(".")
        if len(parts) == 2:
            return cls(parts[0], parts[1])
        elif len(parts) == 3:
            return cls(parts[0], parts[1], parts[2])
        else:
            raise ValueError("Invalid identifier format: {}".format(full_name))

    def get_full_name(self) -> str:
        if self.branch_name:
            return "{}.{}.{}".format(self.database_name, self.object_name, self.branch_name)
        return "{}.{}".format(self.database_name, self.object_name)

    def get_database_name(self) -> str:
        return self.database_name

    def get_table_name(self) -> str:
        return self.object_name

    def get_object_name(self) -> str:
        return self.object_name

    def get_branch_name(self) -> Optional[str]:
        return self.branch_name

    def is_system_table(self) -> bool:
        return self.object_name.startswith('$')
