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

from dataclasses import dataclass
from typing import Optional, TypeVar, Dict

T = TypeVar("T")


@dataclass
class Identifier:
    """Table/View/Function identifier"""

    database_name: str
    object_name: str
    branch_name: Optional[str] = None

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
            raise ValueError(f"Invalid identifier format: {full_name}")

    def get_full_name(self) -> str:
        if self.branch_name:
            return f"{self.database_name}.{self.object_name}.{self.branch_name}"
        return f"{self.database_name}.{self.object_name}"

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


@dataclass
class RESTAuthParameter:
    method: str
    path: str
    data: str
    parameters: Dict[str, str]


class RESTCatalogOptions:
    URI = "uri"
    WAREHOUSE = "warehouse"
    TOKEN_PROVIDER = "token.provider"
    DLF_REGION = "dlf.region"
    DLF_ACCESS_KEY_ID = "dlf.access-key-id"
    DLF_ACCESS_KEY_SECRET = "dlf.access-key-secret"
    DLF_ACCESS_SECURITY_TOKEN = "dlf.security-token"
    DLF_TOKEN_LOADER = "dlf.token-loader"
    DLF_TOKEN_ECS_ROLE_NAME = "dlf.token-ecs-role-name"
    DLF_TOKEN_ECS_METADATA_URL = "dlf.token-ecs-metadata-url"
    PREFIX = 'prefix'
