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
from typing import Optional

from pypaimon.api.typedef import RESTCatalogOptions


class ResourcePaths:
    """Resource path constants"""
    V1 = "v1"
    DATABASES = "databases"
    TABLES = "tables"
    TABLE_DETAILS = "table-details"
    VIEWS = "views"
    FUNCTIONS = "functions"
    SNAPSHOTS = "snapshots"
    ROLLBACK = "rollback"

    def __init__(self, prefix: str = ""):
        self.prefix = prefix.rstrip('/')
        self.base_path = f"/{self.V1}/{self.prefix}"

    @classmethod
    def for_catalog_properties(
            cls, options: dict[str, str]) -> "ResourcePaths":
        return cls(options.get(RESTCatalogOptions.PREFIX, ""))

    def config(self) -> str:
        return f"/{self.V1}/config"

    def databases(self) -> str:
        return f"{self.base_path}/databases"

    def database(self, name: str) -> str:
        return f"{self.base_path}/{self.DATABASES}/{name}"

    def tables(self, database_name: Optional[str] = None) -> str:
        if database_name:
            return f"{self.base_path}/{self.DATABASES}/{database_name}/{self.TABLES}"
        return f"{self.base_path}/{self.TABLES}"

    def table(self, database_name: str, table_name: str) -> str:
        return f"{self.base_path}/{self.DATABASES}/{database_name}/{self.TABLES}/{table_name}"

    def table_details(self, database_name: str) -> str:
        return f"{self.base_path}/{self.DATABASES}/{database_name}/{self.TABLE_DETAILS}"
