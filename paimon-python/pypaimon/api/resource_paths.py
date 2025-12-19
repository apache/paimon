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

from typing import Optional

from pypaimon.api.rest_util import RESTUtil
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions


class ResourcePaths:
    V1 = "v1"
    DATABASES = "databases"
    TABLES = "tables"
    TABLE_DETAILS = "table-details"

    def __init__(self, prefix: str):
        self.base_path = "/{}/{}".format(self.V1, prefix).rstrip("/")

    @classmethod
    def for_catalog_properties(
            cls, options: Options) -> "ResourcePaths":
        prefix = options.get(CatalogOptions.PREFIX, "")
        return cls(prefix)

    @staticmethod
    def config() -> str:
        return "/{}/config".format(ResourcePaths.V1)

    def databases(self) -> str:
        return "{}/{}".format(self.base_path, self.DATABASES)

    def database(self, name: str) -> str:
        return "{}/{}/{}".format(self.base_path, self.DATABASES, RESTUtil.encode_string(name))

    def tables(self, database_name: Optional[str] = None) -> str:
        if database_name:
            return "{}/{}/{}/{}".format(self.base_path, self.DATABASES,
                                        RESTUtil.encode_string(database_name), self.TABLES)
        return "{}/{}".format(self.base_path, self.TABLES)

    def table(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                self.TABLES, RESTUtil.encode_string(table_name)))

    def table_details(self, database_name: str) -> str:
        return "{}/{}/{}/{}".format(self.base_path, self.DATABASES, database_name, self.TABLE_DETAILS)

    def table_token(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/token".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                self.TABLES, RESTUtil.encode_string(table_name)))

    def rename_table(self) -> str:
        return "{}/{}/rename".format(self.base_path, self.TABLES)

    def commit_table(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/commit".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                self.TABLES, RESTUtil.encode_string(table_name)))
