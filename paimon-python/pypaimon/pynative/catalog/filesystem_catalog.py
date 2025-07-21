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

from typing import Optional

from pypaimon.api import Schema
from pypaimon.pynative.catalog.abstract_catalog import AbstractCatalog
from pypaimon.pynative.catalog.catalog_constant import CatalogConstants
from pypaimon.pynative.catalog.catalog_exception import TableNotExistException
from pypaimon.pynative.common.identifier import TableIdentifier
from pypaimon.pynative.table.schema_manager import SchemaManager


class FileSystemCatalog(AbstractCatalog):

    def __init__(self, catalog_options: dict):
        super().__init__(catalog_options)

    @staticmethod
    def identifier() -> str:
        return "filesystem"

    def allow_custom_table_path(self) -> bool:
        return False

    def create_database_impl(self, name: str, properties: Optional[dict] = None):
        if properties and CatalogConstants.DB_LOCATION_PROP in properties:
            raise ValueError("Cannot specify location for a database when using fileSystem catalog.")
        path = self.get_database_path(name)
        self.file_io.mkdirs(path)

    def create_table_impl(self, table_identifier: TableIdentifier, schema: Schema):
        table_path = self.get_table_path(table_identifier)
        schema_manager = SchemaManager(self.file_io, table_path)
        schema_manager.create_table(schema)

    def get_table_schema(self, table_identifier: TableIdentifier):
        table_path = self.get_table_path(table_identifier)
        table_schema = SchemaManager(self.file_io, table_path).latest()
        if table_schema is None:
            raise TableNotExistException(table_identifier.get_full_name())
        return table_schema

    def lock_factory(self):
        pass

    def metastore_client_factory(self):
        return None
