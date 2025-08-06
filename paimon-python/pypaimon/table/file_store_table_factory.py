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
from pathlib import Path

from pypaimon.common.file_io import FileIO
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.catalog_environment import CatalogEnvironment
from pypaimon.table.file_store_table import FileStoreTable


class FileStoreTableFactory:
    @staticmethod
    def create(
            file_io: FileIO,
            table_path: Path,
            table_schema: TableSchema,
            catalog_environment: CatalogEnvironment
    ) -> FileStoreTable:
        """Create FileStoreTable with dynamic options and catalog environment"""
        return FileStoreTable(file_io, catalog_environment.identifier, table_path, table_schema)
