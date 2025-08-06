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
from typing import Any, Callable

from pypaimon.catalog.table_metadata import TableMetadata
from pypaimon.common.core_options import CoreOptions
from pypaimon.common.identifier import Identifier
from pypaimon.table.catalog_environment import CatalogEnvironment
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.file_store_table_factory import FileStoreTableFactory


class CatalogUtils:
    @staticmethod
    def load_table(
            identifier: Identifier,
            internal_file_io: Callable[[Path], Any],
            external_file_io: Callable[[Path], Any],
            metadata_loader: Callable[[Identifier], TableMetadata],
    ) -> FileStoreTable:
        metadata = metadata_loader(identifier)
        schema = metadata.schema
        data_file_io = external_file_io if metadata.is_external else internal_file_io
        catalog_env = CatalogEnvironment(
            identifier=identifier,
            uuid=metadata.uuid,
            catalog_loader=None,
            supports_version_management=False
        )

        path = Path(schema.options.get(CoreOptions.PATH))
        table = FileStoreTableFactory.create(data_file_io(path), path, schema, catalog_env)
        return table

    @staticmethod
    def is_system_database(database_name: str) -> bool:
        return Catalog.SYSTEM_DATABASE_NAME.equals(database_name)
