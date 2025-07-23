from typing import Callable, Any

from pypaimon.api import Identifier
from pypaimon.pynative.catalog.catalog import Catalog
from pypaimon.api.path import Path
from pypaimon.pynative.catalog.table_metadata import TableMetadata
from pypaimon.pynative.common.core_options import CoreOptions
from pypaimon.pynative.table.catalog_environment import CatalogEnvironment
from pypaimon.pynative.table.file_store_table import FileStoreTable
from pypaimon.pynative.table.file_store_table_factory import FileStoreTableFactory


class CatalogUtils:
    @staticmethod
    def load_table(
            catalog: Catalog,
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
