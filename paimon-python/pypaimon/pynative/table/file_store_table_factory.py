
from pypaimon.api.path import Path
from pypaimon.api.table_schema import TableSchema

from pypaimon.pynative.common.file_io import FileIO
from pypaimon.pynative.table.catalog_environment import CatalogEnvironment
from pypaimon.pynative.table.file_store_table import FileStoreTable


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
