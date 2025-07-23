from typing import List, Dict, Optional

from pypaimon.api import RESTApi, RESTCatalogOptions, Identifier
from pypaimon.api.api_response import PagedList, GetTableResponse
from pypaimon.api.path import Path
from pypaimon.api.schema import Schema
from pypaimon.api.table_schema import TableSchema
from pypaimon.pynative.catalog.catalog import Catalog
from pypaimon.pynative.catalog.catalog_context import CatalogContext
from pypaimon.pynative.catalog.catalog_utils import CatalogUtils
from pypaimon.pynative.catalog.database import Database
from pypaimon.pynative.catalog.property_change import PropertyChange
from pypaimon.pynative.catalog.table_metadata import TableMetadata
from pypaimon.pynative.common.core_options import CoreOptions
from pypaimon.pynative.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.pynative.table.file_store_table import FileStoreTable


class RESTCatalog(Catalog):
    def __init__(self, context: CatalogContext, config_required: Optional[bool] = True):
        self.api = RESTApi(context.options, config_required)
        self.context = CatalogContext.create(self.api.options, context.hadoop_conf, context.prefer_io_loader,
                                             context.fallback_io_loader)
        self.data_token_enabled = self.api.options.get(RESTCatalogOptions.DATA_TOKEN_ENABLED)

    @classmethod
    def from_context(cls, context: CatalogContext):
        return cls(context, True)

    def options(self) -> dict:
        return self.context.options.to_map()

    def list_databases(self) -> List[str]:
        return self.api.list_databases()

    def list_databases_paged(self, max_results: int, page_token: str, database_name_pattern: str) -> PagedList[str]:
        return self.api.list_databases_paged(max_results, page_token, database_name_pattern)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Dict[str, str] = None):
        self.api.create_database(name, properties)

    def get_database(self, name: str) -> Database:
        response = self.api.get_database(name)
        if response is not None:
            return Database(name, response.options)

    def drop_database(self, name: str):
        self.api.drop_database(name)

    def alter_database(self, name: str, changes: List[PropertyChange], ignore_if_exists: bool):
        set_properties, remove_keys = PropertyChange.get_set_properties_to_remove_keys(changes)
        self.api.alter_database(name, list(remove_keys), set_properties)

    def list_tables(self, database_name: str) -> List[str]:
        return self.api.list_tables(database_name)

    def list_tables_paged(
            self,
            database_name: str,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            table_name_pattern: Optional[str] = None
    ) -> PagedList[str]:
        return self.api.list_tables_paged(
            database_name,
            max_results,
            page_token,
            table_name_pattern
        )

    def get_table(self, identifier: Identifier) -> FileStoreTable:
        return CatalogUtils.load_table(
            self,
            identifier,
            lambda path: self.file_io_for_data(path, identifier),
            self.file_io_from_options,
            self.load_table_metadata,
        )
        # table_identifier = TableIdentifier(identifier)
        # if CoreOptions.SCAN_FALLBACK_BRANCH in self.catalog_options:
        #     raise PyNativeNotImplementedError(CoreOptions.SCAN_FALLBACK_BRANCH)
        # response = self.load_table_metadata(Identifier.from_string(identifier))
        # path = Path(response.get_path())
        # return FileStoreTable(self.file_io_for_data(path, table_identifier), table_identifier, path,
        #                       TableSchema.from_schema(Schema(response.get_schema())))

    def load_table_metadata(self, identifier: Identifier) -> TableMetadata:
        response = self.api.get_table(identifier)
        return self.to_table_metadata(identifier.get_database_name(), response)

    def to_table_metadata(self, db: str, response: GetTableResponse) -> TableMetadata:
        schema = TableSchema.create(response.get_schema_id(), Schema.from_dict(response.get_schema()))
        options: Dict[str, str] = dict(schema.options)
        options[CoreOptions.PATH] = response.get_path()
        response.put_audit_options_to(options)

        identifier = Identifier.create(db, response.get_name())
        if identifier.get_branch_name() is not None:
            options[CoreOptions.BRANCH] = identifier.get_branch_name()

        return TableMetadata(
            schema=schema.copy(options),
            is_external=response.get_is_external(),
            uuid=response.get_id()
        )

    def file_io_from_options(self, path: Path):
        return None

    def file_io_for_data(self, path: Path, identifier: Identifier):
        return RESTTokenFileIO(identifier, path, None, None) if self.data_token_enabled else None

    def create_table(self, identifier: str, schema: Schema, ignore_if_exists: bool):
        pass
