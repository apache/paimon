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
