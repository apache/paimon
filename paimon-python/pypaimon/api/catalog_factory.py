from pypaimon.api.catalog import Catalog


class CatalogFactory:

    @staticmethod
    def create(catalog_options: dict) -> Catalog:
        from pypaimon.pynative.catalog.catalog_option import CatalogOptions
        from pypaimon.pynative.catalog.abstract_catalog import AbstractCatalog
        from pypaimon.pynative.catalog.filesystem_catalog import FileSystemCatalog  # noqa: F401
        from pypaimon.pynative.catalog.hive_catalog import HiveCatalog  # noqa: F401

        identifier = catalog_options.get(CatalogOptions.METASTORE, "filesystem")
        subclasses = AbstractCatalog.__subclasses__()
        for subclass in subclasses:
            if subclass.identifier() == identifier:
                return subclass(catalog_options)
        raise ValueError(f"Unknown catalog identifier: {identifier}")
