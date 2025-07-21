
from pypaimon.api import Catalog
from pypaimon.pynative.catalog.catalog_option import CatalogOptions
from pypaimon.pynative.common.file_io import FileIO


class RESTCatalog(Catalog):
    def __init__(self, catalog_options: dict):
        if CatalogOptions.WAREHOUSE not in catalog_options:
            raise ValueError(f"Paimon '{CatalogOptions.WAREHOUSE}' path must be set")
        self.warehouse = catalog_options.get(CatalogOptions.WAREHOUSE)
        self.catalog_options = catalog_options
        self.file_io = FileIO(self.warehouse, self.catalog_options)
