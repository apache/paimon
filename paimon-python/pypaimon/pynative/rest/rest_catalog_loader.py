from pypaimon.pynative.catalog.catalog_context import CatalogContext
from pypaimon.pynative.catalog.catalog_loader import CatalogLoader
from pypaimon.pynative.rest.rest_catalog import RESTCatalog


class RESTCatalogLoader(CatalogLoader):
    def __init__(self, context: CatalogContext):
        self.context = context

    def load(self) -> RESTCatalog:
        return RESTCatalog(self.context, False)
