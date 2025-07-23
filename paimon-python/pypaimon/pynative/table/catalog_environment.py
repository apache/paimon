from typing import Optional

from pypaimon.api import Identifier
from pypaimon.pynative.catalog.catalog_loader import CatalogLoader


class CatalogEnvironment:

    def __init__(
        self,
        identifier: Optional[Identifier] = None,
        uuid: Optional[str] = None,
        catalog_loader: Optional[CatalogLoader] = None,
        supports_version_management: bool = False
    ):
        self.identifier = identifier
        self.uuid = uuid
        self.catalog_loader = catalog_loader
        self.supports_version_management = supports_version_management

    @classmethod
    def empty(cls) -> 'CatalogEnvironment':
        """Create an empty CatalogEnvironment instance"""
        return cls(
            identifier=None,
            uuid=None,
            catalog_loader=None,
            supports_version_management=False
        )

    def copy(self, new_identifier: Optional[Identifier] = None) -> 'CatalogEnvironment':

        return CatalogEnvironment(
            identifier=new_identifier if new_identifier is not None else self.identifier,
            uuid=self.uuid,
            catalog_loader=self.catalog_loader,
            supports_version_management=self.supports_version_management
        )
