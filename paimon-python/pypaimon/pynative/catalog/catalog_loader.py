from abc import ABC, abstractmethod


class CatalogLoader(ABC):
    @abstractmethod
    def load(self):
        pass
