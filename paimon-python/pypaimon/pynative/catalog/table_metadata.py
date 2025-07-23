from abc import ABC
from typing import Optional

from pypaimon.api import Identifier
from pypaimon.api.table_schema import TableSchema


class TableMetadata:

    def __init__(self, schema: TableSchema, is_external: bool, uuid: Optional[str] = None):
        self._schema = schema
        self._is_external = is_external
        self._uuid = uuid

    @property
    def schema(self) -> TableSchema:
        return self._schema

    @property
    def is_external(self) -> bool:
        return self._is_external

    @property
    def uuid(self) -> Optional[str]:
        return self._uuid


class Loader(ABC):

    def load(self, identifier: Identifier) -> TableMetadata:
        ...
