# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Dict, Iterable, Optional

import pyarrow as pa

from pypaimon import CatalogFactory, Schema as PaimonSchema
from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException,
)
from pypaimon.multimodal.table import MultimodalTable, _to_arrow_table

_DEFAULT_OPTIONS = {
    "row-tracking.enabled": "true",
    "data-evolution.enabled": "true",
    "deletion-vectors.enabled": "true",
    "blob-as-descriptor": "true",
    "file.format": "vortex",
    "global-index.search-mode": "full",
    "vector.file.format": "vortex",
}

_DEFAULT_DATABASE = Catalog.DEFAULT_DATABASE


def connect(
        *,
        database: str = _DEFAULT_DATABASE,
        options: Optional[Dict[str, str]] = None):
    """Connect to a Paimon catalog through the multimodal facade."""
    return MultimodalConnection(
        _resolve_catalog(options),
        database=database,
    )


class MultimodalConnection:
    """High-level entry point backed by a Paimon catalog and database."""

    def __init__(self, catalog, database: str = _DEFAULT_DATABASE):
        if not database:
            raise ValueError("database is required.")
        self.catalog = catalog
        self.database = database

    def create_table(
            self,
            name: str,
            data=None,
            schema=None,
            options: Optional[Dict[str, str]] = None,
            partitioned: Optional[Iterable[str]] = None,
            ignore_if_exists: bool = False):
        """Create a multimodal table and optionally add initial data."""
        identifier = self._identifier(name)
        already_exists = _table_exists(self.catalog, identifier)
        paimon_schema = _to_paimon_schema(schema, data, options, partitioned)

        self._create_database_for(identifier)
        try:
            self.catalog.create_table(
                identifier, paimon_schema, ignore_if_exists)
        except TableAlreadyExistException:
            if not ignore_if_exists:
                raise

        table = self.get_table(name)
        if data is not None and not already_exists:
            table.add(data)
        return table

    def get_table(self, name: str):
        identifier = self._identifier(name)
        raw_table = self.catalog.get_table(identifier)
        _validate_multimodal_table(raw_table, identifier)
        return MultimodalTable(
            self.catalog,
            identifier,
            raw_table,
        )

    def drop_table(self, name: str, ignore_if_not_exists: bool = False):
        self.catalog.drop_table(
            self._identifier(name),
            ignore_if_not_exists=ignore_if_not_exists,
        )

    def _identifier(self, name: str) -> str:
        return _resolve_identifier(name, self.database)

    def _create_database_for(self, identifier: str):
        database_name = _database_name(identifier)
        if database_name is not None:
            self.catalog.create_database(database_name, ignore_if_exists=True)


def _resolve_catalog(options):
    resolved_options = {}
    if options:
        resolved_options.update(options)
    if not resolved_options:
        raise ValueError("options is required.")
    return CatalogFactory.create(resolved_options)


def _table_exists(catalog, identifier: str) -> bool:
    try:
        catalog.get_table(identifier)
        return True
    except (DatabaseNotExistException, TableNotExistException):
        return False


def _validate_multimodal_table(table, identifier: str):
    table_schema = table.table_schema
    options = table_schema.options
    if str(options.get("data-evolution.enabled", "false")).lower() != "true":
        raise ValueError(
            "Table %s is not a data-evolution table; "
            "data-evolution.enabled must be true." % identifier)
    if table_schema.primary_keys:
        raise ValueError(
            "Table %s has primary keys %s; multimodal tables must not have "
            "primary keys." % (identifier, table_schema.primary_keys))


def _resolve_identifier(name: str, database: str) -> str:
    if not name:
        raise ValueError("table name is required.")
    if "." in name:
        return name
    return "%s.%s" % (database, name)


def _to_paimon_schema(schema, data, options, partitioned):
    if schema is None:
        if data is None:
            raise ValueError("schema or data is required.")
        pa_schema, inferred_options = _infer_pyarrow_schema(data)
    else:
        pa_schema = _to_pyarrow_schema(schema)
        inferred_options = {}
    return PaimonSchema.from_pyarrow_schema(
        pa_schema,
        partition_keys=_normalize_partitioned(partitioned),
        options=_merge_options(inferred_options, options),
    )


def _merge_options(*option_groups):
    table_options = dict(_DEFAULT_OPTIONS)
    for options in option_groups:
        if options:
            table_options.update({str(k): str(v) for k, v in options.items()})
    return table_options


def _normalize_partitioned(partitioned):
    if partitioned is None:
        return []
    if isinstance(partitioned, str):
        return [partitioned]
    return list(partitioned)


def _to_pyarrow_schema(schema):
    if isinstance(schema, pa.Schema):
        return schema
    raise ValueError("schema must be a pyarrow.Schema.")


def _infer_pyarrow_schema(data):
    table = _to_arrow_table(data)
    if table.num_columns == 0:
        raise ValueError("Cannot infer schema from empty data.")
    return table.schema, {}


def _database_name(identifier: str):
    parts = identifier.split(".")
    if len(parts) == 2 and parts[0]:
        return parts[0]
    return None
