################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

import sqlite3
from contextlib import closing
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_exception import (
    DatabaseAlreadyExistException,
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException
)
from pypaimon.catalog.database import Database
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.config import CatalogOptions, JdbcCatalogOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.table import Table


class _JdbcConnection:
    def __init__(self, options: Dict[str, str]):
        self.options = options
        self.uri = options.get(CatalogOptions.URI.key())
        if not self.uri:
            raise ValueError(f"Paimon '{CatalogOptions.URI.key()}' must be set for jdbc catalog")
        self.protocol, self.placeholder, self.connection = self._connect(self.uri, options)

    def close(self):
        self.connection.close()

    def execute(self, sql: str, args: Tuple = ()):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(self._sql(sql), args)
            self.connection.commit()
            return cursor.rowcount

    def fetch_all(self, sql: str, args: Tuple = ()):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(self._sql(sql), args)
            return cursor.fetchall()

    def fetch_one(self, sql: str, args: Tuple = ()):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(self._sql(sql), args)
            return cursor.fetchone()

    def _sql(self, sql: str) -> str:
        if self.placeholder == "?":
            return sql
        return sql.replace("?", self.placeholder)

    @staticmethod
    def _jdbc_properties(options: Dict[str, str]) -> Dict[str, str]:
        result = {}
        for key, value in options.items():
            if key.startswith("jdbc."):
                result[key[len("jdbc."):]] = value
        return result

    def _connect(self, uri: str, options: Dict[str, str]):
        if uri.startswith("jdbc:sqlite:"):
            return self._connect_sqlite(uri)
        if uri.startswith("jdbc:mysql:"):
            return self._connect_mysql(uri, options)
        if uri.startswith("jdbc:postgresql:"):
            return self._connect_postgresql(uri, options)
        raise ValueError(f"Unsupported jdbc catalog URI: {uri}")

    def _connect_sqlite(self, uri: str):
        sqlite_uri = uri[len("jdbc:sqlite:"):]
        if sqlite_uri.startswith("file:"):
            connection = sqlite3.connect(sqlite_uri, uri=True)
        else:
            connection = sqlite3.connect(sqlite_uri)
        return "sqlite", "?", connection

    def _connect_mysql(self, uri: str, options: Dict[str, str]):
        try:
            import pymysql
            connector = "pymysql"
        except ImportError:
            try:
                import mysql.connector as mysql_connector
                connector = "mysql-connector"
            except ImportError as e:
                raise ImportError(
                    "PyPaimon jdbc catalog requires pymysql or mysql-connector-python "
                    "to connect to MySQL."
                ) from e

        parsed = urlparse(uri[len("jdbc:"):])
        props = self._jdbc_properties(options)
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        props.update(query)
        user = props.pop("user", props.pop("username", None))
        password = props.pop("password", None)
        database = parsed.path.lstrip("/")
        port = parsed.port or 3306
        if connector == "pymysql":
            connection = pymysql.connect(
                host=parsed.hostname,
                port=port,
                user=user,
                password=password,
                database=database,
                autocommit=False,
                **props
            )
        else:
            connection = mysql_connector.connect(
                host=parsed.hostname,
                port=port,
                user=user,
                password=password,
                database=database,
                **props
            )
        return "mysql", "%s", connection

    def _connect_postgresql(self, uri: str, options: Dict[str, str]):
        try:
            import psycopg2
            connector = "psycopg2"
        except ImportError:
            try:
                import psycopg
                connector = "psycopg"
            except ImportError as e:
                raise ImportError(
                    "PyPaimon jdbc catalog requires psycopg2 or psycopg "
                    "to connect to PostgreSQL."
                ) from e

        parsed = urlparse(uri[len("jdbc:"):])
        props = self._jdbc_properties(options)
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        props.update(query)
        user = props.pop("user", props.pop("username", None))
        password = props.pop("password", None)
        database = parsed.path.lstrip("/")
        port = parsed.port or 5432
        connect_kwargs = {
            "host": parsed.hostname,
            "port": port,
            "user": user,
            "password": password,
            "dbname": database,
        }
        connect_kwargs.update(props)
        if connector == "psycopg2":
            connection = psycopg2.connect(**connect_kwargs)
        else:
            connection = psycopg.connect(**connect_kwargs)
        return "postgresql", "%s", connection


class JdbcCatalog(Catalog):
    CATALOG_TABLE_NAME = "paimon_tables"
    DATABASE_PROPERTIES_TABLE_NAME = "paimon_database_properties"
    TABLE_PROPERTIES_TABLE_NAME = "paimon_table_properties"
    CATALOG_KEY = "catalog_key"
    TABLE_DATABASE = "database_name"
    TABLE_NAME = "table_name"
    PROPERTY_KEY = "property_key"
    PROPERTY_VALUE = "property_value"
    DATABASE_EXISTS_PROPERTY = "exists"

    def __init__(self, context: CatalogContext):
        catalog_options = context.options
        if not catalog_options.contains(CatalogOptions.WAREHOUSE):
            raise ValueError(f"Paimon '{CatalogOptions.WAREHOUSE.key()}' path must be set")
        self.context = context
        self.catalog_options = catalog_options
        self.options = catalog_options.to_map()
        self.warehouse = catalog_options.get(CatalogOptions.WAREHOUSE)
        self.catalog_key = catalog_options.get(JdbcCatalogOptions.CATALOG_KEY)
        self.file_io = FileIO.get(self.warehouse, self.catalog_options)
        self.connection = _JdbcConnection(self.options)
        self._initialize_catalog_tables()

    def close(self):
        self.connection.close()

    def _initialize_catalog_tables(self):
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS paimon_tables ("
            "catalog_key VARCHAR(255) NOT NULL, "
            "database_name VARCHAR(255) NOT NULL, "
            "table_name VARCHAR(255) NOT NULL, "
            "PRIMARY KEY (catalog_key, database_name, table_name))"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS paimon_database_properties ("
            "catalog_key VARCHAR(255) NOT NULL, "
            "database_name VARCHAR(255) NOT NULL, "
            "property_key VARCHAR(255), "
            "property_value VARCHAR(1000), "
            "PRIMARY KEY (catalog_key, database_name, property_key))"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS paimon_table_properties ("
            "catalog_key VARCHAR(255) NOT NULL, "
            "database_name VARCHAR(255) NOT NULL, "
            "table_name VARCHAR(255) NOT NULL, "
            "property_key VARCHAR(255) NOT NULL, "
            "property_value VARCHAR(1000), "
            "PRIMARY KEY (catalog_key, database_name, table_name, property_key))"
        )

    def list_databases(self) -> List[str]:
        table_rows = self.connection.fetch_all(
            "SELECT DISTINCT database_name FROM paimon_tables WHERE catalog_key = ?",
            (self.catalog_key,)
        )
        property_rows = self.connection.fetch_all(
            "SELECT DISTINCT database_name FROM paimon_database_properties WHERE catalog_key = ?",
            (self.catalog_key,)
        )
        databases = {row[0] for row in table_rows}
        databases.update(row[0] for row in property_rows)
        return sorted(databases)

    def get_database(self, name: str) -> Database:
        if not self._database_exists(name):
            raise DatabaseNotExistException(name)
        properties = self._fetch_database_properties(name)
        if Catalog.DB_LOCATION_PROP not in properties:
            properties[Catalog.DB_LOCATION_PROP] = self.get_database_path(name)
        properties.pop(self.DATABASE_EXISTS_PROPERTY, None)
        return Database(name, properties)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        if self._database_exists(name):
            if not ignore_if_exists:
                raise DatabaseAlreadyExistException(name)
            return
        create_props = {self.DATABASE_EXISTS_PROPERTY: "true"}
        if properties:
            create_props.update(properties)
        if Catalog.DB_LOCATION_PROP not in create_props:
            create_props[Catalog.DB_LOCATION_PROP] = self.get_database_path(name)
        self._insert_database_properties(name, create_props)

    def drop_database(self, name: str, ignore_if_not_exists: bool = False, cascade: bool = False):
        if not self._database_exists(name):
            if not ignore_if_not_exists:
                raise DatabaseNotExistException(name)
            return
        tables = self.list_tables(name)
        if tables and not cascade:
            raise ValueError(f"Database {name} is not empty. Use cascade=True to drop all tables first.")
        if cascade:
            for table in tables:
                self.drop_table(Identifier.create(name, table), True)
        self.connection.execute(
            "DELETE FROM paimon_tables WHERE catalog_key = ? AND database_name = ?",
            (self.catalog_key, name)
        )
        self.connection.execute(
            "DELETE FROM paimon_database_properties WHERE catalog_key = ? AND database_name = ?",
            (self.catalog_key, name)
        )
        self.connection.execute(
            "DELETE FROM paimon_table_properties WHERE catalog_key = ? AND database_name = ?",
            (self.catalog_key, name)
        )

    def alter_database(self, name: str, changes: list):
        self.get_database(name)
        from pypaimon.catalog.rest.property_change import PropertyChange
        set_properties, remove_keys = PropertyChange.get_set_properties_to_remove_keys(changes)
        current = self._fetch_database_properties(name)
        for key, value in set_properties.items():
            if key in current:
                self.connection.execute(
                    "UPDATE paimon_database_properties SET property_value = ? "
                    "WHERE catalog_key = ? AND database_name = ? AND property_key = ?",
                    (value, self.catalog_key, name, key)
                )
            else:
                self._insert_database_properties(name, {key: value})
        for key in remove_keys:
            self.connection.execute(
                "DELETE FROM paimon_database_properties "
                "WHERE catalog_key = ? AND database_name = ? AND property_key = ?",
                (self.catalog_key, name, key)
            )

    def list_tables(self, database_name: str) -> List[str]:
        self.get_database(database_name)
        rows = self.connection.fetch_all(
            "SELECT table_name FROM paimon_tables WHERE catalog_key = ? AND database_name = ?",
            (self.catalog_key, database_name)
        )
        return sorted(row[0] for row in rows)

    def get_table(self, identifier: Union[str, Identifier]) -> Table:
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        if self.catalog_options.contains(CoreOptions.SCAN_FALLBACK_BRANCH):
            raise ValueError(f"Unsupported CoreOption {CoreOptions.SCAN_FALLBACK_BRANCH}")
        if not self._table_exists(identifier):
            raise TableNotExistException(identifier)
        table_path = self.get_table_path(identifier)
        table_schema = self.get_table_schema(identifier)
        from pypaimon.catalog.jdbc_catalog_loader import JdbcCatalogLoader
        catalog_environment = CatalogEnvironment(
            identifier=identifier,
            uuid=None,
            catalog_loader=JdbcCatalogLoader(self.context),
            supports_version_management=False
        )
        return FileStoreTable(self.file_io, identifier, table_path, table_schema, catalog_environment)

    def create_table(self, identifier: Union[str, Identifier], schema: 'Schema', ignore_if_exists: bool):
        if schema.options and schema.options.get(CoreOptions.AUTO_CREATE.key()):
            raise ValueError(f"The value of {CoreOptions.AUTO_CREATE.key()} property should be False.")
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        self.get_database(identifier.get_database_name())
        if self._table_exists(identifier):
            if not ignore_if_exists:
                raise TableAlreadyExistException(identifier)
            return
        if schema.options and CoreOptions.TYPE.key() in schema.options and schema.options.get(
                CoreOptions.TYPE.key()) != "table":
            raise ValueError(f"Table Type: {schema.options.get(CoreOptions.TYPE.key())}")

        table_path = self.get_table_path(identifier)
        schema_manager = SchemaManager(self.file_io, table_path)
        table_schema = schema_manager.create_table(schema)
        try:
            self.connection.execute(
                "INSERT INTO paimon_tables (catalog_key, database_name, table_name) VALUES (?, ?, ?)",
                (self.catalog_key, identifier.get_database_name(), identifier.get_table_name())
            )
            if self._sync_all_properties():
                self._insert_table_properties(identifier, self._collect_table_properties(table_schema))
        except Exception:
            self.file_io.delete_directory_quietly(table_path)
            raise

    def drop_table(self, identifier: Union[str, Identifier], ignore_if_not_exists: bool = False):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        if not self._table_exists(identifier):
            if not ignore_if_not_exists:
                raise TableNotExistException(identifier)
            return
        table_path = self.get_table_path(identifier)
        self.connection.execute(
            "DELETE FROM paimon_tables WHERE catalog_key = ? AND database_name = ? AND table_name = ?",
            (self.catalog_key, identifier.get_database_name(), identifier.get_table_name())
        )
        self.connection.execute(
            "DELETE FROM paimon_table_properties WHERE catalog_key = ? AND database_name = ? AND table_name = ?",
            (self.catalog_key, identifier.get_database_name(), identifier.get_table_name())
        )
        self.file_io.delete_directory_quietly(table_path)

    def rename_table(self, source_identifier: Union[str, Identifier], target_identifier: Union[str, Identifier]):
        if not isinstance(source_identifier, Identifier):
            source_identifier = Identifier.from_string(source_identifier)
        if not isinstance(target_identifier, Identifier):
            target_identifier = Identifier.from_string(target_identifier)
        if not self._table_exists(source_identifier):
            raise TableNotExistException(source_identifier)
        self.get_database(target_identifier.get_database_name())
        if self._table_exists(target_identifier):
            raise TableAlreadyExistException(target_identifier)

        self.connection.execute(
            "UPDATE paimon_tables SET database_name = ?, table_name = ? "
            "WHERE catalog_key = ? AND database_name = ? AND table_name = ?",
            (
                target_identifier.get_database_name(),
                target_identifier.get_table_name(),
                self.catalog_key,
                source_identifier.get_database_name(),
                source_identifier.get_table_name()
            )
        )
        self.connection.execute(
            "UPDATE paimon_table_properties SET database_name = ?, table_name = ? "
            "WHERE catalog_key = ? AND database_name = ? AND table_name = ?",
            (
                target_identifier.get_database_name(),
                target_identifier.get_table_name(),
                self.catalog_key,
                source_identifier.get_database_name(),
                source_identifier.get_table_name()
            )
        )
        source_path = self.get_table_path(source_identifier)
        target_path = self.get_table_path(target_identifier)
        if self.file_io.exists(source_path):
            self.file_io.rename(source_path, target_path)

    def alter_table(
        self,
        identifier: Union[str, Identifier],
        changes: List[SchemaChange],
        ignore_if_not_exists: bool = False
    ):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        if not self._table_exists(identifier):
            if not ignore_if_not_exists:
                raise TableNotExistException(identifier)
            return
        schema_manager = SchemaManager(self.file_io, self.get_table_path(identifier))
        table_schema = schema_manager.commit_changes(changes)
        if self._sync_all_properties():
            self.connection.execute(
                "DELETE FROM paimon_table_properties "
                "WHERE catalog_key = ? AND database_name = ? AND table_name = ?",
                (self.catalog_key, identifier.get_database_name(), identifier.get_table_name())
            )
            self._insert_table_properties(identifier, self._collect_table_properties(table_schema))

    def get_table_schema(self, identifier: Identifier):
        table_schema = SchemaManager(self.file_io, self.get_table_path(identifier)).latest()
        if table_schema is None:
            raise TableNotExistException(identifier)
        return table_schema

    def get_database_path(self, name: str) -> str:
        warehouse = self.warehouse.rstrip('/')
        return f"{warehouse}/{name}{Catalog.DB_SUFFIX}"

    def get_table_path(self, identifier: Identifier) -> str:
        db_path = self.get_database_path(identifier.get_database_name())
        return f"{db_path}/{identifier.get_table_name()}"

    def load_snapshot(self, identifier: Identifier):
        raise NotImplementedError("JDBC catalog does not support load_snapshot")

    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        raise NotImplementedError("This catalog does not support commit catalog")

    def _database_exists(self, database_name: str) -> bool:
        row = self.connection.fetch_one(
            "SELECT database_name FROM paimon_tables "
            "WHERE catalog_key = ? AND database_name = ? LIMIT 1",
            (self.catalog_key, database_name)
        )
        if row is not None:
            return True
        row = self.connection.fetch_one(
            "SELECT database_name FROM paimon_database_properties "
            "WHERE catalog_key = ? AND database_name = ? LIMIT 1",
            (self.catalog_key, database_name)
        )
        return row is not None

    def _table_exists(self, identifier: Identifier) -> bool:
        row = self.connection.fetch_one(
            "SELECT table_name FROM paimon_tables "
            "WHERE catalog_key = ? AND database_name = ? AND table_name = ? LIMIT 1",
            (self.catalog_key, identifier.get_database_name(), identifier.get_table_name())
        )
        return row is not None

    def _fetch_database_properties(self, database_name: str) -> Dict[str, str]:
        rows = self.connection.fetch_all(
            "SELECT property_key, property_value FROM paimon_database_properties "
            "WHERE catalog_key = ? AND database_name = ?",
            (self.catalog_key, database_name)
        )
        return {row[0]: row[1] for row in rows}

    def _insert_database_properties(self, database_name: str, properties: Dict[str, str]):
        for key, value in properties.items():
            self.connection.execute(
                "INSERT INTO paimon_database_properties "
                "(catalog_key, database_name, property_key, property_value) VALUES (?, ?, ?, ?)",
                (self.catalog_key, database_name, key, value)
            )

    def _insert_table_properties(self, identifier: Identifier, properties: Dict[str, str]):
        for key, value in properties.items():
            self.connection.execute(
                "INSERT INTO paimon_table_properties "
                "(catalog_key, database_name, table_name, property_key, property_value) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    self.catalog_key,
                    identifier.get_database_name(),
                    identifier.get_table_name(),
                    key,
                    value
                )
            )

    def _sync_all_properties(self) -> bool:
        from pypaimon.common.options.options_utils import OptionsUtils
        return OptionsUtils.convert_to_boolean(
            self.catalog_options.get(CatalogOptions.SYNC_ALL_PROPERTIES))

    @staticmethod
    def _collect_table_properties(table_schema) -> Dict[str, str]:
        properties = dict(table_schema.options or {})
        if table_schema.primary_keys:
            properties["primary-key"] = ",".join(table_schema.primary_keys)
        if table_schema.partition_keys:
            properties["partition"] = ",".join(table_schema.partition_keys)
        return properties
