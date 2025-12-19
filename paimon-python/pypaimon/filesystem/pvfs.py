#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import datetime
import importlib
import time
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple, Union

import fsspec
from cachetools import LRUCache, TTLCache
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from readerwriterlock import rwlock

from pypaimon.api.api_response import GetTableResponse, GetTableTokenResponse
from pypaimon.api.client import AlreadyExistsException, NoSuchResourceException
from pypaimon.api.rest_api import RESTApi
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions, PVFSOptions
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema import Schema

PROTOCOL_NAME = "pvfs"


class StorageType(Enum):
    LOCAL = "file"
    OSS = "oss"


class PVFSIdentifier(ABC):
    catalog: str
    endpoint: str

    def get_cache_key(self) -> str:
        return "{}.{}".format(self.catalog, self.__remove_endpoint_schema(self.endpoint))

    @staticmethod
    def __remove_endpoint_schema(url):
        if url.startswith('https://'):
            return url[8:]
        elif url.startswith('http://'):
            return url[7:]
        return url


@dataclass
class PVFSCatalogIdentifier(PVFSIdentifier):
    catalog: str
    endpoint: str


@dataclass
class PVFSDatabaseIdentifier(PVFSIdentifier):
    catalog: str
    endpoint: str
    database: str


@dataclass
class PVFSTableIdentifier(PVFSIdentifier):
    catalog: str
    endpoint: str
    database: str
    table: str
    sub_path: str = None

    def __hash__(self) -> int:
        return hash((self.catalog, self.database, self.table))

    def __eq__(self, __value: Any) -> bool:
        if isinstance(__value, PVFSTableIdentifier):
            return self.catalog == __value.catalog and self.database == __value.database and self.table == __value.table
        return False

    def get_actual_path(self, storage_location: str):
        if self.sub_path:
            return '{}/{}'.format(storage_location.rstrip("/"), self.sub_path.lstrip("/"))
        return storage_location

    def get_virtual_location(self):
        return ('{}://{}'.format(PROTOCOL_NAME, self.catalog) +
                '/{}/{}'.format(self.database, self.table))

    def get_identifier(self):
        return Identifier.create(self.database, self.table)

    def name(self):
        return '{}.{}.{}'.format(self.catalog, self.database, self.table)


@dataclass
class PaimonRealStorage:
    TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000

    token: Dict[str, str]
    expires_at_millis: Optional[int]
    file_system: AbstractFileSystem

    def need_refresh(self) -> bool:
        if self.expires_at_millis is not None:
            return self.expires_at_millis - int(time.time() * 1000) < self.TOKEN_EXPIRATION_SAFE_TIME_MILLIS
        return False


@dataclass
class TableStore:
    path: str
    created: datetime
    modified: datetime


class PaimonVirtualFileSystem(fsspec.AbstractFileSystem):
    options: Options

    protocol = PROTOCOL_NAME

    def __init__(self, options: Union[Options, Dict[str, str]] = None, **kwargs):
        if isinstance(options, dict):
            options = Options(options)
        options.set(CatalogOptions.HTTP_USER_AGENT_HEADER, 'PythonPVFS')
        self.options = options
        self.warehouse = options.get(CatalogOptions.WAREHOUSE)
        cache_expired_time = (
            PVFSOptions.DEFAULT_TABLE_CACHE_TTL
            if options is None
            else options.get(
                PVFSOptions.TABLE_CACHE_TTL, PVFSOptions.DEFAULT_TABLE_CACHE_TTL
            )
        )
        self._cache_enable = options.get(PVFSOptions.CACHE_ENABLED, True)
        if self._cache_enable:
            self._table_cache = TTLCache(maxsize=PVFSOptions.DEFAULT_CACHE_SIZE, ttl=cache_expired_time)
            self._table_cache_lock = rwlock.RWLockFair()
        self._rest_api_cache = LRUCache(PVFSOptions.DEFAULT_CACHE_SIZE)
        self._fs_cache = LRUCache(maxsize=PVFSOptions.DEFAULT_CACHE_SIZE)
        self._table_cache_lock = rwlock.RWLockFair()
        self._rest_api_cache_lock = rwlock.RWLockFair()
        self._fs_cache_lock = rwlock.RWLockFair()
        super().__init__(**kwargs)

    @property
    def fsid(self):
        return PROTOCOL_NAME

    def sign(self, path, expiration=None, **kwargs):
        """We do not support to create a signed URL representing the given path in pvfs."""
        raise Exception(
            "Sign is not implemented for Paimon Virtual FileSystem."
        )

    def ls(self, path, detail=True, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        rest_api = self.__rest_api(pvfs_identifier)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            databases = rest_api.list_databases()
            if detail:
                return [
                    self._create_dir_detail(
                        self._convert_database_virtual_path(pvfs_identifier.catalog, database)
                    )
                    for database in databases
                ]
            return [
                self._convert_database_virtual_path(pvfs_identifier.catalog, database)
                for database in databases
            ]
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            tables = rest_api.list_tables(pvfs_identifier.database)
            if detail:
                return [
                    self._create_dir_detail(
                        self._convert_table_virtual_path(pvfs_identifier.catalog, pvfs_identifier.database, table)
                    )
                    for table in tables
                ]
            return [
                self._convert_table_virtual_path(pvfs_identifier.catalog, pvfs_identifier.database, table)
                for table in tables
            ]
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_path = self._get_table_store(rest_api, pvfs_identifier).path
            storage_type = self._get_storage_type(table_path)
            storage_location = table_path
            actual_path = pvfs_identifier.get_actual_path(storage_location)
            virtual_location = pvfs_identifier.get_virtual_location()
            fs = self._get_filesystem(pvfs_identifier, storage_type)
            entries = fs.ls(actual_path, detail=detail, **kwargs)
            if detail:
                virtual_entities = [
                    self._convert_actual_info(entry, storage_type, storage_location, virtual_location)
                    for entry in entries
                ]
                return virtual_entities
            else:
                virtual_entry_paths = [
                    self._convert_actual_path(
                        storage_type, entry_path, storage_location, virtual_location
                    )
                    for entry_path in entries
                ]
                return virtual_entry_paths

    def info(self, path, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            return self._create_dir_detail('{}://{}'.format(PROTOCOL_NAME, pvfs_identifier.catalog))
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            return self._create_dir_detail(
                self._convert_database_virtual_path(pvfs_identifier.catalog, pvfs_identifier.database)
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            rest_api = self.__rest_api(pvfs_identifier)
            table_path = self._get_table_store(rest_api, pvfs_identifier).path
            storage_type = self._get_storage_type(table_path)
            storage_location = table_path
            actual_path = pvfs_identifier.get_actual_path(storage_location)
            virtual_location = pvfs_identifier.get_virtual_location()
            fs = self._get_filesystem(pvfs_identifier, storage_type)
            entry = fs.info(actual_path)
            return self._convert_actual_info(entry, storage_type, storage_location, virtual_location)

    def exists(self, path, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            return True
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            rest_api = self.__rest_api(pvfs_identifier)
            try:
                rest_api.get_database(pvfs_identifier.database)
                return True
            except NoSuchResourceException:
                return False
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            rest_api = self.__rest_api(pvfs_identifier)
            try:
                table_path = self._get_table_store(rest_api, pvfs_identifier).path
                if table_path is not None and pvfs_identifier.sub_path is None:
                    return True
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.exists(actual_path)
            except NoSuchResourceException:
                return False

    def cp_file(self, path1, path2, **kwargs):
        source = self._extract_pvfs_identifier(path1)
        target = self._extract_pvfs_identifier(path2)
        if ((isinstance(source, PVFSTableIdentifier)
             and isinstance(target, PVFSTableIdentifier))
                and target.sub_path is not None
                and source.sub_path is not None
                and source == target):
            rest_api = self.__rest_api(source)
            table_path = self._get_table_store(rest_api, source).path
            storage_type = self._get_storage_type(table_path)
            storage_location = table_path
            source_actual_path = source.get_actual_path(storage_location)
            target_actual_path = target.get_actual_path(storage_location)
            fs = self._get_filesystem(source, storage_type)
            fs.cp_file(
                self._strip_storage_protocol(storage_type, source_actual_path),
                self._strip_storage_protocol(storage_type, target_actual_path),
            )
            return None
        raise Exception("cp is not supported for path: {} to path: {}".format(path1, path2))

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        source = self._extract_pvfs_identifier(path1)
        target = self._extract_pvfs_identifier(path2)
        if (isinstance(source, PVFSTableIdentifier) and
                isinstance(target, PVFSTableIdentifier) and
                target.catalog == source.catalog):
            rest_api = self.__rest_api(source)
            if target.sub_path is None and source.sub_path is None:
                source_identifier = Identifier.create(source.database, source.table)
                target_identifier = Identifier.create(target.database, target.table)
                rest_api.rename_table(source_identifier, target_identifier)
                return None
            elif target.sub_path is not None and source.sub_path is not None and target == source:
                table_path = self._get_table_store(rest_api, source).path
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                source_actual_path = source.get_actual_path(storage_location)
                target_actual_path = target.get_actual_path(storage_location)
                fs = self._get_filesystem(source, storage_type)
                if storage_type == StorageType.LOCAL:
                    fs.mv(
                        self._strip_storage_protocol(storage_type, source_actual_path),
                        self._strip_storage_protocol(storage_type, target_actual_path),
                        recursive=recursive,
                        maxdepth=maxdepth
                    )
                else:
                    fs.mv(
                        self._strip_storage_protocol(storage_type, source_actual_path),
                        self._strip_storage_protocol(storage_type, target_actual_path),
                    )
                return None
        raise Exception("Mv is not supported for path: {} to path: {}".format(path1, path2))

    def rm(self, path, recursive=False, maxdepth=None):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        rest_api = self.__rest_api(pvfs_identifier)
        if isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            database_name = pvfs_identifier.database
            if not recursive and len(rest_api.list_tables(database_name)) > 0:
                raise Exception('Recursive is False but database is not empty')
            rest_api.drop_database(database_name)
            if self._table_cache:
                for table in rest_api.list_tables(database_name):
                    table_pvfs_identifier = PVFSTableIdentifier(
                        pvfs_identifier.catalog, database_name, table
                    ).name()
                    self._table_cache.pop(table_pvfs_identifier)
            return True
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = pvfs_identifier.get_identifier()
            table_path = self._get_table_store(rest_api, pvfs_identifier).path
            if pvfs_identifier.sub_path is None:
                rest_api.drop_table(table_identifier)
                self._table_cache.pop(table_identifier.name())
                return True
            storage_type = self._get_storage_type(table_path)
            storage_location = table_path
            actual_path = pvfs_identifier.get_actual_path(storage_location)
            fs = self._get_filesystem(pvfs_identifier, storage_type)
            return fs.rm(
                self._strip_storage_protocol(storage_type, actual_path),
                recursive,
                maxdepth,
            )
        raise Exception(
            "Rm is not supported for path: {}.".format(path)
        )

    def rm_file(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSTableIdentifier):
            rest_api = self.__rest_api(pvfs_identifier)
            table_path = self._get_table_store(rest_api, pvfs_identifier).path
            if pvfs_identifier.sub_path is not None:
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.rm_file(
                    self._strip_storage_protocol(storage_type, actual_path),
                )
        raise Exception("Rm is not supported for path: {}.".format(path))

    def rmdir(self, path):
        files = self.ls(path)
        if len(files) == 0:
            pvfs_identifier = self._extract_pvfs_identifier(path)
            rest_api = self.__rest_api(pvfs_identifier)
            if isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
                database_name = pvfs_identifier.database
                rest_api.drop_database(database_name)
                return True
            elif isinstance(pvfs_identifier, PVFSTableIdentifier):
                table_identifier = pvfs_identifier.get_identifier()
                table_path = self._get_table_store(rest_api, pvfs_identifier).path
                if pvfs_identifier.sub_path is None:
                    rest_api.drop_table(table_identifier)
                    self._cache.pop(pvfs_identifier)
                    return True
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.rmdir(
                    self._strip_storage_protocol(storage_type, actual_path)
                )
            raise Exception("Rm dir is not supported for path: {}.".format(path))
        else:
            raise Exception("Rm dir is not supported for path: {} as it is not empty.".format(path))

    def open(
            self,
            path,
            mode="rb",
            block_size=None,
            cache_options=None,
            compression=None,
            **kwargs
    ):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception("open is not supported for path: {}".format(path))
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            raise Exception(
                "open is not supported for path: {}".format(path)
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            rest_api = self.__rest_api(pvfs_identifier)
            table_path = self._get_table_store(rest_api, pvfs_identifier).path
            if pvfs_identifier.sub_path is None:
                raise Exception("open is not supported for path: {}".format(path))
            else:
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.open(
                    self._strip_storage_protocol(storage_type, actual_path),
                    mode,
                    block_size,
                    cache_options,
                    compression,
                    **kwargs
                )

    def mkdir(self, path, create_parents=True, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        rest_api = self.__rest_api(pvfs_identifier)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception("mkdir is not supported for path: {}".format(path))
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            rest_api.create_database(pvfs_identifier.database, {})
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_path: str
            if pvfs_identifier.sub_path is None:
                if create_parents:
                    try:
                        rest_api.create_database(pvfs_identifier.database, {})
                    except AlreadyExistsException:
                        pass
                self._create_object_table(pvfs_identifier)
            else:
                if create_parents:
                    try:
                        rest_api.create_database(pvfs_identifier.database, {})
                    except AlreadyExistsException:
                        pass
                    try:
                        table_path = self._get_table_store(rest_api, pvfs_identifier).path
                    except NoSuchResourceException:
                        try:
                            self._create_object_table(pvfs_identifier)
                        except AlreadyExistsException:
                            pass
                        finally:
                            table_path = self._get_table_store(rest_api, pvfs_identifier).path
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.mkdir(
                    self._strip_storage_protocol(storage_type, actual_path),
                    create_parents,
                    **kwargs
                )

    def makedirs(self, path, exist_ok=True):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        rest_api = self.__rest_api(pvfs_identifier)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception("makedirs is not supported for path: {}".format(path))
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            try:
                rest_api.create_database(pvfs_identifier.catalog, {})
            except AlreadyExistsException as e:
                if exist_ok:
                    pass
                raise e
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            if pvfs_identifier.sub_path is None:
                try:
                    self._create_object_table(pvfs_identifier)
                except AlreadyExistsException as e:
                    if exist_ok:
                        pass
                    raise e
            else:
                try:
                    self._create_object_table(pvfs_identifier)
                except AlreadyExistsException as e:
                    if exist_ok:
                        pass
                    else:
                        raise e
                table_path = self._get_table_store(rest_api, pvfs_identifier).path
                storage_type = self._get_storage_type(table_path)
                storage_location = table_path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.makedirs(
                    self._strip_storage_protocol(storage_type, actual_path),
                    exist_ok
                )

    def created(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        rest_api = self.__rest_api(pvfs_identifier)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception("created is not supported for path: {}".format(path))
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            return self.__converse_ts_to_datatime(rest_api.get_database(pvfs_identifier.database).created_at)
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            if pvfs_identifier.sub_path is None:
                return self._get_table_store(rest_api, pvfs_identifier).created
            else:
                table = self._get_table_store(rest_api, pvfs_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.created(
                    self._strip_storage_protocol(storage_type, actual_path)
                )

    def modified(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        rest_api = self.__rest_api(pvfs_identifier)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception("modified is not supported for path: {}".format(path))
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            return self.__converse_ts_to_datatime(rest_api.get_database(pvfs_identifier.database).updated_at)
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = pvfs_identifier.get_identifier()
            if pvfs_identifier.sub_path is None:
                return self._get_table_store(rest_api, pvfs_identifier).modified
            else:
                table = rest_api.get_table(table_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.modified(
                    self._strip_storage_protocol(storage_type, actual_path)
                )

    def cat_file(self, path, start=None, end=None, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception(
                "cat file is not supported for path: {}".format(path)
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            raise Exception(
                "cat file is not supported for path: {}".format(path)
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            if pvfs_identifier.sub_path is None:
                raise Exception(
                    "cat file is not supported for path: {}".format(path)
                )
            else:
                rest_api = self.__rest_api(pvfs_identifier)
                table = self._get_table_store(rest_api, pvfs_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.cat_file(
                    self._strip_storage_protocol(storage_type, actual_path),
                    start,
                    end,
                    **kwargs,
                )

    def get_file(self, rpath, lpath, callback=None, outfile=None, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(rpath)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception(
                "get file is not supported for path: {}".format(rpath)
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            raise Exception(
                "get file is not supported for path: {}".format(rpath)
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            rest_api = self.__rest_api(pvfs_identifier)
            if pvfs_identifier.sub_path is None:
                raise Exception(
                    "get file is not supported for path: {}".format(rpath)
                )
            else:
                table = self._get_table_store(rest_api, pvfs_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.get_file(
                    self._strip_storage_protocol(storage_type, actual_path),
                    lpath,
                    **kwargs
                )

    def _rm(self, path):
        raise Exception(
            "_rm is not implemented for Paimon Virtual FileSystem."
        )

    def _create_object_table(self, pvfs_identifier: PVFSTableIdentifier):
        rest_api = self.__rest_api(pvfs_identifier)
        schema = Schema(options={'type': 'object-table'})
        table_identifier = pvfs_identifier.get_identifier()
        rest_api.create_table(table_identifier, schema)

    @staticmethod
    def __converse_ts_to_datatime(ts: int):
        return datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)

    @staticmethod
    def _strip_storage_protocol(storage_type: StorageType, path: str):
        if storage_type == StorageType.LOCAL:
            return path[len("{}:".format(StorageType.LOCAL.value)):]

        # OSS has different behavior than S3 and GCS, if we do not remove the
        # protocol, it will always return an empty array.
        if storage_type == StorageType.OSS:
            if path.startswith("{}://".format(StorageType.OSS.value)):
                return path[len("{}://".format(StorageType.OSS.value)):]
            return path

        raise Exception(
            "Storage type:{} doesn't support now.".format(storage_type)
        )

    @staticmethod
    def _convert_actual_info(
            entry: Dict,
            storage_type: StorageType,
            storage_location: str,
            virtual_location: str,
    ):
        path = PaimonVirtualFileSystem._convert_actual_path(storage_type, entry["name"], storage_location,
                                                            virtual_location)

        if "mtime" in entry:
            # HDFS and GCS
            return PaimonVirtualFileSystem._create_file_detail(path, entry["size"], entry["type"], entry["mtime"])
        elif "LastModified" in entry:
            # S3 and OSS
            return PaimonVirtualFileSystem._create_file_detail(path, entry["size"], entry["type"],
                                                               entry["LastModified"])
        # Unknown
        return PaimonVirtualFileSystem._create_file_detail(path, entry["size"], entry["type"])

    @staticmethod
    def _create_dir_detail(path: str) -> Dict[str, Any]:
        return PaimonVirtualFileSystem._create_file_detail(path, 0, 'directory', None)

    @staticmethod
    def _create_file_detail(name: str, size: int, type: str, mtime: int = None) -> Dict[str, Any]:
        return {
            "name": name,
            "size": size,
            "type": type,
            "mtime": mtime,
        }

    @staticmethod
    def _convert_database_virtual_path(
            catalog_name: str,
            database_name: str
    ):
        return '{}://{}/{}'.format(PROTOCOL_NAME, catalog_name, database_name)

    @staticmethod
    def _convert_table_virtual_path(
            catalog_name: str,
            database_name: str,
            table_name: str
    ):
        return '{}://{}/{}/{}'.format(PROTOCOL_NAME, catalog_name, database_name, table_name)

    @staticmethod
    def _convert_actual_path(
            storage_type: StorageType,
            actual_path: str,
            storage_location: str,
            virtual_location: str,
    ):
        actual_path = PaimonVirtualFileSystem._get_path_without_schema(storage_type, actual_path)
        storage_location = PaimonVirtualFileSystem._get_path_without_schema(storage_type, storage_location)
        normalized_pvfs = virtual_location.rstrip('/')
        sub_location = actual_path[len(storage_location):].lstrip("/")
        if len(sub_location) == 0:
            return normalized_pvfs
        else:
            return '{}/{}'.format(normalized_pvfs, sub_location)

    @staticmethod
    def _get_path_without_schema(storage_type: StorageType, path: str) -> str:
        if storage_type == StorageType.LOCAL and path.startswith(StorageType.LOCAL.value):
            return path[len("{}://".format(StorageType.LOCAL.value)):]
        elif storage_type == StorageType.OSS and path.startswith(StorageType.OSS.value):
            return path[len("{}://".format(StorageType.OSS.value)):]
        return path

    def _extract_pvfs_identifier(self, path: str) -> Optional['PVFSIdentifier']:
        if not isinstance(path, str):
            raise Exception("path is not a string")
        path_without_protocol = path
        if path.startswith('{}://'.format(PROTOCOL_NAME)):
            path_without_protocol = path[7:]

        if not path_without_protocol:
            return None

        components = [component for component in path_without_protocol.rstrip('/').split('/') if component]
        catalog: str = None
        endpoint: str = self.options.get(CatalogOptions.URI)
        if len(components) > 0:
            if '.' in components[0]:
                (catalog, endpoint) = components[0].split('.', 1)
            else:
                catalog = components[0]
        if len(components) == 0:
            return None
        elif len(components) == 1:
            return (PVFSCatalogIdentifier(endpoint=endpoint, catalog=catalog))
        elif len(components) == 2:
            return PVFSDatabaseIdentifier(endpoint=endpoint, catalog=catalog, database=components[1])
        elif len(components) == 3:
            return PVFSTableIdentifier(endpoint=endpoint, catalog=catalog, database=components[1], table=components[2])
        elif len(components) > 3:
            sub_path = '/'.join(components[3:])
            return PVFSTableIdentifier(
                endpoint=endpoint,
                catalog=catalog, database=components[1],
                table=components[2], sub_path=sub_path
            )
        return None

    def _get_table_store(self, rest_api: RESTApi, pvfs_identifier: PVFSTableIdentifier) -> Optional['TableStore']:
        if not self._cache_enable:
            table = rest_api.get_table(Identifier.create(pvfs_identifier.database, pvfs_identifier.table))
            return self._create_table_store(table)
        read_lock = self._table_cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[str, TableStore] = self._table_cache.get(
                pvfs_identifier.name()
            )
            if cache_value is not None:
                return cache_value
        finally:
            read_lock.release()
        write_lock = self._table_cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            table = rest_api.get_table(Identifier.create(pvfs_identifier.database, pvfs_identifier.table))
            if table is not None:
                table_store = self._create_table_store(table)
                self._table_cache[pvfs_identifier.name()] = table_store
                return table_store
            else:
                return None
        finally:
            write_lock.release()

    def _create_table_store(self, table: GetTableResponse):
        created = self.__converse_ts_to_datatime(table.created_at)
        modified = self.__converse_ts_to_datatime(table.updated_at)
        return TableStore(path=table.path, created=created, modified=modified)

    def __rest_api(self, pvfs_identifier: PVFSIdentifier):
        read_lock = self._rest_api_cache_lock.gen_rlock()
        catalog = pvfs_identifier.catalog
        if pvfs_identifier.endpoint is None or catalog is None:
            raise ValueError("Endpoint or catalog is not set.")
        key = pvfs_identifier.get_cache_key()
        try:
            read_lock.acquire()
            rest_api = self._rest_api_cache.get(key)
            if rest_api is not None:
                return rest_api
        finally:
            read_lock.release()

        write_lock = self._rest_api_cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            rest_api = self._rest_api_cache.get(key)
            if rest_api is None:
                options = self.options.copy()
                options.data.update(
                    {CatalogOptions.WAREHOUSE.key(): catalog, CatalogOptions.URI.key(): pvfs_identifier.endpoint})
                rest_api = RESTApi(options)
                self._rest_api_cache[catalog] = rest_api
            return rest_api
        finally:
            write_lock.release()

    def _get_filesystem(self, pvfs_table_identifier: PVFSTableIdentifier, storage_type: StorageType) -> 'FileSystem':
        read_lock = self._fs_cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[StorageType, AbstractFileSystem] = self._fs_cache.get(
                storage_type
            )
            if cache_value is not None:
                return cache_value
        finally:
            read_lock.release()

        write_lock = self._table_cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: PaimonRealStorage = self._fs_cache.get(pvfs_table_identifier)
            if cache_value is not None and cache_value.need_refresh() is False:
                return cache_value.file_system
            if storage_type == StorageType.LOCAL:
                fs = LocalFileSystem()
            elif storage_type == StorageType.OSS:
                rest_api = self.__rest_api(pvfs_table_identifier)
                load_token_response: GetTableTokenResponse = rest_api.load_table_token(
                    Identifier.create(pvfs_table_identifier.database, pvfs_table_identifier.table))
                fs = self._get_oss_filesystem(load_token_response.token)
                paimon_real_storage = PaimonRealStorage(
                    token=load_token_response.token,
                    expires_at_millis=load_token_response.expires_at_millis,
                    file_system=fs
                )
                self._fs_cache[pvfs_table_identifier] = paimon_real_storage
            else:
                raise Exception(
                    "Storage type: `{}` doesn't support now.".format(storage_type)
                )
            return fs
        finally:
            write_lock.release()

    @staticmethod
    def _get_storage_type(path: str):
        if path.startswith("{}:/".format(StorageType.LOCAL.value)):
            return StorageType.LOCAL
        elif path.startswith("{}://".format(StorageType.OSS.value)):
            return StorageType.OSS
        raise Exception(
            "Storage type doesn't support now. Path:{}".format(path)
        )

    @staticmethod
    def _get_oss_filesystem(options: Options) -> AbstractFileSystem:
        access_key_id = options.get(OssOptions.OSS_ACCESS_KEY_ID)
        if access_key_id is None:
            raise Exception(
                "OSS access key id is not found in the options."
            )

        access_key_secret = options.get(
            OssOptions.OSS_ACCESS_KEY_SECRET
        )
        if access_key_secret is None:
            raise Exception(
                "OSS access key secret is not found in the options."
            )
        oss_endpoint_url = options.get(OssOptions.OSS_ENDPOINT)
        if oss_endpoint_url is None:
            raise Exception(
                "OSS endpoint url is not found in the options."
            )
        token = options.get(OssOptions.OSS_SECURITY_TOKEN)
        return importlib.import_module("ossfs").OSSFileSystem(
            key=access_key_id,
            secret=access_key_secret,
            token=token,
            endpoint=oss_endpoint_url
        )
