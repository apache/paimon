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

import importlib
import re
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional, Tuple

from cachetools import LRUCache
from readerwriterlock import rwlock

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from pypaimon.api import RESTApi, GetTableTokenResponse, Schema
from pypaimon.api.client import NoSuchResourceException, AlreadyExistsException
from pypaimon.api.typedef import Identifier, RESTCatalogOptions
from pypaimon.pvfs.pvfs_config import PVFSConfig

PROTOCOL_NAME = "pvfs"


class StorageType(Enum):
    LOCAL = "file"
    OSS = "oss"


class PVFSIdentifier(ABC):
    name: str


@dataclass
class PVFSCatalogIdentifier(PVFSIdentifier):
    name: str


@dataclass
class PVFSDatabaseIdentifier(PVFSIdentifier):
    catalog: str
    name: str


@dataclass
class PVFSTableIdentifier(PVFSIdentifier):
    catalog: str
    database: str
    name: str
    sub_path: str = None

    def __hash__(self) -> int:
        return hash((self.catalog, self.database, self.name))

    def get_actual_path(self, storage_location: str):
        if self.sub_path:
            return f'{storage_location.rstrip("/")}/{self.sub_path.lstrip("/")}'
        return storage_location

    def get_virtual_location(self):
        return (f'{PROTOCOL_NAME}://{self.catalog}'
                f'/{self.database}/{self.name}')


@dataclass
class PaimonRealStorage:
    token: Dict[str, str]
    expires_at_millis: Optional[int]
    file_system: AbstractFileSystem


class PaimonVirtualFileSystem(fsspec.AbstractFileSystem):
    rest_api: RESTApi
    warehouse: str
    options: Dict[str, Any]

    protocol = PROTOCOL_NAME
    _identifier_pattern = re.compile("^pvfs://([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$")

    def __init__(self, options: Dict = None, **kwargs):
        self.rest_api = RESTApi(options)
        self.options = options
        self.warehouse = options.get(RESTCatalogOptions.WAREHOUSE)
        cache_size = (
            PVFSConfig.DEFAULT_CACHE_SIZE
            if options is None
            else options.get(PVFSConfig.CACHE_SIZE, PVFSConfig.DEFAULT_CACHE_SIZE)
        )
        self._cache = LRUCache(maxsize=cache_size)
        self._cache_lock = rwlock.RWLockFair()
        super().__init__(**kwargs)

    @property
    def fsid(self):
        return PROTOCOL_NAME

    def sign(self, path, expiration=None, **kwargs):
        """We do not support to create a signed URL representing the given path in gvfs."""
        raise Exception(
            "Sign is not implemented for Paimon Virtual FileSystem."
        )

    def ls(self, path, detail=True, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            databases = self.rest_api.list_databases()
            if detail:
                return [
                    self._create_dir_detail(
                        self._convert_database_virtual_path(pvfs_identifier.name, database)
                    )
                    for database in databases
                ]
            return [
                self._convert_database_virtual_path(pvfs_identifier.name, database)
                for database in databases
            ]
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            tables = self.rest_api.list_tables(pvfs_identifier.name)
            if detail:
                return [
                    self._create_dir_detail(
                        self._convert_table_virtual_path(pvfs_identifier.catalog, pvfs_identifier.name, table)
                    )
                    for table in tables
                ]
            return [
                self._convert_table_virtual_path(pvfs_identifier.catalog, pvfs_identifier.name, table)
                for table in tables
            ]
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table = self.rest_api.get_table(Identifier.create(pvfs_identifier.database, pvfs_identifier.name))
            storage_type = self._get_storage_type(table.path)
            storage_location = table.path
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
            return self._create_dir_detail(f'{PROTOCOL_NAME}://{pvfs_identifier.name}')
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            return self._create_dir_detail(
                self._convert_database_virtual_path(pvfs_identifier.catalog, pvfs_identifier.name)
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table = self.rest_api.get_table(Identifier.create(pvfs_identifier.database, pvfs_identifier.name))
            storage_type = self._get_storage_type(table.path)
            storage_location = table.path
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
            try:
                self.rest_api.get_database(pvfs_identifier.name)
                return True
            except NoSuchResourceException:
                return False
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            try:
                table = self.rest_api.get_table(Identifier.create(pvfs_identifier.database, pvfs_identifier.name))
                if pvfs_identifier.sub_path is None:
                    return True
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.exists(actual_path)
            except NoSuchResourceException:
                return False

    def cp_file(self, path1, path2, **kwargs):
        source_pvfs_identifier = self._extract_pvfs_identifier(path1)
        target_pvfs_identifier = self._extract_pvfs_identifier(path2)
        if ((isinstance(source_pvfs_identifier, PVFSTableIdentifier)
             and isinstance(target_pvfs_identifier, PVFSTableIdentifier))
                and target_pvfs_identifier.sub_path is not None
                and source_pvfs_identifier.sub_path is not None):
            table_identifier = Identifier.create(source_pvfs_identifier.database, source_pvfs_identifier.name)
            table = self.rest_api.get_table(table_identifier)
            storage_type = self._get_storage_type(table.path)
            storage_location = table.path
            source_actual_path = source_pvfs_identifier.get_actual_path(storage_location)
            target_actual_path = target_pvfs_identifier.get_actual_path(storage_location)
            fs = self._get_filesystem(source_pvfs_identifier, storage_type)
            fs.cp_file(
                self._strip_storage_protocol(storage_type, source_actual_path),
                self._strip_storage_protocol(storage_type, target_actual_path),
            )
            return None
        raise Exception(
            f"cp is not supported for path: {path1}"
        )

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        source_pvfs_identifier = self._extract_pvfs_identifier(path1)
        if isinstance(source_pvfs_identifier, PVFSTableIdentifier):
            target_pvfs_identifier = self._extract_pvfs_identifier(path2)
            if isinstance(target_pvfs_identifier, PVFSTableIdentifier):
                if target_pvfs_identifier.sub_path is None and source_pvfs_identifier.sub_path is None:
                    source_identifier = Identifier.create(source_pvfs_identifier.database, source_pvfs_identifier.name)
                    target_identifier = Identifier.create(target_pvfs_identifier.database, target_pvfs_identifier.name)
                    self.rest_api.rename_table(source_identifier, target_identifier)
                    return None
        raise Exception(
            f"Mv is not supported for path: {path1}"
        )

    def lazy_load_class(self, module_name, class_name):
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    def rm(self, path, recursive=False, maxdepth=None):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            database_name = pvfs_identifier.name
            if recursive:
                for table_name in self.rest_api.list_tables(database_name):
                    self.rest_api.drop_table(Identifier.create(database_name, table_name))
            self.rest_api.drop_database(database_name)
            return True
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            table = self.rest_api.get_table(table_identifier)
            if pvfs_identifier.sub_path is None:
                self.rest_api.drop_table(table_identifier)
                return True
            storage_type = self._get_storage_type(table.path)
            storage_location = table.path
            actual_path = pvfs_identifier.get_actual_path(storage_location)
            fs = self._get_filesystem(pvfs_identifier, storage_type)
            return fs.rm(
                self._strip_storage_protocol(storage_type, actual_path),
                recursive,
                maxdepth,
            )
        raise Exception(
            f"Rm is not supported for path: {path}."
        )

    def rm_file(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            table = self.rest_api.get_table(table_identifier)
            if pvfs_identifier.sub_path is not None:
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.rm_file(
                    self._strip_storage_protocol(storage_type, actual_path),
                )
        raise Exception(
            f"Rm file is not supported for path: {path}."
        )

    def rmdir(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            database_name = pvfs_identifier.name
            for table_name in self.rest_api.list_tables(database_name):
                self.rest_api.drop_table(Identifier.create(database_name, table_name))
            self.rest_api.drop_database(database_name)
            return True
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            table = self.rest_api.get_table(table_identifier)
            if pvfs_identifier.sub_path is None:
                self.rest_api.drop_table(table_identifier)
                return True
            storage_type = self._get_storage_type(table.path)
            storage_location = table.path
            actual_path = pvfs_identifier.get_actual_path(storage_location)
            fs = self._get_filesystem(pvfs_identifier, storage_type)
            return fs.rmdir(
                self._strip_storage_protocol(storage_type, actual_path)
            )
        raise Exception(
            f"Rm dir is not supported for path: {path}."
        )

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
            raise Exception(
                f"open is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            raise Exception(
                f"open is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            table = self.rest_api.get_table(table_identifier)
            if pvfs_identifier.sub_path is None:
                raise Exception(
                    f"open is not supported for path: {path}"
                )
            else:
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
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
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception(
                f"mkdir is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            self.rest_api.create_database(pvfs_identifier.name, {})
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            if pvfs_identifier.sub_path is None:
                if create_parents:
                    try:
                        self.rest_api.create_database(pvfs_identifier.database, {})
                    except AlreadyExistsException:
                        pass
                self._create_object_table(pvfs_identifier)
            else:
                if create_parents:
                    try:
                        self.rest_api.create_database(pvfs_identifier.database, {})
                    except AlreadyExistsException:
                        pass
                    try:
                        self._create_object_table(pvfs_identifier)
                    except AlreadyExistsException:
                        pass
                table = self.rest_api.get_table(table_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.mkdir(
                    self._strip_storage_protocol(storage_type, actual_path),
                    create_parents,
                    **kwargs
                )

    def makedirs(self, path, exist_ok=True):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception(
                f"makedirs is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            try:
                self.rest_api.create_database(pvfs_identifier.name, {})
            except AlreadyExistsException as e:
                if exist_ok:
                    pass
                raise e
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
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
                    raise e
                table = self.rest_api.get_table(table_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.makedirs(
                    self._strip_storage_protocol(storage_type, actual_path),
                    exist_ok
                )

    def created(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception(
                f"created is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            return self.rest_api.get_database(pvfs_identifier.name).created_at
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            if pvfs_identifier.sub_path is None:
                return self.rest_api.get_table(table_identifier).created_at
            else:
                table = self.rest_api.get_table(table_identifier)
                storage_type = self._get_storage_type(table.path)
                storage_location = table.path
                actual_path = pvfs_identifier.get_actual_path(storage_location)
                fs = self._get_filesystem(pvfs_identifier, storage_type)
                return fs.created(
                    self._strip_storage_protocol(storage_type, actual_path)
                )

    def modified(self, path):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            raise Exception(
                f"modified is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            return self.rest_api.get_database(pvfs_identifier.name).updated_at
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            if pvfs_identifier.sub_path is None:
                return self.rest_api.get_table(table_identifier).updated_at
            else:
                table = self.rest_api.get_table(table_identifier)
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
                f"cat file is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            raise Exception(
                f"cat file is not supported for path: {path}"
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            if pvfs_identifier.sub_path is None:
                raise Exception(
                    f"cat file is not supported for path: {path}"
                )
            else:
                table = self.rest_api.get_table(table_identifier)
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
                f"get file is not supported for path: {rpath}"
            )
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            raise Exception(
                f"get file is not supported for path: {rpath}"
            )
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
            if pvfs_identifier.sub_path is None:
                raise Exception(
                    f"get file is not supported for path: {rpath}"
                )
            else:
                table = self.rest_api.get_table(table_identifier)
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
        schema = Schema(options={'type': 'object-table'})
        table_identifier = Identifier.create(pvfs_identifier.database, pvfs_identifier.name)
        self.rest_api.create_table(table_identifier, schema)

    @staticmethod
    def _strip_storage_protocol(storage_type: StorageType, path: str):
        if storage_type == StorageType.LOCAL:
            return path[len(f"{StorageType.LOCAL.value}:"):]

        # OSS has different behavior than S3 and GCS, if we do not remove the
        # protocol, it will always return an empty array.
        if storage_type == StorageType.OSS:
            if path.startswith(f"{StorageType.OSS.value}://"):
                return path[len(f"{StorageType.OSS.value}://"):]
            return path

        raise Exception(
            f"Storage type:{storage_type} doesn't support now."
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
        return f'{PROTOCOL_NAME}://{catalog_name}/{database_name}'

    @staticmethod
    def _convert_table_virtual_path(
            catalog_name: str,
            database_name: str,
            table_name: str
    ):
        return f'{PROTOCOL_NAME}://{catalog_name}/{database_name}/{table_name}'

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
            return f'{normalized_pvfs}/{sub_location}'

    @staticmethod
    def _get_path_without_schema(storage_type: StorageType, path: str) -> str:
        if storage_type == StorageType.LOCAL and path.startswith(StorageType.LOCAL.value):
            return path[len(f"{StorageType.LOCAL.value}://"):]
        elif storage_type == StorageType.OSS and path.startswith(StorageType.OSS.value):
            return path[len(f"{StorageType.OSS.value}://"):]
        return path

    @staticmethod
    def _extract_pvfs_identifier(path: str) -> Optional['PVFSIdentifier']:
        if not isinstance(path, str):
            raise Exception("path is not a string")
        path_without_protocol = path
        if path.startswith(f'{PROTOCOL_NAME}://'):
            path_without_protocol = path[7:]

        if not path_without_protocol:
            return None

        components = path_without_protocol.rstrip('/').split('/')

        components = [comp for comp in components if comp]

        if len(components) == 0:
            return None
        elif len(components) == 1:
            return PVFSCatalogIdentifier(components[0])
        elif len(components) == 2:
            return PVFSDatabaseIdentifier(catalog=components[0], name=components[1])
        elif len(components) == 3:
            return PVFSTableIdentifier(catalog=components[0], database=components[1], name=components[2])
        elif len(components) > 3:
            sub_path = '/'.join(components[3:])
            return PVFSTableIdentifier(
                catalog=components[0], database=components[1],
                name=components[2], sub_path=sub_path
            )
        return None

    def _get_filesystem(self, pvfs_table_identifier: PVFSTableIdentifier, storage_type: StorageType) -> 'FileSystem':
        read_lock = self._cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[StorageType, AbstractFileSystem] = self._cache.get(
                storage_type
            )
            if cache_value is not None:
                return cache_value
        finally:
            read_lock.release()

        write_lock = self._cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[PVFSTableIdentifier, AbstractFileSystem] = self._cache.get(
                pvfs_table_identifier
            )
            if cache_value is not None:
                return cache_value
            elif storage_type == StorageType.LOCAL:
                fs = LocalFileSystem()
            elif storage_type == StorageType.OSS:
                load_token_response: GetTableTokenResponse = self.rest_api.load_table_token(
                    Identifier.create(pvfs_table_identifier.database, pvfs_table_identifier.name))
                fs = self._get_oss_filesystem(load_token_response.token)
                paimon_real_storage = PaimonRealStorage(
                    token=load_token_response.token,
                    expires_at_millis=load_token_response.expires_at_millis,
                    file_system=fs
                )
                self._cache[pvfs_table_identifier] = paimon_real_storage
            else:
                raise Exception(
                    f"Storage type: `{storage_type}` doesn't support now."
                )
            return fs
        finally:
            write_lock.release()

    @staticmethod
    def _get_storage_type(path: str):
        if path.startswith(f"{StorageType.LOCAL.value}:/"):
            return StorageType.LOCAL
        elif path.startswith(f"{StorageType.OSS.value}://"):
            return StorageType.OSS
        raise Exception(
            f"Storage type doesn't support now. Path:{path}"
        )

    @staticmethod
    def _get_oss_filesystem(options: Dict[str, str]) -> AbstractFileSystem:
        access_key_id = options.get(PVFSConfig.OSS_ACCESS_KEY_ID)
        if access_key_id is None:
            raise Exception(
                "OSS access key id is not found in the options."
            )

        access_key_secret = options.get(
            PVFSConfig.OSS_ACCESS_KEY_SECRET
        )
        if access_key_secret is None:
            raise Exception(
                "OSS access key secret is not found in the options."
            )
        oss_endpoint_url = 'oss-cn-hangzhou.aliyuncs.com'
        if oss_endpoint_url is None:
            raise Exception(
                "OSS endpoint url is not found in the options."
            )
        token = options.get(PVFSConfig.OSS_SECURITY_TOKEN)
        return importlib.import_module("ossfs").OSSFileSystem(
            key=access_key_id,
            secret=access_key_secret,
            token=token,
            endpoint=oss_endpoint_url
        )
