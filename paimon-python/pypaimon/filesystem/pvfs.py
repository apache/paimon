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

from pypaimon.api import RESTApi, GetTableTokenResponse
from pypaimon.api.typedef import Identifier
from pypaimon.filesystem.pvfs_config import PVFSConfig

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


@dataclass
class PaimonRealStorage:
    token: Dict[str, str]
    expires_at_millis: Optional[int]
    file_system: AbstractFileSystem


class PaimonVirtualFileSystem(fsspec.AbstractFileSystem):
    rest_api: RESTApi
    options: Dict[str, Any]

    _identifier_pattern = re.compile("^pvfs://([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$")

    def __init__(self, options: Dict = None, **kwargs):
        self.rest_api = RESTApi(options)
        self.options = options
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
            "Sign is not implemented for Gravitino Virtual FileSystem."
        )

    def ls(self, path, detail=True, **kwargs):
        pvfs_identifier = self._extract_pvfs_identifier(path)
        if isinstance(pvfs_identifier, PVFSCatalogIdentifier):
            databases = self.rest_api.list_databases(pvfs_identifier.name)
            return [
                self._convert_database_actual_path(pvfs_identifier.name, database)
                for database in databases
            ]
        elif isinstance(pvfs_identifier, PVFSDatabaseIdentifier):
            tables = self.rest_api.list_tables(pvfs_identifier.name)
            return [
                self._convert_table_actual_path(pvfs_identifier.catalog, pvfs_identifier.name, table)
                for table in tables
            ]
        elif isinstance(pvfs_identifier, PVFSTableIdentifier):
            table = self.rest_api.get_table(Identifier.create(pvfs_identifier.database, pvfs_identifier.name))
            storage_type = self._get_storage_type(table.path)
            fs = self._get_filesystem(pvfs_identifier, storage_type)
            virtual_location = (f'{PROTOCOL_NAME}://{pvfs_identifier.catalog}'
                                f'/{pvfs_identifier.database}/{pvfs_identifier.name}')
            actual_path = table.path
            if pvfs_identifier.sub_path:
                actual_path = f'{table.path.rstrip("/")}/{pvfs_identifier.sub_path.lstrip("/")}'
            storage_location = table.path
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

    def _convert_actual_info(
            self,
            entry: Dict,
            storage_type: StorageType,
            storage_location: str,
            virtual_location: str,
    ):
        path = self._convert_actual_path(storage_type, entry["name"], storage_location, virtual_location)

        if "mtime" in entry:
            # HDFS and GCS
            return {
                "name": path,
                "size": entry["size"],
                "type": entry["type"],
                "mtime": entry["mtime"],
            }

        if "LastModified" in entry:
            # S3 and OSS
            return {
                "name": path,
                "size": entry["size"],
                "type": entry["type"],
                "mtime": entry["LastModified"],
            }

        # Unknown
        return {
            "name": path,
            "size": entry["size"],
            "type": entry["type"],
            "mtime": None,
        }

    @staticmethod
    def _convert_database_actual_path(
            catalog_name: str,
            database_name: str
    ):
        return f'pvfs://{catalog_name}/{database_name}'

    @staticmethod
    def _convert_table_actual_path(
            catalog_name: str,
            database_name: str,
            table_name: str
    ):
        return f'pvfs://{catalog_name}/{database_name}/{table_name}'

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
        if not isinstance(path, str) or not path.startswith('pvfs://'):
            return None

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
