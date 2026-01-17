"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import logging
import threading
import time
from typing import Optional, Union

from cachetools import TTLCache

from api.rest_util import RESTUtil
from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.common.file_io import FileIO
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.common.uri_reader import UriReaderFactory


class RESTTokenFileIO(FileIO):
    """
    A FileIO to support getting token from REST Server.
    """
    
    _FILE_IO_CACHE_MAXSIZE = 1000
    _FILE_IO_CACHE_TTL = 36000  # 10 hours in seconds
    
    def __init__(self, identifier: Identifier, path: str,
                 catalog_options: Optional[Union[dict, Options]] = None):
        self.identifier = identifier
        self.path = path
        # Convert dict to Options if needed for consistent API usage
        if catalog_options is None:
            self.catalog_options = None
        elif isinstance(catalog_options, dict):
            self.catalog_options = Options(catalog_options)
        else:
            # Assume it's already an Options object
            self.catalog_options = catalog_options
        self.properties = self.catalog_options or Options({})  # For compatibility with refresh_token()
        self.token: Optional[RESTToken] = None
        self.api_instance: Optional[RESTApi] = None
        self.lock = threading.Lock()
        self.log = logging.getLogger(__name__)
        self._uri_reader_factory_cache: Optional[UriReaderFactory] = None
        self._file_io_cache: TTLCache = TTLCache(
            maxsize=self._FILE_IO_CACHE_MAXSIZE,
            ttl=self._FILE_IO_CACHE_TTL
        )

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state.pop('lock', None)
        state.pop('api_instance', None)
        state.pop('_file_io_cache', None)
        state.pop('_uri_reader_factory_cache', None)
        # token can be serialized, but we'll refresh it on deserialization
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Recreate lock and cache after deserialization
        self.lock = threading.Lock()
        self._file_io_cache = TTLCache(
            maxsize=self._FILE_IO_CACHE_MAXSIZE,
            ttl=self._FILE_IO_CACHE_TTL
        )
        self._uri_reader_factory_cache = None
        # api_instance will be recreated when needed
        self.api_instance = None

    def file_io(self) -> FileIO:
        self.try_to_refresh_token()

        if self.token is None:
            return FileIO.get(self.path, self.catalog_options or Options({}))
        
        cache_key = self.token
        
        file_io = self._file_io_cache.get(cache_key)
        if file_io is not None:
            return file_io
        
        with self.lock:
            file_io = self._file_io_cache.get(cache_key)
            if file_io is not None:
                return file_io
            
            merged_token = self._merge_token_with_catalog_options(self.token.token)
            merged_properties = RESTUtil.merge(
                self.catalog_options.to_map() if self.catalog_options else {},
                merged_token
            )
            merged_options = Options(merged_properties)
            
            file_io = PyArrowFileIO(self.path, merged_options)
            self._file_io_cache[cache_key] = file_io
            return file_io

    def _merge_token_with_catalog_options(self, token: dict) -> dict:
        """Merge token with catalog options, DLF OSS endpoint should override the standard OSS endpoint."""
        merged_token = dict(token)
        if self.catalog_options:
            dlf_oss_endpoint = self.catalog_options.get(CatalogOptions.DLF_OSS_ENDPOINT)
            if dlf_oss_endpoint and dlf_oss_endpoint.strip():
                merged_token[OssOptions.OSS_ENDPOINT.key()] = dlf_oss_endpoint
        return merged_token

    def new_input_stream(self, path: str):
        return self.file_io().new_input_stream(path)

    def new_output_stream(self, path: str):
        return self.file_io().new_output_stream(path)

    def get_file_status(self, path: str):
        return self.file_io().get_file_status(path)

    def list_status(self, path: str):
        return self.file_io().list_status(path)

    def exists(self, path: str) -> bool:
        return self.file_io().exists(path)

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self.file_io().delete(path, recursive)

    def mkdirs(self, path: str) -> bool:
        return self.file_io().mkdirs(path)

    def rename(self, src: str, dst: str) -> bool:
        return self.file_io().rename(src, dst)

    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        return self.file_io().copy_file(source_path, target_path, overwrite)

    def to_filesystem_path(self, path: str) -> str:
        return self.file_io().to_filesystem_path(path)

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        return self.file_io().try_to_write_atomic(path, content)

    def write_parquet(self, path: str, data, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        return self.file_io().write_parquet(path, data, compression, zstd_level, **kwargs)

    def write_orc(self, path: str, data, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        return self.file_io().write_orc(path, data, compression, zstd_level, **kwargs)

    def write_avro(self, path: str, data, avro_schema=None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        return self.file_io().write_avro(path, data, avro_schema, compression, zstd_level, **kwargs)

    def write_lance(self, path: str, data, **kwargs):
        return self.file_io().write_lance(path, data, **kwargs)

    def write_blob(self, path: str, data, blob_as_descriptor: bool, **kwargs):
        return self.file_io().write_blob(path, data, blob_as_descriptor, **kwargs)

    @property
    def uri_reader_factory(self):
        if self._uri_reader_factory_cache is None:
            catalog_options = self.catalog_options or Options({})
            self._uri_reader_factory_cache = UriReaderFactory(catalog_options)
        
        return self._uri_reader_factory_cache

    @property
    def filesystem(self):
        return self.file_io().filesystem

    def try_to_refresh_token(self):
        if self.should_refresh():
            with self.lock:
                if self.should_refresh():
                    self.refresh_token()

    def should_refresh(self):
        if self.token is None:
            return True
        current_time = int(time.time() * 1000)
        return (self.token.expire_at_millis - current_time) < RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS

    def refresh_token(self):
        self.log.info(f"begin refresh data token for identifier [{self.identifier}]")
        if self.api_instance is None:
            self.api_instance = RESTApi(self.properties, False)

        response = self.api_instance.load_table_token(self.identifier)
        self.log.info(
            f"end refresh data token for identifier [{self.identifier}] expiresAtMillis [{response.expires_at_millis}]"
        )
        self.token = RESTToken(response.token, response.expires_at_millis)

    def valid_token(self):
        self.try_to_refresh_token()
        return self.token

    def close(self):
        with self.lock:
            for file_io in self._file_io_cache.values():
                try:
                    file_io.close()
                except Exception as e:
                    self.log.warning(f"Error closing cached FileIO: {e}")
            self._file_io_cache.clear()
