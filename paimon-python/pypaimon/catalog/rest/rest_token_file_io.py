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

import logging
import os
import threading
import time
import weakref
from datetime import timedelta
from typing import Optional, Union

from cachetools import TTLCache

from pypaimon.api.auth.bearer import BearTokenAuthProvider
from pypaimon.api.auth.dlf_provider import DLFAuthProvider
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_util import RESTUtil
from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.common.file_io import FileIO
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

    # Process-wide like Java's FILE_IO_CACHE, so a token refresh keeps the FileIO
    # that streams opened with the previous token still use.
    _FILE_IO_CACHE = TTLCache(maxsize=_FILE_IO_CACHE_MAXSIZE, ttl=_FILE_IO_CACHE_TTL)
    _FILE_IO_CACHE_LOCK = threading.Lock()

    _TOKEN_CACHE: dict = {}
    _TOKEN_LOCKS: dict = {}
    _TOKEN_LOCKS_LOCK = threading.Lock()

    # Objects whose lock a forked child resets.
    _INSTANCES = weakref.WeakSet()

    # Cache misses log a summary at most this often; idle processes log nothing.
    _STATS_LOG_INTERVAL = 600
    _FILE_IO_STATS = {"created": 0, "evicted": 0, "warned": False, "next_log": 0.0,
                      "fds": None, "next_scan": 0.0}
    _SHORT_TOKEN_STATS = {"count": 0, "next_log": 0.0}

    def __init__(self, identifier: Identifier, path: str,
                 catalog_options: Optional[Union[dict, Options]] = None):
        self.identifier = identifier
        self.path = path
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
        self.log = logging.getLogger(__name__)
        self._uri_reader_factory_cache: Optional[UriReaderFactory] = None
        self._file_io_scope: Optional[str] = None
        self._token_lock = threading.Lock()
        self._register_for_fork()

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state.pop('api_instance', None)
        state.pop('_uri_reader_factory_cache', None)
        state.pop('_file_io_scope', None)
        state.pop('_token_lock', None)
        # token can be serialized, but we'll refresh it on deserialization
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._uri_reader_factory_cache = None
        # api_instance will be recreated when needed
        self.api_instance = None
        self._file_io_scope = None
        self._token_lock = threading.Lock()
        self._register_for_fork()

    def _register_for_fork(self):
        # The cache lock is held across fork, so a child never sees the set mid-update.
        with self._FILE_IO_CACHE_LOCK:
            RESTTokenFileIO._INSTANCES.add(self)

    @classmethod
    def _before_fork(cls):
        cls._FILE_IO_CACHE_LOCK.acquire()

    @classmethod
    def _after_fork_in_parent(cls):
        cls._FILE_IO_CACHE_LOCK.release()

    @classmethod
    def _after_fork_in_child(cls):
        # Parent threads that held these locks do not exist in the child.
        cls._FILE_IO_CACHE_LOCK = threading.Lock()
        cls._TOKEN_LOCKS_LOCK = threading.Lock()
        cls._TOKEN_LOCKS = {}
        for file_io in list(cls._INSTANCES):
            file_io._token_lock = threading.Lock()

    def file_io(self) -> FileIO:
        return self._file_io_with_token()[0]

    def _file_io_with_token(self, minimum_validity_millis: int = 0):
        with self._token_lock:
            if minimum_validity_millis:
                self.try_to_refresh_token(minimum_validity_millis)
                if not self._has_remaining_lifetime(
                        self.token, minimum_validity_millis):
                    raise ValueError(
                        "Requested presigned URL validity exceeds the remaining "
                        "REST credential lifetime after refresh.")
            else:
                self.try_to_refresh_token()

            token = self.token
            if token is None:
                return (FileIO.get(
                    self.path, self.catalog_options or Options({})), None)

            cache_key = (token, self._get_file_io_scope())
            with self._FILE_IO_CACHE_LOCK:
                cache = self._FILE_IO_CACHE
                file_io = cache.get(cache_key)
                if file_io is None:
                    file_io = FileIO.get(self.path, self._merged_options(token))
                    self._log_file_io_created(cache)
                # Re-inserting on every access expires idle entries only, like expireAfterAccess.
                cache[cache_key] = file_io
            return file_io, token

    def _log_file_io_created(self, cache):
        # Runs under the cache lock right before the new FileIO is inserted.
        stats = RESTTokenFileIO._FILE_IO_STATS
        stats["created"] += 1
        cache.expire()
        if len(cache) >= cache.maxsize:
            stats["evicted"] += 1
        now = time.monotonic()
        quiet = now < stats["next_log"]
        if quiet and stats["warned"]:
            return
        fds = self._sample_open_files(stats, now)
        crowded = stats["evicted"] > 0 or (fds is not None and fds[0] * 5 > fds[1] * 4)
        # The first sign of trouble is logged at once; anything else waits for the interval.
        if quiet and not crowded:
            return
        self.log.log(
            logging.WARNING if crowded else logging.INFO,
            "RESTTokenFileIO cache holds %d/%d FileIOs and %d tokens; %d FileIOs created and "
            "%d evicted for size since the last summary; open files %s",
            min(len(cache) + 1, cache.maxsize), cache.maxsize, len(self._TOKEN_CACHE),
            stats["created"], stats["evicted"], "unknown" if fds is None else "%d/%d" % fds)
        stats.update(created=0, evicted=0, warned=crowded,
                     next_log=now + self._STATS_LOG_INTERVAL)

    @staticmethod
    def _sample_open_files(stats, now):
        # Listing /proc/self/fd takes about 0.5 ms per 1,000 descriptors, so scan at most 1% of the time.
        if now >= stats["next_scan"]:
            stats["fds"] = _open_files()
            stats["next_scan"] = now + 100 * (time.monotonic() - now)
        return stats["fds"]

    def _get_file_io_scope(self) -> str:
        # FileIO.get binds its result to the path (e.g. the OSS bucket) and the catalog options.
        if self._file_io_scope is None:
            options = self.catalog_options.to_map() if self.catalog_options else {}
            self._file_io_scope = repr((self.path, sorted(
                (str(key), repr(value)) for key, value in options.items())))
        return self._file_io_scope

    def _merged_options(self, token: RESTToken) -> Options:
        merged_properties = RESTUtil.merge(
            self.catalog_options.to_map() if self.catalog_options else {},
            token.token
        )
        if self.catalog_options:
            dlf_oss_endpoint = self.catalog_options.get(CatalogOptions.DLF_OSS_ENDPOINT)
            if dlf_oss_endpoint and dlf_oss_endpoint.strip():
                merged_properties[OssOptions.OSS_ENDPOINT.key()] = dlf_oss_endpoint
        return Options(merged_properties)

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

    def create_blob_presigned_url(self, table_root, descriptor, validity) -> str:
        if table_root != self.path:
            raise ValueError("Table root does not match RESTTokenFileIO bound table root.")
        if not isinstance(validity, timedelta):
            raise TypeError("Blob presigned URL validity must be datetime.timedelta.")
        if validity <= timedelta(0) or validity.microseconds != 0:
            raise ValueError("Blob presigned URL validity must be positive whole seconds.")
        validity_millis = int(validity.total_seconds() * 1000)
        file_io, signing_token = self._file_io_with_token(validity_millis)
        url = file_io.create_blob_presigned_url(
            table_root, descriptor, validity)
        if self._has_remaining_lifetime(signing_token, validity_millis):
            return url

        # The first call materialized the range. Refresh and sign the cached
        # object again so the returned URL has the requested lifetime.
        file_io, signing_token = self._file_io_with_token(validity_millis)
        url = file_io.create_blob_presigned_url(
            table_root, descriptor, validity)
        if not self._has_remaining_lifetime(signing_token, validity_millis):
            raise ValueError(
                "Requested presigned URL validity exceeds the remaining "
                "REST credential lifetime after refresh.")
        return url

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

    def write_blob(self, path: str, data, **kwargs):
        return self.file_io().write_blob(path, data, **kwargs)

    def write_row(self, path: str, data, fields=None, zstd_level: int = 1, **kwargs):
        return self.file_io().write_row(path, data, fields, zstd_level, **kwargs)

    @property
    def uri_reader_factory(self):
        if self._uri_reader_factory_cache is None:
            catalog_options = self.catalog_options or Options({})
            self._uri_reader_factory_cache = UriReaderFactory(catalog_options)

        return self._uri_reader_factory_cache

    @property
    def filesystem(self):
        return self.file_io().filesystem

    def _build_cache_key(self) -> str:
        """Build cache key with user identity to isolate tokens across different credentials."""
        return f"{self._get_user_identity()}:{self.identifier}"

    def _get_user_identity(self) -> str:
        """Get user identity for token cache isolation.

        For DLF auth (AK/SK, STS, ECS Role): returns access_key_id via get_token().
        For Bear Token auth: returns the bearer token string.
        For unknown auth: returns 'anonymous'.
        """
        if self.api_instance is None:
            self.api_instance = RESTApi(self.properties, False)
        auth_provider = self.api_instance.rest_auth_function.auth_provider
        if isinstance(auth_provider, DLFAuthProvider):
            return auth_provider.get_token().access_key_id or 'anonymous'
        if isinstance(auth_provider, BearTokenAuthProvider):
            return auth_provider.token or 'anonymous'
        return 'anonymous'

    def try_to_refresh_token(self, minimum_validity_millis: int = 0):
        identifier_str = self._build_cache_key()

        if self.token is not None and not self._is_token_expired(self.token, minimum_validity_millis):
            return

        cached_token = self._get_cached_token(identifier_str)
        if cached_token and not self._is_token_expired(cached_token, minimum_validity_millis):
            self.token = cached_token
            return

        global_lock = self._get_global_token_lock(identifier_str)

        with global_lock:
            cached_token = self._get_cached_token(identifier_str)
            if cached_token and not self._is_token_expired(cached_token, minimum_validity_millis):
                self.token = cached_token
                return

            token_to_check = cached_token if cached_token else self.token
            if token_to_check is None or self._is_token_expired(token_to_check, minimum_validity_millis):
                self.refresh_token()
                self._set_cached_token(identifier_str, self.token)

    def _get_cached_token(self, identifier_str: str) -> Optional[RESTToken]:
        with self._TOKEN_LOCKS_LOCK:
            return self._TOKEN_CACHE.get(identifier_str)

    def _set_cached_token(self, identifier_str: str, token: RESTToken):
        with self._TOKEN_LOCKS_LOCK:
            self._TOKEN_CACHE[identifier_str] = token

    def _is_token_expired(
            self, token: Optional[RESTToken], minimum_validity_millis: int = 0) -> bool:
        if token is None:
            return True
        current_time = int(time.time() * 1000)
        return (token.expire_at_millis - current_time) < max(
            RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS, minimum_validity_millis)

    @staticmethod
    def _has_remaining_lifetime(
            token: Optional[RESTToken], minimum_validity_millis: int) -> bool:
        return (token is None or
                token.expire_at_millis - int(time.time() * 1000)
                >= minimum_validity_millis)

    def _get_global_token_lock(self, identifier_str: str) -> threading.Lock:
        with self._TOKEN_LOCKS_LOCK:
            if identifier_str not in self._TOKEN_LOCKS:
                self._TOKEN_LOCKS[identifier_str] = threading.Lock()
            return self._TOKEN_LOCKS[identifier_str]

    def refresh_token(self):
        self.log.info(f"begin refresh data token for identifier [{self.identifier}]")
        if self.api_instance is None:
            self.api_instance = RESTApi(self.properties, False)

        table_identifier = self.identifier
        if self.identifier.is_system_table():
            # Strip the system-table suffix; preserve the branch so the token
            # request resolves against the correct branch backing.
            table_identifier = Identifier.create(
                self.identifier.get_database_name(),
                self.identifier.get_table_name(),
                branch=self.identifier.get_branch_name(),
            )

        response = self.api_instance.load_table_token(table_identifier)
        self.log.info(
            f"end refresh data token for identifier [{self.identifier}] expiresAtMillis [{response.expires_at_millis}]"
        )

        merged_token_dict = self._merge_token_with_catalog_options(response.token)
        new_token = RESTToken(merged_token_dict, response.expires_at_millis)
        self.token = new_token
        if self._is_token_expired(new_token):
            self._log_short_token(new_token)

    def _log_short_token(self, token: RESTToken):
        with self._TOKEN_LOCKS_LOCK:
            stats = RESTTokenFileIO._SHORT_TOKEN_STATS
            stats["count"] += 1
            now = time.monotonic()
            if now < stats["next_log"]:
                return
            count = stats["count"]
            stats.update(count=0, next_log=now + self._STATS_LOG_INTERVAL)
        self.log.warning(
            "Data token for [%s] expires in %d s, within the %d s refresh margin, so every "
            "access refreshes it; %d such tokens in the last %d s",
            self.identifier, (token.expire_at_millis - int(time.time() * 1000)) // 1000,
            RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS // 1000, count, self._STATS_LOG_INTERVAL)

    def valid_token(self):
        self.try_to_refresh_token()
        return self.token

    def _existing_valid_token(self) -> Optional[RESTToken]:
        """Return this instance's valid token without locks or refresh requests."""
        token = self.token
        return token if not self._is_token_expired(token) else None

    def close(self):
        pass


def _open_files():
    try:
        import resource
        return len(os.listdir("/proc/self/fd")), resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    except (ImportError, OSError):
        return None


if hasattr(os, "register_at_fork"):
    # Like logging: a fork waits for the cache lock, and the child starts with free locks.
    os.register_at_fork(before=RESTTokenFileIO._before_fork,
                        after_in_parent=RESTTokenFileIO._after_fork_in_parent,
                        after_in_child=RESTTokenFileIO._after_fork_in_child)
