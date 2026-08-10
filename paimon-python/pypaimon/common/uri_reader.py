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

import io
from abc import ABC, abstractmethod
from typing import Any, Optional, Union
from urllib.parse import urlparse, ParseResult

import requests
from cachetools import LRUCache
from readerwriterlock import rwlock

from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions


class UriReader(ABC):
    @classmethod
    def from_http(cls) -> 'HttpUriReader':
        return HttpUriReader()

    @classmethod
    def from_file(cls, file_io: Any) -> 'FileUriReader':
        return FileUriReader(file_io)

    @classmethod
    def get_file_path(cls, uri: str):
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == 'file':
            return parsed_uri.path
        elif parsed_uri.scheme and parsed_uri.scheme != '':
            return f"{parsed_uri.netloc}{parsed_uri.path}"
        else:
            return uri

    @abstractmethod
    def new_input_stream(self, uri: str):
        pass


class FileUriReader(UriReader):

    def __init__(self, file_io: Any):
        self._file_io = file_io

    @property
    def file_io(self) -> Any:
        return self._file_io

    def new_input_stream(self, uri: str):
        try:
            return self._file_io.new_input_stream(uri)
        except Exception as e:
            raise IOError(f"Failed to read file {uri}: {e}")


class HttpUriReader(UriReader):

    def new_input_stream(self, uri: str):
        try:
            response = requests.get(uri)
            if response.status_code != 200:
                raise RuntimeError(f"Failed to read HTTP URI {uri} status code {response.status_code}")
            return io.BytesIO(response.content)
        except Exception as e:
            raise RuntimeError(f"Failed to read HTTP URI {uri}: {e}")


class UriKey:

    def __init__(self, scheme: Optional[str], authority: Optional[str]) -> None:
        self._scheme = scheme
        self._authority = authority
        self._hash = hash((self._scheme, self._authority))

    @property
    def scheme(self) -> Optional[str]:
        return self._scheme

    @property
    def authority(self) -> Optional[str]:
        return self._authority

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, UriKey):
            return False

        return (self._scheme == other._scheme and
                self._authority == other._authority)

    def __hash__(self) -> int:
        return self._hash

    def __repr__(self) -> str:
        return f"UriKey(scheme='{self._scheme}', authority='{self._authority}')"


class UriReaderFactory:

    def __init__(self, catalog_options: Union[Options, dict]) -> None:
        self.catalog_options = catalog_options if isinstance(catalog_options, Options) else Options(catalog_options)
        self._readers_lock = rwlock.RWLockFair()
        self._owned_file_ios = []
        self._closing = False
        self._readers = self._new_reader_cache()

    @staticmethod
    def from_file_io(file_io: Any) -> 'UriReaderFactory':
        """Reuse a token-aware FileIO for non-HTTP URIs (Java fromFileIO)."""
        cached = getattr(file_io, '_paimon_from_file_io_uri_reader_factory', None)
        if cached is not None:
            return cached
        factory = _ProvidedFileIOUriReaderFactory(file_io)
        try:
            file_io._paimon_from_file_io_uri_reader_factory = factory
        except Exception:
            pass
        return factory

    def _new_reader_cache(self) -> LRUCache:
        return LRUCache(CatalogOptions.BLOB_FILE_IO_DEFAULT_CACHE_SIZE)

    def create(self, input_uri: str) -> UriReader:
        try:
            parsed_uri = urlparse(input_uri)
        except Exception as e:
            raise ValueError(f"Invalid URI: {input_uri}") from e

        key = UriKey(parsed_uri.scheme, parsed_uri.netloc or None)
        rlock = self._readers_lock.gen_rlock()
        rlock.acquire()
        try:
            reader = self._readers.get(key)
            if reader is not None:
                return reader
        finally:
            rlock.release()
        wlock = self._readers_lock.gen_wlock()
        wlock.acquire()
        try:
            reader = self._readers.get(key)
            if reader is not None:
                return reader
            reader = self._new_reader(key, parsed_uri)
            self._readers[key] = reader
            return reader
        finally:
            wlock.release()

    def _new_reader(self, key: UriKey, parsed_uri: ParseResult) -> UriReader:
        scheme = key.scheme
        if scheme in ('http', 'https'):
            return UriReader.from_http()
        try:
            # Import FileIO here to avoid circular imports
            from pypaimon.common.file_io import FileIO
            uri_string = parsed_uri.geturl()
            file_io = FileIO.get(uri_string, self.catalog_options)
            self._owned_file_ios.append(file_io)
            return UriReader.from_file(file_io)
        except Exception as e:
            raise RuntimeError(f"Failed to create reader for URI {parsed_uri.geturl()}") from e

    def clear_cache(self) -> None:
        if self._closing:
            return
        self._closing = True
        wlock = self._readers_lock.gen_wlock()
        wlock.acquire()
        try:
            file_ios = list(self._owned_file_ios)
            self._owned_file_ios = []
            self._readers = self._new_reader_cache()
        finally:
            wlock.release()
        first_error = None
        try:
            for file_io in file_ios:
                try:
                    file_io.close()
                except Exception as error:
                    if first_error is None:
                        first_error = error
        finally:
            self._closing = False
        if first_error is not None:
            raise first_error

    def close(self) -> None:
        self.clear_cache()

    def get_cache_size(self) -> int:
        return len(self._readers)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_readers_lock']
        del state['_readers']
        del state['_owned_file_ios']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._readers_lock = rwlock.RWLockFair()
        self._owned_file_ios = []
        self._closing = False
        self._readers = self._new_reader_cache()


class _ProvidedFileIOUriReaderFactory(UriReaderFactory):
    """Resolves HTTP(S) via HttpUriReader and every other URI through file_io."""

    def __init__(self, file_io: Any) -> None:
        super().__init__({})
        self._provided_file_io = file_io

    def _new_reader(self, key: UriKey, parsed_uri: ParseResult) -> UriReader:
        scheme = (key.scheme or '').lower()
        if scheme in ('http', 'https'):
            return UriReader.from_http()
        return UriReader.from_file(self._provided_file_io)
