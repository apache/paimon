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
import re
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

    def new_input_stream(self, uri: str):
        try:
            return self._file_io.new_input_stream(uri)
        except Exception as e:
            raise IOError(f"Failed to read file {uri}: {e}")


class HttpUriReader(UriReader):

    def new_input_stream(self, uri: str):
        return HttpRangeInputStream(uri)


class HttpRangeInputStream(io.RawIOBase):

    def __init__(self, uri: str):
        super().__init__()
        self._uri = uri
        self._session = requests.Session()
        self._pos = 0
        self._length = None

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def readinto(self, b):
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def read(self, size=-1):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if size is None:
            size = -1
        if size == 0:
            return b''

        end = None
        if size >= 0:
            end = self._pos + size - 1
        range_end = "" if end is None else str(end)
        headers = {"Range": f"bytes={self._pos}-{range_end}"}

        try:
            response = self._session.get(self._uri, headers=headers)
            if response.status_code == 416:
                return b''
            if response.status_code == 206:
                self._update_length(response.headers.get("Content-Range"))
                data = response.content
            elif response.status_code == 200 and self._pos == 0:
                data = response.content if size < 0 else response.content[:size]
                self._update_length_from_content_length(response.headers.get("Content-Length"))
            else:
                raise RuntimeError(
                    f"HTTP server did not honor range request for {self._uri}: "
                    f"status code {response.status_code}"
                )
            self._pos += len(data)
            return data
        except Exception as e:
            raise RuntimeError(f"Failed to read HTTP URI {self._uri}: {e}") from e

    def seek(self, pos, whence=io.SEEK_SET):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if whence == io.SEEK_SET:
            target = pos
        elif whence == io.SEEK_CUR:
            target = self._pos + pos
        elif whence == io.SEEK_END:
            target = self._get_length() + pos
        else:
            raise ValueError(f"Invalid whence: {whence}")
        if target < 0:
            raise ValueError(f"Negative seek position: {target}")
        self._pos = target
        return self._pos

    def close(self):
        if not self.closed:
            self._session.close()
        super().close()

    def _get_length(self) -> int:
        if self._length is not None:
            return self._length

        try:
            response = self._session.head(self._uri)
            if response.status_code == 200:
                self._update_length_from_content_length(response.headers.get("Content-Length"))
                if self._length is not None:
                    return self._length
        except Exception:
            pass

        response = self._session.get(self._uri, headers={"Range": "bytes=0-0"})
        if response.status_code == 206:
            self._update_length(response.headers.get("Content-Range"))
            if self._length is not None:
                return self._length
        if response.status_code == 200:
            self._update_length_from_content_length(response.headers.get("Content-Length"))
            if self._length is not None:
                return self._length
        raise RuntimeError(f"Failed to determine HTTP URI length: {self._uri}")

    def _update_length(self, content_range: Optional[str]) -> None:
        if not content_range:
            return
        match = re.match(r"bytes\s+\d+-\d+/(\d+)", content_range)
        if match:
            self._length = int(match.group(1))

    def _update_length_from_content_length(self, content_length: Optional[str]) -> None:
        if content_length:
            self._length = int(content_length)


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
        self._readers = LRUCache(CatalogOptions.BLOB_FILE_IO_DEFAULT_CACHE_SIZE)
        self._readers_lock = rwlock.RWLockFair()

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
            return UriReader.from_file(file_io)
        except Exception as e:
            raise RuntimeError(f"Failed to create reader for URI {parsed_uri.geturl()}") from e

    def clear_cache(self) -> None:
        self._readers.clear()

    def get_cache_size(self) -> int:
        return len(self._readers)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_readers_lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._readers_lock = rwlock.RWLockFair()
