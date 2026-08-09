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

from collections import defaultdict
from typing import Dict, List
from urllib.parse import urlparse

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions


class ResolvingFileIO(FileIO):
    """A FileIO that dynamically selects the appropriate FileIO based on the
    URI scheme of the given path. Caches FileIO instances by (scheme, authority)
    to avoid repeated creation.

    This is the Python equivalent of Java's ``ResolvingFileIO``.
    """

    def __init__(self, catalog_options: Options):
        opts_map = dict(catalog_options.to_map())
        opts_map.pop(CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(), None)
        self._options = Options(opts_map)
        self._fileio_cache: Dict[tuple, FileIO] = {}

    def _cache_key(self, path: str) -> tuple:
        uri = urlparse(path)
        return (uri.scheme or 'file', uri.netloc or '')

    def _get_fileio(self, path: str) -> FileIO:
        cache_key = self._cache_key(path)
        fileio = self._fileio_cache.get(cache_key)
        if fileio is None:
            fileio = FileIO.get(path, self._options)
            self._fileio_cache[cache_key] = fileio
        return fileio

    @property
    def properties(self):
        return self._options

    def is_object_store(self) -> bool:
        """Check if the warehouse path points to an object store (not local or HDFS)."""
        warehouse = self._options.to_map().get(CatalogOptions.WAREHOUSE.key())
        if not warehouse:
            return False
        uri = urlparse(warehouse)
        scheme = (uri.scheme or '').lower()
        return scheme != '' and scheme != 'file' and scheme != 'hdfs'

    def new_input_stream(self, path: str):
        return self._get_fileio(path).new_input_stream(path)

    def new_output_stream(self, path: str):
        return self._get_fileio(path).new_output_stream(path)

    def get_file_status(self, path: str):
        return self._get_fileio(path).get_file_status(path)

    def list_status(self, path: str):
        return self._get_fileio(path).list_status(path)

    def exists(self, path: str) -> bool:
        return self._get_fileio(path).exists(path)

    def exists_batch(self, paths: List[str]) -> Dict[str, bool]:
        groups: Dict[tuple, List[str]] = defaultdict(list)
        for path in paths:
            groups[self._cache_key(path)].append(path)
        result = {}
        for key, group_paths in groups.items():
            fio = self._get_fileio(group_paths[0])
            result.update(fio.exists_batch(group_paths))
        return result

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self._get_fileio(path).delete(path, recursive)

    def mkdirs(self, path: str) -> bool:
        return self._get_fileio(path).mkdirs(path)

    def rename(self, src: str, dst: str) -> bool:
        return self._get_fileio(src).rename(src, dst)

    def get_file_size(self, path: str) -> int:
        return self._get_fileio(path).get_file_size(path)

    def is_dir(self, path: str) -> bool:
        return self._get_fileio(path).is_dir(path)

    def to_filesystem_path(self, path: str) -> str:
        return self._get_fileio(path).to_filesystem_path(path)

    def write_parquet(self, path: str, data, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        return self._get_fileio(path).write_parquet(path, data, compression,
                                                    zstd_level, **kwargs)

    def write_orc(self, path: str, data, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        return self._get_fileio(path).write_orc(path, data, compression,
                                                zstd_level, **kwargs)

    def write_avro(self, path: str, data, avro_schema=None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        return self._get_fileio(path).write_avro(path, data, avro_schema,
                                                 compression, zstd_level, **kwargs)

    def write_lance(self, path: str, data, **kwargs):
        return self._get_fileio(path).write_lance(path, data, **kwargs)

    def write_blob(self, path: str, data, **kwargs):
        return self._get_fileio(path).write_blob(path, data, **kwargs)

    def write_mosaic(self, path: str, data, **kwargs):
        return self._get_fileio(path).write_mosaic(path, data, **kwargs)

    def write_vortex(self, path: str, data, **kwargs):
        return self._get_fileio(path).write_vortex(path, data, **kwargs)

    def write_row(self, path: str, data, fields=None, zstd_level: int = 1, **kwargs):
        return self._get_fileio(path).write_row(path, data, fields,
                                                zstd_level, **kwargs)

    def close(self):
        for fileio in self._fileio_cache.values():
            fileio.close()
        self._fileio_cache.clear()
