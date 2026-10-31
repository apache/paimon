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
import shutil
import threading
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse

import pyarrow
import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO, create_temp_path, normalize_naive_datetimes
from pypaimon.common.options import Options
from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.filesystem.local import PaimonLocalFileSystem
from pypaimon.schema.data_types import PyarrowFieldParser


def _fsync_file_obj(file_obj) -> None:
    """Flush and fsync an open file object so its data reaches durable storage.

    Raises OSError if the data cannot be synced; callers must not publish the
    path as durable when this fails.
    """
    file_obj.flush()
    os.fsync(file_obj.fileno())


def _fsync_directory(dir_path: Path) -> None:
    """Best-effort fsync of a directory so renames are ordered on disk.

    Some platforms reject directory fsync; failures are ignored there. File data
    durability still depends on a successful prior file fsync.
    """
    try:
        fd = os.open(str(dir_path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class _FsyncOnCloseFile:
    """Binary file wrapper: close() fsyncs file data before releasing the fd.

    If file fsync fails, the underlying file is still closed and the error is
    re-raised so callers do not treat the write as durable. Parent-directory
    fsync runs only after a successful file fsync.
    """

    def __init__(self, file_obj, parent_dir: Optional[Path] = None):
        self._file = file_obj
        self._parent_dir = parent_dir
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._file, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        if self._closed:
            return
        sync_error = None
        try:
            if not self._file.closed:
                try:
                    _fsync_file_obj(self._file)
                except OSError as err:
                    sync_error = err
        finally:
            if not self._file.closed:
                self._file.close()
            self._closed = True
        if sync_error is not None:
            raise sync_error
        if self._parent_dir is not None:
            _fsync_directory(self._parent_dir)


def _file_uri_path(parsed, windows=None) -> str:
    """Decode a file URI path, preserving Windows drives and UNC hosts."""
    if windows is None:
        windows = os.name == "nt"
    if windows:
        from nturl2path import url2pathname as windows_url2pathname
        if parsed.netloc and not parsed.netloc.endswith(":"):
            value = "//%s%s" % (parsed.netloc, parsed.path)
        elif parsed.netloc:
            value = "/%s%s" % (parsed.netloc, parsed.path)
        else:
            value = parsed.path
        return windows_url2pathname(value)
    if parsed.netloc and parsed.netloc != "localhost":
        return "//%s%s" % (parsed.netloc, unquote(parsed.path))
    return unquote(parsed.path)


class LocalFileIO(FileIO):
    """
    Local file system implementation of FileIO.
    """
    
    RENAME_LOCK = threading.Lock()
    
    def __init__(self, path: str = None, catalog_options: Optional[Options] = None):
        self.logger = logging.getLogger(__name__)
        self.path = path
        self.properties = catalog_options or Options({})
        self.filesystem = PaimonLocalFileSystem()
        self.uri_reader_factory = UriReaderFactory(self.properties)
    
    @staticmethod
    def create():
        return LocalFileIO()
    
    @staticmethod
    def _to_file(path: str) -> Path:
        parsed = urlparse(path)
        
        if parsed.scheme and len(parsed.scheme) == 1 and not parsed.netloc:
            return Path(path)
        
        if parsed.scheme == 'file' and parsed.netloc and parsed.netloc.endswith(':'):
            drive_letter = parsed.netloc.rstrip(':')
            path_part = unquote(parsed.path).lstrip('/') if parsed.path else ''
            if path_part:
                return Path(f"{drive_letter}:/{path_part}")
            else:
                return Path(f"{drive_letter}:")
        
        local_path = (
            _file_uri_path(parsed) if parsed.scheme == 'file' else path)
        
        if not local_path:
            return Path(".")
        
        return Path(local_path)
    
    def new_input_stream(self, path: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking new_input_stream for {path}")
        
        file_path = self._to_file(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File {path} does not exist")
        
        return open(file_path, 'rb')
    
    def new_output_stream(self, path: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking new_output_stream for {path}")
        
        file_path = self._to_file(path)
        # Create parent directories if needed
        parent = file_path.parent
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
            _fsync_directory(parent)
        
        stream = open(file_path, 'wb')
        return _FsyncOnCloseFile(stream, parent if parent else None)
    
    def get_file_status(self, path: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking get_file_status for {path}")
        
        file_path = self._to_file(path)
        if not file_path.exists():
            import getpass
            user = getpass.getuser()
            raise FileNotFoundError(
                f"File {path} does not exist or the user running "
                f"Paimon ('{user}') has insufficient permissions to access it."
            )
        
        class LocalFileStatus:
            def __init__(self, file_path: Path, original_path: str):
                stat_info = file_path.stat()
                self.path = file_path.absolute().as_posix()
                self.original_path = original_path
                self.base_name = os.path.basename(original_path)
                self.size = stat_info.st_size if file_path.is_file() else None
                self.type = (
                    pafs.FileType.Directory if file_path.is_dir()
                    else pafs.FileType.File if file_path.is_file()
                    else pafs.FileType.NotFound
                )
                self.mtime = stat_info.st_mtime
        
        return LocalFileStatus(file_path, path)
    
    def list_status(self, path: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking list_status for {path}")
        
        file_path = self._to_file(path)
        results = []
        
        if not file_path.exists():
            return results
        
        if file_path.is_file():
            results.append(self.get_file_status(path))
        elif file_path.is_dir():
            for item in file_path.iterdir():
                try:
                    if path.startswith('file://'):
                        item_path = item.absolute().as_uri()
                    else:
                        item_path = str(item)
                    results.append(self.get_file_status(item_path))
                except FileNotFoundError:
                    # A child may disappear between listing and stat.
                    pass
        
        return results
    
    def exists(self, path: str) -> bool:
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking exists for {path}")
        
        file_path = self._to_file(path)
        return file_path.exists()
    
    def delete(self, path: str, recursive: bool = False) -> bool:
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking delete for {path}")
        
        file_path = self._to_file(path)
        
        if not file_path.exists():
            return False
        
        if file_path.is_file():
            file_path.unlink()
            return True
        elif file_path.is_dir():
            if not recursive:
                try:
                    items = list(file_path.iterdir())
                    if items:
                        raise OSError(f"Directory {path} is not empty")
                except PermissionError:
                    raise OSError(
                        f"Directory {path} does not exist or an I/O error occurred"
                    )
                file_path.rmdir()
            else:
                shutil.rmtree(file_path)
            return True
        
        return False
    
    def mkdirs(self, path: str) -> bool:
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking mkdirs for {path}")
        
        file_path = self._to_file(path)
        
        if file_path.is_dir():
            return True
        elif file_path.exists() and not file_path.is_dir():
            raise FileExistsError(f"Path exists but is not a directory: {path}")
        
        file_path.mkdir(parents=True, exist_ok=True)
        return True
    
    def rename(self, src: str, dst: str) -> bool:
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Invoking rename for {src} to {dst}")
        
        src_file = self._to_file(src)
        dst_file = self._to_file(dst)
        
        dst_parent = dst_file.parent
        if dst_parent and not dst_parent.exists():
            dst_parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with LocalFileIO.RENAME_LOCK:
                if dst_file.exists():
                    if dst_file.is_file():
                        return False
                    # Make it compatible with HadoopFileIO: if dst is an existing directory,
                    # dst=dst/srcFileName
                    dst_file = dst_file / src_file.name
                    if dst_file.exists():
                        return False
                
                # Perform atomic move
                src_file.rename(dst_file)
                return True
        except FileNotFoundError:
            return False
        except (PermissionError, OSError):
            return False
    
    def try_to_write_atomic(self, path: str, content: str) -> bool:
        file_path = self._to_file(path)
        if file_path.exists() and file_path.is_dir():
            return False
        
        parent = file_path.parent
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        
        temp_path = Path(create_temp_path(str(file_path)))
        success = False
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
                try:
                    _fsync_file_obj(f)
                except OSError:
                    # Do not rename: publishing would claim a durable snapshot/schema.
                    return False
            success = self.rename(str(temp_path), path)
            if success and parent:
                _fsync_directory(parent)
        finally:
            if not success and temp_path.exists():
                self.delete_quietly(str(temp_path))
        return success
    
    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")
        
        source_file = self._to_file(source_path)
        target_file = self._to_file(target_path)
        
        target_parent = target_file.parent
        if target_parent and not target_parent.exists():
            target_parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(source_file, target_file)
    
    def to_filesystem_path(self, path: str) -> str:
        file_path = self._to_file(path)
        result = str(file_path)
        parsed = urlparse(path)
        original_path = parsed.path if parsed.scheme else path
        if original_path.startswith('./') and not result.startswith('./'):
            result = './' + result
        return result
    
    @staticmethod
    def parse_location(location: str):
        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        elif uri.scheme == "file":
            return "file", uri.netloc, uri.path
        else:
            raise ValueError(f"LocalFileIO only supports file:// scheme, got {uri.scheme}")
    
    def write_parquet(self, path: str, data: pyarrow.Table, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        try:
            import pyarrow.parquet as pq
            
            file_path = self._to_file(path)
            parent = file_path.parent
            if parent and not parent.exists():
                parent.mkdir(parents=True, exist_ok=True)
            
            with self.new_output_stream(path) as f:
                if compression.lower() == 'zstd':
                    kwargs['compression_level'] = zstd_level
                pq.write_table(data, f, compression=compression, **kwargs)
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e
    
    def write_orc(self, path: str, data: pyarrow.Table, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        try:
            import pyarrow.orc as orc

            file_path = self._to_file(path)
            parent = file_path.parent
            if parent and not parent.exists():
                parent.mkdir(parents=True, exist_ok=True)
            
            data = self._cast_time_columns_for_orc(data)
            
            with self.new_output_stream(path) as f:
                # ORC compression= was added in PyArrow 7.0; PyArrow 6 lacks it.
                from pypaimon.filesystem.pyarrow_file_io import _pyarrow_lt_7
                if _pyarrow_lt_7():
                    orc.write_table(data, f, **kwargs)
                else:
                    orc.write_table(data, f, compression=compression, **kwargs)
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write ORC file {path}: {e}") from e
    
    def write_avro(self, path: str, data: pyarrow.Table,
                   avro_schema: Optional[Dict[str, Any]] = None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        import fastavro
        if avro_schema is None:
            avro_schema = PyarrowFieldParser.to_avro_schema(data.schema)
        
        records_dict = data.to_pydict()
        
        def record_generator():
            num_rows = len(list(records_dict.values())[0])
            for i in range(num_rows):
                record = {}
                for col in records_dict.keys():
                    value = records_dict[col][i]
                    record[col] = normalize_naive_datetimes(value)
                yield record
        
        records = record_generator()
        
        codec_map = {
            'null': 'null',
            'deflate': 'deflate',
            'snappy': 'snappy',
            'bzip2': 'bzip2',
            'xz': 'xz',
            'zstandard': 'zstandard',
            'zstd': 'zstandard',
        }
        compression_lower = compression.lower()
        
        codec = codec_map.get(compression_lower)
        if codec is None:
            raise ValueError(
                f"Unsupported compression '{compression}' for Avro format. "
                f"Supported compressions: {', '.join(sorted(codec_map.keys()))}."
            )
        
        file_path = self._to_file(path)
        parent = file_path.parent
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        
        with self.new_output_stream(path) as output_stream:
            if codec == 'zstandard':
                kwargs['codec_compression_level'] = zstd_level
            fastavro.writer(output_stream, avro_schema, records, codec=codec, **kwargs)
    
    def write_mosaic(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import mosaic
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with self.new_output_stream(path) as f:
                mosaic.write_table(data, f, options=kwargs.get("options"))
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Mosaic file {path}: {e}") from e


class FuseLocalFileIO(LocalFileIO):
    """LocalFileIO that translates remote OSS paths to FUSE-mounted local paths.

    All file operations receive paths like:
        oss://clg-paimon-xxx/db-xxx/tbl-xxx/manifest/manifest-xxx
    This class replaces the path prefix with fuse_path so the actual
    I/O goes through the FUSE mount point.
    """

    def __init__(self, path: str, fuse_path: str,
                 catalog_options: Optional[Options] = None):
        super().__init__(path=fuse_path, catalog_options=catalog_options)
        self.path = path
        self.fuse_path = fuse_path

    def _to_file(self, path: str) -> Path:
        return super()._to_file(self._translate(path))

    def _translate(self, path: str) -> str:
        if path == self.path or path.startswith(self.path + "/"):
            return self.fuse_path + path[len(self.path):]
        return path
