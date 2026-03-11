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
################################################################################
import logging

import pyarrow as pa
from pyarrow import PythonFile
from pyarrow._fs import FileSystemHandler
from pyarrow.fs import FileInfo, FileSelector, FileType

try:
    import pyjindo.fs as jfs
    import pyjindo.util as jutil
    JINDO_AVAILABLE = True
except ImportError:
    JINDO_AVAILABLE = False
    jfs = None
    jutil = None

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions

class JindoInputFile:
    def __init__(self, jindo_stream):
        self._stream = jindo_stream
        self._closed = False

    @property
    def closed(self):
        if hasattr(self._stream, 'closed'):
            return self._stream.closed
        return self._closed

    def read(self, nbytes: int = -1):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if nbytes is None or nbytes < 0:
            return self._stream.read()
        return self._stream.read(nbytes)

    def seek(self, position: int, whence: int = 0):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        self._stream.seek(position, whence)

    def tell(self) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self._stream.tell()

    def read_at(self, nbytes: int, offset: int):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self._stream.pread(nbytes, offset)

    def close(self):
        if not self._closed:
            self._stream.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class JindoOutputFile:
    def __init__(self, jindo_stream):
        self._stream = jindo_stream
        self._closed = False

    @property
    def closed(self):
        if hasattr(self._stream, 'closed'):
            return self._stream.closed
        return self._closed

    def write(self, data: bytes) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if isinstance(data, pa.Buffer):
            data = data.to_pybytes()
        elif not isinstance(data, bytes):
            raise TypeError("Unsupported data type")
        return self._stream.write(data)

    def flush(self):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if hasattr(self._stream, 'flush'):
            self._stream.flush()

    def close(self):
        if not self._closed:
            self._stream.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

class JindoFileSystemHandler(FileSystemHandler):
    def __init__(self, root_path: str, catalog_options: Options):
        if not JINDO_AVAILABLE:
            raise ImportError("pyjindo is not available. Please install pyjindo.")

        self.logger = logging.getLogger(__name__)
        self.root_path = root_path
        self.properties = catalog_options

        # 从 catalog_options 构建 jindo 配置
        config = jutil.Config()

        access_key_id = catalog_options.get(OssOptions.OSS_ACCESS_KEY_ID)
        access_key_secret = catalog_options.get(OssOptions.OSS_ACCESS_KEY_SECRET)
        security_token = catalog_options.get(OssOptions.OSS_SECURITY_TOKEN)
        endpoint = catalog_options.get(OssOptions.OSS_ENDPOINT)
        region = catalog_options.get(OssOptions.OSS_REGION)

        if access_key_id:
            config.set("fs.oss.accessKeyId", access_key_id)
        if access_key_secret:
            config.set("fs.oss.accessKeySecret", access_key_secret)
        if security_token:
            config.set("fs.oss.securityToken", security_token)
        if endpoint:
            endpoint_clean = endpoint.replace('http://', '').replace('https://', '')
            config.set("fs.oss.endpoint", endpoint_clean)
        if region:
            config.set("fs.oss.region", region)

        self._jindo_fs = jfs.connect(self.root_path, "root", config)

    def __eq__(self, other):
        if isinstance(other, JindoFileSystemHandler):
            return self.root_path == other.root_path
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, JindoFileSystemHandler):
            return not self.__eq__(other)
        return NotImplemented

    def _normalize_path(self, path: str) -> str:
        if path.startswith('oss://'):
            return path

        if not path or path == '.':
            return self.root_path.rstrip('/') + '/'

        path_clean = path.lstrip('/')
        return self.root_path.rstrip('/') + '/' + path_clean

    def _convert_file_type(self, jindo_type) -> FileType:
        if jindo_type == jfs.FileType.File:
            return FileType.File
        elif jindo_type == jfs.FileType.Directory:
            return FileType.Directory
        else:
            return FileType.Unknown

    def _convert_file_info(self, jindo_info) -> FileInfo:
        pa_type = self._convert_file_type(jindo_info.type)
        return FileInfo(
            path=jindo_info.path,
            type=pa_type,
            size=jindo_info.size if jindo_info.type == jfs.FileType.File else None,
            mtime=jindo_info.mtime if hasattr(jindo_info, 'mtime') else None,
        )

    def get_type_name(self) -> str:
        return "jindo"

    def get_file_info(self, paths) -> list:
        infos = []
        for path in paths:
            normalized = self._normalize_path(path)
            try:
                jindo_info = self._jindo_fs.get_file_info(normalized)
                infos.append(self._convert_file_info(jindo_info))
            except FileNotFoundError:
                infos.append(FileInfo(normalized, FileType.NotFound))
        return infos

    def get_file_info_selector(self, selector: FileSelector) -> list:
        normalized = self._normalize_path(selector.base_dir)
        try:
            items = self._jindo_fs.listdir(normalized, recursive=selector.recursive)
            return [self._convert_file_info(item) for item in items]
        except FileNotFoundError as e:
            if selector.allow_not_found:
                return []
            raise

    def create_dir(self, path: str, recursive: bool):
        normalized = self._normalize_path(path)
        self._jindo_fs.mkdir(normalized)

    def delete_dir(self, path: str):
        normalized = self._normalize_path(path)
        self._jindo_fs.remove(normalized)

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False):
        normalized = self._normalize_path(path)
        if normalized == self.root_path:
            raise ValueError(
                "delete_dir_contents() does not accept root path"
            )
        self._delete_dir_contents(path, missing_dir_ok)

    def delete_root_dir_contents(self):
        self._delete_dir_contents("/", missing_dir_ok=False)

    def _delete_dir_contents(self, path: str, missing_dir_ok: bool):
        normalized = self._normalize_path(path)
        try:
            items = self._jindo_fs.listdir(normalized, recursive=False)
        except FileNotFoundError:
            if missing_dir_ok:
                return
            raise
        except Exception as e:
            self.logger.warning(f"Error listing {path}: {e}")
            raise
        for item in items:
            self._jindo_fs.remove(item.path)

    def delete_file(self, path: str):
        normalized = self._normalize_path(path)
        self._jindo_fs.remove(normalized)

    def move(self, src: str, dest: str):
        src_norm = self._normalize_path(src)
        dst_norm = self._normalize_path(dest)
        self._jindo_fs.rename(src_norm, dst_norm)

    def copy_file(self, src: str, dest: str):
        src_norm = self._normalize_path(src)
        dst_norm = self._normalize_path(dest)
        self._jindo_fs.copy_file(src_norm, dst_norm)
        # with self._jindo_fs.open(src_norm, "rb") as src_f:
        #     with self._jindo_fs.open(dst_norm, "wb") as dst_f:
        #         while True:
        #             chunk = src_f.read(1024 * 1024)  # 1 MB
        #             if not chunk:
        #                 break
        #             dst_f.write(chunk)

    def open_input_stream(self, path: str):
        normalized = self._normalize_path(path)
        jindo_stream = self._jindo_fs.open(normalized, "rb")
        return PythonFile(JindoInputFile(jindo_stream), mode="r")

    def open_input_file(self, path: str):
        normalized = self._normalize_path(path)
        jindo_stream = self._jindo_fs.open(normalized, "rb")
        return PythonFile(JindoInputFile(jindo_stream), mode="r")

    def open_output_stream(self, path: str, metadata):
        normalized = self._normalize_path(path)
        jindo_stream = self._jindo_fs.open(normalized, "wb")
        return PythonFile(JindoOutputFile(jindo_stream), mode="w")

    def open_append_stream(self, path: str, metadata):
        raise IOError("append mode is not supported")

    def normalize_path(self, path: str) -> str:
        return self._normalize_path(path)

