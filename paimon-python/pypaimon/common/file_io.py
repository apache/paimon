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
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import splitport, urlparse

import pyarrow
from pyarrow._fs import FileSystem

from pypaimon.common.config import OssOptions, S3Options
from pypaimon.schema.data_types import PyarrowFieldParser


class FileIO:
    def __init__(self, warehouse: str, catalog_options: dict):
        self.properties = catalog_options
        self.logger = logging.getLogger(__name__)
        scheme, netloc, path = self.parse_location(warehouse)
        if scheme in {"oss"}:
            self.filesystem = self._initialize_oss_fs()
        elif scheme in {"s3", "s3a", "s3n"}:
            self.filesystem = self._initialize_s3_fs()
        elif scheme in {"hdfs", "viewfs"}:
            self.filesystem = self._initialize_hdfs_fs(scheme, netloc)
        elif scheme in {"file"}:
            self.filesystem = self._initialize_local_fs()
        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    @staticmethod
    def parse_location(location: str):
        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        elif uri.scheme in ("hdfs", "viewfs"):
            return uri.scheme, uri.netloc, uri.path
        else:
            return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"

    def _initialize_oss_fs(self) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        client_kwargs = {
            "endpoint_override": self.properties.get(OssOptions.OSS_ENDPOINT),
            "access_key": self.properties.get(OssOptions.OSS_ACCESS_KEY_ID),
            "secret_key": self.properties.get(OssOptions.OSS_ACCESS_KEY_SECRET),
            "session_token": self.properties.get(OssOptions.OSS_SECURITY_TOKEN),
            "region": self.properties.get(OssOptions.OSS_REGION),
            "force_virtual_addressing": True,
        }

        return S3FileSystem(**client_kwargs)

    def _initialize_s3_fs(self) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        client_kwargs = {
            "endpoint_override": self.properties.get(S3Options.S3_ENDPOINT),
            "access_key": self.properties.get(S3Options.S3_ACCESS_KEY_ID),
            "secret_key": self.properties.get(S3Options.S3_ACCESS_KEY_SECRET),
            "session_token": self.properties.get(S3Options.S3_SECURITY_TOKEN),
            "region": self.properties.get(S3Options.S3_REGION),
            "force_virtual_addressing": True,
        }

        return S3FileSystem(**client_kwargs)

    def _initialize_hdfs_fs(self, scheme: str, netloc: Optional[str]) -> FileSystem:
        from pyarrow.fs import HadoopFileSystem

        if 'HADOOP_HOME' not in os.environ:
            raise RuntimeError("HADOOP_HOME environment variable is not set.")
        if 'HADOOP_CONF_DIR' not in os.environ:
            raise RuntimeError("HADOOP_CONF_DIR environment variable is not set.")

        hadoop_home = os.environ.get("HADOOP_HOME")
        native_lib_path = f"{hadoop_home}/lib/native"
        os.environ['LD_LIBRARY_PATH'] = f"{native_lib_path}:{os.environ.get('LD_LIBRARY_PATH', '')}"

        class_paths = subprocess.run(
            [f'{hadoop_home}/bin/hadoop', 'classpath', '--glob'],
            capture_output=True,
            text=True,
            check=True
        )
        os.environ['CLASSPATH'] = class_paths.stdout.strip()

        host, port_str = splitport(netloc)
        return HadoopFileSystem(
            host=host,
            port=int(port_str),
            user=os.environ.get('HADOOP_USER_NAME', 'hadoop')
        )

    def _initialize_local_fs(self) -> FileSystem:
        from pyarrow.fs import LocalFileSystem

        return LocalFileSystem()

    def new_input_stream(self, path: Path):
        return self.filesystem.open_input_file(str(path))

    def new_output_stream(self, path: Path):
        parent_dir = path.parent
        if str(parent_dir) and not self.exists(parent_dir):
            self.mkdirs(parent_dir)

        return self.filesystem.open_output_stream(str(path))

    def get_file_status(self, path: Path):
        file_infos = self.filesystem.get_file_info([str(path)])
        return file_infos[0]

    def list_status(self, path: Path):
        selector = pyarrow.fs.FileSelector(str(path), recursive=False, allow_not_found=True)
        return self.filesystem.get_file_info(selector)

    def list_directories(self, path: Path):
        file_infos = self.list_status(path)
        return [info for info in file_infos if info.type == pyarrow.fs.FileType.Directory]

    def exists(self, path: Path) -> bool:
        try:
            file_info = self.filesystem.get_file_info([str(path)])[0]
            return file_info.type != pyarrow.fs.FileType.NotFound
        except Exception:
            return False

    def delete(self, path: Path, recursive: bool = False) -> bool:
        try:
            file_info = self.filesystem.get_file_info([str(path)])[0]
            if file_info.type == pyarrow.fs.FileType.Directory:
                if recursive:
                    self.filesystem.delete_dir_contents(str(path))
                else:
                    self.filesystem.delete_dir(str(path))
            else:
                self.filesystem.delete_file(str(path))
            return True
        except Exception as e:
            self.logger.warning(f"Failed to delete {path}: {e}")
            return False

    def mkdirs(self, path: Path) -> bool:
        try:
            self.filesystem.create_dir(str(path), recursive=True)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to create directory {path}: {e}")
            return False

    def rename(self, src: Path, dst: Path) -> bool:
        try:
            dst_parent = dst.parent
            if str(dst_parent) and not self.exists(dst_parent):
                self.mkdirs(dst_parent)

            self.filesystem.move(str(src), str(dst))
            return True
        except Exception as e:
            self.logger.warning(f"Failed to rename {src} to {dst}: {e}")
            return False

    def delete_quietly(self, path: Path):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {path}")

        try:
            if not self.delete(path, False) and self.exists(path):
                self.logger.warning(f"Failed to delete file {path}")
        except Exception:
            self.logger.warning(f"Exception occurs when deleting file {path}", exc_info=True)

    def delete_files_quietly(self, files: List[Path]):
        for file_path in files:
            self.delete_quietly(file_path)

    def delete_directory_quietly(self, directory: Path):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {directory}")

        try:
            if not self.delete(directory, True) and self.exists(directory):
                self.logger.warning(f"Failed to delete directory {directory}")
        except Exception:
            self.logger.warning(f"Exception occurs when deleting directory {directory}", exc_info=True)

    def get_file_size(self, path: Path) -> int:
        file_info = self.get_file_status(path)
        if file_info.size is None:
            raise ValueError(f"File size not available for {path}")
        return file_info.size

    def is_dir(self, path: Path) -> bool:
        file_info = self.get_file_status(path)
        return file_info.type == pyarrow.fs.FileType.Directory

    def check_or_mkdirs(self, path: Path):
        if self.exists(path):
            if not self.is_dir(path):
                raise ValueError(f"The path '{path}' should be a directory.")
        else:
            self.mkdirs(path)

    def read_file_utf8(self, path: Path) -> str:
        with self.new_input_stream(path) as input_stream:
            return input_stream.read().decode('utf-8')

    def try_to_write_atomic(self, path: Path, content: str) -> bool:
        temp_path = path.with_suffix(path.suffix + ".tmp") if path.suffix else Path(str(path) + ".tmp")
        success = False
        try:
            self.write_file(temp_path, content, False)
            success = self.rename(temp_path, path)
        finally:
            if not success:
                self.delete_quietly(temp_path)
            return success

    def write_file(self, path: Path, content: str, overwrite: bool = False):
        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def overwrite_file_utf8(self, path: Path, content: str):
        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def copy_file(self, source_path: Path, target_path: Path, overwrite: bool = False):
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")

        self.filesystem.copy_file(str(source_path), str(target_path))

    def copy_files(self, source_directory: Path, target_directory: Path, overwrite: bool = False):
        file_infos = self.list_status(source_directory)
        for file_info in file_infos:
            if file_info.type == pyarrow.fs.FileType.File:
                source_file = Path(file_info.path)
                target_file = target_directory / source_file.name
                self.copy_file(source_file, target_file, overwrite)

    def read_overwritten_file_utf8(self, path: Path) -> Optional[str]:
        retry_number = 0
        exception = None
        while retry_number < 5:
            try:
                return self.read_file_utf8(path)
            except FileNotFoundError:
                return None
            except Exception as e:
                if not self.exists(path):
                    return None

                if (str(type(e).__name__).endswith("RemoteFileChangedException") or
                        (str(e) and "Blocklist for" in str(e) and "has changed" in str(e))):
                    exception = e
                    retry_number += 1
                else:
                    raise e

        if exception:
            if isinstance(exception, Exception):
                raise exception
            else:
                raise RuntimeError(exception)

        return None

    def write_parquet(self, path: Path, data: pyarrow.RecordBatch, compression: str = 'snappy', **kwargs):
        try:
            import pyarrow.parquet as pq

            with self.new_output_stream(path) as output_stream:
                with pq.ParquetWriter(output_stream, data.schema, compression=compression, **kwargs) as pw:
                    pw.write_batch(data)

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e

    def write_orc(self, path: Path, data: pyarrow.RecordBatch, compression: str = 'zstd', **kwargs):
        try:
            import pyarrow.orc as orc
            table = pyarrow.Table.from_batches([data])
            with self.new_output_stream(path) as output_stream:
                orc.write_table(
                    table,
                    output_stream,
                    compression=compression,
                    **kwargs
                )

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write ORC file {path}: {e}") from e

    def write_avro(self, path: Path, data: pyarrow.RecordBatch, avro_schema: Optional[Dict[str, Any]] = None, **kwargs):
        import fastavro

        if avro_schema is None:
            avro_schema = PyarrowFieldParser.to_avro_schema(data.schema)
        records = data.to_pylist()
        with self.new_output_stream(path) as output_stream:
            fastavro.writer(output_stream, avro_schema, records, **kwargs)
