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
from packaging.version import parse
from pyarrow._fs import FileSystem

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions, S3Options
from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.schema.data_types import DataField, AtomicType, PyarrowFieldParser
from pypaimon.table.row.blob import BlobData, BlobDescriptor, Blob
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.blob_format_writer import BlobFormatWriter


class FileIO:
    def __init__(self, path: str, catalog_options: Options):
        self.properties = catalog_options
        self.logger = logging.getLogger(__name__)
        scheme, netloc, _ = self.parse_location(path)
        self.uri_reader_factory = UriReaderFactory(catalog_options)
        if scheme in {"oss"}:
            self.filesystem = self._initialize_oss_fs(path)
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

    @staticmethod
    def _create_s3_retry_config(
            max_attempts: int = 10,
            request_timeout: int = 60,
            connect_timeout: int = 60
    ) -> Dict[str, Any]:
        """
        AwsStandardS3RetryStrategy and timeout parameters are only available
        in PyArrow >= 8.0.0.
        """
        if parse(pyarrow.__version__) >= parse("8.0.0"):
            config = {
                'request_timeout': request_timeout,
                'connect_timeout': connect_timeout
            }
            try:
                from pyarrow.fs import AwsStandardS3RetryStrategy
                retry_strategy = AwsStandardS3RetryStrategy(max_attempts=max_attempts)
                config['retry_strategy'] = retry_strategy
            except ImportError:
                pass
            return config
        else:
            return {}

    def _extract_oss_bucket(self, location) -> str:
        uri = urlparse(location)
        if uri.scheme and uri.scheme != "oss":
            raise ValueError("Not an OSS URI: {}".format(location))

        netloc = uri.netloc or ""
        # parse oss://access_id:secret_key@Endpoint/bucket/path/to/object
        if (getattr(uri, "username", None) or getattr(uri, "password", None)) or ("@" in netloc):
            first_segment = uri.path.lstrip("/").split("/", 1)[0]
            if not first_segment:
                raise ValueError("Invalid OSS URI without bucket: {}".format(location))
            return first_segment

        # parse oss://bucket/... or oss://bucket.endpoint/...
        host = getattr(uri, "hostname", None) or netloc
        if not host:
            raise ValueError("Invalid OSS URI without host: {}".format(location))
        bucket = host.split(".", 1)[0]
        if not bucket:
            raise ValueError("Invalid OSS URI without bucket: {}".format(location))
        return bucket

    def _initialize_oss_fs(self, path) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        client_kwargs = {
            "access_key": self.properties.get(OssOptions.OSS_ACCESS_KEY_ID),
            "secret_key": self.properties.get(OssOptions.OSS_ACCESS_KEY_SECRET),
            "session_token": self.properties.get(OssOptions.OSS_SECURITY_TOKEN),
            "region": self.properties.get(OssOptions.OSS_REGION),
        }

        # Based on https://github.com/apache/arrow/issues/40506
        if parse(pyarrow.__version__) >= parse("7.0.0"):
            client_kwargs['force_virtual_addressing'] = True
            client_kwargs['endpoint_override'] = self.properties.get(OssOptions.OSS_ENDPOINT)
        else:
            oss_bucket = self._extract_oss_bucket(path)
            client_kwargs['endpoint_override'] = (oss_bucket + "." +
                                                  self.properties.get(OssOptions.OSS_ENDPOINT))

        retry_config = self._create_s3_retry_config()
        client_kwargs.update(retry_config)

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

        retry_config = self._create_s3_retry_config()
        client_kwargs.update(retry_config)

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

    def new_input_stream(self, path: str):
        path_str = self.to_filesystem_path(path)
        return self.filesystem.open_input_file(path_str)

    def new_output_stream(self, path: str):
        path_str = self.to_filesystem_path(path)
        parent_dir = Path(path_str).parent
        if str(parent_dir) and not self.exists(str(parent_dir)):
            self.mkdirs(str(parent_dir))

        return self.filesystem.open_output_stream(path_str)

    def get_file_status(self, path: str):
        path_str = self.to_filesystem_path(path)
        file_infos = self.filesystem.get_file_info([path_str])
        return file_infos[0]

    def list_status(self, path: str):
        path_str = self.to_filesystem_path(path)
        selector = pyarrow.fs.FileSelector(path_str, recursive=False, allow_not_found=True)
        return self.filesystem.get_file_info(selector)

    def list_directories(self, path: str):
        file_infos = self.list_status(path)
        return [info for info in file_infos if info.type == pyarrow.fs.FileType.Directory]

    def exists(self, path: str) -> bool:
        try:
            path_str = self.to_filesystem_path(path)
            file_info = self.filesystem.get_file_info([path_str])[0]
            result = file_info.type != pyarrow.fs.FileType.NotFound
            return result
        except Exception:
            return False

    def delete(self, path: str, recursive: bool = False) -> bool:
        try:
            path_str = self.to_filesystem_path(path)
            file_info = self.filesystem.get_file_info([path_str])[0]
            if file_info.type == pyarrow.fs.FileType.Directory:
                if recursive:
                    self.filesystem.delete_dir_contents(path_str)
                else:
                    self.filesystem.delete_dir(path_str)
            else:
                self.filesystem.delete_file(path_str)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to delete {path}: {e}")
            return False

    def mkdirs(self, path: str) -> bool:
        try:
            path_str = self.to_filesystem_path(path)
            self.filesystem.create_dir(path_str, recursive=True)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to create directory {path}: {e}")
            return False

    def rename(self, src: str, dst: str) -> bool:
        try:
            dst_str = self.to_filesystem_path(dst)
            dst_parent = Path(dst_str).parent
            if str(dst_parent) and not self.exists(str(dst_parent)):
                self.mkdirs(str(dst_parent))

            src_str = self.to_filesystem_path(src)
            self.filesystem.move(src_str, dst_str)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to rename {src} to {dst}: {e}")
            return False

    def delete_quietly(self, path: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {path}")

        try:
            if not self.delete(path, False) and self.exists(path):
                self.logger.warning(f"Failed to delete file {path}")
        except Exception:
            self.logger.warning(f"Exception occurs when deleting file {path}", exc_info=True)

    def delete_files_quietly(self, files: List[str]):
        for file_path in files:
            self.delete_quietly(file_path)

    def delete_directory_quietly(self, directory: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {directory}")

        try:
            if not self.delete(directory, True) and self.exists(directory):
                self.logger.warning(f"Failed to delete directory {directory}")
        except Exception:
            self.logger.warning(f"Exception occurs when deleting directory {directory}", exc_info=True)

    def get_file_size(self, path: str) -> int:
        file_info = self.get_file_status(path)
        if file_info.size is None:
            raise ValueError(f"File size not available for {path}")
        return file_info.size

    def is_dir(self, path: str) -> bool:
        file_info = self.get_file_status(path)
        return file_info.type == pyarrow.fs.FileType.Directory

    def check_or_mkdirs(self, path: str):
        if self.exists(path):
            if not self.is_dir(path):
                raise ValueError(f"The path '{path}' should be a directory.")
        else:
            self.mkdirs(path)

    def read_file_utf8(self, path: str) -> str:
        with self.new_input_stream(path) as input_stream:
            return input_stream.read().decode('utf-8')

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        temp_path = path + ".tmp"
        success = False
        try:
            self.write_file(temp_path, content, False)
            success = self.rename(temp_path, path)
        finally:
            if not success:
                self.delete_quietly(temp_path)
            return success

    def write_file(self, path: str, content: str, overwrite: bool = False):
        if not overwrite and self.exists(path):
            raise FileExistsError(f"File {path} already exists and overwrite=False")

        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def overwrite_file_utf8(self, path: str, content: str):
        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")

        source_str = self.to_filesystem_path(source_path)
        target_str = self.to_filesystem_path(target_path)
        self.filesystem.copy_file(source_str, target_str)

    def copy_files(self, source_directory: str, target_directory: str, overwrite: bool = False):
        file_infos = self.list_status(source_directory)
        for file_info in file_infos:
            if file_info.type == pyarrow.fs.FileType.File:
                source_file = file_info.path
                file_name = source_file.split('/')[-1]
                target_file = f"{target_directory.rstrip('/')}/{file_name}" if target_directory else file_name
                self.copy_file(source_file, target_file, overwrite)

    def read_overwritten_file_utf8(self, path: str) -> Optional[str]:
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

    def write_parquet(self, path: str, data: pyarrow.Table, compression: str = 'snappy', **kwargs):
        try:
            import pyarrow.parquet as pq

            with self.new_output_stream(path) as output_stream:
                pq.write_table(data, output_stream, compression=compression, **kwargs)

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e

    def write_orc(self, path: str, data: pyarrow.Table, compression: str = 'zstd', **kwargs):
        try:
            """Write ORC file using PyArrow ORC writer."""
            import sys
            import pyarrow.orc as orc

            with self.new_output_stream(path) as output_stream:
                # Check Python version - if 3.6, don't use compression parameter
                if sys.version_info[:2] == (3, 6):
                    orc.write_table(data, output_stream, **kwargs)
                else:
                    orc.write_table(
                        data,
                        output_stream,
                        compression=compression,
                        **kwargs
                    )

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write ORC file {path}: {e}") from e

    def write_avro(self, path: str, data: pyarrow.Table, avro_schema: Optional[Dict[str, Any]] = None, **kwargs):
        import fastavro
        if avro_schema is None:
            from pypaimon.schema.data_types import PyarrowFieldParser
            avro_schema = PyarrowFieldParser.to_avro_schema(data.schema)

        records_dict = data.to_pydict()

        def record_generator():
            num_rows = len(list(records_dict.values())[0])
            for i in range(num_rows):
                yield {col: records_dict[col][i] for col in records_dict.keys()}

        records = record_generator()

        with self.new_output_stream(path) as output_stream:
            fastavro.writer(output_stream, avro_schema, records, **kwargs)

    def write_lance(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import lance
            from pypaimon.read.reader.lance_utils import to_lance_specified
            file_path_for_lance, storage_options = to_lance_specified(self, path)

            writer = lance.file.LanceFileWriter(
                file_path_for_lance, data.schema, storage_options=storage_options, **kwargs)
            try:
                # Write all batches
                for batch in data.to_batches():
                    writer.write_batch(batch)
            finally:
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Lance file {path}: {e}") from e

    def write_blob(self, path: str, data: pyarrow.Table, blob_as_descriptor: bool, **kwargs):
        try:
            # Validate input constraints
            if data.num_columns != 1:
                raise RuntimeError(f"Blob format only supports a single column, got {data.num_columns} columns")
            # Check for null values
            column = data.column(0)
            if column.null_count > 0:
                raise RuntimeError("Blob format does not support null values")
            # Convert PyArrow schema to Paimon DataFields
            # For blob files, we expect exactly one blob column
            field = data.schema[0]
            if pyarrow.types.is_large_binary(field.type):
                fields = [DataField(0, field.name, AtomicType("BLOB"))]
            else:
                # Convert other types as needed
                paimon_type = PyarrowFieldParser.to_paimon_type(field.type, field.nullable)
                fields = [DataField(0, field.name, paimon_type)]
            # Convert PyArrow Table to records
            records_dict = data.to_pydict()
            num_rows = data.num_rows
            field_name = fields[0].name
            with self.new_output_stream(path) as output_stream:
                writer = BlobFormatWriter(output_stream)
                # Write each row
                for i in range(num_rows):
                    col_data = records_dict[field_name][i]
                    # Convert to appropriate type based on field type
                    if hasattr(fields[0].type, 'type') and fields[0].type.type == "BLOB":
                        if blob_as_descriptor:
                            blob_descriptor = BlobDescriptor.deserialize(col_data)
                            uri_reader = self.uri_reader_factory.create(blob_descriptor.uri)
                            blob_data = Blob.from_descriptor(uri_reader, blob_descriptor)
                        elif isinstance(col_data, bytes):
                            blob_data = BlobData(col_data)
                        else:
                            # Convert to bytes if needed
                            if hasattr(col_data, 'as_py'):
                                col_data = col_data.as_py()
                            if isinstance(col_data, str):
                                col_data = col_data.encode('utf-8')
                            blob_data = BlobData(col_data)
                        row_values = [blob_data]
                    else:
                        row_values = [col_data]
                    # Create GenericRow and write
                    row = GenericRow(row_values, fields, RowKind.INSERT)
                    writer.add_element(row)
                writer.close()

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write blob file {path}: {e}") from e

    def to_filesystem_path(self, path: str) -> str:
        from pyarrow.fs import S3FileSystem
        import re

        parsed = urlparse(path)
        normalized_path = re.sub(r'/+', '/', parsed.path) if parsed.path else ''

        if parsed.scheme and len(parsed.scheme) == 1 and not parsed.netloc:
            # This is likely a Windows drive letter, not a URI scheme
            return str(path)

        if parsed.scheme == 'file' and parsed.netloc and parsed.netloc.endswith(':'):
            # file://C:/path format - netloc is 'C:', need to reconstruct path with drive letter
            drive_letter = parsed.netloc.rstrip(':')
            path_part = normalized_path.lstrip('/')
            return f"{drive_letter}:/{path_part}" if path_part else f"{drive_letter}:"

        if isinstance(self.filesystem, S3FileSystem):
            # For S3, return "bucket/path" format
            if parsed.scheme:
                if parsed.netloc:
                    # Has netloc (bucket): return "bucket/path" format
                    path_part = normalized_path.lstrip('/')
                    return f"{parsed.netloc}/{path_part}" if path_part else parsed.netloc
                else:
                    # Has scheme but no netloc: return path without scheme
                    result = normalized_path.lstrip('/')
                    return result if result else '.'
            return str(path)

        if parsed.scheme:
            # Handle empty path: return '.' for current directory
            if not normalized_path:
                return '.'
            return normalized_path

        return str(path)
