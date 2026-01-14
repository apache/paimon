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
#  limitations under the License.
################################################################################
import logging
import os
import shutil
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import pyarrow

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.schema.data_types import DataField, AtomicType, PyarrowFieldParser
from pypaimon.table.row.blob import BlobData, BlobDescriptor, Blob
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.blob_format_writer import BlobFormatWriter


class LocalFileIO(FileIO):
    """
    Local file system implementation of FileIO.
    """
    
    RENAME_LOCK = threading.Lock()
    
    INSTANCE = None
    
    def __init__(self, path: str = None, catalog_options: Optional[Options] = None):
        self.logger = logging.getLogger(__name__)
        self.path = path
        self.properties = catalog_options or Options({})
    
    @staticmethod
    def create():
        return LocalFileIO()
    
    def _to_file(self, path: str) -> Path:
        parsed = urlparse(path)
        local_path = parsed.path if parsed.scheme else path
        
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
        
        return open(file_path, 'wb')
    
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
                self.path = str(file_path.absolute())
                self.original_path = original_path
                self.size = stat_info.st_size if file_path.is_file() else None
                self.type = (
                    pyarrow.fs.FileType.Directory if file_path.is_dir()
                    else pyarrow.fs.FileType.File if file_path.is_file()
                    else pyarrow.fs.FileType.NotFound
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
            try:
                for item in file_path.iterdir():
                    try:
                        if path.startswith('file://'):
                            item_path = f"file://{item}"
                        else:
                            item_path = str(item)
                        results.append(self.get_file_status(item_path))
                    except FileNotFoundError:
                        pass
            except PermissionError:
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
            raise FileExistsError(str(file_path.absolute()))
        
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
        
        temp_path = file_path.parent / f"{file_path.name}.{uuid.uuid4()}.tmp"
        success = False
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
            success = self.rename(str(temp_path), path)
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
        return str(file_path)
    
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
            
            with open(file_path, 'wb') as f:
                if compression.lower() == 'zstd':
                    kwargs['compression_level'] = zstd_level
                pq.write_table(data, f, compression=compression, **kwargs)
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e
    
    def write_orc(self, path: str, data: pyarrow.Table, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        try:
            import sys
            import pyarrow.orc as orc
            
            file_path = self._to_file(path)
            parent = file_path.parent
            if parent and not parent.exists():
                parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                if sys.version_info[:2] == (3, 6):
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
                yield {col: records_dict[col][i] for col in records_dict.keys()}
        
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
        
        with open(file_path, 'wb') as output_stream:
            if codec == 'zstandard':
                kwargs['codec_compression_level'] = zstd_level
            fastavro.writer(output_stream, avro_schema, records, codec=codec, **kwargs)
    
    def write_lance(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import lance
            from pypaimon.read.reader.lance_utils import to_lance_specified
            file_path_for_lance, storage_options = to_lance_specified(self, path)
            
            writer = lance.file.LanceFileWriter(
                file_path_for_lance, data.schema, storage_options=storage_options, **kwargs)
            try:
                for batch in data.to_batches():
                    writer.write_batch(batch)
            finally:
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Lance file {path}: {e}") from e
    
    def write_blob(self, path: str, data: pyarrow.Table, blob_as_descriptor: bool, **kwargs):
        try:
            if data.num_columns != 1:
                raise RuntimeError(f"Blob format only supports a single column, got {data.num_columns} columns")
            
            column = data.column(0)
            if column.null_count > 0:
                raise RuntimeError("Blob format does not support null values")
            
            field = data.schema[0]
            if pyarrow.types.is_large_binary(field.type):
                fields = [DataField(0, field.name, AtomicType("BLOB"))]
            else:
                paimon_type = PyarrowFieldParser.to_paimon_type(field.type, field.nullable)
                fields = [DataField(0, field.name, paimon_type)]
            
            records_dict = data.to_pydict()
            num_rows = data.num_rows
            field_name = fields[0].name
            
            file_path = self._to_file(path)
            parent = file_path.parent
            if parent and not parent.exists():
                parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as output_stream:
                writer = BlobFormatWriter(output_stream)
                for i in range(num_rows):
                    col_data = records_dict[field_name][i]
                    if hasattr(fields[0].type, 'type') and fields[0].type.type == "BLOB":
                        if blob_as_descriptor:
                            blob_descriptor = BlobDescriptor.deserialize(col_data)
                            from pypaimon.common.uri_reader import UriReaderFactory
                            uri_reader_factory = UriReaderFactory(self.properties)
                            uri_reader = uri_reader_factory.create(blob_descriptor.uri)
                            blob_data = Blob.from_descriptor(uri_reader, blob_descriptor)
                        elif isinstance(col_data, bytes):
                            blob_data = BlobData(col_data)
                        else:
                            if hasattr(col_data, 'as_py'):
                                col_data = col_data.as_py()
                            if isinstance(col_data, str):
                                col_data = col_data.encode('utf-8')
                            blob_data = BlobData(col_data)
                        row_values = [blob_data]
                    else:
                        row_values = [col_data]
                    row = GenericRow(row_values, fields, RowKind.INSERT)
                    writer.add_element(row)
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write blob file {path}: {e}") from e


LocalFileIO.INSTANCE = LocalFileIO()

