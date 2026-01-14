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
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

import pyarrow

from pypaimon.common.options import Options


class FileIO(ABC):
    """
    File IO interface to read and write files.
    """
    
    @abstractmethod
    def new_input_stream(self, path: str):
        pass

    @abstractmethod
    def new_output_stream(self, path: str):
        pass
    
    @abstractmethod
    def get_file_status(self, path: str):
        pass
    
    @abstractmethod
    def list_status(self, path: str):
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def delete(self, path: str, recursive: bool = False) -> bool:
        pass
    
    @abstractmethod
    def mkdirs(self, path: str) -> bool:
        pass
    
    @abstractmethod
    def rename(self, src: str, dst: str) -> bool:
        pass
    
    def delete_quietly(self, path: str):
        logger = logging.getLogger(__name__)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Ready to delete {path}")

        try:
            if not self.delete(path, False) and self.exists(path):
                logger.warning(f"Failed to delete file {path}")
        except Exception:
            logger.warning(f"Exception occurs when deleting file {path}", exc_info=True)

    def delete_files_quietly(self, files: List[str]):
        for file_path in files:
            self.delete_quietly(file_path)

    def delete_directory_quietly(self, directory: str):
        logger = logging.getLogger(__name__)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Ready to delete {directory}")

        try:
            if not self.delete(directory, True) and self.exists(directory):
                logger.warning(f"Failed to delete directory {directory}")
        except Exception:
            logger.warning(f"Exception occurs when deleting directory {directory}", exc_info=True)

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
        if self.exists(path):
            if self.is_dir(path):
                return False
        
        temp_path = path + str(uuid.uuid4()) + ".tmp"
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

        target_str = self.to_filesystem_path(target_path)
        target_parent = Path(target_str).parent
        
        if str(target_parent) and not self.exists(str(target_parent)):
            self.mkdirs(str(target_parent))

        with self.new_input_stream(source_path) as input_stream:
            with self.new_output_stream(target_path) as output_stream:
                output_stream.write(input_stream.read())

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

    def to_filesystem_path(self, path: str) -> str:
        return path
    
    @staticmethod
    def get(path: str, catalog_options: Optional[Options] = None) -> 'FileIO':
        """
        Returns a FileIO instance for accessing the file system identified by the given path.
        - LocalFileIO for local file system (file:// or no scheme)
        - PyArrowFileIO for remote file systems (oss://, s3://, hdfs://, etc.)
        """
        from urllib.parse import urlparse
        
        uri = urlparse(path)
        scheme = uri.scheme
        
        if not scheme or scheme == "file":
            from pypaimon.filesystem.local_file_io import LocalFileIO
            return LocalFileIO(path, catalog_options)
        
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
        return PyArrowFileIO(path, catalog_options or Options({}))
