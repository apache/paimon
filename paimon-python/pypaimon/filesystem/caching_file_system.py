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

import pyarrow as pa
import pyarrow.fs as pafs


class CachedFileSystemHandler(pafs.FileSystemHandler):
    """Route one fragment's input through FileIO while preserving filesystem operations."""

    def __init__(self, file_io, path, file_size=None):
        self._file_io = file_io
        self._path = path
        self._delegate = file_io.filesystem
        self._filesystem_path = self._delegate.normalize_path(file_io.to_filesystem_path(path))
        self._file_size = file_size

    def get_type_name(self):
        return 'paimon-cached-file'

    def open_input_file(self, path):
        if self._delegate.normalize_path(path) != self._filesystem_path:
            return self._delegate.open_input_file(path)
        return pa.PythonFile(
            self._file_io.new_input_stream(self._path, file_size=self._file_size), mode='r')

    def open_input_stream(self, path):
        return self.open_input_file(path)

    def normalize_path(self, path):
        return self._delegate.normalize_path(path)

    def get_file_info(self, paths):
        return self._delegate.get_file_info(paths)

    def get_file_info_selector(self, selector):
        return self._delegate.get_file_info(selector)

    def create_dir(self, path, recursive):
        return self._delegate.create_dir(path, recursive=recursive)

    def delete_dir(self, path):
        return self._delegate.delete_dir(path)

    def delete_dir_contents(self, path, missing_dir_ok):
        return self._delegate.delete_dir_contents(path, missing_dir_ok=missing_dir_ok)

    def delete_root_dir_contents(self):
        return self._delegate.delete_dir_contents("", accept_root_dir=True)

    def delete_file(self, path):
        return self._delegate.delete_file(path)

    def move(self, src, dest):
        return self._delegate.move(src, dest)

    def copy_file(self, src, dest):
        return self._delegate.copy_file(src, dest)

    def open_output_stream(self, path, metadata):
        return self._delegate.open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path, metadata):
        return self._delegate.open_append_stream(path, metadata=metadata)
