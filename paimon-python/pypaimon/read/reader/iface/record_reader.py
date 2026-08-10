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

from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from pypaimon.read.reader.iface.record_iterator import RecordIterator

T = TypeVar('T')


class RecordReader(Generic[T], ABC):
    """
    The reader that reads the batches of records.
    """

    @abstractmethod
    def read_batch(self) -> Optional[RecordIterator[T]]:
        """
        Reads one batch as a RecordIterator. The method should return null when reaching the end of the input.
        """

    @abstractmethod
    def close(self):
        """
        Closes the reader and should release all resources.
        """

    def _adopt_blob_metadata(self, reader) -> None:
        self.file_io = getattr(reader, 'file_io', None)
        self.blob_field_indices = getattr(reader, 'blob_field_indices', None)
        self.descriptor_field_indices = getattr(reader, 'descriptor_field_indices', None)
        self.blob_view_lookup = getattr(reader, 'blob_view_lookup', None)
        self.vector_field_indices = getattr(reader, 'vector_field_indices', None)

    def _refresh_blob_view_lookup(self, reader) -> None:
        # BlobInlineConvertReader fills lookup during the first prescan, after
        # wrappers have already copied metadata in __init__.
        self.blob_view_lookup = getattr(
            reader, 'blob_view_lookup', self.blob_view_lookup)
        self.descriptor_field_indices = getattr(
            reader, 'descriptor_field_indices', self.descriptor_field_indices)
