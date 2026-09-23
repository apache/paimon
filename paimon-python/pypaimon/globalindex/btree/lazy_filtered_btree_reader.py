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

"""BTree global index reader for sorted index files."""

from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.sorted_file_global_index_reader import SortedFileGlobalIndexReader


class LazyFilteredBTreeReader(SortedFileGlobalIndexReader):
    """Manages multiple BTree index files for one row-id range."""

    def open_reader(self, meta: GlobalIndexIOMeta) -> BTreeIndexReader:
        return BTreeIndexReader(
            key_serializer=self._key_serializer,
            file_io=self._file_io,
            index_path=self._index_path,
            io_meta=meta,
        )
