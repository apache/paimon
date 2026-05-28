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

"""The BTreeIndexReader implementation for btree index.

Synchronous index reader for a single BTree index file. Parallelism across
multiple files is handled by LazyFilteredBTreeReader.
"""

import struct
import threading
import zlib
from typing import List, Optional

from pypaimon.common.file_io import FileIO, supports_pread, pread
from pypaimon.globalindex.btree.btree_index_meta import BTreeIndexMeta
from pypaimon.globalindex.btree.key_serializer import KeySerializer
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.utils.roaring_bitmap import RoaringBitmap64
from pypaimon.globalindex.btree.btree_file_footer import BTreeFileFooter
from pypaimon.globalindex.btree.sst_file_reader import SstFileReader
from pypaimon.globalindex.btree.memory_slice_input import MemorySliceInput


def _deserialize_row_ids(data: bytes) -> List[int]:
    data_input = MemorySliceInput(data)
    length = data_input.read_var_len_int()

    if length <= 0:
        raise ValueError(f"Invalid row id length: {length}")

    row_ids = []
    for _ in range(length):
        row_ids.append(data_input.read_var_len_long())

    return row_ids


class BTreeIndexReader:
    """Synchronous index reader for a single BTree index file.

    Parallelism across multiple files is handled by LazyFilteredBTreeReader.
    """

    FOOTER_ENCODED_LENGTH = 52

    def __init__(
        self,
        key_serializer: KeySerializer,
        file_io: FileIO,
        index_path: str,
        io_meta: GlobalIndexIOMeta,
    ):
        self.key_serializer = key_serializer
        self.comparator = key_serializer.create_comparator()
        self.io_meta = io_meta

        # Deserialize index metadata
        index_meta = BTreeIndexMeta.deserialize(io_meta.metadata)

        if index_meta.first_key is not None:
            self.min_key = key_serializer.deserialize(index_meta.first_key)
            self.max_key = key_serializer.deserialize(index_meta.last_key)
        else:
            self.min_key = None
            self.max_key = None

        self.has_nulls = index_meta.has_nulls
        file_path = (io_meta.external_path
                     if io_meta.external_path
                     else index_path + "/" + io_meta.file_name)
        self.input_stream = file_io.new_input_stream(file_path)
        self._supports_pread = supports_pread(self.input_stream)
        self._io_lock = threading.Lock()

        # Lazy-loaded null bitmap
        self._null_bitmap: Optional[RoaringBitmap64] = None
        self._null_bitmap_lock = threading.Lock()

        # Read footer to get index and bloom filter handles
        self.footer = self._read_footer()

        # Initialize SST file reader
        self.reader = self._create_sst_reader()

    def _read_from(self, offset: int, length: int) -> bytes:
        if self._supports_pread:
            return pread(self.input_stream, length, offset)
        with self._io_lock:
            self.input_stream.seek(offset)
            return self.input_stream.read(length)

    def _read_footer(self) -> BTreeFileFooter:
        file_size = self.io_meta.file_size
        footer_data = self._read_from(
            file_size - BTreeFileFooter.ENCODED_LENGTH,
            BTreeFileFooter.ENCODED_LENGTH)
        return BTreeFileFooter.read_footer(footer_data)

    def _create_sst_reader(self) -> SstFileReader:
        def comparator(a: bytes, b: bytes) -> int:
            o1 = self.key_serializer.deserialize(a)
            o2 = self.key_serializer.deserialize(b)
            return self.comparator(o1, o2)

        return SstFileReader(
            self.input_stream, comparator, self.footer.index_block_handle,
            use_pread=self._supports_pread, io_lock=self._io_lock)

    def _read_null_bitmap(self) -> RoaringBitmap64:
        if self._null_bitmap is not None:
            return self._null_bitmap

        with self._null_bitmap_lock:
            if self._null_bitmap is not None:
                return self._null_bitmap

            bitmap = RoaringBitmap64()

            if self.footer.null_bitmap_handle is not None:
                data = self._read_from(
                    self.footer.null_bitmap_handle.offset,
                    self.footer.null_bitmap_handle.size + 4)
                bitmap_length = len(data) - 4
                bitmap_bytes = data[:bitmap_length]
                crc32_value = struct.unpack('<I', data[bitmap_length:bitmap_length + 4])[0]

                actual_crc32 = zlib.crc32(bitmap_bytes) & 0xFFFFFFFF
                if actual_crc32 != crc32_value:
                    raise ValueError(f"CRC32 mismatch: expected {crc32_value}, got {actual_crc32}")
                bitmap = RoaringBitmap64.deserialize(bitmap_bytes)

            self._null_bitmap = bitmap
            return bitmap

    def _all_non_null_rows(self) -> RoaringBitmap64:
        if self.min_key is None:
            return RoaringBitmap64()
        return self._range_query(self.min_key, self.max_key, True, True)

    def _range_query(
        self,
        from_key: object,
        to_key: object,
        from_inclusive: bool,
        to_inclusive: bool
    ) -> RoaringBitmap64:
        result = RoaringBitmap64()

        file_iter = self.reader.create_iterator()
        file_iter.seek_to(self.key_serializer.serialize(from_key))

        while True:
            data_iter = file_iter.read_batch()
            if data_iter is None:
                break

            while data_iter.has_next():
                entry = data_iter.__next__()
                if entry is None:
                    break

                key_bytes = entry.key
                value_bytes = entry.value

                key = self.key_serializer.deserialize(key_bytes)

                if not from_inclusive and self.comparator(key, from_key) == 0:
                    continue

                difference = self.comparator(key, to_key)
                if difference > 0 or (not to_inclusive and difference == 0):
                    return result

                row_ids = _deserialize_row_ids(value_bytes)
                for row_id in row_ids:
                    result.add(row_id)

        return result

    def visit_is_not_null(self) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._all_non_null_rows())

    def visit_is_null(self) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._read_null_bitmap())

    def visit_less_than(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(
            self._range_query(self.min_key, literal, True, False))

    def visit_greater_or_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(
            self._range_query(literal, self.max_key, True, True))

    def visit_not_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        result = self._all_non_null_rows()
        equal_result = self._range_query(literal, literal, True, True)
        return GlobalIndexResult.create(
            RoaringBitmap64.remove_all(result, equal_result))

    def visit_less_or_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(
            self._range_query(self.min_key, literal, True, True))

    def visit_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(
            self._range_query(literal, literal, True, True))

    def visit_greater_than(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(
            self._range_query(literal, self.max_key, False, True))

    def visit_in(self, literals: List[object]) -> Optional[GlobalIndexResult]:
        result = RoaringBitmap64()
        for literal in literals:
            range_result = self._range_query(literal, literal, True, True)
            result = RoaringBitmap64.or_(result, range_result)
        return GlobalIndexResult.create(result)

    def visit_not_in(self, literals: List[object]) -> Optional[GlobalIndexResult]:
        result = self._all_non_null_rows()
        for literal in literals:
            range_result = self._range_query(literal, literal, True, True)
            result = RoaringBitmap64.remove_all(result, range_result)
        return GlobalIndexResult.create(result)

    def visit_starts_with(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._all_non_null_rows())

    def visit_ends_with(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._all_non_null_rows())

    def visit_contains(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._all_non_null_rows())

    def visit_like(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._all_non_null_rows())

    def visit_between(self, min_v: object, max_v: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(
            self._range_query(min_v, max_v, True, True))

    def close(self) -> None:
        if self.input_stream is not None:
            self.input_stream.close()
