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

"""
The BTreeIndexReader implementation for btree index.

This reader provides efficient querying capabilities for B-tree based global indexes,
supporting various predicate operations like equality, range, and null checks.
"""

import struct
import zlib
from typing import List, Optional

from pypaimon.common.file_io import FileIO
from pypaimon.globalindex.btree.btree_index_meta import BTreeIndexMeta
from pypaimon.globalindex.btree.key_serializer import KeySerializer
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import FieldRef, GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.roaring_bitmap import RoaringBitmap64
from pypaimon.globalindex.btree.btree_file_footer import BTreeFileFooter
from pypaimon.globalindex.btree.sst_file_reader import SstFileReader
from pypaimon.globalindex.btree.memory_slice_input import MemorySliceInput


def _deserialize_row_ids(data: bytes) -> List[int]:
    input = MemorySliceInput(data)
    length = input.read_var_len_int()

    if length <= 0:
        raise ValueError(f"Invalid row id length: {length}")

    row_ids = []
    for _ in range(length):
        row_ids.append(input.read_var_len_long())

    return row_ids


class BTreeIndexReader(GlobalIndexReader):
    """
    The GlobalIndexReader implementation for btree index.
    
    This reader provides efficient querying capabilities for B-tree based global indexes,
    supporting various predicate operations like equality, range, and null checks.
    """

    FOOTER_ENCODED_LENGTH = 48

    def __init__(
        self,
        key_serializer: KeySerializer,
        file_io: FileIO,
        index_path: str,
        io_meta: GlobalIndexIOMeta
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
            # This is possible if this btree index file only stores nulls
            self.min_key = None
            self.max_key = None
        
        self.has_nulls = index_meta.has_nulls
        self.input_stream = file_io.new_input_stream(index_path + "/" + io_meta.file_name)
        
        # Lazy-loaded null bitmap
        self._null_bitmap: Optional[RoaringBitmap64] = None
        
        # Read footer to get index and bloom filter handles
        self.footer = self._read_footer()
        
        # Initialize SST file reader (simplified version)
        self.reader = self._create_sst_reader()

    def _read_footer(self) -> BTreeFileFooter:
        """
        Read the file footer to get metadata handles.

        Returns:
            BTreeFileFooter containing index_block_handle and bloom_filter_handle
        """
        file_size = self.io_meta.file_size
        # Seek to footer position
        self.input_stream.seek(file_size - BTreeFileFooter.ENCODED_LENGTH)
        footer_data = self.input_stream.read(BTreeFileFooter.ENCODED_LENGTH)

        # Parse footer
        return BTreeFileFooter.read_footer(footer_data)

    def _create_sst_reader(self) -> SstFileReader:
        def comparator(a: bytes, b: bytes) -> int:
            o1 = self.key_serializer.deserialize(a)
            o2 = self.key_serializer.deserialize(b)
            return self.comparator(o1, o2)

        return SstFileReader(self.input_stream, comparator, self.footer.index_block_handle)

    def _read_null_bitmap(self) -> RoaringBitmap64:
        """
        Read the null bitmap from the index file.
        
        Returns:
            RoaringBitmap64 containing null row IDs
        """
        if self._null_bitmap is not None:
            return self._null_bitmap
        
        bitmap = RoaringBitmap64()
        
        # Read from the null bitmap block handle if available
        if self.footer.null_bitmap_handle is not None:
            self.input_stream.seek(self.footer.null_bitmap_handle.offset)
            data = self.input_stream.read(self.footer.null_bitmap_handle.size)

            if len(data) >= 4:
                # Read bitmap data (excluding CRC32)
                bitmap_length = len(data) - 4
                bitmap_bytes = data[:bitmap_length]
                crc32_value = struct.unpack('>I', data[bitmap_length:bitmap_length + 4])[0]

                # Verify CRC32
                actual_crc32 = zlib.crc32(bitmap_bytes) & 0xFFFFFFFF
                if actual_crc32 == crc32_value:
                    bitmap = RoaringBitmap64.deserialize(bitmap_bytes)

        self._null_bitmap = bitmap
        return bitmap

    def _all_non_null_rows(self) -> RoaringBitmap64:
        """
        Get all non-null row IDs.
        
        This traverses all data to avoid returning null values, which is very
        advantageous in situations where there are many null values.
        
        Returns:
            RoaringBitmap64 containing all non-null row IDs
        """
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
        """
        Range query on underlying SST File.
        
        Args:
            from_key: Lower bound key
            to_key: Upper bound key
            from_inclusive: Whether to include lower bound
            to_inclusive: Whether to include upper bound
            
        Returns:
            RoaringBitmap64 containing all qualified row IDs
        """
        result = RoaringBitmap64()
        
        # Use SST reader to efficiently scan the data blocks
        serialized_from = self.key_serializer.serialize(from_key)
        serialized_to = self.key_serializer.serialize(to_key)

        # Create iterator and seek to start key
        file_iter = self.reader.create_iterator()
        file_iter.seek_to(serialized_from)

        # Iterate through data blocks
        while True:
            data_iter = file_iter.read_batch()
            if data_iter is None:
                break

            # Process entries in current block
            while data_iter.has_next():
                entry = data_iter.__next__()
                if entry is None:
                    break

                key_bytes = entry.key
                value_bytes = entry.value

                # Check if key is within range using byte comparison
                if self._is_in_range_bytes(key_bytes, serialized_from, serialized_to, from_inclusive, to_inclusive):
                    row_ids = _deserialize_row_ids(value_bytes)
                    for row_id in row_ids:
                        result.add(row_id)
                elif self._compare_bytes(key_bytes, serialized_to) > 0:
                    # Key is beyond the range, stop processing
                    return result

        return result

    def _is_in_range_bytes(
        self,
        key_bytes: bytes,
        from_bytes: bytes,
        to_bytes: bytes,
        from_inclusive: bool,
        to_inclusive: bool
    ) -> bool:
        """
        Check if a key (as bytes) falls within the specified range.

        Args:
            key_bytes: The key bytes to check
            from_bytes: Lower bound bytes
            to_bytes: Upper bound bytes
            from_inclusive: Whether lower bound is inclusive
            to_inclusive: Whether upper bound is inclusive

        Returns:
            True if key is in range, False otherwise
        """
        if not from_inclusive and self._compare_bytes(key_bytes, from_bytes) == 0:
            return False

        cmp_to = self._compare_bytes(key_bytes, to_bytes)
        if cmp_to > 0:
            return False
        if not to_inclusive and cmp_to == 0:
            return False

        return True

    def _compare_bytes(self, a: bytes, b: bytes) -> int:
        """
        Compare two byte arrays.

        Args:
            a: First byte array
            b: Second byte array

        Returns:
            -1 if a < b, 0 if a == b, 1 if a > b
        """
        if a < b:
            return -1
        elif a > b:
            return 1
        return 0

    def _is_in_range(
        self,
        key: object,
        from_key: object,
        to_key: object,
        from_inclusive: bool,
        to_inclusive: bool
    ) -> bool:
        """
        Check if a key falls within the specified range.
        
        Args:
            key: The key to check
            from_key: Lower bound
            to_key: Upper bound
            from_inclusive: Whether lower bound is inclusive
            to_inclusive: Whether upper bound is inclusive
            
        Returns:
            True if key is in range, False otherwise
        """
        if not from_inclusive and self.comparator(key, from_key) == 0:
            return False
        
        cmp_to = self.comparator(key, to_key)
        if cmp_to > 0:
            return False
        if not to_inclusive and cmp_to == 0:
            return False
        
        return True

    def visit_is_not_null(self, field_ref: FieldRef) -> Optional[GlobalIndexResult]:
        """
        Visit an is-not-null predicate.
        
        Nulls are stored separately in null bitmap.
        """
        def supplier():
            try:
                return self._all_non_null_rows()
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_is_null(self, field_ref: FieldRef) -> Optional[GlobalIndexResult]:
        """
        Visit an is-null predicate.
        
        Nulls are stored separately in null bitmap.
        """
        return GlobalIndexResult.create(self._read_null_bitmap)

    def visit_less_than(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a less-than predicate."""
        def supplier():
            try:
                return self._range_query(self.min_key, literal, True, False)
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_greater_or_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a greater-or-equal predicate."""
        def supplier():
            try:
                return self._range_query(literal, self.max_key, True, True)
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_not_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a not-equal predicate."""
        def supplier():
            try:
                result = self._all_non_null_rows()
                equal_result = self._range_query(literal, literal, True, True)
                result._data = result._data - equal_result._data
                return result
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_less_or_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a less-or-equal predicate."""
        def supplier():
            try:
                return self._range_query(self.min_key, literal, True, True)
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit an equality predicate."""
        def supplier():
            return self._range_query(literal, literal, True, True)

        return GlobalIndexResult.create(supplier)

    def visit_greater_than(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a greater-than predicate."""
        def supplier():
            try:
                return self._range_query(literal, self.max_key, False, True)
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_in(self, field_ref: FieldRef, literals: List[object]) -> Optional[GlobalIndexResult]:
        """Visit an in predicate."""
        def supplier():
            try:
                result = RoaringBitmap64()
                for literal in literals:
                    range_result = self._range_query(literal, literal, True, True)
                    result._data = result._data | range_result._data
                return result
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_not_in(self, field_ref: FieldRef, literals: List[object]) -> Optional[GlobalIndexResult]:
        """Visit a not-in predicate."""
        def supplier():
            try:
                result = self._all_non_null_rows()
                in_result = self.visit_in(field_ref, literals).results()
                result._data = result._data - in_result._data
                return result
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_starts_with(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """
        Visit a starts-with predicate.
        
        Note: `startsWith` can also be covered by btree index in future.
        """
        def supplier():
            try:
                return self._all_non_null_rows()
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_ends_with(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit an ends-with predicate."""
        def supplier():
            try:
                return self._all_non_null_rows()
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_contains(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a contains predicate."""
        def supplier():
            try:
                return self._all_non_null_rows()
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def visit_like(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        """Visit a like predicate."""
        def supplier():
            try:
                return self._all_non_null_rows()
            except Exception as e:
                raise RuntimeError("fail to read btree index file.", e)
        
        return GlobalIndexResult.create(supplier)

    def close(self) -> None:
        """Close the reader and release resources."""
        if hasattr(self, 'input_stream') and self.input_stream:
            try:
                self.input_stream.close()
            except Exception:
                pass
