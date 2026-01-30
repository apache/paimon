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

"""Reader for a block."""

import struct
from typing import Callable, Optional

from pypaimon.globalindex.btree.block_aligned_type import BlockAlignedType
from pypaimon.globalindex.btree.block_entry import BlockEntry
from pypaimon.globalindex.btree.memory_slice_input import MemorySliceInput


class BlockReader:
    """Reader for a block."""
    
    def __init__(
        self,
        block: bytes,
        record_count: int,
        comparator: Callable[[bytes, bytes], int]
    ):
        """Initialize BlockReader.
        
        Args:
            block: The block data bytes
            record_count: Number of records in the block
            comparator: Optional comparator function for keys
        """
        self.block = block
        self.record_count = record_count
        self.comparator = comparator
    
    def block_input(self) -> MemorySliceInput:
        """Create a MemorySliceInput for this block."""
        return MemorySliceInput(self.block)
    
    def iterator(self) -> 'BlockIterator':
        """Create a BlockIterator for this reader."""
        return BlockIterator(self)
    
    def seek_to(self, record_position: int) -> int:
        """Seek to slice position from record position.
        
        Args:
            record_position: The record position to seek to
            
        Returns:
            The slice position
            
        Raises:
            NotImplementedError: If not implemented in subclass
        """
        raise NotImplementedError("seekTo must be implemented in subclass")
    
    @staticmethod
    def create(
        block: bytes,
        comparator: Optional[Callable[[bytes, bytes], int]] = None
    ) -> 'BlockReader':
        """Create a BlockReader from block data.
        
        Args:
            block: The block data bytes
            comparator: Optional comparator function for keys
            
        Returns:
            A BlockReader instance (AlignedBlockReader or UnalignedBlockReader)
        """
        # Read block trailer: last byte is aligned type, previous 4 bytes is record size or index length
        aligned_type_byte = block[-1]
        aligned_type = BlockAlignedType.from_byte(aligned_type_byte)
        int_value = struct.unpack('<I', block[-5:-1])[0]
        
        if aligned_type == BlockAlignedType.ALIGNED:
            # Aligned block: records have fixed size
            data = block[:-5]
            return AlignedBlockReader(data, int_value, comparator)
        else:
            # Unaligned block: uses index
            index_length = int_value * 4
            index_offset = len(block) - 5 - index_length
            data = block[:index_offset]
            index = block[index_offset:index_offset + index_length]
            return UnalignedBlockReader(data, index, comparator)


class AlignedBlockReader(BlockReader):
    """Block reader for aligned blocks (fixed record size)."""
    
    def __init__(
        self,
        data: bytes,
        record_size: int,
        comparator: Optional[Callable[[bytes, bytes], int]] = None
    ):
        """Initialize AlignedBlockReader.
        
        Args:
            data: The block data bytes
            record_size: The fixed size of each record
            comparator: Optional comparator function for keys
        """
        record_count = len(data) // record_size
        super().__init__(data, record_count, comparator)
        self.record_size = record_size
    
    def seek_to(self, record_position: int) -> int:
        """Seek to slice position from record position.
        
        Args:
            record_position: The record position to seek to
            
        Returns:
            The slice position
        """
        return record_position * self.record_size


class UnalignedBlockReader(BlockReader):
    """Block reader for unaligned blocks (uses index)."""
    
    def __init__(
        self,
        data: bytes,
        index: bytes,
        comparator: Optional[Callable[[bytes, bytes], int]] = None
    ):
        """Initialize UnalignedBlockReader.
        
        Args:
            data: The block data bytes
            index: The index bytes (4 bytes per record)
            comparator: Optional comparator function for keys
        """
        record_count = len(index) // 4
        super().__init__(data, record_count, comparator)
        self.index = index
    
    def seek_to(self, record_position: int) -> int:
        """Seek to slice position from record position.
        
        Args:
            record_position: The record position to seek to
            
        Returns:
            The slice position
        """
        # Read 4-byte integer from index at record_position * 4
        offset = record_position * 4
        return struct.unpack('<I', self.index[offset:offset + 4])[0]


class BlockIterator:
    """Iterator for block entries."""
    
    def __init__(self, reader: BlockReader):
        """Initialize BlockIterator.
        
        Args:
            reader: The BlockReader to iterate over
        """
        self.reader = reader
        self.input = reader.block_input()
        self.polled: Optional[BlockEntry] = None
    
    def __iter__(self):
        """Return self as iterator."""
        return self
    
    def __next__(self) -> BlockEntry:
        """Get next entry.
        
        Returns:
            The next BlockEntry
            
        Raises:
            StopIteration: If no more entries
        """
        if not self.has_next():
            raise StopIteration
        
        if self.polled is not None:
            result = self.polled
            self.polled = None
            return result
        
        return self.read_entry()
    
    def has_next(self) -> bool:
        """Check if there are more entries."""
        return self.polled is not None or self.input.is_readable()
    
    def seek_to(self, target_key: bytes) -> bool:
        """Seek to the first key >= target_key using binary search.
        
        Args:
            target_key: The target key to seek to
            
        Returns:
            True if exact match found, False otherwise
        """
        left = 0
        right = self.reader.record_count - 1
        
        while left <= right:
            mid = left + (right - left) // 2
            
            self.input.set_position(self.reader.seek_to(mid))
            mid_entry = self.read_entry()
            compare = self.reader.comparator(mid_entry.key, target_key) if self.reader.comparator else -1
            
            if compare == 0:
                self.polled = mid_entry
                return True
            elif compare > 0:
                self.polled = mid_entry
                right = mid - 1
            else:
                self.polled = None
                left = mid + 1
        
        return False
    
    def read_entry(self) -> BlockEntry:
        """Read a key-value entry.
        
        Returns:
            A BlockEntry containing key and value
        """
        # Read key
        key_length = self.input.read_var_len_int()
        key = self.input.read_slice(key_length)
        
        # Read value
        value_length = self.input.read_var_len_int()
        value = self.input.read_slice(value_length)
        
        return BlockEntry(key, value)
