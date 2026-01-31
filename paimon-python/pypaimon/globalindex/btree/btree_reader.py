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

"""B-tree file footer for storing index metadata."""

from dataclasses import dataclass
from typing import Optional
import struct

from pypaimon.globalindex.btree.block_handle import BlockHandle


@dataclass
class BloomFilterHandle:
    """Handle for bloom filter block."""
    
    offset: int
    size: int
    expected_entries: int
    
    def is_null(self) -> bool:
        """Check if this handle represents a null bloom filter."""
        return self.offset == 0 and self.size == 0 and self.expected_entries == 0


class BTreeFileFooter:
    """
    The Footer for BTree file.
    
    This footer contains the handles to the bloom filter, index block, and null bitmap,
    allowing efficient navigation of the B-tree index file.
    """
    
    MAGIC_NUMBER = 198732882
    ENCODED_LENGTH = 48
    
    def __init__(
        self,
        bloom_filter_handle: Optional[BloomFilterHandle],
        index_block_handle: BlockHandle,
        null_bitmap_handle: Optional[BlockHandle]
    ):
        """
        Initialize the BTree file footer.
        
        Args:
            bloom_filter_handle: Handle to the bloom filter block (maybe None)
            index_block_handle: Handle to the index block
            null_bitmap_handle: Handle to the null bitmap block (maybe None)
        """
        self.bloom_filter_handle = bloom_filter_handle
        self.index_block_handle = index_block_handle
        self.null_bitmap_handle = null_bitmap_handle
    
    @classmethod
    def read_footer(cls, data: bytes) -> 'BTreeFileFooter':
        """
        Read footer from byte data.
        
        Args:
            data: Byte data containing the footer
            
        Returns:
            BTreeFileFooter instance
            
        Raises:
            ValueError: If magic number doesn't match
        """
        offset = 0
        
        # Read bloom filter handle
        bf_offset = struct.unpack('<Q', data[offset:offset + 8])[0]
        offset += 8
        bf_size = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        bf_expected = struct.unpack('<Q', data[offset:offset + 8])[0]
        offset += 8
        
        bloom_filter_handle = None
        if not (bf_offset == 0 and bf_size == 0 and bf_expected == 0):
            bloom_filter_handle = BloomFilterHandle(bf_offset, bf_size, bf_expected)
        
        # Read index block handle
        index_offset = struct.unpack('<Q', data[offset:offset + 8])[0]
        offset += 8
        index_size = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        
        index_block_handle = BlockHandle(index_offset, index_size)
        
        # Read null bitmap handle
        nb_offset = struct.unpack('<Q', data[offset:offset + 8])[0]
        offset += 8
        nb_size = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        
        null_bitmap_handle = None
        if not (nb_offset == 0 and nb_size == 0):
            null_bitmap_handle = BlockHandle(nb_offset, nb_size)
        
        # Skip padding
        offset = cls.ENCODED_LENGTH - 4
        
        # Read and verify magic number
        magic_number = struct.unpack('<I', data[offset:offset + 4])[0]
        if magic_number != cls.MAGIC_NUMBER:
            raise ValueError("File is not a table (bad magic number)")
        
        return cls(bloom_filter_handle, index_block_handle, null_bitmap_handle)
    
    def write_footer(self) -> bytes:
        """
        Write footer to byte data.
        
        Returns:
            Byte data containing the footer
        """
        buffer = bytearray()
        
        # Write bloom filter handle
        if self.bloom_filter_handle is None:
            buffer.extend(struct.pack('>Q', 0))
            buffer.extend(struct.pack('>I', 0))
            buffer.extend(struct.pack('>Q', 0))
        else:
            buffer.extend(struct.pack('>Q', self.bloom_filter_handle.offset))
            buffer.extend(struct.pack('>I', self.bloom_filter_handle.size))
            buffer.extend(struct.pack('>Q', self.bloom_filter_handle.expected_entries))
        
        # Write index block handle
        buffer.extend(struct.pack('>Q', self.index_block_handle.offset))
        buffer.extend(struct.pack('>I', self.index_block_handle.size))
        
        # Write null bitmap handle
        if self.null_bitmap_handle is None:
            buffer.extend(struct.pack('>Q', 0))
            buffer.extend(struct.pack('>I', 0))
        else:
            buffer.extend(struct.pack('>Q', self.null_bitmap_handle.offset))
            buffer.extend(struct.pack('>I', self.null_bitmap_handle.size))
        
        # Write magic number
        buffer.extend(struct.pack('>I', self.MAGIC_NUMBER))
        
        # Pad to ENCODED_LENGTH
        while len(buffer) < self.ENCODED_LENGTH:
            buffer.extend(b'\x00')
        
        return bytes(buffer)
