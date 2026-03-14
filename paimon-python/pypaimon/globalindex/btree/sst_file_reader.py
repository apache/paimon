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
An SST File Reader which serves point queries and range queries.

Users can call createIterator to create a file iterator and then use seek
and read methods to do range queries.

Note that this class is NOT thread-safe.
"""

import struct
import zlib
from typing import Optional, Callable
from typing import BinaryIO

from pypaimon.globalindex.btree.btree_file_footer import BlockHandle
from pypaimon.globalindex.btree.block_entry import BlockEntry
from pypaimon.globalindex.btree.block_reader import BlockReader, BlockIterator
from pypaimon.globalindex.btree.memory_slice_input import MemorySliceInput


class SstFileIterator:
    """
    Iterator for range queries on SST file.
    
    Allows seeking to a position and reading batches of records.
    """
    
    def __init__(self, read_block: Callable[[BlockHandle], BlockReader], index_block_iterator: BlockIterator):
        self.read_block = read_block
        self.index_iterator = index_block_iterator
        self.sought_data_block: Optional[BlockIterator] = None
    
    def seek_to(self, key: bytes) -> None:
        """
        Seek to the position of the record whose key is exactly equal to or
        greater than the specified key.
        
        Args:
            key: The key to seek to
        """
        self.index_iterator.seek_to(key)
        
        if self.index_iterator.has_next():
            index_entry: BlockEntry = self.index_iterator.__next__()
            block_handle_bytes = index_entry.__getattribute__("value")
            handle_input = MemorySliceInput(block_handle_bytes)

            # Parse block handle
            block_handle = BlockHandle(
                handle_input.read_var_len_long(),
                handle_input.read_var_len_int()
            )
            
            # Create data block reader and seek
            data_block_reader = self.read_block(block_handle)
            self.sought_data_block = data_block_reader.iterator()
            self.sought_data_block.seek_to(key)
        else:
            self.sought_data_block = None
    
    def read_batch(self) -> Optional[BlockIterator]:
        """
        Read a batch of records from this SST File and move current record
        position to the next batch.
        
        Returns:
            BlockIterator for the current batch, or None if at file end
        """
        if self.sought_data_block is not None:
            result = self.sought_data_block
            self.sought_data_block = None
            return result
        
        if not self.index_iterator.has_next():
            return None
        
        index_entry = self.index_iterator.__next__()
        block_handle_bytes = index_entry.value
        
        # Parse block handle
        block_handle = BlockHandle(
            struct.unpack('<Q', block_handle_bytes[0:8])[0],
            struct.unpack('<I', block_handle_bytes[8:12])[0]
        )
        
        # Create data block reader
        data_block_reader = self.read_block(block_handle)
        return data_block_reader.iterator()


class SstFileReader:
    """
    An SST File Reader which serves point queries and range queries.
    
    Users can call createIterator to create a file iterator and then use seek
    and read methods to do range queries.
    
    Note that this class is NOT thread-safe.
    """
    
    def __init__(
        self,
        input_stream: BinaryIO,
        comparator: Callable[[bytes, bytes], int],
        index_block_handle: BlockHandle
    ):
        self.comparator = comparator
        self.input_stream = input_stream
        self.index_block = self._read_block(index_block_handle)

    def _read_block(self, block_handle: BlockHandle) -> BlockReader:
        self.input_stream.seek(block_handle.offset)
        # Read block data + 5 bytes trailer (1 byte compression type + 4 bytes CRC32)
        block_data = self.input_stream.read(block_handle.size + 5)
        # Parse block trailer (last 5 bytes: 1 byte compression type + 4 bytes CRC32)
        if len(block_data) < 5:
            raise ValueError("Block data too short to contain trailer")

        trailer_offset = len(block_data) - 5
        compression_type = block_data[trailer_offset]
        crc32_value = struct.unpack('<I', block_data[trailer_offset + 1:trailer_offset + 5])[0]

        # Extract block data (without trailer)
        block_bytes = block_data[:trailer_offset]

        # Verify CRC32
        actual_crc32 = self.crc32c(block_bytes, compression_type)
        if actual_crc32 != crc32_value:
            raise ValueError(f"CRC32 mismatch: expected {crc32_value}, got {actual_crc32}")

        # Decompress if needed
        if compression_type == 1:  # ZSTD
            import zstandard as zstd
            from io import BytesIO
            decompressor = zstd.ZstdDecompressor()
            memory_input = MemorySliceInput(block_bytes)
            expected_len = memory_input.read_var_len_int()
            compressed = block_bytes[memory_input.position():]
            with decompressor.stream_reader(BytesIO(compressed)) as reader:
                block_bytes = reader.read()
            if len(block_bytes) != expected_len:
                raise ValueError("Corrupted data, decompression failed.")
        elif compression_type != 0:  # Not NONE
            raise ValueError(f"Compression type {compression_type} not supported")

        return BlockReader.create(block_bytes, self.comparator)

    def create_iterator(self) -> SstFileIterator:
        def read_block(block: BlockHandle) -> BlockReader:
            return self._read_block(block)

        return SstFileIterator(
            read_block,
            self.index_block.iterator())

    @staticmethod
    def crc32c(bytes_data: bytes, compression_type_id: int) -> int:
        """
        Calculate CRC32 checksum for the given bytes and compression type.

        Args:
            bytes_data: The byte array to calculate checksum for
            compression_type_id: The persistent ID of the compression type (0-255)

        Returns:
            The CRC32 checksum value
        """
        # Calculate CRC32 for the data bytes
        crc_value = zlib.crc32(bytes_data)

        # Update with compression type ID (lower 8 bits only)
        crc_value = zlib.crc32(bytes([compression_type_id & 0xFF]), crc_value)

        # Return as unsigned 32-bit integer
        return crc_value & 0xFFFFFFFF
    
    def close(self) -> None:
        """Close the reader and release resources."""
        # No resources to release in this implementation
        pass
