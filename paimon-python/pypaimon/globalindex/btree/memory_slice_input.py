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

"""Input for byte array."""

import struct


class MemorySliceInput:
    """Input for byte array."""
    
    def __init__(self, data: bytes):
        """Initialize MemorySliceInput.
        
        Args:
            data: The byte array to read from
        """
        self.data = data
        self._position = 0
    
    def position(self) -> int:
        """Get current position."""
        return self._position
    
    def set_position(self, position: int) -> None:
        """Set current position.
        
        Args:
            position: The new position
            
        Raises:
            IndexError: If position is out of bounds
        """
        if position < 0 or position > len(self.data):
            raise IndexError(f"Position {position} out of bounds [0, {len(self.data)}]")
        self._position = position
    
    def is_readable(self) -> bool:
        """Check if there are more bytes to read."""
        return self.available() > 0
    
    def available(self) -> int:
        """Get number of available bytes."""
        return len(self.data) - self._position
    
    def read_byte(self) -> int:
        """Read a single byte.
        
        Returns:
            The byte value (0-255)
            
        Raises:
            IndexError: If no more bytes available
        """
        if self._position >= len(self.data):
            raise IndexError("No more bytes available")
        value = self.data[self._position]
        self._position += 1
        return value
    
    def read_unsigned_byte(self) -> int:
        """Read an unsigned byte.
        
        Returns:
            The byte value (0-255)
        """
        return self.read_byte() & 0xFF
    
    def read_int(self) -> int:
        """Read a 4-byte integer (big-endian).
        
        Returns:
            The integer value
        """
        if self._position + 4 > len(self.data):
            raise IndexError("Not enough bytes to read int")
        value = struct.unpack('>I', self.data[self._position:self._position + 4])[0]
        self._position += 4
        return value
    
    def read_var_len_int(self) -> int:
        """Read a variable-length integer.
        
        Returns:
            The integer value
            
        Raises:
            ValueError: If integer is malformed
        """
        result = 0
        offset = 0
        while offset < 32:
            b = self.read_unsigned_byte()
            result |= (b & 0x7F) << offset
            if (b & 0x80) == 0:
                return result
            offset += 7
        raise ValueError("Malformed integer")
    
    def read_long(self) -> int:
        """Read an 8-byte long (big-endian).
        
        Returns:
            The long value
        """
        if self._position + 8 > len(self.data):
            raise IndexError("Not enough bytes to read long")
        value = struct.unpack('>Q', self.data[self._position:self._position + 8])[0]
        self._position += 8
        return value
    
    def read_var_len_long(self) -> int:
        """Read a variable-length long.
        
        Returns:
            The long value
            
        Raises:
            ValueError: If long is malformed
        """
        result = 0
        offset = 0
        while offset < 64:
            b = self.read_unsigned_byte()
            result |= (b & 0x7F) << offset
            if (b & 0x80) == 0:
                return result
            offset += 7
        raise ValueError("Malformed long")
    
    def read_slice(self, length: int) -> bytes:
        """Read a slice of bytes.
        
        Args:
            length: Number of bytes to read
            
        Returns:
            The slice of bytes
            
        Raises:
            IndexError: If not enough bytes available
        """
        if self._position + length > len(self.data):
            raise IndexError(f"Not enough bytes to read slice of length {length}")
        value = self.data[self._position:self._position + length]
        self._position += length
        return value
