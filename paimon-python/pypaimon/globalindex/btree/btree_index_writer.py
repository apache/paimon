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

"""BTree global index writer compatible with Java's SST-backed format."""

import struct
import zlib
from typing import List, Optional

from pypaimon.globalindex.index_file_utils import (
    PositionOutput,
    new_global_index_file_name,
    write_uncompressed_block,
    write_var_len_int,
    write_var_len_long,
)
from pypaimon.globalindex.result_entry import ResultEntry
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


BTREE_IDENTIFIER = "btree"
_BTREE_FOOTER_LENGTH = 52
_BTREE_MAGIC_NUMBER = 0x50425449
_BTREE_CURRENT_VERSION = 1
_BLOCK_ALIGNED = 0
_BLOCK_UNALIGNED = 1


def _write_block_handle(offset: int, size: int) -> bytes:
    return write_var_len_long(offset) + write_var_len_int(size)


class _BlockWriter:
    def __init__(self):
        self._positions: List[int] = []
        self._block = bytearray()
        self._aligned_size = 0
        self._aligned = True

    def reset(self) -> None:
        self._positions = []
        self._block = bytearray()
        self._aligned_size = 0
        self._aligned = True

    def add(self, key: bytes, value: bytes) -> None:
        start_position = len(self._block)
        self._block.extend(write_var_len_int(len(key)))
        self._block.extend(key)
        self._block.extend(write_var_len_int(len(value)))
        self._block.extend(value)
        end_position = len(self._block)

        self._positions.append(start_position)
        if self._aligned:
            current_size = end_position - start_position
            if self._aligned_size == 0:
                self._aligned_size = current_size
            else:
                self._aligned = self._aligned_size == current_size

    def size(self) -> int:
        return len(self._positions)

    def memory(self) -> int:
        size = len(self._block) + 5
        if not self._aligned:
            size += len(self._positions) * 4
        return size

    def finish(self) -> bytes:
        if not self._positions:
            self._aligned = False

        result = bytearray(self._block)
        if self._aligned:
            result.extend(struct.pack('<I', self._aligned_size))
            result.append(_BLOCK_ALIGNED)
        else:
            for position in self._positions:
                result.extend(struct.pack('<I', position))
            result.extend(struct.pack('<I', len(self._positions)))
            result.append(_BLOCK_UNALIGNED)
        return bytes(result)


class _SstFileWriter:
    def __init__(self, out: PositionOutput, block_size: int):
        self._out = out
        self._block_size = block_size
        self._data_block_writer = _BlockWriter()
        self._index_block_writer = _BlockWriter()
        self._last_key: Optional[bytes] = None

    def put(self, key: bytes, value: bytes) -> None:
        self._data_block_writer.add(key, value)
        self._last_key = key

        if self._data_block_writer.memory() > self._block_size:
            self.flush()

    def flush(self) -> None:
        if self._data_block_writer.size() == 0:
            return
        block_offset, block_size = self._write_block(self._data_block_writer)
        self._index_block_writer.add(
            self._last_key, _write_block_handle(block_offset, block_size))

    def write_bloom_filter(self):
        return None

    def write_index_block(self):
        return self._write_block(self._index_block_writer)

    def write_slice(self, data: bytes) -> None:
        self._out.write(data)

    def _write_block(self, block_writer: _BlockWriter):
        block = block_writer.finish()
        block_info = write_uncompressed_block(self._out, block)
        block_writer.reset()
        return block_info.offset, block_info.length


class BTreeIndexWriter:
    """Writer for one BTree global index file.

    Keys must be written in non-decreasing order, matching Java's writer
    contract. Row IDs are local to the manifest row range.
    """

    def __init__(
        self,
        file_io,
        index_path: str,
        key_serializer,
        block_size: int = 64 * 1024,
    ):
        self.file_name = new_global_index_file_name(BTREE_IDENTIFIER)
        self._file_io = file_io
        self._index_path = index_path.rstrip('/')
        self._key_serializer = key_serializer
        self._comparator = key_serializer.create_comparator()
        self._current_row_ids: List[int] = []
        self._null_bitmap: Optional[RoaringBitmap64] = None
        self._first_key = None
        self._last_key = None
        self._row_count = 0
        self._closed = False

        self._file_io.check_or_mkdirs(self._index_path)
        self._output = PositionOutput(
            self._file_io.new_output_stream(self._file_path()))
        self._sst = _SstFileWriter(self._output, block_size)

    def write(self, key, row_id: int) -> None:
        self._row_count += 1
        if key is None:
            if self._null_bitmap is None:
                self._null_bitmap = RoaringBitmap64()
            self._null_bitmap.add(row_id)
            return

        if self._last_key is not None and self._comparator(key, self._last_key) != 0:
            self._flush()

        self._last_key = key
        self._current_row_ids.append(row_id)
        if self._first_key is None:
            self._first_key = key

    def finish(self) -> List[ResultEntry]:
        if self._closed:
            raise RuntimeError("BTreeIndexWriter is already closed.")
        self._closed = True

        try:
            self._flush()
            self._sst.flush()
            null_bitmap_handle = self._write_null_bitmap()
            bloom_filter_handle = self._sst.write_bloom_filter()
            index_block_handle = self._sst.write_index_block()
            self._sst.write_slice(
                _write_footer(
                    bloom_filter_handle, index_block_handle, null_bitmap_handle))
            self._output.close()
        except Exception:
            self._file_io.delete_quietly(self._file_path())
            raise

        if self._first_key is None and self._null_bitmap is None:
            raise RuntimeError("Should never write an empty btree index file.")

        meta = SortedIndexFileMeta(
            None if self._first_key is None
            else self._key_serializer.serialize(self._first_key),
            None if self._last_key is None
            else self._key_serializer.serialize(self._last_key),
            self._null_bitmap is not None,
        ).serialize()
        return [ResultEntry(self.file_name, self._row_count, meta)]

    def _flush(self) -> None:
        if not self._current_row_ids:
            return

        value = bytearray()
        value.extend(write_var_len_int(len(self._current_row_ids)))
        for row_id in self._current_row_ids:
            value.extend(write_var_len_long(row_id))
        self._current_row_ids = []
        self._sst.put(self._key_serializer.serialize(self._last_key), bytes(value))

    def _write_null_bitmap(self):
        if self._null_bitmap is None:
            return None

        serialized = self._null_bitmap.serialize()
        offset = self._output.pos
        self._output.write(serialized)
        self._output.write(struct.pack('<I', zlib.crc32(serialized) & 0xFFFFFFFF))
        return offset, len(serialized)

    def _file_path(self) -> str:
        return "%s/%s" % (self._index_path, self.file_name)


def _write_footer(bloom_filter_handle, index_block_handle, null_bitmap_handle) -> bytes:
    result = bytearray()
    if bloom_filter_handle is None:
        result.extend(struct.pack('<QIQ', 0, 0, 0))
    else:
        result.extend(
            struct.pack(
                '<QIQ',
                bloom_filter_handle[0],
                bloom_filter_handle[1],
                bloom_filter_handle[2],
            ))

    result.extend(struct.pack('<QI', index_block_handle[0], index_block_handle[1]))

    if null_bitmap_handle is None:
        result.extend(struct.pack('<QI', 0, 0))
    else:
        result.extend(
            struct.pack('<QI', null_bitmap_handle[0], null_bitmap_handle[1]))

    result.extend(struct.pack('<I', _BTREE_CURRENT_VERSION))
    result.extend(struct.pack('<I', _BTREE_MAGIC_NUMBER))
    if len(result) != _BTREE_FOOTER_LENGTH:
        raise AssertionError("Unexpected BTree footer length: %s" % len(result))
    return bytes(result)
