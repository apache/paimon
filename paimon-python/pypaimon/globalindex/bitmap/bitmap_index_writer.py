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

"""Bitmap global index writer compatible with Java's bitmap format."""

from dataclasses import dataclass
import struct
from typing import Dict, List

from pypaimon.globalindex.index_file_utils import (
    BlockInfo,
    PositionOutput,
    new_global_index_file_name,
    var_len_int_size,
    var_len_long_size,
    write_uncompressed_block,
    write_var_len_int,
    write_var_len_long,
)
from pypaimon.globalindex.result_entry import ResultEntry
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


BITMAP_IDENTIFIER = "bitmap"
_BITMAP_FOOTER_LENGTH = 48
_BITMAP_MAGIC = 0x42474958
_BITMAP_VERSION = 1
_DEFAULT_DICTIONARY_BLOCK_SIZE = 16 * 1024


@dataclass(frozen=True)
class _DictionaryBlockMeta(BlockInfo):
    first_key: bytes


@dataclass(frozen=True)
class _DictionaryEntry:
    key: bytes
    bitmap_block: BlockInfo

    def estimated_size(self) -> int:
        return (
            var_len_int_size(len(self.key))
            + len(self.key)
            + var_len_long_size(self.bitmap_block.offset)
            + var_len_int_size(self.bitmap_block.length)
        )


class _DictionaryBlockBuilder:
    def __init__(self):
        self.entries: List[_DictionaryEntry] = []
        self._entries_size = 0

    def has_entries(self) -> bool:
        return len(self.entries) > 0

    def estimated_size(self) -> int:
        return var_len_int_size(len(self.entries)) + self._entries_size

    def estimated_size_after(self, entry: _DictionaryEntry) -> int:
        return (
            var_len_int_size(len(self.entries) + 1)
            + self._entries_size
            + entry.estimated_size()
        )

    def add(self, entry: _DictionaryEntry) -> None:
        self.entries.append(entry)
        self._entries_size += entry.estimated_size()

    def first_key(self) -> bytes:
        return self.entries[0].key


class BitmapIndexWriter:
    """Writer for one bitmap global index file.

    Row IDs are local to the manifest row range, matching the Java writer
    contract used by sorted global index builders.
    """

    def __init__(
        self,
        file_io,
        index_path: str,
        key_serializer,
        dictionary_block_size: int = _DEFAULT_DICTIONARY_BLOCK_SIZE,
        compression: str = "none",
    ):
        if dictionary_block_size <= 0:
            raise ValueError("bitmap-index.dictionary-block-size must be positive.")
        if compression is None:
            compression = "none"
        compression = str(compression).lower().strip()
        if compression != "none":
            raise ValueError(
                "Python bitmap global index build currently supports only "
                "bitmap-index.compression=none, got '%s'." % compression
            )

        self.file_name = new_global_index_file_name(BITMAP_IDENTIFIER)
        self._file_io = file_io
        self._index_path = index_path.rstrip("/")
        self._key_serializer = key_serializer
        self._comparator = key_serializer.create_comparator()
        self._dictionary_block_size = dictionary_block_size
        self._bitmaps: Dict[bytes, RoaringBitmap64] = {}
        self._null_rows = RoaringBitmap64()
        self._non_null_rows = RoaringBitmap64()
        self._row_count = 0
        self._first_key = None
        self._last_key = None
        self._closed = False

    def write(self, key, row_id: int) -> None:
        self._row_count += 1
        if key is None:
            self._null_rows.add(row_id)
            return

        self._non_null_rows.add(row_id)
        self._update_min_max(key)
        serialized_key = self._key_serializer.serialize(key)
        bitmap = self._bitmaps.get(serialized_key)
        if bitmap is None:
            bitmap = RoaringBitmap64()
            self._bitmaps[serialized_key] = bitmap
        bitmap.add(row_id)

    def finish(self) -> List[ResultEntry]:
        if self._closed:
            raise RuntimeError("BitmapIndexWriter is already closed.")
        self._closed = True
        if self._row_count == 0:
            return []

        self._file_io.check_or_mkdirs(self._index_path)
        output = PositionOutput(
            self._file_io.new_output_stream(self._file_path()))
        try:
            self._write(output)
            output.close()
        except Exception:
            self._file_io.delete_quietly(self._file_path())
            raise

        meta = SortedIndexFileMeta(
            None if self._first_key is None
            else self._key_serializer.serialize(self._first_key),
            None if self._last_key is None
            else self._key_serializer.serialize(self._last_key),
            not self._null_rows.is_empty(),
        ).serialize()
        return [ResultEntry(self.file_name, self._row_count, meta)]

    def _write(self, output: PositionOutput) -> None:
        null_rows_block = _write_bitmap_block(output, self._null_rows)
        non_null_rows_block = _write_bitmap_block(output, self._non_null_rows)
        dictionary_blocks, value_count = self._write_dictionary_and_bitmap_blocks(
            output)
        index_block = _write_index_block(output, dictionary_blocks)
        output.write(
            _write_footer(
                null_rows_block,
                non_null_rows_block,
                index_block,
                value_count,
            )
        )

    def _write_dictionary_and_bitmap_blocks(self, output: PositionOutput):
        dictionary_blocks = []
        current = _DictionaryBlockBuilder()
        value_count = 0

        for serialized_key, bitmap in sorted(self._bitmaps.items()):
            bitmap_block = _write_bitmap_block(output, bitmap)
            entry = _DictionaryEntry(serialized_key, bitmap_block)
            if (
                current.has_entries()
                and current.estimated_size_after(entry) > self._dictionary_block_size
            ):
                dictionary_blocks.append(_write_dictionary_block(output, current))
                current = _DictionaryBlockBuilder()
            current.add(entry)
            value_count += 1

        if current.has_entries():
            dictionary_blocks.append(_write_dictionary_block(output, current))
        return dictionary_blocks, value_count

    def _update_min_max(self, key) -> None:
        if self._first_key is None or self._comparator(key, self._first_key) < 0:
            self._first_key = key
        if self._last_key is None or self._comparator(key, self._last_key) > 0:
            self._last_key = key

    def _file_path(self) -> str:
        return "%s/%s" % (self._index_path, self.file_name)


def _write_dictionary_block(
    output: PositionOutput,
    block: _DictionaryBlockBuilder,
) -> _DictionaryBlockMeta:
    data = bytearray()
    data.extend(write_var_len_int(len(block.entries)))
    for entry in block.entries:
        data.extend(write_var_len_int(len(entry.key)))
        data.extend(entry.key)
        data.extend(write_var_len_long(entry.bitmap_block.offset))
        data.extend(write_var_len_int(entry.bitmap_block.length))
    block_info = _write_compressible_block(output, bytes(data))
    return _DictionaryBlockMeta(
        block_info.offset,
        block_info.length,
        block.first_key(),
    )


def _write_index_block(
    output: PositionOutput,
    blocks: List[_DictionaryBlockMeta],
) -> BlockInfo:
    data = bytearray()
    data.extend(write_var_len_int(len(blocks)))
    for block in blocks:
        data.extend(write_var_len_int(len(block.first_key)))
        data.extend(block.first_key)
        data.extend(write_var_len_long(block.offset))
        data.extend(write_var_len_int(block.length))
    return _write_compressible_block(output, bytes(data))


def _write_bitmap_block(
    output: PositionOutput,
    bitmap: RoaringBitmap64,
) -> BlockInfo:
    data = bitmap.serialize()
    offset = output.pos
    output.write(data)
    return BlockInfo(offset, len(data))


def _write_compressible_block(output: PositionOutput, data: bytes) -> BlockInfo:
    return write_uncompressed_block(output, data)


def _write_footer(
    null_rows_block: BlockInfo,
    non_null_rows_block: BlockInfo,
    index_block: BlockInfo,
    value_count: int,
) -> bytes:
    result = struct.pack(
        ">q i q i q i i i i",
        null_rows_block.offset,
        null_rows_block.length,
        non_null_rows_block.offset,
        non_null_rows_block.length,
        index_block.offset,
        index_block.length,
        value_count,
        _BITMAP_VERSION,
        _BITMAP_MAGIC,
    )
    if len(result) != _BITMAP_FOOTER_LENGTH:
        raise AssertionError("Unexpected bitmap footer length: %s" % len(result))
    return result
