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

"""Reader for one bitmap global index file."""

from dataclasses import dataclass
import struct
import threading
from typing import Callable, Dict, List, Optional

from pypaimon.common.file_io import FileIO, pread, supports_pread
from pypaimon.globalindex.block_compression import (
    BLOCK_TRAILER_LENGTH,
    decompress_block_with_trailer,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.key_serializer import KeySerializer
from pypaimon.globalindex.memory_slice_input import MemorySliceInput
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


@dataclass(frozen=True)
class _BlockInfo:
    offset: int
    length: int


@dataclass(frozen=True)
class _DictionaryBlockMeta(_BlockInfo):
    first_key: bytes


@dataclass(frozen=True)
class _DictionaryEntry:
    key: bytes
    bitmap_block: _BlockInfo


@dataclass(frozen=True)
class _Footer:
    null_rows_block: _BlockInfo
    non_null_rows_block: _BlockInfo
    index_block: _BlockInfo


class BitmapIndexReader:
    """Synchronous reader for one bitmap global index file."""

    _FOOTER_LENGTH = 48
    _MAGIC = 0x42474958
    _VERSION = 1

    def __init__(
        self,
        key_serializer: KeySerializer,
        file_io: FileIO,
        index_path: str,
        io_meta: GlobalIndexIOMeta,
    ):
        self._key_serializer = key_serializer
        self._comparator = key_serializer.create_comparator()
        self._io_meta = io_meta
        file_path = (io_meta.external_path
                     if io_meta.external_path
                     else index_path + "/" + io_meta.file_name)
        self._input_stream = file_io.new_input_stream(file_path)
        self._supports_pread = supports_pread(self._input_stream)
        self._io_lock = threading.Lock()

        self._footer = self._read_footer()
        self._null_rows: Optional[RoaringBitmap64] = None
        self._non_null_rows: Optional[RoaringBitmap64] = None
        self._dictionary_blocks: Optional[List[_DictionaryBlockMeta]] = None
        self._dictionary_block_cache: Dict[_DictionaryBlockMeta, List[_DictionaryEntry]] = {}
        self._lazy_lock = threading.Lock()

    def _read_from(self, offset: int, length: int) -> bytes:
        if self._supports_pread:
            return pread(self._input_stream, length, offset)
        with self._io_lock:
            self._input_stream.seek(offset)
            return self._input_stream.read(length)

    def _read_footer(self) -> _Footer:
        if self._io_meta.file_size < self._FOOTER_LENGTH:
            raise ValueError("Invalid bitmap global index file size.")
        footer_data = self._read_from(
            self._io_meta.file_size - self._FOOTER_LENGTH,
            self._FOOTER_LENGTH)
        (null_offset, null_length,
         non_null_offset, non_null_length,
         index_offset, index_length,
         value_count, version, magic) = struct.unpack('>q i q i q i i i i', footer_data)
        if magic != self._MAGIC:
            raise ValueError("File is not a bitmap global index file.")
        if version != self._VERSION:
            raise ValueError(f"Unsupported bitmap global index file version: {version}")
        if value_count < 0:
            raise ValueError("Invalid bitmap value count.")
        return _Footer(
            _BlockInfo(null_offset, null_length),
            _BlockInfo(non_null_offset, non_null_length),
            _BlockInfo(index_offset, index_length))

    def _read_bitmap(self, block: _BlockInfo) -> RoaringBitmap64:
        return RoaringBitmap64.deserialize(self._read_from(block.offset, block.length))

    def _read_compressible_block(self, block: _BlockInfo) -> bytes:
        block_and_trailer = self._read_from(
            block.offset, block.length + BLOCK_TRAILER_LENGTH)
        return decompress_block_with_trailer(block_and_trailer, block.length)

    def _read_index_block(self) -> List[_DictionaryBlockMeta]:
        data_input = MemorySliceInput(
            self._read_compressible_block(self._footer.index_block))
        block_count = data_input.read_var_len_int()
        if block_count < 0:
            raise ValueError("Invalid bitmap dictionary block count.")

        blocks = []
        for _ in range(block_count):
            key_length = data_input.read_var_len_int()
            if key_length < 0:
                raise ValueError("Invalid bitmap key length.")
            first_key = data_input.read_slice(key_length)
            offset = data_input.read_var_len_long()
            length = data_input.read_var_len_int()
            blocks.append(_DictionaryBlockMeta(offset, length, first_key))
        return sorted(blocks, key=lambda block: block.first_key)

    def _read_dictionary_block(self, block: _DictionaryBlockMeta) -> List[_DictionaryEntry]:
        data_input = MemorySliceInput(self._read_compressible_block(block))
        entry_count = data_input.read_var_len_int()
        if entry_count < 0:
            raise ValueError("Invalid bitmap dictionary entry count.")

        entries = []
        for _ in range(entry_count):
            key_length = data_input.read_var_len_int()
            if key_length < 0:
                raise ValueError("Invalid bitmap key length.")
            key = data_input.read_slice(key_length)
            bitmap_offset = data_input.read_var_len_long()
            bitmap_length = data_input.read_var_len_int()
            entries.append(_DictionaryEntry(key, _BlockInfo(bitmap_offset, bitmap_length)))
        return entries

    def _get_null_rows(self) -> RoaringBitmap64:
        if self._null_rows is None:
            with self._lazy_lock:
                if self._null_rows is None:
                    self._null_rows = self._read_bitmap(self._footer.null_rows_block)
        return self._copy(self._null_rows)

    def _get_non_null_rows(self) -> RoaringBitmap64:
        if self._non_null_rows is None:
            with self._lazy_lock:
                if self._non_null_rows is None:
                    self._non_null_rows = self._read_bitmap(self._footer.non_null_rows_block)
        return self._copy(self._non_null_rows)

    def _get_dictionary_blocks(self) -> List[_DictionaryBlockMeta]:
        if self._dictionary_blocks is None:
            with self._lazy_lock:
                if self._dictionary_blocks is None:
                    self._dictionary_blocks = self._read_index_block()
        return self._dictionary_blocks

    def _get_dictionary_block(self, meta: _DictionaryBlockMeta) -> List[_DictionaryEntry]:
        block = self._dictionary_block_cache.get(meta)
        if block is None:
            with self._lazy_lock:
                block = self._dictionary_block_cache.get(meta)
                if block is None:
                    block = self._read_dictionary_block(meta)
                    self._dictionary_block_cache[meta] = block
        return block

    def _find_dictionary_block_index(self, key: bytes) -> int:
        blocks = self._get_dictionary_blocks()
        low = 0
        high = len(blocks) - 1
        while low <= high:
            mid = (low + high) >> 1
            if blocks[mid].first_key <= key:
                low = mid + 1
            else:
                high = mid - 1
        return high

    def _find_bitmap_block(self, key: bytes) -> Optional[_BlockInfo]:
        blocks = self._get_dictionary_blocks()
        if not blocks:
            return None

        index = self._find_dictionary_block_index(key)
        if index < 0:
            return None

        for entry in self._get_dictionary_block(blocks[index]):
            if entry.key == key:
                return entry.bitmap_block
            if entry.key > key:
                return None
        return None

    def _scan_dictionary(self, predicate: Callable[[object], bool]) -> RoaringBitmap64:
        result = RoaringBitmap64()
        for block_meta in self._get_dictionary_blocks():
            for entry in self._get_dictionary_block(block_meta):
                key = self._key_serializer.deserialize(entry.key)
                if predicate(key):
                    result = RoaringBitmap64.or_(result, self._read_bitmap(entry.bitmap_block))
        return result

    def _scan_serialized_dictionary(self, predicate: Callable[[bytes], bool]) -> RoaringBitmap64:
        result = RoaringBitmap64()
        for block_meta in self._get_dictionary_blocks():
            for entry in self._get_dictionary_block(block_meta):
                if predicate(entry.key):
                    result = RoaringBitmap64.or_(result, self._read_bitmap(entry.bitmap_block))
        return result

    def visit_is_not_null(self) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._get_non_null_rows())

    def visit_is_null(self) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._get_null_rows())

    def visit_starts_with(self, literal: object) -> Optional[GlobalIndexResult]:
        prefix = self._key_serializer.serialize(literal)
        if len(prefix) == 0:
            return GlobalIndexResult.create(self._get_non_null_rows())
        return GlobalIndexResult.create(self._scan_serialized_dictionary(
            lambda key: key.startswith(prefix)))

    def visit_ends_with(self, literal: object) -> Optional[GlobalIndexResult]:
        suffix = self._key_serializer.serialize(literal)
        if len(suffix) == 0:
            return GlobalIndexResult.create(self._get_non_null_rows())
        return GlobalIndexResult.create(self._scan_serialized_dictionary(
            lambda key: key.endswith(suffix)))

    def visit_contains(self, literal: object) -> Optional[GlobalIndexResult]:
        infix = self._key_serializer.serialize(literal)
        if len(infix) == 0:
            return GlobalIndexResult.create(self._get_non_null_rows())
        return GlobalIndexResult.create(self._scan_serialized_dictionary(
            lambda key: infix in key))

    def visit_like(self, literal: object) -> Optional[GlobalIndexResult]:
        from pypaimon.common.predicate import Like
        return GlobalIndexResult.create(
            self.like(lambda key: Like().test_by_value(key, [literal])))

    def visit_less_than(self, literal: object) -> Optional[GlobalIndexResult]:
        if literal is None:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.create(
            self._scan_dictionary(lambda key: self._comparator(key, literal) < 0))

    def visit_greater_or_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        if literal is None:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.create(
            self._scan_dictionary(lambda key: self._comparator(key, literal) >= 0))

    def visit_not_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        if literal is None:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.create(
            RoaringBitmap64.remove_all(self._get_non_null_rows(), self._equal(literal)))

    def visit_less_or_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        if literal is None:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.create(
            self._scan_dictionary(lambda key: self._comparator(key, literal) <= 0))

    def visit_equal(self, literal: object) -> Optional[GlobalIndexResult]:
        return GlobalIndexResult.create(self._equal(literal))

    def visit_greater_than(self, literal: object) -> Optional[GlobalIndexResult]:
        if literal is None:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.create(
            self._scan_dictionary(lambda key: self._comparator(key, literal) > 0))

    def visit_in(self, literals: List[object]) -> Optional[GlobalIndexResult]:
        result = RoaringBitmap64()
        seen = set()
        for literal in literals:
            if literal is None:
                continue
            key = self._key_serializer.serialize(literal)
            if key in seen:
                continue
            seen.add(key)
            bitmap_block = self._find_bitmap_block(key)
            if bitmap_block is not None:
                result = RoaringBitmap64.or_(result, self._read_bitmap(bitmap_block))
        return GlobalIndexResult.create(result)

    def visit_not_in(self, literals: List[object]) -> Optional[GlobalIndexResult]:
        if any(literal is None for literal in literals):
            return GlobalIndexResult.create_empty()
        result = self._get_non_null_rows()
        in_result = self.visit_in(literals).results()
        return GlobalIndexResult.create(RoaringBitmap64.remove_all(result, in_result))

    def visit_between(self, from_v: object, to_v: object) -> Optional[GlobalIndexResult]:
        if from_v is None or to_v is None or self._comparator(from_v, to_v) > 0:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.create(
            self._scan_dictionary(
                lambda key: self._comparator(key, from_v) >= 0
                and self._comparator(key, to_v) <= 0))

    def like(self, key_predicate: Callable[[object], bool]) -> RoaringBitmap64:
        return self._scan_dictionary(key_predicate)

    def less_than(self, literal: object) -> RoaringBitmap64:
        if literal is None:
            return RoaringBitmap64()
        return self._scan_dictionary(lambda key: self._comparator(key, literal) < 0)

    def greater_than(self, literal: object) -> RoaringBitmap64:
        if literal is None:
            return RoaringBitmap64()
        return self._scan_dictionary(lambda key: self._comparator(key, literal) > 0)

    def _equal(self, literal: object) -> RoaringBitmap64:
        if literal is None:
            return RoaringBitmap64()
        bitmap_block = self._find_bitmap_block(self._key_serializer.serialize(literal))
        if bitmap_block is None:
            return RoaringBitmap64()
        return self._read_bitmap(bitmap_block)

    @staticmethod
    def _copy(bitmap: RoaringBitmap64) -> RoaringBitmap64:
        return RoaringBitmap64.or_(RoaringBitmap64(), bitmap)

    def close(self) -> None:
        if self._input_stream is not None:
            self._input_stream.close()
