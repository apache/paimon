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

"""Complete row-id interval unions with Avro block offsets and entry ordinals.

Version 2 uses fixed-width big-endian integers; no library-specific bitmap encoding.
"""

import hashlib
import logging
import struct
from bisect import bisect_left
from concurrent.futures import CancelledError
from dataclasses import dataclass
from io import BytesIO
from typing import Tuple

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.utils.range import Range

LOG = logging.getLogger(__name__)
SUFFIX = '.row-id-index'
MAGIC = b'PAIMRIDX'
MAX_ROW_ID = (1 << 63) - 1
MAX_AVRO_HEADER = 1024 * 1024
HEADER = struct.Struct('>8sHHI32sqqI')
BLOCK = struct.Struct('>qqqqI')
PAIR = struct.Struct('>qq')
LONG = struct.Struct('>q')


@dataclass
class Settings:
    write: bool = False
    read: bool = False
    max_ranges: int = 131072
    max_bytes: int = 8 * 1024 * 1024

    def __post_init__(self):
        if not 1 <= self.max_ranges <= 1048576:
            raise ValueError('manifest.row-id-index.max-ranges must be in [1, 1048576]')
        if not 128 <= self.max_bytes <= 64 * 1024 * 1024:
            raise ValueError('manifest.row-id-index.max-bytes must be in [128, 67108864]')

    @classmethod
    def from_options(cls, options):
        return cls(
            options.options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_WRITE),
            options.options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_READ),
            options.options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_RANGES),
            options.options.get(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_BYTES))


@dataclass(frozen=True)
class Block:
    offset: int
    length: int
    first_record: int
    record_count: int


@dataclass(frozen=True)
class Selection:
    header: bytes
    blocks: Tuple[Block, ...]


class Query:
    def __init__(self, ranges):
        normalized = Range.sort_and_merge_overlap(list(ranges), True)
        self.starts = [r.from_ for r in normalized]
        self.ends = [r.to for r in normalized]

    def intersects(self, first, last):
        candidate = bisect_left(self.ends, first)
        return candidate < len(self.starts) and self.starts[candidate] <= last


class Builder:
    def __init__(self, settings, header):
        self.settings = settings
        self.complete = (header is not None and len(header) <= MAX_AVRO_HEADER
                         and len(header) + HEADER.size + 40 <= settings.max_bytes)
        self.payload = bytearray()
        self.ranges = []
        self.count_position = 4 + len(header) if self.complete else 0
        self.next_offset = len(header) if self.complete else 0
        self.next_record = 0
        self.range_count = 0
        self.blocks = 0
        self.current = None
        self.entries_in_block = 0
        if self.complete:
            self.payload.extend(struct.pack('>I', len(header)))
            self.payload.extend(header)
            self.payload.extend(struct.pack('>I', 0))

    def _disable(self, reason):
        self.complete = False
        self.payload.clear()
        self.ranges.clear()
        LOG.debug('Omitting manifest row-id block index: %s', reason)

    def begin_block(self, offset, length, records):
        if not self.complete:
            return
        _require(self.current is None and offset == self.next_offset and length > 0 and records > 0)
        self.current = Block(offset, length, self.next_record, records)
        self.entries_in_block = 0

    def add(self, first, count):
        if not self.complete:
            return
        _require(self.current is not None)
        self.entries_in_block += 1
        if (first is None or first < 0 or count <= 0
                or first > MAX_ROW_ID or count - 1 > MAX_ROW_ID - first):
            self._disable('unknown or invalid row-id coverage')
            return
        end = first + count - 1
        left = bisect_left(self.ranges, (first, -1))
        if left and self.ranges[left - 1][1] >= first - 1:
            left -= 1
        right = left
        while right < len(self.ranges) and self.ranges[right][0] <= end + 1:
            first = min(first, self.ranges[right][0])
            end = max(end, self.ranges[right][1])
            right += 1
        if self.range_count + len(self.ranges) - (right - left) >= self.settings.max_ranges:
            self._disable('range budget exceeded')
            return
        self.ranges[left:right] = [(first, end)]

    def end_block(self):
        if not self.complete:
            return
        block = self.current
        _require(block is not None and self.entries_in_block == block.record_count and self.ranges)
        if HEADER.size + 32 + len(self.payload) + BLOCK.size + 16 * len(self.ranges) > self.settings.max_bytes:
            self._disable('serialized byte budget exceeded')
            return
        self.payload.extend(BLOCK.pack(block.offset, block.length, block.first_record,
                                       block.record_count, len(self.ranges)))
        for first, end in self.ranges:
            self.payload.extend(PAIR.pack(first, end))
        self.next_offset = block.offset + block.length
        self.next_record = block.first_record + block.record_count
        self.range_count += len(self.ranges)
        self.blocks += 1
        self.ranges.clear()
        self.current = None

    def serialize(self, name, file_size, entry_count):
        if not self.complete:
            return None
        _require(self.current is None and self.next_offset == file_size and self.next_record == entry_count)
        struct.pack_into('>I', self.payload, self.count_position, self.blocks)
        header = HEADER.pack(MAGIC, 2, 2, 1, hashlib.sha256(name.encode('utf-8')).digest(),
                             file_size, entry_count, len(self.payload))
        data = header + self.payload
        return data + hashlib.sha256(data).digest()


def build_from_entries(avro_bytes, entries, name, settings):
    import fastavro
    blocks = iter(fastavro.block_reader(BytesIO(avro_bytes)))
    first_block = next(blocks, None)
    header = avro_bytes[:first_block.offset] if first_block else avro_bytes
    builder = Builder(settings, header)
    position = 0
    block = first_block
    while block is not None and builder.complete:
        builder.begin_block(block.offset, block.size, block.num_records)
        end = position + block.num_records
        _require(end <= len(entries))
        for i in range(position, end):
            entry = entries[i]
            builder.add(entry.file.first_row_id, entry.file.row_count)
            if not builder.complete:
                break
        builder.end_block()
        position = end
        block = next(blocks, None) if builder.complete else None
    return builder.serialize(name, len(avro_bytes), len(entries))


def _require(condition):
    if not condition:
        raise ValueError('Invalid, unsupported, mismatched or over-budget manifest row-id block index')


def select(data, manifest, query, settings):
    if not isinstance(query, Query):
        query = Query(query)
    _require(128 <= len(data) <= settings.max_bytes)
    _require(hashlib.sha256(data[:-32]).digest() == data[-32:])
    magic, version, codec, flags, name_hash, size, entries, length = HEADER.unpack_from(data)
    _require((magic, version, codec, flags) == (MAGIC, 2, 2, 1))
    _require(name_hash == hashlib.sha256(manifest.file_name.encode('utf-8')).digest())
    _require(size == manifest.file_size and entries == manifest.num_added_files + manifest.num_deleted_files)
    _require(length == len(data) - HEADER.size - 32)
    offset = HEADER.size
    header_length, = struct.unpack_from('>I', data, offset)
    offset += 4
    _require(21 <= header_length <= MAX_AVRO_HEADER and header_length <= len(data) - offset - 36)
    header = bytes(data[offset:offset + header_length])
    _require(header[:4] == b'Obj\x01')
    offset += header_length
    blocks, = struct.unpack_from('>I', data, offset)
    offset += 4
    _require(blocks <= (len(data) - 32 - offset) // 52)
    next_offset = header_length
    next_record = 0
    total_ranges = 0
    selected = []
    for _ in range(blocks):
        file_offset, block_length, first, count, ranges = BLOCK.unpack_from(data, offset)
        offset += BLOCK.size
        _require(file_offset == next_offset and 0 < block_length <= size - file_offset)
        _require(first == next_record and 0 < count <= entries - first)
        _require(0 < ranges <= settings.max_ranges - total_ranges and ranges <= (len(data) - 32 - offset) // 16)
        total_ranges += ranges
        ranges_end = offset + PAIR.size * ranges
        min_row_id, first_end = PAIR.unpack_from(data, offset)
        offset += PAIR.size
        # The sorted interval list already contains min/max; no format change or extra fields.
        max_row_id = first_end if ranges == 1 else LONG.unpack_from(data, ranges_end - LONG.size)[0]
        _require(min_row_id >= 0 and first_end >= min_row_id and max_row_id >= first_end)
        candidate = query.intersects(min_row_id, max_row_id)
        hit = candidate and (ranges == 1 or query.intersects(min_row_id, first_end))
        previous_end = first_end
        for _ in range(1, ranges):
            start, end = PAIR.unpack_from(data, offset)
            offset += PAIR.size
            # Retain validation even when min/max rejects the block or an earlier interval hit.
            _require(start >= 0 and end >= start and start > previous_end)
            previous_end = end
            if candidate and not hit:
                hit = query.intersects(start, end)
        if hit:
            selected.append(Block(file_offset, block_length, first, count))
        next_offset = file_offset + block_length
        next_record = first + count
    _require(offset == len(data) - 32 and next_offset == size and next_record == entries)
    return Selection(header, tuple(selected))


def read_index(file_io, manifest_path, manifest, query, settings):
    if manifest.index_file_name is None:
        return None
    index_path = manifest_path.rsplit('/', 1)[0] + '/' + manifest.index_file_name
    try:
        with file_io.new_input_stream(index_path) as stream:
            data = bytearray()
            while True:
                chunk = stream.read(min(8192, settings.max_bytes + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
                _require(len(data) <= settings.max_bytes)
        return select(data, manifest, query, settings)
    except (InterruptedError, CancelledError, MemoryError, RecursionError):
        raise
    except Exception as error:
        LOG.debug('Cannot use row-id block index for %s; reading manifest: %s', manifest_path, error)
        return None


def read_selected_bytes(file_io, manifest_path, selected):
    """Read complete selected blocks with seek; adjacent blocks share one contiguous span.

    The concatenated original header and blocks form a valid Avro OCF. Partial entries
    must not be stored in a cache keyed by the complete manifest.
    """
    data = bytearray(selected.header)
    with file_io.new_input_stream(manifest_path) as stream:
        previous_end = -1
        for block in selected.blocks:
            if block.offset != previous_end:
                stream.seek(block.offset)
            remaining = block.length
            while remaining:
                chunk = stream.read(min(remaining, 1024 * 1024))
                if not chunk:
                    raise EOFError('Truncated manifest block')
                data.extend(chunk)
                remaining -= len(chunk)
            previous_end = block.offset + block.length
    return bytes(data)
