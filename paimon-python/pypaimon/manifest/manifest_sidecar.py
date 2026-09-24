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

"""Independent partition, row-id and bucket coverage for each Avro block."""

import logging
import struct
import zlib
from bisect import bisect_left
from concurrent.futures import CancelledError
from dataclasses import dataclass
from io import BytesIO
from typing import Tuple

from pyarrow import ArrowCancelled

from pypaimon.utils.range import Range
from pypaimon.table.row.generic_row import GenericRowSerializer, GenericRowDeserializer

LOG = logging.getLogger(__name__)
SUFFIX = '.avro.sidecar'
MAGIC = b'PMSC'
FORMAT_VERSION = 1
MAX_ROW_ID = (1 << 63) - 1
MAX_INT = (1 << 31) - 1
READ_BUFFER_BYTES = 1024 * 1024
LONG = struct.Struct('>q')
ROW_BOUNDS = struct.Struct('>qq')
_PROPAGATED_ERRORS = (InterruptedError, CancelledError, ArrowCancelled, MemoryError, RecursionError)


@dataclass(frozen=True)
class Settings:

    enabled: bool = False
    row_id_enabled: bool = True
    bucket_enabled: bool = True

    @classmethod
    def from_options(cls, options):
        return cls(options.manifest_sidecar_enabled(),
                   options.data_evolution_enabled(), options.bucket() != -1)


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


def _require(condition):
    if not condition:
        raise ValueError('Invalid, unsupported or mismatched manifest sidecar')


def _varint(value, maximum=MAX_ROW_ID):
    _require(0 <= value <= maximum)
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return result


def _deltas(values, base=0, signed=False):
    result = _varint(len(values), MAX_INT)
    for value in values:
        _require(0 <= value <= (MAX_INT if signed else MAX_ROW_ID))
        delta = value - base
        result.extend(_varint((delta << 1) ^ (delta >> 63) if signed else delta))
        base = value
    return result


class _Buffer:

    """A bounded cursor over shared bytes; payloads need not allocate memoryview slices."""

    __slots__ = ('data', 'position', 'limit')

    def __init__(self, data, position=0, limit=None):
        self.data = data if isinstance(data, memoryview) else memoryview(data)
        self.position = position
        self.limit = len(self.data) if limit is None else limit

    @property
    def remaining(self):
        return self.limit - self.position

    def take(self, count):
        start = self.position
        end = start + count
        if count < 0 or end > self.limit:
            _require(False)
        self.position = end
        return self.data[start:end]

    def uint(self, maximum=MAX_ROW_ID):
        # Do not create a memoryview slice or call take() for every encoded byte.
        data, position = self.data, self.position
        limit = self.limit
        if position >= limit:
            _require(False)
        byte = data[position]
        position += 1
        if byte < 128:
            self.position = position
            if byte > maximum:
                _require(False)
            return byte
        value = byte & 127
        for shift in (7, 14, 21, 28, 35, 42, 49, 56):
            if position >= limit:
                self.position = position
                _require(False)
            byte = data[position]
            position += 1
            value |= (byte & 127) << shift
            if byte < 128:
                self.position = position
                if byte == 0 or value > maximum:
                    _require(False)
                return value
        self.position = position
        raise ValueError('Invalid variable-length integer')

    def long(self):
        position = self.position
        if position + 8 > self.limit:
            _require(False)
        self.position = position + 8
        return LONG.unpack_from(self.data, position)[0]


def _delta_count(data):
    count = data.uint(MAX_INT)
    if count > data.limit - data.position:
        _require(False)
    return count


class _Deltas:

    __slots__ = ('data', 'value', 'maximum', 'signed', 'count', 'remaining')

    def __init__(self, data, base=0, maximum=MAX_ROW_ID, signed=False, count=None):
        _require(0 <= base <= maximum and (not signed or maximum <= MAX_INT))
        self.data, self.value, self.maximum, self.signed = data, base, maximum, signed
        # select() validates framing before filtering, but only needs a decoder
        # for payloads whose enclosing block survives earlier filters.
        if count is None:
            count = _delta_count(data)
        else:
            _require(0 <= count <= MAX_INT and count <= data.remaining)
        self.count = count
        self.remaining = self.count

    def next(self):
        _require(self.remaining > 0)
        delta = self.data.uint(2 * MAX_INT if self.signed else MAX_ROW_ID)
        if self.signed:
            delta = (delta >> 1) ^ -(delta & 1)
        _require(-self.value <= delta <= self.maximum - self.value)
        self.value += delta
        self.remaining -= 1
        return self.value


class Builder:

    def __init__(self, settings, header):
        self.settings, self.header = settings, header
        self.dictionary = {}
        self.blocks = []
        self.ranges = []
        self.partition_ids = set()
        self.bucket_pairs = set()
        self.next_offset = len(header)
        self.next_record = 0
        self.current = None

    def begin_block(self, offset, length, records):
        _require(self.current is None and offset == self.next_offset and length > 0 and records > 0)
        self.current = Block(offset, length, self.next_record, records)
        self.entries_in_block = 0
        self.row_available = self.settings.row_id_enabled
        self.partition_available = True
        self.bucket_available = self.settings.bucket_enabled
        self.ranges.clear()
        self.partition_ids.clear()
        self.bucket_pairs.clear()

    def add(self, first, count, partition=None, bucket=None, total_buckets=None):
        _require(self.current is not None)
        self.entries_in_block += 1
        if self.partition_available:
            if partition is None:
                self.partition_available = False
                self.partition_ids.clear()
            else:
                partition = bytes(partition)
                if partition not in self.dictionary:
                    self.dictionary[partition] = len(self.dictionary)
                self.partition_ids.add(self.dictionary[partition])
        if self.bucket_available:
            if bucket is None or total_buckets is None or not 0 <= bucket < total_buckets <= MAX_INT:
                self.bucket_available = False
                self.bucket_pairs.clear()
            else:
                self.bucket_pairs.add((bucket, total_buckets))
        if not self.row_available:
            return
        if first is None or not 0 <= first <= MAX_ROW_ID or count <= 0 or count - 1 > MAX_ROW_ID - first:
            self.row_available = False
            self.ranges.clear()
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
        self.ranges[left:right] = [(first, end)]

    def end_block(self):
        block = self.current
        _require(block is not None and self.entries_in_block == block.record_count)
        partitions = _deltas(sorted(self.partition_ids)) if self.partition_available else b''
        rows = b''
        if self.row_available:
            endpoints = [value for interval in self.ranges for value in interval]
            rows = LONG.pack(endpoints[0]) + LONG.pack(endpoints[-1])
            rows += _deltas(endpoints[1:-1], endpoints[0])
        buckets = b''
        if self.bucket_available:
            pairs = sorted(self.bucket_pairs)
            buckets = _deltas([b for b, _ in pairs]) + _deltas([t for _, t in pairs], signed=True)
        self.blocks.append((block, partitions, rows, buckets))
        self.next_offset = block.offset + block.length
        self.next_record = block.first_record + block.record_count
        self.current = None

    def serialize(self, file_size, entry_count):
        _require(self.current is None and self.next_offset == file_size and self.next_record == entry_count)
        data = bytearray(MAGIC) + _varint(FORMAT_VERSION) + _varint(len(self.header), MAX_INT) + self.header
        data.extend(_varint(len(self.dictionary), MAX_INT))
        for partition in self.dictionary:
            data.extend(_varint(len(partition), MAX_INT))
            data.extend(partition)
        data.extend(_varint(len(self.blocks), MAX_INT))
        for block, *payloads in self.blocks:
            for value in (block.offset, block.length, block.record_count):
                data.extend(_varint(value))
            for payload in payloads:
                data.append(1 if payload else 0)
                if payload:
                    data.extend(_varint(len(payload), MAX_INT))
                    data.extend(payload)
        data.extend(struct.pack('>I', zlib.crc32(data)))
        return bytes(data)


def build_from_entries(avro_bytes, entries, settings):
    import fastavro
    blocks = iter(fastavro.block_reader(BytesIO(avro_bytes)))
    first = next(blocks, None)
    builder = Builder(settings, avro_bytes[:first.offset] if first else avro_bytes)
    position = 0
    block = first
    while block is not None:
        builder.begin_block(block.offset, block.size, block.num_records)
        end = position + block.num_records
        _require(end <= len(entries))
        for entry in entries[position:end]:
            builder.add(entry.file.first_row_id, entry.file.row_count,
                        GenericRowSerializer.to_bytes(entry.partition), entry.bucket, entry.total_buckets)
        builder.end_block()
        position = end
        block = next(blocks, None)
    return builder.serialize(len(avro_bytes), len(entries))


def _payload(data, payload):
    position = data.position
    if position >= data.limit:
        _require(False)
    encoding = data.data[position]
    data.position = position + 1
    if encoding == 0:
        return None
    length = data.uint(MAX_INT)
    start = data.position
    end = start + length
    if end > data.limit:
        _require(False)
    data.position = end
    if encoding != 1:
        return None
    payload.position, payload.limit = start, end
    return payload


def select(data, manifest, query, partition_filter=None, partition_fields=None, bucket_filter=None):
    if query is not None and not isinstance(query, Query):
        query = Query(query)
    _require(len(data) >= 33)
    view = memoryview(data)
    _require(zlib.crc32(view[:-4]) == struct.unpack('>I', view[-4:])[0])
    stream = _Buffer(view[:-4])
    _require(bytes(stream.take(4)) == MAGIC and stream.uint(MAX_INT) == FORMAT_VERSION)
    size = manifest.file_size
    entries = manifest.num_added_files + manifest.num_deleted_files
    _require(0 <= entries <= MAX_ROW_ID)
    header_length = stream.uint(MAX_INT)
    _require(21 <= header_length <= stream.remaining - 2 and header_length <= size)
    header = bytes(stream.take(header_length))
    _require(header[:4] == b'Obj\x01')
    partitions = stream.uint(MAX_INT)
    _require(partitions <= stream.remaining // 13)
    matches = [] if partition_filter is not None else None
    unique = set()
    for _ in range(partitions):
        length = stream.uint(MAX_INT)
        _require(length >= 12)
        partition = bytes(stream.take(length))
        arity, = struct.unpack_from('>i', partition)
        _require(arity >= 0 and 4 + ((arity + 71) // 64) * 8 + arity * 8 <= length)
        _require(partition_fields is None or arity == len(partition_fields))
        _require(partition not in unique)
        unique.add(partition)
        if partition_filter is not None:
            _require(partition_fields is not None)
            matches.append(partition_filter.test(GenericRowDeserializer.from_bytes(partition, partition_fields)))
    blocks = stream.uint(MAX_INT)
    _require(blocks <= stream.remaining // 6)
    next_offset, first_record = header_length, 0
    selected = []
    # Payloads are consumed within their block. Reuse bounded cursors within
    # this invocation, not across files or concurrent queries.
    partition_cursor, row_cursor, bucket_cursor = (_Buffer(stream.data) for _ in range(3))
    read_uint = stream.uint
    intersects = None if query is None else query.intersects
    for _ in range(blocks):
        if stream.limit - stream.position < 6:
            _require(False)
        offset, length, count = read_uint(), read_uint(), read_uint()
        if not (offset == next_offset and 0 < length <= size - offset
                and 0 < count <= entries - first_record):
            _require(False)
        partitions_data = _payload(stream, partition_cursor)
        row_data = _payload(stream, row_cursor)
        bucket_data = _payload(stream, bucket_cursor)
        if partitions_data is not None:
            partition_count = _delta_count(partitions_data)
            if not (0 < partition_count <= count and partition_count <= partitions):
                _require(False)
        if row_data is not None:
            if row_data.limit - row_data.position < 17:
                _require(False)
            minimum, maximum = ROW_BOUNDS.unpack_from(row_data.data, row_data.position)
            row_data.position += ROW_BOUNDS.size
            if not 0 <= minimum <= maximum:
                _require(False)
            endpoint_count = _delta_count(row_data)
            if (endpoint_count % 2 != 0 or endpoint_count // 2 >= count
                    or (endpoint_count == 0 and row_data.position != row_data.limit)):
                _require(False)
        if bucket_data is not None:
            prefix = _Buffer(bucket_data.data, bucket_data.position, bucket_data.limit)
            pairs = prefix.uint(MAX_INT)
            _require(0 < pairs <= count and 2 * pairs + 1 <= prefix.remaining)
        block_first_record = first_record
        first_record += count
        next_offset = offset + length

        if intersects is not None and row_data is not None:
            if not intersects(minimum, maximum):
                continue
            ranges = endpoint_count // 2 + 1
            row_hit, start = ranges == 1, minimum
            rows = None if row_hit else _Deltas(row_data, minimum, maximum, count=endpoint_count)
            for i in range(ranges):
                if row_hit:
                    break
                end = maximum if i + 1 == ranges else rows.next()
                _require(end >= start)
                row_hit = intersects(start, end)
                if not row_hit and i + 1 < ranges:
                    start = rows.next()
                    _require(start > end)
                _require(rows.remaining or row_data.remaining == 0)
            if not row_hit:
                continue

        if partition_filter is not None and partitions_data is not None:
            ids = _Deltas(partitions_data, maximum=partitions - 1, count=partition_count)
            partition_hit, previous = False, -1
            while not partition_hit and ids.remaining:
                id_ = ids.next()
                _require(id_ > previous)
                _require(ids.remaining or partitions_data.remaining == 0)
                previous = id_
                partition_hit = matches[id_]
            if not partition_hit:
                continue

        if bucket_filter is not None and bucket_data is not None:
            directory_data = _Buffer(bucket_data.data, bucket_data.position, bucket_data.limit)
            directory = _Deltas(directory_data, maximum=MAX_INT)
            while directory.remaining:
                directory.next()
            buckets = _Deltas(_Buffer(bucket_data.data, bucket_data.position, directory_data.position), maximum=MAX_INT)
            totals_data = _Buffer(bucket_data.data, directory_data.position, bucket_data.limit)
            totals = _Deltas(totals_data, maximum=MAX_INT, signed=True)
            _require(totals.count == buckets.count)
            previous, bucket_hit = (-1, -1), False
            while not bucket_hit and buckets.remaining:
                bucket, total = buckets.next(), totals.next()
                _require(total > bucket and (bucket, total) > previous)
                _require(buckets.remaining or (buckets.data.remaining == 0 and totals_data.remaining == 0))
                previous = (bucket, total)
                bucket_hit = bucket_filter(bucket, total)
            if not bucket_hit:
                continue
        selected.append(Block(offset, length, block_first_record, count))
    _require(stream.remaining == 0 and next_offset == size and first_record == entries)
    return Selection(header, tuple(selected))


def sidecar_file_name(manifest):
    return next((name for name in manifest.extra_files or [] if name.endswith(SUFFIX)), None)


def read_sidecar(file_io, manifest_path, manifest, query, partition_filter=None, partition_fields=None,
                 bucket_filter=None):
    name = sidecar_file_name(manifest)
    if name is None:
        return None
    sidecar_path = manifest_path.rsplit('/', 1)[0] + '/' + name
    try:
        with file_io.new_input_stream(sidecar_path) as stream:
            data = bytearray()
            while True:
                chunk = stream.read(READ_BUFFER_BYTES)
                if not chunk:
                    break
                data.extend(chunk)
        return select(data, manifest, query, partition_filter, partition_fields, bucket_filter)
    except _PROPAGATED_ERRORS:
        raise
    except Exception as error:
        pending, visited = [error], set()
        while pending:
            cause = pending.pop()
            if id(cause) in visited:
                continue
            visited.add(id(cause))
            if not isinstance(cause, Exception) or isinstance(cause, _PROPAGATED_ERRORS):
                raise cause
            if cause.__cause__ is not None:
                pending.append(cause.__cause__)
            if cause.__context__ is not None:
                pending.append(cause.__context__)
        LOG.debug('Cannot use manifest sidecar for %s; reading manifest: %s', manifest_path, error)
        return None


def read_selected_bytes(file_io, manifest_path, selected):
    """Read complete selected blocks with seek; adjacent blocks share one contiguous span.

    The concatenated original header and blocks form a valid Avro OCF. Partial entries
    must not be stored in a cache keyed by the complete manifest.
    """
    data = bytearray(selected.header)
    with file_io.new_input_stream(manifest_path) as stream:
        block_position = 0
        while block_position < len(selected.blocks):
            block = selected.blocks[block_position]
            block_position += 1
            end = block.offset + block.length
            while (block_position < len(selected.blocks)
                   and selected.blocks[block_position].offset == end):
                end += selected.blocks[block_position].length
                block_position += 1
            stream.seek(block.offset)
            remaining = end - block.offset
            while remaining:
                chunk = stream.read(min(remaining, READ_BUFFER_BYTES))
                if not chunk:
                    raise EOFError('Truncated manifest block')
                data.extend(chunk)
                remaining -= len(chunk)
    return bytes(data)
