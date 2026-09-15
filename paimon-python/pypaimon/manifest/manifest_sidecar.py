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

import hashlib
import logging
import struct
from bisect import bisect_left
from concurrent.futures import CancelledError
from dataclasses import dataclass
from io import BytesIO
from typing import Tuple

from pyarrow import ArrowCancelled

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.utils.range import Range
from pypaimon.table.row.generic_row import GenericRowSerializer, GenericRowDeserializer

LOG = logging.getLogger(__name__)
SUFFIX = '.avro.sidecar'
MAGIC = b'PAIMSCAR'
FORMAT_VERSION = 1
MAX_ROW_ID = (1 << 63) - 1
READ_BUFFER_BYTES = 1024 * 1024
HEADER = struct.Struct('>8sI32sqq')
BLOCK = struct.Struct('>qqq')
BLOCK_BYTES = 27
PAIR = struct.Struct('>qq')
LONG = struct.Struct('>q')
_PROPAGATED_ERRORS = (InterruptedError, CancelledError, ArrowCancelled, MemoryError, RecursionError)


@dataclass
class Settings:
    write: bool = False
    read: bool = False
    max_bytes: int = 2 * CoreOptions.MANIFEST_TARGET_FILE_SIZE.default_value().get_bytes()
    # Control payload generation only; readers use the payloads present in the file.
    partition_enabled: bool = True
    row_id_enabled: bool = True
    bucket_enabled: bool = True

    def __post_init__(self):
        if self.max_bytes < 0:
            raise ValueError('manifest.sidecar.max-bytes must not be negative')
        self.max_bytes = min(self.max_bytes, (1 << 31) - 2)

    @classmethod
    def from_options(cls, options, partition_count):
        write = options.manifest_sidecar_write_enabled()
        read = options.manifest_sidecar_read_enabled()
        partition_enabled = partition_count > 0
        row_id_enabled = options.data_evolution_enabled()
        bucket_enabled = options.bucket() != -1
        # Disabled sidecars must not constrain the manifest target size.
        if not write and not read:
            return cls(
                partition_enabled=partition_enabled, row_id_enabled=row_id_enabled, bucket_enabled=bucket_enabled)
        return cls(
            write, read, options.manifest_sidecar_max_size(), partition_enabled, row_id_enabled, bucket_enabled)


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


def _payload_size(payload):
    return 4 + len(payload) if payload else 0


class Builder:
    def __init__(self, settings, header):
        self.settings = settings
        self.header = header
        self.complete = (header is not None
                         and HEADER.size + 44 + len(header) <= settings.max_bytes)
        self.dictionary = {}
        self.dictionary_bytes = 0
        self.optional_bytes = 0
        self.blocks = []
        self.ranges = []
        self.partition_ids = set()
        self.bucket_pairs = set()
        self.next_offset = len(header) if header is not None else 0
        self.next_record = 0
        self.current = None

    def begin_block(self, offset, length, records):
        if not self.complete:
            return
        _require(self.current is None and offset == self.next_offset and length > 0 and records > 0)
        if (HEADER.size + 44 + len(self.header)
                + (len(self.blocks) + 1) * BLOCK_BYTES > self.settings.max_bytes):
            self.complete = False
            self.blocks.clear()
            self.dictionary.clear()
            return
        self.current = Block(offset, length, self.next_record, records)
        self.entries_in_block = 0
        self.row_available = self.settings.row_id_enabled
        self.partition_available = self.settings.partition_enabled
        self.bucket_available = self.settings.bucket_enabled
        self.coarse = False
        self.min = MAX_ROW_ID
        self.max = -1
        self.ranges.clear()
        self.partition_ids.clear()
        self.bucket_pairs.clear()

    def add(self, first, count, partition=None, bucket=None, total_buckets=None):
        if not self.complete:
            return
        _require(self.current is not None)
        self.entries_in_block += 1
        self._add_partition(partition)
        self._add_bucket(bucket, total_buckets)
        if not self.row_available:
            return
        if (first is None or first < 0 or count <= 0
                or first > MAX_ROW_ID or count - 1 > MAX_ROW_ID - first):
            self.row_available = False
            self.ranges.clear()
            return
        end = first + count - 1
        self.min, self.max = min(self.min, first), max(self.max, end)
        # Even after coarsening, inspect every entry to extend bounds or mark coverage unknown.
        if self.coarse:
            return
        left = bisect_left(self.ranges, (first, -1))
        if left and self.ranges[left - 1][1] >= first - 1:
            left -= 1
        right = left
        while right < len(self.ranges) and self.ranges[right][0] <= end + 1:
            first = min(first, self.ranges[right][0])
            end = max(end, self.ranges[right][1])
            right += 1
        if (8 + 16 * (len(self.ranges) - (right - left) + 1)
                > self.settings.max_bytes - self.optional_bytes):
            self.coarse = True
            self.ranges.clear()
        else:
            self.ranges[left:right] = [(first, end)]

    def _add_bucket(self, bucket, total_buckets):
        if not self.bucket_available:
            return
        if (bucket is None or total_buckets is None or bucket < 0 or total_buckets <= bucket
                or total_buckets > (1 << 31) - 1):
            self.bucket_available = False
            self.bucket_pairs.clear()
            return
        pair = (bucket, total_buckets)
        if (pair not in self.bucket_pairs
                and 8 + 8 * (len(self.bucket_pairs) + 1) > self.settings.max_bytes - self.optional_bytes):
            self.bucket_available = False
            self.bucket_pairs.clear()
        else:
            self.bucket_pairs.add(pair)

    def _add_partition(self, partition):
        if not self.partition_available:
            return
        if partition is None:
            self.partition_available = False
            self.partition_ids.clear()
            return
        partition = bytes(partition)
        id_ = self.dictionary.get(partition)
        if id_ is None:
            if len(partition) + 4 > self.settings.max_bytes - self.dictionary_bytes:
                self.partition_available = False
                self.partition_ids.clear()
                return
            id_ = len(self.dictionary)
            self.dictionary[partition] = id_
            self.dictionary_bytes += 4 + len(partition)
        self.partition_ids.add(id_)

    def end_block(self):
        if not self.complete:
            return
        block = self.current
        _require(block is not None and self.entries_in_block == block.record_count)
        row_payload = partition_payload = b''
        if self.row_available:
            if (self.coarse
                    or 8 + 16 * len(self.ranges) > self.settings.max_bytes - self.optional_bytes):
                self.ranges = [(self.min, self.max)]
            if 8 + 16 * len(self.ranges) <= self.settings.max_bytes - self.optional_bytes:
                row_payload = struct.pack('>I', len(self.ranges)) + b''.join(PAIR.pack(*r) for r in self.ranges)
                self.optional_bytes += _payload_size(row_payload)
        if (self.partition_available
                and 8 + 4 * len(self.partition_ids) <= self.settings.max_bytes - self.optional_bytes):
            partition_payload = struct.pack('>I', len(self.partition_ids))
            partition_payload += b''.join(struct.pack('>I', id_) for id_ in sorted(self.partition_ids))
            self.optional_bytes += _payload_size(partition_payload)
        bucket_payload = b''
        if (self.bucket_available
                and 8 + 8 * len(self.bucket_pairs) <= self.settings.max_bytes - self.optional_bytes):
            bucket_payload = struct.pack('>I', len(self.bucket_pairs))
            bucket_payload += b''.join(struct.pack('>ii', *pair) for pair in sorted(self.bucket_pairs))
            self.optional_bytes += _payload_size(bucket_payload)
        self.blocks.append([block, partition_payload, row_payload, bucket_payload])
        self.next_offset = block.offset + block.length
        self.next_record = block.first_record + block.record_count
        self.ranges.clear()
        self.partition_ids.clear()
        self.bucket_pairs.clear()
        self.current = None

    def serialize(self, name, file_size, entry_count):
        if not self.complete:
            return None
        _require(self.current is None and self.next_offset == file_size and self.next_record == entry_count)
        size = (HEADER.size + 44 + len(self.header) + self.dictionary_bytes
                + len(self.blocks) * BLOCK_BYTES + self.optional_bytes)
        for item in self.blocks:
            if size <= self.settings.max_bytes:
                break
            size -= _payload_size(item[2])
            self.optional_bytes -= _payload_size(item[2])
            item[2] = b''
        for item in self.blocks:
            if size <= self.settings.max_bytes:
                break
            size -= _payload_size(item[3])
            self.optional_bytes -= _payload_size(item[3])
            item[3] = b''
        if size > self.settings.max_bytes:
            size -= self.dictionary_bytes
            self.dictionary_bytes = 0
            self.dictionary.clear()
            for item in self.blocks:
                size -= _payload_size(item[1])
                self.optional_bytes -= _payload_size(item[1])
                item[1] = b''
        _require(size <= self.settings.max_bytes)
        data = bytearray(HEADER.pack(
            MAGIC, FORMAT_VERSION, hashlib.sha256(name.encode('utf-8')).digest(), file_size, entry_count))
        data.extend(struct.pack('>I', len(self.header)))
        data.extend(self.header)
        data.extend(struct.pack('>I', len(self.dictionary)))
        for partition in self.dictionary:
            data.extend(struct.pack('>I', len(partition)))
            data.extend(partition)
        data.extend(struct.pack('>I', len(self.blocks)))
        for block, partitions, row_ids, buckets in self.blocks:
            data.extend(BLOCK.pack(block.offset, block.length, block.record_count))
            for payload in (partitions, row_ids, buckets):
                data.append(1 if payload else 0)
                if payload:
                    data.extend(struct.pack('>I', len(payload)))
                    data.extend(payload)
        return bytes(data) + hashlib.sha256(data).digest()


def build_from_entries(avro_bytes, entries, name, settings):
    if settings.max_bytes < 128:
        return None
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
            builder.add(entry.file.first_row_id if settings.row_id_enabled else None,
                        entry.file.row_count if settings.row_id_enabled else 0,
                        GenericRowSerializer.to_bytes(entry.partition) if settings.partition_enabled else None,
                        entry.bucket if settings.bucket_enabled else None,
                        entry.total_buckets if settings.bucket_enabled else None)
            if not builder.complete:
                break
        builder.end_block()
        position = end
        block = next(blocks, None) if builder.complete else None
    return builder.serialize(name, len(avro_bytes), len(entries))


def _require(condition):
    if not condition:
        raise ValueError('Invalid, unsupported, mismatched or over-budget manifest sidecar')


def select(data, manifest, query, settings, partition_filter=None, partition_fields=None, bucket_filter=None):
    if query is not None and not isinstance(query, Query):
        query = Query(query)
    _require(128 <= len(data) <= settings.max_bytes)
    limit = len(data) - 32
    view = memoryview(data)
    _require(hashlib.sha256(view[:limit]).digest() == data[limit:])
    magic, version, name_hash, size, entries = HEADER.unpack_from(data)
    _require(magic == MAGIC and version == FORMAT_VERSION)
    _require(name_hash == hashlib.sha256(manifest.file_name.encode('utf-8')).digest())
    _require(size == manifest.file_size and entries == manifest.num_added_files + manifest.num_deleted_files)
    header_length, = struct.unpack_from('>I', data, HEADER.size)
    offset = HEADER.size + 4
    _require(21 <= header_length <= limit - offset - 8)
    header = bytes(data[offset:offset + header_length])
    _require(header[:4] == b'Obj\x01')
    offset += header_length
    partitions, = struct.unpack_from('>I', data, offset)
    offset += 4
    _require(partitions <= (limit - offset) // 16)
    matches = None if partition_filter is None else []
    unique = set()
    for _ in range(partitions):
        _require(offset + 4 <= limit)
        length, = struct.unpack_from('>I', data, offset)
        offset += 4
        _require(12 <= length <= limit - offset)
        partition = bytes(data[offset:offset + length])
        arity, = struct.unpack_from('>i', partition)
        _require(arity >= 0 and 4 + ((arity + 71) // 64) * 8 + arity * 8 <= length)
        _require(partition_fields is None or arity == len(partition_fields))
        _require(partition not in unique)
        unique.add(partition)
        if partition_filter is not None:
            _require(partition_fields is not None)
            matches.append(partition_filter.test(GenericRowDeserializer.from_bytes(partition, partition_fields)))
        offset += length
    _require(offset + 4 <= limit)
    blocks, = struct.unpack_from('>I', data, offset)
    offset += 4
    _require(blocks <= (limit - offset) // BLOCK_BYTES)
    next_offset = header_length
    first_record = 0
    selected = []
    for _ in range(blocks):
        _require(offset + BLOCK_BYTES <= limit)
        file_offset, length, count = BLOCK.unpack_from(data, offset)
        offset += BLOCK.size
        _require(file_offset == next_offset and 0 < length <= size - file_offset)
        _require(0 < count <= entries - first_record)
        partition_payload, offset = _payload(view, offset, limit, 4)
        row_payload, offset = _payload(view, offset, limit, 16)
        bucket_payload, offset = _payload(view, offset, limit, 8)
        block_first_record = first_record
        next_offset = file_offset + length
        first_record += count

        if query is not None and row_payload is not None:
            single_range = len(row_payload) == 16
            min_row_id, first_end = PAIR.unpack_from(row_payload)
            max_row_id = first_end if single_range else LONG.unpack_from(row_payload, len(row_payload) - 8)[0]
            _require(min_row_id >= 0 and first_end >= min_row_id and max_row_id >= first_end)
            if not query.intersects(min_row_id, max_row_id):
                continue
            row_hit = single_range or query.intersects(min_row_id, first_end)
            previous = first_end
            for position in range(16, len(row_payload), 16):
                if row_hit:
                    break
                start, end = PAIR.unpack_from(row_payload, position)
                _require(start >= 0 and end >= start and start > previous)
                previous = end
                row_hit = query.intersects(start, end)
            if not row_hit:
                continue

        if partition_filter is not None and partition_payload is not None:
            partition_hit = False
            previous = -1
            for position in range(0, len(partition_payload), 4):
                id_, = struct.unpack_from('>i', partition_payload, position)
                _require(previous < id_ < partitions)
                previous = id_
                partition_hit = matches[id_]
                if partition_hit:
                    break
            if not partition_hit:
                continue

        if bucket_filter is not None and bucket_payload is not None:
            bucket_hit = False
            previous = (-1, -1)
            for position in range(0, len(bucket_payload), 8):
                bucket, total_buckets = struct.unpack_from('>ii', bucket_payload, position)
                _require(0 <= bucket < total_buckets and (bucket, total_buckets) > previous)
                previous = (bucket, total_buckets)
                bucket_hit = bucket_filter(bucket, total_buckets)
                if bucket_hit:
                    break
            if not bucket_hit:
                continue
        selected.append(Block(file_offset, length, block_first_record, count))
    _require(offset == limit and next_offset == size and first_record == entries)
    return Selection(header, tuple(selected))


def _payload(data, offset, limit, element_bytes):
    """Read framing and expose known payload elements without decoding their contents."""
    _require(offset < limit)
    encoding = data[offset]
    offset += 1
    if encoding == 0:
        return None, offset
    _require(offset + 4 <= limit)
    length, = struct.unpack_from('>I', data, offset)
    offset += 4
    _require(length <= limit - offset)
    end = offset + length
    if encoding != 1:
        return None, end
    _require(length >= 4)
    count, = struct.unpack_from('>I', data, offset)
    _require(count > 0 and length == 4 + element_bytes * count)
    return data[offset + 4:end], end


def sidecar_file_name(manifest):
    return next((name for name in manifest.extra_files or [] if name.endswith(SUFFIX)), None)


def read_sidecar(file_io, manifest_path, manifest, query, settings, partition_filter=None, partition_fields=None,
                 bucket_filter=None):
    name = sidecar_file_name(manifest)
    if name is None or settings.max_bytes < 128:
        return None
    sidecar_path = manifest_path.rsplit('/', 1)[0] + '/' + name
    try:
        with file_io.new_input_stream(sidecar_path) as stream:
            data = bytearray()
            while True:
                chunk = stream.read(min(READ_BUFFER_BYTES, settings.max_bytes + 1 - len(data)))
                if not chunk:
                    break
                _require(len(chunk) <= settings.max_bytes - len(data))
                data.extend(chunk)
        return select(data, manifest, query, settings, partition_filter, partition_fields, bucket_filter)
    except _PROPAGATED_ERRORS:
        raise
    except Exception as error:
        pending = [error]
        visited = set()
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
