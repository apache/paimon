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

import zlib
import struct
import random
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.manifest import manifest_sidecar
from pypaimon.manifest.manifest_sidecar import Builder, Settings, select
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
from pypaimon.tests.manifest.manifest_sidecar_test import avro_header, golden, golden_meta, make_sidecar
from pypaimon.utils.range import Range

FIELDS = [DataField(0, 'p', AtomicType('INT')), DataField(1, 'q', AtomicType('STRING'))]


def partition(p, q):
    return GenericRowSerializer.to_bytes(GenericRow([p, q], FIELDS))


def part(p):
    return SimpleNamespace(test=lambda row: row.values[0] == p)


def fixture(key):
    if key == 'avroHeader':
        return avro_header()
    partitions = (partition(7, 'left'), partition(9, None))
    if key == 'partitionA':
        return partitions[0]
    if key == 'partitionB':
        return partitions[1]
    return make_sidecar(partitions if key != 'index' else None, key == 'indexWithBuckets')


def meta(name, size, count):
    return SimpleNamespace(file_name=name, file_size=size, num_added_files=count, num_deleted_files=0)


def test_v1_golden_file_matches_java():
    path = Path(__file__).resolve().parents[4] / 'paimon-core/src/test/resources/compatibility/manifest-sidecar-v1'
    data = path.read_bytes()
    header = avro_header()
    block_length = 1024 * 1024 + 17
    builder = Builder(Settings(), header)
    for block in range(10):
        builder.begin_block(len(header) + block * block_length, block_length, 16)
        step = 16400 if block % 4 == 0 else 16
        for entry in range(16):
            p = (block * 7 + entry * 3) % 16
            builder.add((block << 40) + entry * step, 3,
                        partition(p, None if p % 7 == 0 else 'partition-' + str(p)),
                        block + entry * 13, 512 if entry % 2 == 0 else 1024)
        builder.end_block()
    file_size = len(header) + 10 * block_length
    assert data == builder.serialize(file_size, 160)
    metadata = meta('arbitrary-name', file_size, 160)
    assert len(select(data, metadata, None).blocks) == 10
    for block in range(10):
        step = 16400 if block % 4 == 0 else 16
        for entry in range(16):
            row_id = (block << 40) + entry * step
            p = (block * 7 + entry * 3) % 16
            pair = (block + entry * 13, 512 if entry % 2 == 0 else 1024)
            selected = select(data, metadata, [Range(row_id, row_id + 2)], part(p), FIELDS,
                              lambda b, t: (b, t) == pair)
            assert selected.blocks == (manifest_sidecar.Block(
                len(header) + block * block_length, block_length, block * 16, 16),)
            assert not select(data, metadata, [Range(row_id + 3, row_id + 3)]).blocks
    assert not select(data, metadata, None, part(99), FIELDS).blocks
    assert not select(data, metadata, None, bucket_filter=lambda b, t: t == 4096).blocks
    for length in (0, 32, len(data) // 2, len(data) - 1):
        with pytest.raises(ValueError):
            select(data[:length], metadata, None)


@pytest.mark.parametrize('partition_enabled', [False, True])
@pytest.mark.parametrize('row_id_enabled', [False, True])
@pytest.mark.parametrize('bucket_count', [-2, -1, 4])
def test_settings_derive_payload_generation_from_table_metadata(partition_enabled, row_id_enabled, bucket_count):
    header = avro_header()
    bucket_enabled = bucket_count != -1
    settings = Settings.from_options(CoreOptions(Options({
        'data-evolution.enabled': row_id_enabled, 'bucket': bucket_count})))
    builder = Builder(settings, header)
    fields = FIELDS if partition_enabled else []
    for block in range(2):
        builder.begin_block(len(header) + block * 100, 100, 1)
        p = partition(7 + block, 'p') if partition_enabled else GenericRowSerializer.to_bytes(GenericRow([], []))
        builder.add(100 + block * 100, 10, p, 1, 4)
        builder.end_block()
    data = builder.serialize(len(header) + 200, 2)
    for position in positions(data):
        assert data[position[1]] == 1
        assert data[position[2]] == int(row_id_enabled)
        assert data[position[3]] == int(bucket_enabled)
    metadata = meta('m', len(header) + 200, 2)
    assert len(select(data, metadata, [Range(999, 999)]).blocks) == (0 if row_id_enabled else 2)
    assert not select(data, metadata, None, SimpleNamespace(test=lambda row: False), fields).blocks
    assert len(select(data, metadata, None, bucket_filter=lambda b, t: b == 99).blocks) == (0 if bucket_enabled else 2)


def test_partition_dictionary_golden_tuples_nulls_and_derived_ordinals():
    a, b, header = partition(7, 'left'), partition(9, None), avro_header()
    assert a == fixture('partitionA')
    assert b == fixture('partitionB')
    builder = Builder(Settings(), header)
    values = [(0, 100, [(0, 10, a), (5, 5, a), (20, 5, b)]),
              (100, 200, [((1 << 32) - 2, 5, b), (8254058425445, 1, a)]),
              (300, 100, [(20, 5, a), ((1 << 63) - 1, 1, b)])]
    for offset, length, entries in values:
        builder.begin_block(len(header) + offset, length, len(entries))
        for first, count, p in entries:
            builder.add(first, count, p)
        builder.end_block()
    data = builder.serialize(len(header) + 400, 7)
    assert data == fixture('indexWithPartitions')
    predicate = part(7)
    with patch.object(predicate, 'test', wraps=predicate.test) as evaluated:
        selected = select(data, golden_meta(), [Range(20, 20)], predicate, FIELDS)
    assert evaluated.call_count == 2
    assert [b.first_record for b in selected.blocks] == [0, 5]
    nulls = SimpleNamespace(test=lambda row: row.values[1] is None)
    assert len(select(data, golden_meta(), None, nulls, FIELDS).blocks) == 3
    assert not select(data, golden_meta(), None, part(99), FIELDS).blocks
    assert len(select(golden(), golden_meta(), None, part(99), FIELDS).blocks) == 3


def test_unavailable_dimensions_are_independent_and_dictionary_misses_keep_unknown_blocks():
    settings, header = Settings(), avro_header()
    builder = Builder(settings, header)
    for i, (first, p) in enumerate([(None, partition(7, 'left')), (200, partition(9, 'x' * 600)),
                                   (300, partition(7, 'left'))]):
        builder.begin_block(len(header) + 100 * i, 100, 1)
        builder.add(first, 10, p)
        builder.end_block()
    data = builder.serialize(len(header) + 300, 3)
    metadata = meta('m', len(header) + 300, 3)
    assert [b.first_record for b in select(data, metadata, None, part(9), FIELDS).blocks] == [1]
    selected = select(data, metadata, [Range(999, 999)], part(7), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0]
    selected = select(data, metadata, [Range(200, 200)], part(9), FIELDS)
    assert [b.first_record for b in selected.blocks] == [1]


@pytest.mark.parametrize('unknown', [False, True])
def test_exact_row_ranges_keep_gaps_and_detect_late_unknowns(unknown):
    settings, header = Settings(), avro_header()
    builder = Builder(settings, header)
    builder.begin_block(len(header), 100, 66)
    for i in range(64):
        builder.add(100 + i * 1000, 10, partition(7, 'left'))
    for first, count in [(10, 10), (None if unknown else (1 << 63) - 1, 1)]:
        builder.add(first, count, partition(7, 'left'))
    builder.end_block()
    data = builder.serialize(len(header) + 100, 66)
    metadata = meta('m', len(header) + 100, 66)
    for point in [10, 100, (1 << 63) - 1]:
        assert len(select(data, metadata, [Range(point, point)]).blocks) == 1
    assert len(select(data, metadata, [Range(0, 0)]).blocks) == int(unknown)
    assert len(select(data, metadata, [Range(200, 200)]).blocks) == int(unknown)
    assert not select(data, metadata, None, part(9), FIELDS).blocks


def positions(data):
    reader = manifest_sidecar._Buffer(data[:-4])
    reader.take(4)
    reader.uint()
    reader.take(reader.uint())
    for _ in range(reader.uint()):
        reader.take(reader.uint())
    result = []
    for _ in range(reader.uint()):
        block = reader.position
        for _ in range(3):
            reader.uint()
        payloads = []
        for _ in range(3):
            payloads.append(reader.position)
            if reader.take(1)[0]:
                reader.take(reader.uint())
        result.append((block, *payloads))
    return result


def checksum(data):
    data[-4:] = struct.pack('>I', zlib.crc32(data[:-4]))
    return data


def replace_payload(data, block, dimension, payload, encoding=1):
    start = positions(data)[block][dimension]
    reader = manifest_sidecar._Buffer(data)
    reader.position = start
    if reader.take(1)[0]:
        reader.take(reader.uint())
    framed = bytes([encoding])
    if encoding:
        framed += manifest_sidecar._varint(len(payload)) + payload
    return checksum(bytearray(data[:start] + framed + data[reader.position:]))


def vint(value):
    return bytes(manifest_sidecar._varint(value))


def row_payload(minimum, maximum, deltas):
    return struct.pack('>qq', minimum, maximum) + vint(len(deltas)) + b''.join(vint(d) for d in deltas)


def test_row_miss_skips_partition_and_bucket_payloads():
    data = replace_payload(fixture('indexWithBuckets'), 0, 1, b'\2' + vint(999) + b'\1')
    data = replace_payload(data, 0, 3, b'\2\0\0\2\0\0')
    buckets = Mock(return_value=True)
    assert not select(data, golden_meta(), [Range(15, 15)], part(7), FIELDS, buckets).blocks
    buckets.assert_not_called()


@pytest.mark.parametrize('row_ranges', [None, [Range(0, 0)]])
def test_partition_miss_skips_bucket_matching(row_ranges):
    data = replace_payload(fixture('indexWithBuckets'), 0, 3, b'\2\0\0\2\0\0')
    buckets = Mock(return_value=True)
    assert not select(data, golden_meta(), row_ranges, part(99), FIELDS, buckets).blocks
    buckets.assert_not_called()


def test_absent_partition_filter_keeps_row_and_bucket_matching():
    data = replace_payload(fixture('indexWithBuckets'), 0, 1, b'\2' + vint(999) + b'\1')
    with patch('pypaimon.manifest.manifest_sidecar.GenericRowDeserializer.from_bytes') as decode:
        selected = select(data, golden_meta(), [Range(0, 0)], None, FIELDS, lambda b, t: b == 1)
    assert [b.first_record for b in selected.blocks] == [0]
    decode.assert_not_called()


def test_absent_row_or_bucket_filters_keep_remaining_dimensions():
    data = fixture('indexWithBuckets')
    selected = select(data, golden_meta(), None, part(7), FIELDS, lambda b, t: b == 1)
    assert [b.first_record for b in selected.blocks] == [0]
    assert [b.first_record for b in select(data, golden_meta(), [Range(20, 20)], part(7), FIELDS).blocks] == [0, 5]
    assert len(select(data, golden_meta(), None).blocks) == 3


def test_partition_and_bucket_matches_skip_unused_payload_elements():
    data = replace_payload(fixture('indexWithBuckets'), 0, 1, b'\2\0' + vint(999))
    assert [b.first_record for b in select(data, golden_meta(), [Range(0, 0)], part(7), FIELDS).blocks] == [0]
    with pytest.raises(ValueError):
        select(data, golden_meta(), [Range(0, 0)], part(99), FIELDS)
    data = replace_payload(fixture('indexWithBuckets'), 0, 3, b'\2\1\0\2\10\0')
    selected = select(data, golden_meta(), [Range(0, 0)], None, FIELDS, lambda b, t: b == 1)
    assert [b.first_record for b in selected.blocks] == [0]
    with pytest.raises(ValueError):
        select(data, golden_meta(), [Range(0, 0)], None, FIELDS, lambda b, t: False)


def test_skipped_payloads_still_require_valid_framing_and_directory():
    for dimension in (1, 2, 3):
        for payload in (b'', b'\x80', vint(1 << 31)):
            data = replace_payload(fixture('indexWithBuckets'), 0, dimension, payload)
            with pytest.raises(ValueError):
                select(data, golden_meta(), [Range(999, 999)])
    data = bytearray(fixture('indexWithBuckets'))
    reader = manifest_sidecar._Buffer(data)
    reader.position = positions(data)[0][0]
    reader.uint()
    reader.uint()
    data[reader.position] = 2
    with pytest.raises(ValueError):
        select(checksum(data), golden_meta(), [Range(999, 999)])


def test_unsigned_unknown_encodings_skip_only_one_payload_and_validate_lengths():
    for dimension in (1, 2, 3):
        data = replace_payload(fixture('indexWithBuckets'), 0, dimension, b'\x80', 202)
        point = 999 if dimension == 2 else 0
        selected = select(data, golden_meta(), [Range(point, point)],
                          part(99 if dimension == 1 else 7), FIELDS,
                          lambda b, t: b == (99 if dimension == 3 else 1))
        assert [b.first_record for b in selected.blocks] == [0]
        start = positions(data)[0][dimension]
        data[start + 1] = 127
        with pytest.raises(ValueError):
            select(checksum(data), golden_meta(), None)


def test_complete_directory_and_payloads_are_not_dropped():
    header = avro_header()
    builder = Builder(Settings(), header)
    for i in range(3):
        builder.begin_block(len(header) + 100 * i, 100, 1)
        builder.add(100 * i, 10, partition(7, 'left'))
        builder.end_block()
    data = builder.serialize(len(header) + 300, 3)
    assert not select(data, meta('m', len(header) + 300, 3), [Range(999, 999)], part(99), FIELDS).blocks
    assert len(select(data, meta('m', len(header) + 300, 3), None).blocks) == 3
    assert builder.serialize(len(header) + 300, 3) == data
    builder.begin_block(len(header) + 300, 100, 1)
    builder.add(300, 1, partition(7, 'left'))
    builder.end_block()
    data = builder.serialize(len(header) + 400, 4)
    assert len(select(data, meta('m', len(header) + 400, 4), None).blocks) == 4


def test_randomized_exact_coverage_has_no_false_negatives():
    rng = random.Random(9743)
    header = avro_header()
    for _ in range(60):
        settings = Settings()
        builder = Builder(settings, header)
        blocks = []
        for i in range(5):
            values = [(rng.choice([None, rng.randrange(100)]), rng.randrange(1, 10), rng.randrange(5))
                      for _ in range(6)]
            blocks.append(values)
            builder.begin_block(len(header) + 100 * i, 100, len(values))
            for first, count, p in values:
                builder.add(first, count, partition(p, None))
            builder.end_block()
        data = builder.serialize(len(header) + 500, 30)
        assert data is not None
        metadata = meta('m', len(header) + 500, 30)
        for point in range(0, 110, 11):
            for p in range(5):
                selected = select(data, metadata, [Range(point, point)], part(p), FIELDS)
                ordinals = {b.first_record for b in selected.blocks}
                expected = set()
                for i, values in enumerate(blocks):
                    if (any(row_p == p for _, _, row_p in values)
                            and any(first is None or first <= point < first + count for first, count, _ in values)):
                        expected.add(i * 6)
                assert ordinals == expected


def test_bucket_payload_golden_rescale_and_unavailable_payloads():
    a, b, header = partition(7, 'left'), partition(9, None), avro_header()
    builder = Builder(Settings(), header)
    for offset, length, values in [
            (0, 100, [(0, 10, a, 1, 4), (5, 5, a, 1, 4), (20, 5, b, 1, 8)]),
            (100, 200, [((1 << 32) - 2, 5, b, 2, 4), (8254058425445, 1, a, 2, 8)]),
            (300, 100, [(20, 5, a, 0, 1), ((1 << 63) - 1, 1, b, 3, 4)])]:
        builder.begin_block(len(header) + offset, length, len(values))
        for value in values:
            builder.add(*value)
        builder.end_block()
    data = builder.serialize(len(header) + 400, 7)
    assert data == fixture('indexWithBuckets')
    selected = select(data, golden_meta(), None, bucket_filter=lambda bucket, total: bucket == 1)
    assert [b.first_record for b in selected.blocks] == [0]
    selected = select(data, golden_meta(), None, bucket_filter=lambda bucket, total: (bucket, total) == (2, 8))
    assert [b.first_record for b in selected.blocks] == [3]
    assert not select(data, golden_meta(), [Range(0, 0)], part(7), FIELDS,
                      bucket_filter=lambda bucket, total: bucket == 2).blocks
    for data in [golden(), fixture('indexWithPartitions')]:
        assert len(select(data, golden_meta(), None, bucket_filter=lambda bucket, total: False).blocks) == 3


@pytest.mark.parametrize('pair', [(None, None), (-1, 4), (4, 4), (0, 0), (2, 8)])
def test_unknown_pairs_disable_only_bucket_payload(pair):
    header = avro_header()
    builder = Builder(Settings(), header)
    builder.begin_block(len(header), 100, 2)
    builder.add(100, 10, partition(7, 'left'), 1, 4)
    builder.add(200, 10, partition(7, 'left'), *pair)
    builder.end_block()
    data = builder.serialize(len(header) + 100, 2)
    metadata = meta('m', len(header) + 100, 2)
    valid = pair == (2, 8)
    assert data[positions(data)[0][3]] == int(valid)
    assert len(select(data, metadata, None, bucket_filter=lambda b, t: False).blocks) == int(not valid)
    assert not select(data, metadata, [Range(999, 999)]).blocks


def test_large_payloads_keep_exact_coverage():
    header, settings = avro_header(), Settings()
    builder = Builder(settings, header)
    blocks, entries_per_block, block_bytes = 33, 4097, 1024 * 1024
    entries = blocks * entries_per_block
    for block in range(blocks):
        builder.begin_block(len(header) + block * block_bytes, block_bytes, entries_per_block)
        for i in range(entries_per_block):
            entry = block * entries_per_block + i
            builder.add(entry * 2, 1, partition(entry, None), i, entries_per_block + 1)
        builder.end_block()
    file_size = len(header) + blocks * block_bytes
    data = builder.serialize(file_size, entries)
    assert data is not None
    metadata = meta('m', file_size, entries)
    last = (entries - 1) * 2
    selected = select(data, metadata, [Range(last, last)])
    assert [b.first_record for b in selected.blocks] == [(blocks - 1) * entries_per_block]
    assert not select(data, metadata, [Range(last - 1, last - 1)]).blocks
    assert not select(data, metadata, None, part(entries), FIELDS).blocks
    assert not select(data, metadata, None, bucket_filter=lambda bucket, total: bucket == entries_per_block).blocks


def test_absent_payloads_omit_length_fields_for_every_dimension_combination():
    header, settings = avro_header(), Settings()
    builder = Builder(settings, header)
    for mask in range(8):
        builder.begin_block(len(header) + mask * 100, 100, 1)
        builder.add(100 + mask if mask & 2 else None, 1,
                    partition(7, 'left') if mask & 1 else None,
                    0 if mask & 4 else None, 1 if mask & 4 else None)
        builder.end_block()
    data = builder.serialize(len(header) + 800, 8)
    locations = positions(data)
    for mask in range(8):
        for dimension, present_size in enumerate((4, 19, 6)):
            start = locations[mask][dimension + 1]
            end = (locations[mask][dimension + 2] if dimension < 2
                   else locations[mask + 1][0] if mask < 7 else len(data) - 4)
            present = bool(mask & (1 << dimension))
            assert data[start] == int(present)
            assert end - start == (present_size if present else 1)
    metadata = meta('m', len(header) + 800, 8)
    selected = select(data, metadata, None, part(99), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0, 2, 4, 6]
    selected = select(data, metadata, [Range(999, 999)])
    assert [b.first_record for b in selected.blocks] == [0, 1, 4, 5]
    buckets = lambda bucket, total: False
    selected = select(data, metadata, None, None, FIELDS, buckets)
    assert [b.first_record for b in selected.blocks] == [0, 1, 2, 3]
    selected = select(data, metadata, [Range(999, 999)], part(99), FIELDS, buckets)
    assert [b.first_record for b in selected.blocks] == [0]


def test_malformed_bucket_payload_invalidates_the_container():
    for payload in (b'\0', b'\x80', b'\1\1\1\1', b'\2\1\0\1\10\10',
                    b'\2\1\0\2\10\0', b'\1\1\1' + vint(2 * ((1 << 31) - 1) + 1)):
        data = replace_payload(fixture('indexWithBuckets'), 0, 3, payload)
        with pytest.raises(ValueError):
            select(data, golden_meta(), None, bucket_filter=lambda b, t: False)


def test_varint_cursor_boundaries_and_single_byte_fast_path():
    values = {0, 1, 127, 128, manifest_sidecar.MAX_INT, manifest_sidecar.MAX_ROW_ID}
    for bits in range(7, 63, 7):
        values.update((2 ** bits - 1, 2 ** bits, 2 ** bits + 1))
    rng = random.Random(9908)
    values.update(rng.getrandbits(63) for _ in range(1000))
    for value in sorted(values):
        encoded = vint(value)
        for source in (b'\xff' + encoded + b'\x55', bytearray(b'\xff' + encoded + b'\x55')):
            reader = manifest_sidecar._Buffer(source, 1, 1 + len(encoded))
            assert reader.uint(value) == value
            assert reader.position == reader.limit and reader.remaining == 0
            with pytest.raises(ValueError):
                reader.take(1)
            with pytest.raises(ValueError):
                reader.uint()
        reader = manifest_sidecar._Buffer(memoryview(b'prefix' + encoded + b'\x55')[6:])
        assert reader.uint() == value
        assert bytes(reader.take(1)) == b'\x55'


@pytest.mark.parametrize('encoded', [b'', b'\x80', b'\x80\x00', b'\x81\x00', b'\xff\x00',
                                     b'\x80' * 8, b'\xff' * 9, b'\xff' * 9 + b'\x01',
                                     b'\xff' * 8 + b'\x00'])
def test_varint_rejects_truncation_noncanonical_and_overflow_without_reading_next_payload(encoded):
    # The following byte could terminate a truncated varint, but belongs to a different payload.
    reader = manifest_sidecar._Buffer(b'\x55' + encoded + b'\x01', 1, 1 + len(encoded))
    with pytest.raises(ValueError):
        reader.uint()
    assert reader.position <= reader.limit


@pytest.mark.parametrize('value,maximum', [
    (1, 0), (127, 126), (128, 127),
    (manifest_sidecar.MAX_INT + 1, manifest_sidecar.MAX_INT),
    (manifest_sidecar.MAX_ROW_ID, manifest_sidecar.MAX_INT)])
def test_varint_fast_path_and_multibyte_path_both_enforce_maximum(value, maximum):
    with pytest.raises(ValueError):
        manifest_sidecar._Buffer(vint(value)).uint(maximum)


def test_fixed_long_and_take_respect_payload_window():
    for value in (-(1 << 63), -1, 0, manifest_sidecar.MAX_ROW_ID):
        encoded = b'prefix' + struct.pack('>q', value) + b'suffix'
        reader = manifest_sidecar._Buffer(encoded, 6, 14)
        assert reader.long() == value and reader.remaining == 0
        assert bytes(reader.take(0)) == b''
        for size in (-1, 1):
            with pytest.raises(ValueError):
                reader.take(size)
        with pytest.raises(ValueError):
            reader.long()
        # Underlying bytes contain a full long, but this payload is truncated.
        with pytest.raises(ValueError):
            manifest_sidecar._Buffer(encoded, 6, 13).long()


def test_delta_reader_is_not_created_for_rejected_or_single_interval_blocks():
    header = avro_header()
    builder = Builder(Settings(), header)
    builder.begin_block(len(header), 100, 1)
    builder.add(100, 10, partition(7, 'left'), 1, 4)
    builder.end_block()
    data = builder.serialize(len(header) + 100, 1)
    metadata = meta('m', len(header) + 100, 1)
    with patch.object(manifest_sidecar._Deltas, '__init__', side_effect=AssertionError('Unneeded decoder')):
        assert not select(data, metadata, [Range(999, 999)], part(7), FIELDS).blocks
        assert len(select(data, metadata, [Range(105, 105)]).blocks) == 1
    # Even a guaranteed row miss must validate the other payloads' framing and counts.
    for dimension in (1, 3):
        with pytest.raises(ValueError):
            select(replace_payload(data, 0, dimension, b'\0'), metadata, [Range(999, 999)])
    with pytest.raises(ValueError):
        select(replace_payload(data, 0, 2, row_payload(200, 100, [])), metadata, [Range(999, 999)])


def test_payload_cursor_is_shared_but_cannot_cross_its_declared_limit():
    stream = manifest_sidecar._Buffer(b'\1\1\x80\1\1\0\0')
    cursor = manifest_sidecar._Buffer(stream.data)
    first = manifest_sidecar._payload(stream, cursor)
    assert first is cursor and first.data is stream.data
    with pytest.raises(ValueError):
        first.uint()
    second = manifest_sidecar._payload(stream, cursor)
    assert second is cursor and second.uint() == 0
    assert manifest_sidecar._payload(stream, cursor) is None
    assert stream.remaining == 0
