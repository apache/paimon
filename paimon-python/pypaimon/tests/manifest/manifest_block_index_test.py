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

import base64
import hashlib
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
from pypaimon.tests.manifest.manifest_sidecar_test import avro_header, golden, golden_meta
from pypaimon.utils.range import Range

FIELDS = [DataField(0, 'p', AtomicType('INT')), DataField(1, 'q', AtomicType('STRING'))]


def partition(p, q):
    return GenericRowSerializer.to_bytes(GenericRow([p, q], FIELDS))


def part(p):
    return SimpleNamespace(test=lambda row: row.values[0] == p)


def fixture(key):
    path = Path(__file__).resolve().parents[4] / 'paimon-core/src/test/resources/manifest-sidecar.txt'
    value = next(line.split('=', 1)[1] for line in path.read_text().splitlines() if line.startswith(key + '='))
    return base64.b64decode(value)


def meta(name, size, count):
    return SimpleNamespace(file_name=name, file_size=size, num_added_files=count, num_deleted_files=0)


@pytest.mark.parametrize('partition_enabled', [False, True])
@pytest.mark.parametrize('row_id_enabled', [False, True])
@pytest.mark.parametrize('bucket_count', [-2, -1, 4])
def test_settings_derive_payload_generation_from_table_metadata(partition_enabled, row_id_enabled, bucket_count):
    header = avro_header()
    bucket_enabled = bucket_count != -1
    options = CoreOptions(Options({'data-evolution.enabled': row_id_enabled, 'bucket': bucket_count}))
    settings = Settings.from_options(options, len(FIELDS) if partition_enabled else 0)
    builder = Builder(settings, header)
    for block in range(2):
        builder.begin_block(len(header) + block * 100, 100, 1)
        builder.add(100 + block * 100, 10, partition(7 + block, 'p'), 1, 4)
        builder.end_block()
    data = builder.serialize('m', len(header) + 200, 2)
    assert struct.unpack_from('>I', data, 64 + len(header))[0] == (2 if partition_enabled else 0)
    for position in positions(data):
        assert data[position[1]] == int(partition_enabled)
        assert data[position[2]] == int(row_id_enabled)
        assert data[position[3]] == int(bucket_enabled)
    metadata = meta('m', len(header) + 200, 2)
    assert len(select(data, metadata, [Range(999, 999)], settings).blocks) == (0 if row_id_enabled else 2)
    assert len(select(data, metadata, None, settings, part(99), FIELDS).blocks) == (0 if partition_enabled else 2)

    def buckets(bucket, total):
        return bucket == 99

    assert len(select(data, metadata, None, settings, bucket_filter=buckets).blocks) == (0 if bucket_enabled else 2)
    # Generation settings do not disable payloads already stored in a sidecar.
    existing = fixture('indexWithBuckets')
    assert not select(existing, golden_meta(), [Range(999, 999)], settings).blocks
    assert not select(existing, golden_meta(), None, settings, part(99), FIELDS).blocks
    assert not select(existing, golden_meta(), None, settings, bucket_filter=buckets).blocks


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
    data = builder.serialize('manifest-golden', len(header) + 400, 7)
    assert data == fixture('indexWithPartitions')
    predicate = part(7)
    with patch.object(predicate, 'test', wraps=predicate.test) as evaluated:
        selected = select(data, golden_meta(), [Range(20, 20)], Settings(), predicate, FIELDS)
    assert evaluated.call_count == 2
    assert [b.first_record for b in selected.blocks] == [0, 5]
    nulls = SimpleNamespace(test=lambda row: row.values[1] is None)
    assert len(select(data, golden_meta(), None, Settings(), nulls, FIELDS).blocks) == 3
    assert not select(data, golden_meta(), None, Settings(), part(99), FIELDS).blocks
    assert len(select(golden(), golden_meta(), None, Settings(), part(99), FIELDS).blocks) == 3


def test_unavailable_dimensions_are_independent_and_dictionary_misses_keep_unknown_blocks():
    settings, header = Settings(max_bytes=512), avro_header()
    builder = Builder(settings, header)
    for i, (first, p) in enumerate([(None, partition(7, 'left')), (200, partition(9, 'x' * 600)),
                                   (300, partition(7, 'left'))]):
        builder.begin_block(len(header) + 100 * i, 100, 1)
        builder.add(first, 10, p)
        builder.end_block()
    data = builder.serialize('m', len(header) + 300, 3)
    metadata = meta('m', len(header) + 300, 3)
    assert [b.first_record for b in select(data, metadata, None, settings, part(9), FIELDS).blocks] == [1]
    selected = select(data, metadata, [Range(999, 999)], settings, part(7), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0]
    selected = select(data, metadata, [Range(200, 200)], settings, part(9), FIELDS)
    assert [b.first_record for b in selected.blocks] == [1]


@pytest.mark.parametrize('unknown', [False, True])
def test_coarse_row_ranges_keep_extending_bounds_and_detect_late_unknowns(unknown):
    settings, header = Settings(max_bytes=512), avro_header()
    builder = Builder(settings, header)
    builder.begin_block(len(header), 100, 66)
    for i in range(64):
        builder.add(100 + i * 1000, 10, partition(7, 'left'))
    for first, count in [(10, 10), (None if unknown else (1 << 63) - 1, 1)]:
        builder.add(first, count, partition(7, 'left'))
    builder.end_block()
    data = builder.serialize('m', len(header) + 100, 66)
    metadata = meta('m', len(header) + 100, 66)
    for point in [10, 100, 200, (1 << 63) - 1]:
        assert len(select(data, metadata, [Range(point, point)], settings).blocks) == 1
    assert len(select(data, metadata, [Range(0, 0)], settings).blocks) == int(unknown)
    assert not select(data, metadata, None, settings, part(9), FIELDS).blocks


def positions(data):
    offset = 64 + struct.unpack_from('>I', data, 60)[0]
    count, = struct.unpack_from('>I', data, offset)
    offset += 4
    for _ in range(count):
        length, = struct.unpack_from('>I', data, offset)
        offset += 4 + length
    count, = struct.unpack_from('>I', data, offset)
    offset += 4
    result = []
    for _ in range(count):
        block = offset
        offset += 24
        payloads = []
        for _ in range(3):
            payloads.append(offset)
            encoding = data[offset]
            offset += 1
            if encoding != 0:
                length, = struct.unpack_from('>I', data, offset)
                offset += 4 + length
        result.append((block, *payloads))
    return result


def checksum(data):
    data[-32:] = hashlib.sha256(data[:-32]).digest()
    return data


def test_row_miss_skips_partition_and_bucket_payloads():
    data = bytearray(fixture('indexWithBuckets'))
    _, partition, _, bucket = positions(data)[0]
    struct.pack_into('>i', data, partition + 9, -1)
    struct.pack_into('>i', data, bucket + 9, -1)
    checksum(data)
    buckets = Mock(return_value=True)
    selected = select(data, golden_meta(), [Range(15, 15)], Settings(), part(7), FIELDS, buckets)
    assert not selected.blocks
    buckets.assert_not_called()


@pytest.mark.parametrize('row_ranges', [None, [Range(0, 0)]])
def test_partition_miss_skips_bucket_matching(row_ranges):
    data = bytearray(fixture('indexWithBuckets'))
    struct.pack_into('>i', data, positions(data)[0][3] + 9, -1)
    checksum(data)
    buckets = Mock(return_value=True)
    selected = select(data, golden_meta(), row_ranges, Settings(), part(99), FIELDS, buckets)
    assert not selected.blocks
    buckets.assert_not_called()


def test_absent_partition_filter_keeps_row_and_bucket_matching():
    data = bytearray(fixture('indexWithBuckets'))
    struct.pack_into('>i', data, positions(data)[0][1] + 9, 999)
    checksum(data)
    buckets = Mock(side_effect=lambda bucket, total: bucket == 1)
    with patch('pypaimon.manifest.manifest_sidecar.GenericRowDeserializer.from_bytes') as decode_partition:
        selected = select(data, golden_meta(), [Range(20, 20)],
                          Settings(), None, FIELDS, buckets)
    assert [b.first_record for b in selected.blocks] == [0]
    decode_partition.assert_not_called()
    assert [call.args for call in buckets.call_args_list] == [(1, 4), (0, 1), (3, 4)]


def test_absent_row_or_bucket_filters_keep_remaining_dimensions():
    data = fixture('indexWithBuckets')
    buckets = Mock(side_effect=lambda bucket, total: bucket == 1)
    with patch.object(manifest_sidecar, 'LONG', wraps=manifest_sidecar.LONG) as bounds:
        selected = select(data, golden_meta(), None, Settings(), part(7), FIELDS, buckets)
    assert [b.first_record for b in selected.blocks] == [0]
    bounds.unpack_from.assert_not_called()
    assert [call.args for call in buckets.call_args_list] == [(1, 4), (2, 4), (2, 8), (0, 1), (3, 4)]
    selected = select(data, golden_meta(), [Range(20, 20)], Settings(), part(7), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0, 5]
    selected = select(data, golden_meta(), None, Settings())
    assert [b.first_record for b in selected.blocks] == [0, 3, 5]


def test_partition_and_bucket_matches_skip_unused_payload_elements():
    data = bytearray(fixture('indexWithBuckets'))
    _, partition, _, bucket = positions(data)[0]
    struct.pack_into('>i', data, partition + 13, -1)
    checksum(data)
    assert [block.first_record for block in select(
        data, golden_meta(), [Range(0, 0)], Settings(), part(7), FIELDS).blocks] == [0]
    with pytest.raises(ValueError):
        select(data, golden_meta(), [Range(0, 0)], Settings(), part(99), FIELDS)

    data = bytearray(fixture('indexWithBuckets'))
    struct.pack_into('>i', data, bucket + 17, -1)
    checksum(data)
    assert [block.first_record for block in select(
        data, golden_meta(), [Range(0, 0)], Settings(),
        bucket_filter=lambda b, t: b == 1).blocks] == [0]
    with pytest.raises(ValueError):
        select(data, golden_meta(), [Range(0, 0)], Settings(),
               bucket_filter=lambda b, t: b == 99)


def test_skipped_payloads_still_require_valid_framing_and_directory():
    good = fixture('indexWithBuckets')
    block, partition, row, bucket = positions(good)[0]
    mutations = [('>i', partition + 5, 0), ('>i', bucket + 1, -1), ('>i', row + 5, 0),
                 ('>q', positions(good)[1][0], 0), ('>q', block + 16, 2)]
    for fmt, position, value in mutations:
        data = bytearray(good)
        struct.pack_into(fmt, data, position, value)
        with pytest.raises(ValueError):
            select(checksum(data), golden_meta(), [Range(15, 15)], Settings(), part(7), FIELDS)


def test_unsigned_unknown_encodings_skip_only_one_payload_and_validate_lengths():
    good = fixture('indexWithBuckets')
    block, p, r, b = positions(good)[0]
    data = bytearray(good)
    data[p] = 200
    selected = select(checksum(data), golden_meta(), [Range(0, 0)], Settings(), part(99), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0]
    data = bytearray(good)
    data[r] = 201
    selected = select(checksum(data), golden_meta(), [Range(16, 16)], Settings(), part(7), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0]
    data = bytearray(good)
    data[b] = 202
    # Skip unknown payloads without decoding even an invalid pair count.
    struct.pack_into('>I', data, b + 5, 0)
    checksum(data)
    no_bucket = lambda bucket, total: False
    selected = select(data, golden_meta(), [Range(20, 20)], Settings(), part(7), FIELDS, no_bucket)
    assert [block.first_record for block in selected.blocks] == [0]
    assert not select(data, golden_meta(), [Range(999, 999)], Settings(), part(7), FIELDS, no_bucket).blocks
    assert not select(data, golden_meta(), [Range(20, 20)], Settings(), part(99), FIELDS, no_bucket).blocks
    for position in (p, r, b):
        data = bytearray(good)
        data[position] = 0
        with pytest.raises(ValueError):
            select(checksum(data), golden_meta(), None, Settings())
        data = bytearray(good)
        data[position] = 255
        struct.pack_into('>i', data, position + 1, -1)
        with pytest.raises(ValueError):
            select(checksum(data), golden_meta(), None, Settings())
    data = bytearray(good)
    struct.pack_into('>q', data, block + 16, 2)
    with pytest.raises(ValueError):
        select(checksum(data), golden_meta(), None, Settings())
    data = bytearray(good)
    struct.pack_into('>i', data, p + 9, 999)  # out-of-dictionary ID
    with pytest.raises(ValueError):
        select(checksum(data), golden_meta(), None, Settings(), part(7), FIELDS)
    data = bytearray(good)
    struct.pack_into('>q', data, r + 9 + 16, 9)  # overlap first interval when this interval is needed
    with pytest.raises(ValueError):
        select(checksum(data), golden_meta(), [Range(15, 15)], Settings(), part(7), FIELDS)


def test_optional_payload_exhaustion_never_truncates_the_block_directory():
    settings, header = Settings(max_bytes=250), avro_header()
    builder = Builder(settings, header)
    for i in range(3):
        builder.begin_block(len(header) + 100 * i, 100, 1)
        builder.add(100 * i, 10, partition(7, 'left'))
        builder.end_block()
    data = builder.serialize('m', len(header) + 300, 3)
    assert len(data) <= 250
    selected = select(data, meta('m', len(header) + 300, 3), [Range(999, 999)], settings, part(99), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0, 1, 2]
    assert builder.serialize('m', len(header) + 300, 3) == data
    builder.begin_block(len(header) + 300, 100, 1)
    builder.add(300, 1, partition(7, 'left'))
    builder.end_block()
    assert builder.serialize('m', len(header) + 400, 4) is None


def test_randomized_budget_degradation_has_no_false_negatives():
    rng = random.Random(9743)
    header = avro_header()
    for _ in range(60):
        settings = Settings(max_bytes=rng.choice([384, 512, 1024, 8192]))
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
        data = builder.serialize('m', len(header) + 500, 30)
        assert data is not None and len(data) <= settings.max_bytes
        metadata = meta('m', len(header) + 500, 30)
        for point in range(0, 110, 11):
            for p in range(5):
                selected = select(data, metadata, [Range(point, point)], settings, part(p), FIELDS)
                ordinals = {b.first_record for b in selected.blocks}
                for i, values in enumerate(blocks):
                    if (any(row_p == p for _, _, row_p in values)
                            and any(first is None or first <= point < first + count for first, count, _ in values)):
                        assert i * 6 in ordinals


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
    data = builder.serialize('manifest-golden', len(header) + 400, 7)
    assert data == fixture('indexWithBuckets')
    selected = select(data, golden_meta(), None, Settings(), bucket_filter=lambda bucket, total: bucket == 1)
    assert [b.first_record for b in selected.blocks] == [0]
    selected = select(data, golden_meta(), None, Settings(),
                      bucket_filter=lambda bucket, total: (bucket, total) == (2, 8))
    assert [b.first_record for b in selected.blocks] == [3]
    assert not select(data, golden_meta(), [Range(0, 0)], Settings(), part(7), FIELDS,
                      bucket_filter=lambda bucket, total: bucket == 2).blocks
    for data in [golden(), fixture('indexWithPartitions')]:
        assert len(select(data, golden_meta(), None, Settings(), bucket_filter=lambda bucket, total: False).blocks) == 3


@pytest.mark.parametrize('pair', [(None, None), (-1, 4), (4, 4), (0, 0), (2, 8)])
def test_bucket_budget_and_unknown_pairs_degrade_only_bucket_payload(pair):
    header, settings = avro_header(), Settings(max_bytes=512)
    builder = Builder(settings, header)
    extra_pairs = 65 if pair == (2, 8) else 0
    builder.begin_block(len(header), 100, 2 + extra_pairs)
    builder.add(100, 10, partition(7, 'left'), 1, 4)
    builder.add(200, 10, partition(7, 'left'), *pair)
    for i in range(extra_pairs):
        builder.add(200, 10, partition(7, 'left'), i, 100)
    builder.end_block()
    builder.begin_block(len(header) + 100, 100, 1)
    builder.add(300, 10, partition(7, 'left'), 1, 4)
    builder.end_block()
    data = builder.serialize('m', len(header) + 200, 3 + extra_pairs)
    bucket = positions(data)[0][3]
    assert data[bucket] == 0
    assert positions(data)[1][0] == bucket + 1
    metadata = meta('m', len(header) + 200, 3 + extra_pairs)
    selected = select(data, metadata, None, settings, bucket_filter=lambda bucket, total: False)
    assert [b.first_record for b in selected.blocks] == [0]
    assert not select(data, metadata, [Range(999, 999)], settings, bucket_filter=lambda bucket, total: False).blocks


def test_payloads_can_exceed_former_limits_within_byte_budget():
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
    data = builder.serialize('m', file_size, entries)
    assert len(data) <= settings.max_bytes
    metadata = meta('m', file_size, entries)
    last = (entries - 1) * 2
    selected = select(data, metadata, [Range(last, last)], settings)
    assert [b.first_record for b in selected.blocks] == [(blocks - 1) * entries_per_block]
    assert not select(data, metadata, [Range(last - 1, last - 1)], settings).blocks
    assert not select(data, metadata, None, settings, part(entries), FIELDS).blocks
    assert not select(data, metadata, None, settings,
                      bucket_filter=lambda bucket, total: bucket == entries_per_block).blocks


def test_absent_payloads_omit_length_fields_for_every_dimension_combination():
    header, settings = avro_header(), Settings()
    builder = Builder(settings, header)
    for mask in range(8):
        builder.begin_block(len(header) + mask * 100, 100, 1)
        builder.add(100 + mask if mask & 2 else None, 1,
                    partition(7, 'left') if mask & 1 else None,
                    0 if mask & 4 else None, 1 if mask & 4 else None)
        builder.end_block()
    data = builder.serialize('m', len(header) + 800, 8)
    locations = positions(data)
    for mask in range(8):
        for dimension, present_size in enumerate((13, 25, 17)):
            start = locations[mask][dimension + 1]
            end = (locations[mask][dimension + 2] if dimension < 2
                   else locations[mask + 1][0] if mask < 7 else len(data) - 32)
            present = bool(mask & (1 << dimension))
            assert data[start] == int(present)
            assert end - start == (present_size if present else 1)
    metadata = meta('m', len(header) + 800, 8)
    selected = select(data, metadata, None, settings, part(99), FIELDS)
    assert [b.first_record for b in selected.blocks] == [0, 2, 4, 6]
    selected = select(data, metadata, [Range(999, 999)], settings)
    assert [b.first_record for b in selected.blocks] == [0, 1, 4, 5]
    buckets = lambda bucket, total: False
    selected = select(data, metadata, None, settings, None, FIELDS, buckets)
    assert [b.first_record for b in selected.blocks] == [0, 1, 2, 3]
    selected = select(data, metadata, [Range(999, 999)], settings, part(99), FIELDS, buckets)
    assert [b.first_record for b in selected.blocks] == [0]


def test_malformed_bucket_payload_invalidates_the_container():
    good = fixture('indexWithBuckets')
    payload = positions(good)[0][3] + 1
    for offset, value in [(payload, -2), (payload, (1 << 31) - 1), (payload, 0), (payload + 4, 0),
                          (payload + 8, -1), (payload + 12, 1), (payload + 16, 0)]:
        bad = bytearray(good)
        struct.pack_into('>i', bad, offset, value)
        with pytest.raises(ValueError):
            select(checksum(bad), golden_meta(), None, Settings(), bucket_filter=lambda bucket, total: False)
