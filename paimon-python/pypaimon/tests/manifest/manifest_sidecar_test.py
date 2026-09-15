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
import os
import struct
import unittest
from concurrent.futures import CancelledError
from copy import deepcopy
from io import BytesIO
from itertools import product

import fastavro
from pyarrow import ArrowCancelled
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.filesystem.caching_file_io import CachingFileIO
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.manifest import manifest_sidecar
from pypaimon.manifest.manifest_sidecar import (
    Block, Builder, Selection, Settings, SUFFIX, MAX_ROW_ID, Query, select, read_sidecar,
    read_selected_bytes, sidecar_file_name,
)
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.manifest_file_meta import MANIFEST_FILE_META_SCHEMA
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.read.scan_stats import ScanStats
from pypaimon.tests.manifest import manifest_entry_identifier_test as existing
from pypaimon.schema.schema import Schema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


def fixture():
    path = (Path(__file__).resolve().parents[4] / 'paimon-core/src/test/resources' /
            'manifest-sidecar.txt')
    return dict(line.split('=', 1) for line in path.read_text().splitlines()
                if line.startswith(('index=', 'avroHeader=')))


def golden():
    return base64.b64decode(fixture()['index'])


def avro_header():
    return base64.b64decode(fixture()['avroHeader'])


def golden_meta():
    return SimpleNamespace(file_name='manifest-golden', file_size=len(avro_header()) + 400,
                           num_added_files=7, num_deleted_files=0)


def intersects(data, meta, ranges, settings):
    return bool(select(data, meta, ranges, settings).blocks)


class CountingInput(BytesIO):
    def __init__(self, data, max_read=None):
        super().__init__(data)
        self.max_read = max_read
        self.reads = []
        self.requests = []
        self.seeks = []

    def read(self, size=-1):
        if size < 0:
            raise AssertionError('Unbounded read')
        self.requests.append(size)
        position = self.tell()
        data = super().read(size if self.max_read is None else min(size, self.max_read))
        if data:
            self.reads.append((position, len(data)))
        return data

    def seek(self, offset, whence=0):
        self.seeks.append(offset)
        return super().seek(offset, whence)


class FailingIndexInput(BytesIO):
    def __init__(self, data, failure, phase, close_failure=None):
        super().__init__(data)
        self.failure = failure
        self.phase = phase
        self.close_failure = close_failure

    def read(self, size=-1):
        if self.phase == 'read':
            raise self.failure
        return super().read(size)

    def close(self):
        was_closed = self.closed
        super().close()
        if not was_closed and self.close_failure is not None:
            raise self.close_failure
        if self.phase == 'close' and not was_closed:
            raise self.failure


class ManifestSidecarReadTest(unittest.TestCase):
    def test_insufficient_byte_budget_skips_sidecar_io(self):
        meta = golden_meta()
        meta.extra_files = [meta.file_name + SUFFIX]
        file_io = SimpleNamespace(new_input_stream=Mock())
        for size in (0, 1, 127):
            settings = Settings(max_bytes=size)
            self.assertIsNone(read_sidecar(file_io, '/manifest/manifest-golden', meta, None, settings))
            self.assertIsNone(manifest_sidecar.build_from_entries(b'', [], meta.file_name, settings))
        file_io.new_input_stream.assert_not_called()

    def test_local_cache_shares_sidecar_bytes_across_queries_and_readers(self):
        data, meta = golden(), golden_meta()
        meta.extra_files = ['custom' + SUFFIX]
        for disk in (False, True):
            with self.subTest(disk=disk), TemporaryDirectory() as directory:
                options = Options({'local-cache.enabled': True, 'local-cache.max-size': '1 mb',
                                   'local-cache.block-size': '128 bytes'})
                if disk:
                    options.set(CoreOptions.LOCAL_CACHE_DIR, directory)
                cache = CachingFileIO.create_cache_manager(options)
                delegate = SimpleNamespace(new_input_stream=Mock(side_effect=lambda path: BytesIO(data)),
                                           get_file_size=Mock(return_value=len(data)))
                for parent in ('/table-a', '/table-b'):
                    path = parent + '/' + meta.file_name
                    for point, expected in ((20, [0, 5]), (0, [0]), (16, [])):
                        file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
                        selected = read_sidecar(file_io, path, meta, [Range(point, point)], Settings())
                        self.assertEqual([b.first_record for b in selected.blocks], expected)
                    self.assertIsNone(read_sidecar(file_io, path, meta, [Range(20, 20)], Settings(max_bytes=128)))
                expected_calls = [call('/table-a/custom' + SUFFIX), call('/table-b/custom' + SUFFIX)]
                self.assertEqual(delegate.new_input_stream.call_args_list, expected_calls)
                self.assertEqual(delegate.get_file_size.call_args_list, expected_calls)

    def test_local_cache_respects_disable_whitelist_and_byte_budget(self):
        data, meta = golden(), golden_meta()
        meta.extra_files = ['custom' + SUFFIX]
        for overrides in ({'local-cache.enabled': False}, {'local-cache.whitelist': 'global-index'},
                          {'local-cache.max-size': '128 bytes'}):
            with self.subTest(overrides=overrides):
                options = Options({'local-cache.enabled': True, 'local-cache.max-size': '1 mb',
                                   'local-cache.block-size': '128 bytes', **overrides})
                cache = CachingFileIO.create_cache_manager(options)
                delegate = SimpleNamespace(new_input_stream=Mock(side_effect=lambda path: BytesIO(data)),
                                           get_file_size=Mock(return_value=len(data)))
                file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
                for _ in range(2):
                    selected = read_sidecar(file_io, '/table/' + meta.file_name, meta,
                                            [Range(20, 20)], Settings())
                    self.assertEqual([b.first_record for b in selected.blocks], [0, 5])
                self.assertEqual(delegate.new_input_stream.call_count, 2)

    def test_index_reads_use_bounded_bulk_requests(self):
        header = avro_header()
        for block_count in (5000, 25000, 131073):
            with self.subTest(block_count=block_count):
                builder = Builder(Settings(), header)
                for block_number in range(block_count):
                    builder.begin_block(len(header) + block_number * 100, 100, 1)
                    builder.add(block_number, 1)
                    builder.end_block()
                size = len(header) + block_count * 100
                data = builder.serialize('manifest-large', size, block_count)
                meta = SimpleNamespace(file_name='manifest-large', file_size=size,
                                       num_added_files=block_count, num_deleted_files=0,
                                       extra_files=['manifest-large' + SUFFIX])
                stream = CountingInput(data)
                file_io = SimpleNamespace(new_input_stream=lambda path: stream)
                actual = read_sidecar(file_io, '/manifest/manifest-large', meta,
                                      [Range(0, 0)], Settings())
                self.assertEqual(actual, select(data, meta, [Range(0, 0)], Settings()))
                self.assertEqual(len(stream.reads), (len(data) + (1 << 20) - 1) // (1 << 20))
                self.assertLessEqual(max(stream.requests), 1 << 20)
                self.assertTrue(stream.closed)

    def test_index_short_reads_and_exact_budget(self):
        data, meta = golden(), golden_meta()
        meta.extra_files = [meta.file_name + SUFFIX]
        for max_read in (None, 7):
            with self.subTest(max_read=max_read):
                stream = CountingInput(data, max_read)
                file_io = SimpleNamespace(new_input_stream=lambda path: stream)
                settings = Settings(max_bytes=len(data))
                actual = read_sidecar(file_io, '/manifest/manifest-golden', meta,
                                      [Range(20, 20)], settings)
                self.assertEqual(actual, select(data, meta, [Range(20, 20)], settings))
                self.assertTrue(stream.closed)

    def test_index_over_budget_stops_after_one_extra_byte(self):
        data, meta = golden(), golden_meta()
        meta.extra_files = [meta.file_name + SUFFIX]
        stream = CountingInput(data)
        file_io = SimpleNamespace(new_input_stream=lambda path: stream)
        self.assertIsNone(read_sidecar(file_io, '/manifest/manifest-golden', meta,
                                       [Range(20, 20)], Settings(max_bytes=128)))
        self.assertEqual(stream.reads, [(0, 129)])
        self.assertTrue(stream.closed)

    def test_adjacent_blocks_share_reads_without_reading_gaps(self):
        header = avro_header()
        body = bytes(range(200)) * 2
        for points, spans in [([0, 8254058425445], [(0, 300)]),
                              ([20], [(0, 100), (300, 100)]), ([16], [])]:
            with self.subTest(points=points):
                selected = select(golden(), golden_meta(),
                                  [Range(point, point) for point in points], Settings())
                stream = CountingInput(header + body)
                file_io = SimpleNamespace(new_input_stream=lambda path: stream)
                actual = read_selected_bytes(file_io, '/manifest/manifest-golden', selected)
                expected = header + b''.join(body[start:start + size] for start, size in spans)
                self.assertEqual(actual, expected)
                self.assertEqual(stream.reads, [(len(header) + start, size) for start, size in spans])
                self.assertEqual(stream.seeks, [len(header) + start for start, _ in spans])
                self.assertTrue(stream.closed)

    def test_large_block_spans_use_bounded_reads(self):
        header = avro_header()
        block_size = 512 * 1024
        body = bytes(2 * block_size + 257)
        selected = Selection(header, (Block(len(header), block_size, 0, 1),
                                      Block(len(header) + block_size, block_size, 1, 1),
                                      Block(len(header) + 2 * block_size, 257, 2, 1)))
        stream = CountingInput(header + body)
        file_io = SimpleNamespace(new_input_stream=lambda path: stream)
        self.assertEqual(read_selected_bytes(file_io, '/manifest/manifest-large', selected), header + body)
        self.assertEqual(stream.reads, [(len(header), 1 << 20), (len(header) + (1 << 20), 257)])
        self.assertEqual(stream.seeks, [len(header)])
        self.assertTrue(stream.closed)

    def test_block_short_reads_and_truncation(self):
        header = avro_header()
        body = bytes(range(200)) * 2
        selected = select(golden(), golden_meta(), [Range(0, MAX_ROW_ID)], Settings())
        stream = CountingInput(header + body, 7)
        file_io = SimpleNamespace(new_input_stream=lambda path: stream)
        self.assertEqual(read_selected_bytes(file_io, '/manifest/manifest-golden', selected), header + body)
        self.assertTrue(stream.closed)
        stream = CountingInput(header + body[:-1], 7)
        with self.assertRaises(EOFError):
            read_selected_bytes(file_io, '/manifest/manifest-golden', selected)
        self.assertTrue(stream.closed)


class ManifestSidecarFormatTest(unittest.TestCase):
    def test_settings_default_to_manifest_sort_with_independent_overrides(self):
        self.assertIsNone(CoreOptions.MANIFEST_SIDECAR_READ.default_value())
        self.assertIsNone(CoreOptions.MANIFEST_SIDECAR_WRITE.default_value())
        for sort, read, write in product((None, False, True), repeat=3):
            with self.subTest(sort=sort, read=read, write=write):
                values = {'manifest-sort.enabled': sort, 'manifest.sidecar.read': read, 'manifest.sidecar.write': write}
                options = CoreOptions(Options({key: value for key, value in values.items() if value is not None}))
                settings = Settings.from_options(options, 0)
                self.assertEqual(settings.read, bool(sort) if read is None else read)
                self.assertEqual(settings.write, bool(sort) if write is None else write)

    def test_large_avro_headers_within_byte_budget(self):
        stream = BytesIO()
        fastavro.writer(stream, 'long', [42], metadata={'large-metadata': 'x' * (1 << 20)})
        avro_bytes = stream.getvalue()
        block = next(fastavro.block_reader(BytesIO(avro_bytes)))
        header = avro_bytes[:block.offset]
        self.assertGreater(len(header), 1 << 20)
        builder = Builder(Settings(), header)
        builder.begin_block(block.offset, block.size, block.num_records)
        builder.add(42, 1)
        builder.end_block()
        meta = SimpleNamespace(file_name='large-header.avro', file_size=len(avro_bytes),
                               num_added_files=1, num_deleted_files=0)
        data = builder.serialize(meta.file_name, meta.file_size, 1)
        self.assertIsNotNone(data)
        selected = select(data, meta, [Range(42, 42)], Settings())
        self.assertEqual(selected.header, header)
        file_io = SimpleNamespace(new_input_stream=lambda path: BytesIO(avro_bytes))
        restored = read_selected_bytes(file_io, meta.file_name, selected)
        self.assertEqual(restored, avro_bytes)
        self.assertEqual(list(fastavro.reader(BytesIO(restored))), [42])
        small_budget = Settings(max_bytes=1 << 20)
        self.assertIsNone(Builder(small_budget, header).serialize(meta.file_name, meta.file_size, 1))
        with self.assertRaises(ValueError):
            select(data, meta, [Range(42, 42)], small_budget)

    def test_settings_use_memory_sizes_and_follow_the_manifest_target(self):
        options = CoreOptions(Options({}))
        self.assertEqual(Settings().max_bytes, 16 * 1024 * 1024)
        self.assertEqual(Settings.from_options(options, 0).max_bytes, 16 * 1024 * 1024)
        options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, True)
        options.options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, '12 mb')
        self.assertEqual(Settings.from_options(options, 0).max_bytes, 24 * 1024 * 1024)
        options.options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, '64 mb')
        self.assertEqual(Settings.from_options(options, 0).max_bytes, 128 * 1024 * 1024)
        for target in ('1 gb', '9223372036854775807 bytes'):
            options.options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, target)
            self.assertEqual(Settings.from_options(options, 0).max_bytes, (1 << 31) - 2)
        options.options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, '512 kb')
        options.options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, '1 gb')
        self.assertEqual(Settings.from_options(options, 0).max_bytes, 512 * 1024)
        options.options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, '2147483646 bytes')
        self.assertEqual(Settings.from_options(options, 0).max_bytes, (1 << 31) - 2)
        for value, expected in (('0 bytes', 0), ('127 bytes', 127),
                                ('2147483647 bytes', (1 << 31) - 2), ('2 gb', (1 << 31) - 2)):
            with self.subTest(value=value):
                options.options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, value)
                self.assertEqual(Settings.from_options(options, 0).max_bytes, expected)
        options.options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, '-1 bytes')
        with self.assertRaises(ValueError):
            Settings.from_options(options, 0)

    def test_disabled_sidecars_do_not_constrain_manifest_target_size(self):
        for target in ('1 bytes', '1 gb'):
            with self.subTest(target=target):
                settings = Settings.from_options(CoreOptions(Options({
                    'manifest.target-file-size': target, 'manifest-sort.enabled': True,
                    'manifest.sidecar.read': False, 'manifest.sidecar.write': False})), 0)
                self.assertFalse(settings.read)
                self.assertFalse(settings.write)

    def test_cross_language_and_block_ordinals(self):
        data, meta, header = golden(), golden_meta(), avro_header()
        for point in (0, 9, 20, 24, (1 << 32) - 2, 1 << 32, (1 << 32) + 2,
                      8254058425445, MAX_ROW_ID):
            self.assertTrue(intersects(data, meta, [Range(point, point)], Settings()))
        for point in (10, 19, 25, (1 << 32) - 3, (1 << 32) + 3, 8254058425444, MAX_ROW_ID - 1):
            self.assertFalse(intersects(data, meta, [Range(point, point)], Settings()))
        selected = select(data, meta, [Range(20, 20)], Settings())
        self.assertEqual([b.first_record for b in selected.blocks], [0, 5])
        self.assertEqual([b.offset for b in selected.blocks], [len(header), len(header) + 300])
        self.assertEqual([b.length for b in selected.blocks], [100, 100])

        gap = select(data, meta, [Range(16, 16)], Settings())

        self.assertFalse(gap.blocks)
        ranges = [Range(10, 19), Range(25, 40)]
        self.assertFalse(intersects(data, meta, ranges, Settings()))
        self.assertEqual(ranges, [Range(10, 19), Range(25, 40)])
        b = Builder(Settings(), header)
        for offset, length, values in [
                (len(header), 100, [(0, 10), (5, 5), (20, 5)]),
                (len(header) + 100, 200, [((1 << 32) - 2, 5), (8254058425445, 1)]),
                (len(header) + 300, 100, [(20, 5), (MAX_ROW_ID, 1)])]:
            b.begin_block(offset, length, len(values))
            for first, count in values:
                b.add(first, count)
            b.end_block()
        self.assertEqual(b.serialize(meta.file_name, meta.file_size, 7),
                         golden())

    def test_minmax_skips_exact_checks_and_one_interval_is_already_exact(self):
        header = avro_header()
        builder = Builder(Settings(), header)
        for offset, values in [(0, [(0, 10), (20, 10)]),
                               (100, [(100, 10), (200, 10)]),
                               (200, [(1 << 32, 10)])]:
            builder.begin_block(len(header) + offset, 100, len(values))
            for first, count in values:
                builder.add(first, count)
            builder.end_block()
        data = builder.serialize('m', len(header) + 300, 5)
        meta = SimpleNamespace(file_name='m', file_size=len(header) + 300,
                               num_added_files=5, num_deleted_files=0)
        for point, expected in [(50, 0), ((1 << 32) + 9, 1)]:
            query = Query([Range(point, point)])
            with patch.object(query, 'intersects', wraps=query.intersects) as check:
                selected = select(data, meta, query, Settings())

                self.assertEqual(len(selected.blocks), expected)
                self.assertEqual(check.call_count, 3)
                check.assert_any_call(0, 29)
                check.assert_any_call(100, 209)
                check.assert_any_call(1 << 32, (1 << 32) + 9)

    def test_single_interval_decodes_pair_once(self):
        header = avro_header()
        for first, count in [(0, 1), (42, 10), (MAX_ROW_ID, 1)]:
            builder = Builder(Settings(), header)
            builder.begin_block(len(header), 100, 1)
            builder.add(first, count)
            builder.end_block()
            data = builder.serialize('m', len(header) + 100, 1)
            meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                                   num_added_files=1, num_deleted_files=0)
            last = first + count - 1
            missing = first - 1 if first > 0 else last + 1
            for ranges, expected in [(None, 1), ([], 0), ([Range(first, first)], 1),
                                     ([Range(last, last)], 1), ([Range(missing, missing)], 0)]:
                with self.subTest(first=first, count=count, ranges=ranges), \
                        patch.object(manifest_sidecar, 'PAIR', wraps=manifest_sidecar.PAIR) as pairs, \
                        patch.object(manifest_sidecar, 'LONG', wraps=manifest_sidecar.LONG) as bounds:
                    selected = select(data, meta, ranges, Settings())
                    self.assertEqual(len(selected.blocks), expected)
                    if ranges is None:
                        pairs.unpack_from.assert_not_called()
                    else:
                        pairs.unpack_from.assert_called_once()
                    bounds.unpack_from.assert_not_called()

    def test_consumed_intervals_still_require_valid_contents(self):
        first_block_intervals = 60 + 4 + len(avro_header()) + 4 + 4 + 24 + 1 + 5 + 4
        for relative_offset, value in [(0, -1), (8, -1), (8, 30), (16, 9), (24, 19)]:
            data = bytearray(golden())
            struct.pack_into('>q', data, first_block_intervals + relative_offset, value)
            data[-32:] = hashlib.sha256(data[:-32]).digest()
            for ranges in ([Range(15, 15)],):
                with self.subTest(offset=relative_offset, value=value, ranges=ranges), \
                        self.assertRaises(ValueError):
                    select(data, golden_meta(), ranges, Settings())

    def test_row_bounds_and_matches_skip_unused_intervals(self):
        header = avro_header()
        builder = Builder(Settings(), header)
        builder.begin_block(len(header), 100, 3)
        for first in (0, 20, 40):
            builder.add(first, 10)
        builder.end_block()
        data = bytearray(builder.serialize('m', len(header) + 100, 3))
        intervals = 60 + 4 + len(header) + 4 + 4 + 24 + 1 + 5 + 4
        struct.pack_into('>q', data, intervals + 32, 19)
        data[-32:] = hashlib.sha256(data[:-32]).digest()
        meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                               num_added_files=3, num_deleted_files=0)
        for ranges, expected in [([Range(0, 0)], 1), ([Range(20, 20)], 1),
                                 ([Range(100, 100)], 0), ([], 0)]:
            selected = select(data, meta, ranges, Settings())
            self.assertEqual(len(selected.blocks), expected)
        with self.assertRaises(ValueError):
            select(data, meta, [Range(35, 35)], Settings())

    def test_coverage_and_budgets(self):
        header = avro_header()
        for first, count in [(None, 1), (-1, 1), (10, 0), (10, -1), (MAX_ROW_ID, 2)]:
            b = Builder(Settings(), header)
            b.begin_block(len(header), 100, 1)
            b.add(first, count)
            b.end_block()
            data = b.serialize('m', len(header) + 100, 1)
            meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                                   num_added_files=1, num_deleted_files=0)
            self.assertEqual(len(select(data, meta, [Range(100, 100)], Settings()).blocks), 1)
        b = Builder(Settings(), header)
        b.begin_block(len(header), 100, 2)
        b.add(0, MAX_ROW_ID)
        b.add(MAX_ROW_ID, 1)
        b.end_block()
        self.assertLess(len(b.serialize('m', len(header) + 100, 2)), 512)
        b = Builder(Settings(max_bytes=512), header)
        b.begin_block(len(header), 100, 64)
        for i in range(64):
            b.add(1 + i * 100, 1)
        b.end_block()
        data = b.serialize('m', len(header) + 100, 64)
        meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                               num_added_files=64, num_deleted_files=0)
        self.assertEqual(len(select(data, meta, [Range(10, 10)], Settings()).blocks), 1)
        b = Builder(Settings(max_bytes=128), header)
        self.assertIsNone(b.serialize('m', 1, 1))

    def test_invalid_envelopes(self):
        meta, data = golden_meta(), golden()
        for index in (0, 9, 11, 15, 16, 55, 63, 67, 75, len(data) - 1):
            bad = bytearray(data)
            bad[index] ^= 2
            with self.assertRaises(ValueError):
                select(bad, meta, [Range(10, 10)], Settings())
        for version in (0, 2, 99):
            bad = bytearray(data[:-32])
            struct.pack_into('>I', bad, 8, version)
            bad.extend(hashlib.sha256(bad).digest())
            with self.assertRaises(ValueError):
                select(bad, meta, [Range(10, 10)], Settings())
        with self.assertRaises(ValueError):
            select(data[:-1], meta, [Range(10, 10)], Settings())
        meta.file_name = 'mismatch'
        with self.assertRaises(ValueError):
            select(data, meta, [Range(10, 10)], Settings())


class ManifestSidecarScanTest(existing.ManifestEntryIdentifierTest):
    def setUp(self):
        super().setUp()
        self.table.options.options.set(CoreOptions.MANIFEST_SORT_ENABLED, True)
        self.table.options.options.set(CoreOptions.DATA_EVOLUTION_ENABLED, True)

    def entry(self, name, first, count=10, kind=0):
        return ManifestEntry(kind, self._create_file_meta('unused').min_key, 0, 1,
                             replace(self._create_file_meta(name), first_row_id=first, row_count=count))

    def write_meta(self, name, entries):
        manager = self.manifest_file_manager
        return manager.write(name, entries)

    def test_default_sidecar_budget_does_not_reject_manifest_target_sizes(self):
        for i, target in enumerate(('1 bytes', '1 gb', '9223372036854775807 bytes')):
            with self.subTest(target=target):
                self.table.options.options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, target)
                entry = self.entry('file.parquet', 100)
                meta = self.write_meta(f'budget-{i}', [entry])
                self.assertEqual(
                    [e.file.file_name for e in self.manifest_file_manager.read(meta.file_name)],
                    [entry.file.file_name])
                if target == '1 bytes':
                    self.assertIsNone(sidecar_file_name(meta))
                else:
                    self.assertIsNotNone(sidecar_file_name(meta))

    def test_payloads_follow_table_metadata(self):
        import pyarrow as pa

        for partitioned, data_evolution, bucket in product((False, True), (False, True), (-1, 4)):
            with self.subTest(partitioned=partitioned, data_evolution=data_evolution, bucket=bucket):
                name = f'default.sidecar_settings_{partitioned}_{data_evolution}_{bucket + 1}'
                schema = Schema.from_pyarrow_schema(
                    pa.schema([('id', pa.int32()), ('value', pa.string())]),
                    partition_keys=['id'] if partitioned else [],
                    options={'manifest.sidecar.write': 'true', 'manifest.sidecar.read': 'true',
                             'data-evolution.enabled': str(data_evolution).lower(), 'bucket': str(bucket)})
                self.catalog.create_table(name, schema, False)
                self.table = self.catalog.get_table(name)
                manager = ManifestFileManager(self.table)
                entry = replace(self.entry('file.parquet', 100), bucket=1, total_buckets=4,
                                partition=GenericRow([7] if partitioned else [], self.table.partition_keys_fields))
                metadata = manager.write('settings', [entry])
                with manager.file_io.new_input_stream(
                        manager.manifest_path + '/' + sidecar_file_name(metadata)) as stream:
                    data = stream.read()
                header_length, = struct.unpack_from('>I', data, 60)
                partitions, = struct.unpack_from('>I', data, 64 + header_length)
                self.assertEqual(partitions, 1 if partitioned else 0)
                selected = select(data, metadata, None, manager._sidecar_settings(),
                                  bucket_filter=lambda bucket, total: bucket == 99)
                self.assertEqual(len(selected.blocks), 1 if bucket == -1 else 0)
                self.assertEqual(
                    len(select(data, metadata, [Range(99, 99)], manager._sidecar_settings()).blocks),
                    0 if data_evolution else 1)
                self.assertEqual([e.file.file_name for e in manager.read(metadata.file_name)], ['file.parquet'])

    def test_bucket_point_lookup_with_rescale_and_delete_entries(self):
        self.table.options.options.set(CoreOptions.DATA_EVOLUTION_ENABLED, False)
        self.table.options.options.set(CoreOptions.BUCKET, 4)
        from pypaimon.common.predicate import Predicate
        from pypaimon.read.scanner.bucket_select_converter import create_bucket_selector
        selector = create_bucket_selector(Predicate('equal', 0, 'id', [7]), self.table.fields[:1])
        self.assertIsNotNone(selector)
        entries = [replace(self.entry('%s-%s-%s.parquet' % (total, bucket, i), None),
                           bucket=bucket, total_buckets=total)
                   for total in (4, 8) for bucket in range(total) for i in range(300)]
        metadata = self.write_meta('buckets', entries)
        results = []
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, enabled)
            scanner = FileScanner(self.table, lambda: ([metadata], None))
            scanner._bucket_selector = selector
            with patch('pypaimon.manifest.manifest_file_manager.read_selected_bytes',
                       wraps=read_selected_bytes) as read_blocks:
                actual = scanner.read_manifest_entries([metadata])
            results.append([e.file.file_name for e in actual])
            if enabled:
                read_blocks.assert_called_once()
                selected = read_blocks.call_args[0][2]
                self.assertLess(sum(b.record_count for b in selected.blocks), 1000)
            else:
                read_blocks.assert_not_called()
        self.assertEqual(results[0], results[1])
        self.assertEqual(len(results[1]), 600)
        chosen = next(bucket for bucket in range(4) if selector(bucket, 4))
        added = [replace(self.entry('point.' + suffix, None), bucket=chosen, total_buckets=4)
                 for suffix in ('parquet', 'blob')]
        metas = [self.write_meta('point-add', added),
                 self.write_meta('point-delete', [replace(e, kind=1) for e in added])]
        self.assertEqual(scanner.read_manifest_entries(metas), [])

    def test_partition_only_and_conjunctive_planning_keep_entry_and_delete_filters(self):
        import pyarrow as pa
        schema = Schema.from_pyarrow_schema(
            pa.schema([('p', pa.int32()), ('q', pa.string()), ('value', pa.string())]),
            partition_keys=['p', 'q'],
            options={'manifest.sidecar.write': 'true', 'manifest.sidecar.read': 'true'})
        self.catalog.create_table('default.partition_block_index', schema, False)
        self.table = self.catalog.get_table('default.partition_block_index')
        self.manifest_file_manager = ManifestFileManager(self.table)
        fields = self.table.partition_keys_fields

        def entry(name, first, p, kind=0):
            return replace(self.entry(name, first, kind=kind), partition=GenericRow([p, None], fields))

        # Build many blocks without any row tracking; partition-only planning must use the index.
        entries = [entry('part-%d.parquet' % i, None, i // 1000) for i in range(4000)]
        manifest = self.write_meta('partitioned', entries)
        from pypaimon.common.predicate import Predicate
        predicate = Predicate('equal', 0, 'p', [1])
        results = []
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, enabled)
            scanner = FileScanner(self.table, lambda: ([manifest], None), partition_predicate=predicate)
            with patch('pypaimon.manifest.manifest_file_manager.read_selected_bytes',
                       wraps=read_selected_bytes) as read_blocks:
                actual = scanner.read_manifest_entries([manifest])
            results.append([e.file.file_name for e in actual])
            if enabled:
                read_blocks.assert_called_once()
                selected = read_blocks.call_args[0][2]
                self.assertLess(sum(b.record_count for b in selected.blocks), 1500)
            else:
                read_blocks.assert_not_called()
        self.assertEqual(results[0], results[1])
        self.assertEqual(len(results[1]), 1000)

        # A block can match the two dimensions through different entries; keep entry filtering.
        mixed = self.write_meta('mixed', [entry('a.parquet', 100, 1), entry('b.parquet', 5, 2)])
        scanner = FileScanner(self.table, lambda: ([mixed], None), partition_predicate=predicate)
        self.assertEqual(scanner.read_manifest_entries([mixed], row_ranges=[Range(5, 5)]), [])
        # Both column groups and DELETE blocks must survive the same partition + row-id filter.
        add = [entry('data.parquet', 100, 1), entry('data.blob', 100, 1)]
        metas = [self.write_meta('adds', add), self.write_meta('deletes', [replace(e, kind=1) for e in add])]
        self.assertEqual(scanner.read_manifest_entries(metas, row_ranges=[Range(105, 105)]), [])

    def test_explain_keeps_complete_entry_counts_with_sidecar_enabled(self):
        import pyarrow as pa
        from pypaimon.common.predicate import Predicate

        schema = Schema.from_pyarrow_schema(
            pa.schema([('p', pa.int32()), ('q', pa.string()), ('value', pa.string())]),
            partition_keys=['p', 'q'],
            options={'manifest.sidecar.write': 'true', 'manifest.sidecar.read': 'true',
                     'data-evolution.enabled': 'true'})
        self.catalog.create_table('default.explain_sidecar', schema, False)
        self.table = self.catalog.get_table('default.explain_sidecar')
        self.manifest_file_manager = ManifestFileManager(self.table)
        fields = self.table.partition_keys_fields
        entries = [replace(self.entry('part-%d.parquet' % i, i * 1000),
                           partition=GenericRow([i // 1000, None], fields)) for i in range(4000)]
        manifest = self.write_meta('explain-manifest', entries)
        predicate = Predicate('equal', 0, 'p', [1])
        ranges = [Range(1000000, 1000000)]
        for partition_filter, row_ranges in [(predicate, None), (None, ranges), (predicate, ranges)]:
            results = []
            for enabled in (False, True):
                with self.subTest(partition=partition_filter, row_ranges=row_ranges, sidecar=enabled):
                    self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, enabled)
                    scanner = FileScanner(self.table, lambda: ([manifest], None),
                                          partition_predicate=partition_filter)
                    scanner.scan_stats = ScanStats()
                    with patch('pypaimon.manifest.manifest_file_manager.read_sidecar',
                               wraps=read_sidecar) as read_metadata:
                        actual = scanner.read_manifest_entries([manifest], row_ranges=row_ranges)
                    read_metadata.assert_not_called()
                    stats = scanner.scan_stats
                    self.assertEqual(stats.entries_potential_total, 4000)
                    self.assertEqual(stats.entries_total, 4000)
                    self.assertEqual(stats.entries_after_partition, 1000 if partition_filter else 4000)
                    self.assertEqual(stats.partition_keys_before, {(p, None) for p in range(4)})
                    results.append([entry.file.file_name for entry in actual])
            self.assertEqual(results[0], results[1])

    def test_explicit_reference_and_null_does_not_probe(self):
        manager = self.manifest_file_manager
        written = self.write_meta('explicit', [self.entry('data.parquet', 100)])
        self.assertEqual(sidecar_file_name(written), written.file_name + SUFFIX)
        index_path = Path(manager.manifest_path, sidecar_file_name(written))
        explicit_path = index_path.with_name('independent-index' + SUFFIX)
        index_path.rename(explicit_path)
        other_path = index_path.with_name('other-partition-index')
        other_path.write_bytes(b'not a manifest sidecar')
        indexed = replace(written, extra_files=[other_path.name, explicit_path.name])
        with patch.object(self.table.file_io, 'new_input_stream',
                          wraps=self.table.file_io.new_input_stream) as opened:
            self.assertEqual(manager.read_entries_parallel([indexed], row_ranges=[Range(0, 0)]), [])
        self.assertEqual([call[0][0] for call in opened.call_args_list], [str(explicit_path)])

        for extra_files in (None, [], [other_path.name]):
            with self.subTest(extra_files=extra_files):
                unindexed = replace(indexed, extra_files=extra_files)
                with patch.object(self.table.file_io, 'new_input_stream',
                                  wraps=self.table.file_io.new_input_stream) as opened:
                    actual = manager.read_entries_parallel([unindexed], row_ranges=[Range(0, 0)])
                self.assertEqual(len(actual), 1)
                self.assertEqual([call[0][0] for call in opened.call_args_list],
                                 [str(Path(manager.manifest_path, written.file_name))])
        manager.delete(indexed)
        self.assertFalse(other_path.exists())
        self.assertFalse(explicit_path.exists())
        self.assertFalse(Path(manager.manifest_path, written.file_name).exists())

    def test_manifest_list_index_reference_compatibility(self):
        indexed = self.write_meta('indexed', [self.entry('data.parquet', 100)])
        indexed = replace(indexed, extra_files=['other-index'] + indexed.extra_files)
        self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_WRITE, False)
        unindexed = self.write_meta('legacy-entry', [self.entry('old.parquet', None)])
        self.assertIsNone(sidecar_file_name(unindexed))
        lists = ManifestListManager(self.table)
        lists.write('references', [indexed, unindexed])
        actual = lists.read('references')
        self.assertEqual([meta.extra_files for meta in actual], [indexed.extra_files, None])
        self.assertEqual([sidecar_file_name(meta) for meta in actual], [sidecar_file_name(indexed), None])
        self.assertEqual([meta.file_name for meta in actual], [indexed.file_name, unindexed.file_name])

        data = Path(lists.manifest_path, 'references').read_bytes()
        legacy_schema = deepcopy(MANIFEST_FILE_META_SCHEMA)
        legacy_schema['fields'] = [field for field in legacy_schema['fields']
                                   if field['name'] != '_EXTRA_FILES']
        legacy_records = list(fastavro.reader(BytesIO(data), reader_schema=legacy_schema))
        self.assertTrue(all('_EXTRA_FILES' not in record for record in legacy_records))
        self.assertEqual([record['_FILE_NAME'] for record in legacy_records],
                         [indexed.file_name, unindexed.file_name])
        with self.table.file_io.new_output_stream(str(Path(lists.manifest_path, 'old-list'))) as stream:
            fastavro.writer(stream, legacy_schema, legacy_records)
        self.assertTrue(all(sidecar_file_name(meta) is None for meta in lists.read('old-list')))

    def test_skips_blocks_inside_a_matching_manifest(self):
        entries = [self.entry('file-%d.parquet' % i, i * 1000) for i in range(4000)]
        meta = self.write_meta('many-blocks', entries)
        outputs = []
        reader = fastavro.reader
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, enabled)
            scanner = FileScanner(self.table, lambda: ([meta], None))
            scanner.with_global_index_result(GlobalIndexResult.from_ranges([Range(2000005, 2000005)]))
            decoded = []

            def observed_reader(stream):
                for record in reader(stream):
                    decoded.append(record)
                    yield record

            with patch('pypaimon.manifest.manifest_file_manager.fastavro.reader', side_effect=observed_reader), \
                    patch('pypaimon.manifest.manifest_file_manager.read_selected_bytes',
                          wraps=read_selected_bytes) as selected_read:
                actual, _ = scanner._create_data_evolution_split_generator()
            outputs.append([e.file.file_name for e in actual])
            if enabled:
                self.assertEqual(selected_read.call_count, 1)
                selected = selected_read.call_args[0][2]
                self.assertEqual(len(selected.blocks), 1)
                self.assertLess(sum(block.length for block in selected.blocks), meta.file_size // 10)
                self.assertLess(len(decoded), 200)
                self.assertEqual(len(decoded), selected.blocks[0].record_count)
            else:
                self.assertEqual(selected_read.call_count, 0)
                self.assertEqual(len(decoded), 4000)
        self.assertEqual(outputs, [['file-2000.parquet']] * 2)

    def test_actual_global_index_scanner_72_to_2(self):
        metas = []
        for i in range(72):
            entries = [self.entry('a%d.parquet' % i, 0), self.entry('b%d.parquet' % i, 100)]
            if i < 2:
                entries.append(self.entry('hit.' + ('parquet' if i == 0 else 'blob'), 45))
            metas.append(self.write_meta('manifest-%d' % i, entries))
        results = []
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, enabled)
            scanner = FileScanner(self.table, lambda: (metas, None))
            scanner.with_global_index_result(GlobalIndexResult.from_ranges([Range(50, 50)]))
            manager = scanner.manifest_file_manager
            with patch.object(manager, 'read', wraps=manager.read) as read_body, \
                    patch('pypaimon.manifest.manifest_file_manager.read_sidecar', wraps=read_sidecar) as read_metadata:
                entries, _ = scanner._create_data_evolution_split_generator()
            results.append(sorted(e.file.file_name for e in entries))
            self.assertEqual(len(read_body.call_args_list), 2 if enabled else 72)
            self.assertEqual(len(read_metadata.call_args_list), 72 if enabled else 0)
        self.assertEqual(results, [['hit.blob', 'hit.parquet']] * 2)

    def test_delete_union_no_resurrection_and_no_query_no_index_io(self):
        add = self.entry('data.parquet', 45)
        blob = self.entry('data.blob', 45)
        metas = [self.write_meta('add', [add, blob]),
                 self.write_meta('delete', [replace(add, kind=1), replace(blob, kind=1)]),
                 self.write_meta('gap', [self.entry('lo', 0), self.entry('hi', 100)])]
        manager = self.manifest_file_manager
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_READ, enabled)
            with patch.object(manager, 'read', wraps=manager.read) as read_body:
                entries = manager.read_entries_parallel(metas[:2], row_ranges=[Range(50, 50)])
            self.assertEqual(entries, [])
            self.assertEqual(len(read_body.call_args_list), 2)
        with patch.object(self.table.file_io, 'new_input_stream',
                          wraps=self.table.file_io.new_input_stream) as opened:
            manager.read_entries_parallel(metas)
        self.assertTrue(all(not call[0][0].endswith(SUFFIX) for call in opened.call_args_list))
        # Missing and corrupt objects retain their manifests and read the full body.
        path = manager.manifest_path + '/gap' + SUFFIX
        for bad in (None, b'partial'):
            if bad is None:
                os.unlink(path)
            else:
                Path(path).write_bytes(bad)
            with patch.object(manager, 'read', wraps=manager.read) as read_body:
                entries = manager.read_entries_parallel(metas[2:], row_ranges=[Range(50, 50)])
            self.assertEqual(len(entries), 2)
            self.assertEqual(read_body.call_count, 1)
            self.assertIsNone(read_body.call_args[1]['selected_blocks'])
        with patch.object(self.table.file_io, 'new_input_stream', side_effect=InterruptedError('stop')):
            with self.assertRaises(InterruptedError):
                read_sidecar(self.table.file_io, path, metas[0], [Range(0, 0)], Settings())

    def test_sidecar_cancellation_during_open(self):
        self._check_sidecar_cancellation('open')

    def test_sidecar_cancellation_during_read(self):
        self._check_sidecar_cancellation('read')

    def test_sidecar_cancellation_during_close(self):
        self._check_sidecar_cancellation('close')

    def _check_sidecar_cancellation(self, phase):
        for failure_type in (ArrowCancelled, CancelledError, InterruptedError):
            with self.subTest(phase=phase, failure_type=failure_type):
                self._check_sidecar_io_failure(phase, failure_type('cancelled'), cancelled=True)

    def test_sidecar_io_failures_fall_back_to_manifest(self):
        for phase in ('open', 'read', 'close'):
            for failure_type in (FileNotFoundError, TimeoutError, OSError):
                with self.subTest(phase=phase, failure_type=failure_type):
                    self._check_sidecar_io_failure(phase, failure_type('unavailable'), cancelled=False)

    def test_sidecar_cancellation_survives_close_failure(self):
        for failure_type in (ArrowCancelled, CancelledError, InterruptedError):
            with self.subTest(failure_type=failure_type):
                self._check_sidecar_io_failure('read', failure_type('cancelled'), cancelled=True,
                                               close_failure=OSError('close failed'))

    def test_sidecar_wrapped_cancellation_propagates(self):
        for failure_type in (ArrowCancelled, CancelledError, InterruptedError):
            with self.subTest(failure_type=failure_type):
                cancellation = failure_type('cancelled')
                wrapped = OSError('wrapped failure')
                wrapped.__cause__ = cancellation
                self._check_sidecar_io_failure('open', wrapped, cancelled=True,
                                               expected_failure=cancellation)

    def test_sidecar_exception_cycle_falls_back(self):
        first = OSError('first')
        second = OSError('second')
        first.__cause__ = second
        second.__cause__ = first
        self._check_sidecar_io_failure('open', first, cancelled=False)

    def _check_sidecar_io_failure(self, phase, failure, cancelled, close_failure=None,
                                  expected_failure=None):
        manager = self.manifest_file_manager
        meta = self.write_meta('failure-' + phase + '-' + type(failure).__name__,
                               [self.entry('data.parquet', 100)])
        index_path = str(Path(manager.manifest_path, sidecar_file_name(meta)))
        body_path = str(Path(manager.manifest_path, meta.file_name))
        stream = (FailingIndexInput(Path(index_path).read_bytes(), failure, phase, close_failure)
                  if phase != 'open' else None)
        original_open = self.table.file_io.new_input_stream

        def open_stream(path):
            if path == index_path:
                if phase == 'open':
                    raise failure
                return stream
            return original_open(path)

        with patch.object(self.table.file_io, 'new_input_stream', side_effect=open_stream) as opened, \
                patch.object(manager, 'read', wraps=manager.read) as read_body:
            if cancelled:
                expected = failure if expected_failure is None else expected_failure
                with self.assertRaises(type(expected)) as raised:
                    manager.read_entries_parallel([meta], row_ranges=[Range(100, 100)])
                self.assertIs(raised.exception, expected)
                read_body.assert_not_called()
                self.assertEqual([call.args[0] for call in opened.call_args_list], [index_path])
            else:
                entries = manager.read_entries_parallel([meta], row_ranges=[Range(100, 100)])
                self.assertEqual([entry.file.file_name for entry in entries], ['data.parquet'])
                read_body.assert_called_once()
                self.assertIsNone(read_body.call_args.kwargs['selected_blocks'])
                self.assertEqual([call.args[0] for call in opened.call_args_list], [index_path, body_path])
        if stream is not None:
            self.assertTrue(stream.closed)

    def test_rolling_merge_limits_and_abort_cleanup(self):
        entries = [self.entry('file-%d' % i, i * 1000) for i in range(300)]
        manager = self.manifest_file_manager
        metas = manager.rolling_write(entries, 300, 'rolling')
        self.assertGreater(len(metas), 1)
        for meta in metas:
            actual = manager.read(meta.file_name)
            data = Path(manager.manifest_path, meta.file_name + SUFFIX).read_bytes()
            for e in actual:
                self.assertTrue(intersects(data, meta, [Range(e.file.first_row_id, e.file.first_row_id)], Settings()))
            gap = actual[0].file.first_row_id + 10
            self.assertFalse(intersects(data, meta, [Range(gap, gap)], Settings()))
        from pypaimon.manifest.manifest_file_merger import ManifestFileMerger
        merger = ManifestFileMerger(manager, 1000000, 2)
        merged = merger.merge(metas)
        # Merger returns both the final manifest list and newly written outputs.
        outputs = merged[0] if isinstance(merged, tuple) else merged
        for meta in outputs:
            self.assertTrue(Path(manager.manifest_path, meta.file_name + SUFFIX).exists())
        for meta in metas:
            self.assertIsNotNone(sidecar_file_name(meta))
            manager.delete(meta)
            self.assertFalse(Path(manager.manifest_path, meta.file_name + SUFFIX).exists())
        original = self.table.file_io.new_output_stream

        def fail(path):
            if path.endswith(SUFFIX):
                raise OSError('sidecar write failed')
            return original(path)
        with patch.object(self.table.file_io, 'new_output_stream', side_effect=fail):
            with self.assertRaises(RuntimeError):
                manager.write('failed', entries[:1])
        self.assertFalse(Path(manager.manifest_path, 'failed').exists())
        self.assertFalse(Path(manager.manifest_path, 'failed' + SUFFIX).exists())
        unknown = manager.write('unknown', [self.entry('legacy', None)])
        self.assertIsNotNone(sidecar_file_name(unknown))
        data = Path(manager.manifest_path, sidecar_file_name(unknown)).read_bytes()
        self.assertEqual(len(select(data, unknown, [Range(100, 100)], Settings()).blocks), 1)
        huge = manager.write('huge', [self.entry('one', 0, MAX_ROW_ID),
                                      self.entry('two', MAX_ROW_ID, 1)])
        self.assertIsNotNone(sidecar_file_name(huge))
        data = Path(manager.manifest_path, sidecar_file_name(huge)).read_bytes()
        self.assertEqual(len(select(data, huge, [Range(50, 50)], Settings()).blocks), 1)
