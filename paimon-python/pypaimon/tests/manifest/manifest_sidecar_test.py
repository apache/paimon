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

import os
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


def golden():
    return make_sidecar()


def avro_header():
    return b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\x0c"long"\x00' + bytes(16)


def make_sidecar(partitions=None, buckets=False):
    header = avro_header()
    builder = Builder(Settings(), header)
    blocks = [(0, 100, [(0, 10, 0, 1, 4), (5, 5, 0, 1, 4), (20, 5, 1, 1, 8)]),
              (100, 200, [((1 << 32) - 2, 5, 1, 2, 4), (8254058425445, 1, 0, 2, 8)]),
              (300, 100, [(20, 5, 0, 0, 1), (MAX_ROW_ID, 1, 1, 3, 4)])]
    for offset, length, entries in blocks:
        builder.begin_block(len(header) + offset, length, len(entries))
        for first, count, p, bucket, total in entries:
            builder.add(first, count, partitions[p] if partitions is not None else None,
                        bucket if buckets else None, total if buckets else None)
        builder.end_block()
    return builder.serialize(len(header) + 400, 7)


def golden_meta():
    return SimpleNamespace(file_name='manifest-golden', file_size=len(avro_header()) + 400,
                           num_added_files=7, num_deleted_files=0)


def intersects(data, meta, ranges, settings):
    return bool(select(data, meta, ranges).blocks)


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
                        selected = read_sidecar(file_io, path, meta, [Range(point, point)])
                        self.assertEqual([b.first_record for b in selected.blocks], expected)
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
                                            [Range(20, 20)])
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
                data = builder.serialize(size, block_count)
                meta = SimpleNamespace(file_name='manifest-large', file_size=size,
                                       num_added_files=block_count, num_deleted_files=0,
                                       extra_files=['manifest-large' + SUFFIX])
                stream = CountingInput(data)
                file_io = SimpleNamespace(new_input_stream=lambda path: stream)
                actual = read_sidecar(file_io, '/manifest/manifest-large', meta,
                                      [Range(0, 0)])
                self.assertEqual(actual, select(data, meta, [Range(0, 0)]))
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
                actual = read_sidecar(file_io, '/manifest/manifest-golden', meta,
                                      [Range(20, 20)])
                self.assertEqual(actual, select(data, meta, [Range(20, 20)]))
                self.assertTrue(stream.closed)

    def test_adjacent_blocks_share_reads_without_reading_gaps(self):
        header = avro_header()
        body = bytes(range(200)) * 2
        for points, spans in [([0, 8254058425445], [(0, 300)]),
                              ([20], [(0, 100), (300, 100)]), ([16], [])]:
            with self.subTest(points=points):
                selected = select(golden(), golden_meta(),
                                  [Range(point, point) for point in points])
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
        selected = select(golden(), golden_meta(), [Range(0, MAX_ROW_ID)])
        stream = CountingInput(header + body, 7)
        file_io = SimpleNamespace(new_input_stream=lambda path: stream)
        self.assertEqual(read_selected_bytes(file_io, '/manifest/manifest-golden', selected), header + body)
        self.assertTrue(stream.closed)
        stream = CountingInput(header + body[:-1], 7)
        with self.assertRaises(EOFError):
            read_selected_bytes(file_io, '/manifest/manifest-golden', selected)
        self.assertTrue(stream.closed)


class ManifestSidecarFormatTest(unittest.TestCase):

    def test_settings_default_to_manifest_sort_with_explicit_override(self):
        self.assertIsNone(CoreOptions.MANIFEST_SIDECAR_ENABLED.default_value())
        for sort, enabled in product((None, False, True), repeat=2):
            with self.subTest(sort=sort, enabled=enabled):
                values = {'manifest-sort.enabled': sort, 'manifest.sidecar.enabled': enabled}
                options = CoreOptions(Options({key: value for key, value in values.items() if value is not None}))
                settings = Settings.from_options(options)
                self.assertEqual(settings.enabled, bool(sort) if enabled is None else enabled)

    def test_large_avro_headers(self):
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
        data = builder.serialize(meta.file_size, 1)
        self.assertIsNotNone(data)
        selected = select(data, meta, [Range(42, 42)])
        self.assertEqual(selected.header, header)
        file_io = SimpleNamespace(new_input_stream=lambda path: BytesIO(avro_bytes))
        restored = read_selected_bytes(file_io, meta.file_name, selected)
        self.assertEqual(restored, avro_bytes)
        self.assertEqual(list(fastavro.reader(BytesIO(restored))), [42])

    def test_disabled_option_is_independent_of_manifest_target_size(self):
        for target in ('1 bytes', '1 gb'):
            with self.subTest(target=target):
                settings = Settings.from_options(CoreOptions(Options({
                    'manifest.target-file-size': target, 'manifest-sort.enabled': True,
                    'manifest.sidecar.enabled': False})))
                self.assertFalse(settings.enabled)

    def test_row_id_coverage_and_block_ordinals(self):
        data, meta, header = golden(), golden_meta(), avro_header()
        for point in (0, 9, 20, 24, (1 << 32) - 2, 1 << 32, (1 << 32) + 2,
                      8254058425445, MAX_ROW_ID):
            self.assertTrue(intersects(data, meta, [Range(point, point)], Settings()))
        for point in (10, 19, 25, (1 << 32) - 3, (1 << 32) + 3, 8254058425444, MAX_ROW_ID - 1):
            self.assertFalse(intersects(data, meta, [Range(point, point)], Settings()))
        selected = select(data, meta, [Range(20, 20)])
        self.assertEqual([b.first_record for b in selected.blocks], [0, 5])
        self.assertEqual([b.offset for b in selected.blocks], [len(header), len(header) + 300])
        self.assertEqual([b.length for b in selected.blocks], [100, 100])

        gap = select(data, meta, [Range(16, 16)])

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
        self.assertEqual(b.serialize(meta.file_size, 7),
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
        data = builder.serialize(len(header) + 300, 5)
        meta = SimpleNamespace(file_name='m', file_size=len(header) + 300,
                               num_added_files=5, num_deleted_files=0)
        for point, expected in [(50, 0), ((1 << 32) + 9, 1)]:
            query = Query([Range(point, point)])
            with patch.object(query, 'intersects', wraps=query.intersects) as check:
                selected = select(data, meta, query)

                self.assertEqual(len(selected.blocks), expected)
                self.assertEqual(check.call_count, 3)
                check.assert_any_call(0, 29)
                check.assert_any_call(100, 209)
                check.assert_any_call(1 << 32, (1 << 32) + 9)

    def test_single_interval_needs_no_delta_decoding(self):
        header = avro_header()
        for first, count in [(0, 1), (42, 10), (MAX_ROW_ID, 1)]:
            builder = Builder(Settings(), header)
            builder.begin_block(len(header), 100, 1)
            builder.add(first, count)
            builder.end_block()
            data = builder.serialize(len(header) + 100, 1)
            meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                                   num_added_files=1, num_deleted_files=0)
            last = first + count - 1
            missing = first - 1 if first > 0 else last + 1
            for ranges, expected in [(None, 1), ([], 0), ([Range(first, first)], 1),
                                     ([Range(last, last)], 1), ([Range(missing, missing)], 0)]:
                with patch.object(manifest_sidecar._Deltas, 'next', side_effect=AssertionError('No deltas')) as decode:
                    self.assertEqual(len(select(data, meta, ranges).blocks), expected)
                    decode.assert_not_called()

    def test_consumed_intervals_still_require_valid_contents(self):
        from pypaimon.tests.manifest.manifest_block_index_test import replace_payload, row_payload
        for minimum, maximum, deltas in [(-1, 24, [10, 11]), (30, 24, [0, 1]), (0, 24, [9, 0]),
                                         (0, 24, [9, 100])]:
            data = replace_payload(golden(), 0, 2, row_payload(minimum, maximum, deltas))
            with self.assertRaises(ValueError):
                select(data, golden_meta(), [Range(15, 15)])

    def test_row_bounds_and_matches_skip_unused_intervals(self):
        from pypaimon.tests.manifest.manifest_block_index_test import replace_payload, row_payload
        data = replace_payload(golden(), 0, 2, row_payload(0, 24, [9, 0]))
        self.assertEqual(len(select(data, golden_meta(), [Range(0, 0)]).blocks), 1)
        self.assertFalse(select(data, golden_meta(), [Range(100, 100)]).blocks)
        self.assertFalse(select(data, golden_meta(), []).blocks)
        with self.assertRaises(ValueError):
            select(data, golden_meta(), [Range(15, 15)])

    def test_unknown_coverage_and_exact_gaps(self):
        header = avro_header()
        for first, count in [(None, 1), (-1, 1), (10, 0), (10, -1), (MAX_ROW_ID, 2)]:
            b = Builder(Settings(), header)
            b.begin_block(len(header), 100, 1)
            b.add(first, count)
            b.end_block()
            data = b.serialize(len(header) + 100, 1)
            meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                                   num_added_files=1, num_deleted_files=0)
            self.assertEqual(len(select(data, meta, [Range(100, 100)]).blocks), 1)
        b = Builder(Settings(), header)
        b.begin_block(len(header), 100, 2)
        b.add(0, MAX_ROW_ID)
        b.add(MAX_ROW_ID, 1)
        b.end_block()
        self.assertLess(len(b.serialize(len(header) + 100, 2)), 512)
        b = Builder(Settings(), header)
        b.begin_block(len(header), 100, 64)
        for i in range(64):
            b.add(1 + i * 100, 1)
        b.end_block()
        data = b.serialize(len(header) + 100, 64)
        meta = SimpleNamespace(file_name='m', file_size=len(header) + 100,
                               num_added_files=64, num_deleted_files=0)
        self.assertEqual(len(select(data, meta, [Range(10, 10)]).blocks), 0)

    def test_invalid_envelopes(self):
        from pypaimon.tests.manifest.manifest_block_index_test import checksum
        meta, data = golden_meta(), golden()
        for index in (0, 9, 11, 15, 16, 55, 63, 67, 75, len(data) - 1):
            bad = bytearray(data)
            bad[index] ^= 2
            with self.assertRaises(ValueError):
                select(bad, meta, [Range(10, 10)])
        for version in (0, 2, 99):
            bad = bytearray(data)
            bad[4] = version
            with self.assertRaises(ValueError):
                select(checksum(bad), meta, [Range(10, 10)])
        for length in range(len(data)):
            with self.assertRaises(ValueError):
                select(data[:length], meta, [Range(10, 10)])
        meta.file_name = 'renamed'
        self.assertEqual(len(select(data, meta, [Range(20, 20)]).blocks), 2)
        meta.file_size += 1
        with self.assertRaises(ValueError):
            select(data, meta, [Range(10, 10)])


class ManifestSidecarScanTest(existing.ManifestEntryIdentifierTest):

    def setUp(self):
        super().setUp()
        self.table.options.options.set(CoreOptions.MANIFEST_SORT_ENABLED, True)
        self.table.options.options.set(CoreOptions.DATA_EVOLUTION_ENABLED, True)

    def entry(self, name, first, count=10, kind=0):
        return ManifestEntry(kind, self._create_file_meta('unused').min_key, 0, 1,
                             replace(self._create_file_meta(name), first_row_id=first, row_count=count))

    def test_enabling_sidecar_reads_does_not_change_python_writes(self):
        manager = self.manifest_file_manager
        entries = [self.entry('data.parquet', 100)]
        self.assertTrue(self.table.options.manifest_sidecar_enabled())
        self.assertIsNone(manager.write('plain-writer', entries))
        self.assertFalse(Path(manager.manifest_path, 'plain-writer' + SUFFIX).exists())
        for meta in manager.rolling_write(entries, 1, 'rolling-writer'):
            self.assertIsNone(meta.extra_files)
            self.assertFalse(Path(manager.manifest_path, meta.file_name + SUFFIX).exists())

    def write_meta(self, name, entries):
        # Emulate an externally published sidecar; production Python writes are unchanged.
        manager = self.manifest_file_manager
        manager.write(name, entries)
        path = Path(manager.manifest_path, name)
        avro_bytes = path.read_bytes()
        meta = manager._build_meta(name, entries, len(avro_bytes))
        settings = Settings.from_options(self.table.options)
        if settings.enabled:
            data = manifest_sidecar.build_from_entries(avro_bytes, entries, settings)
            sidecar_path = path.with_name(name + SUFFIX)
            sidecar_path.write_bytes(data)
            meta = replace(meta, extra_files=[sidecar_path.name])
        return meta

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
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled)
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
            options={'manifest.sidecar.enabled': 'true'})
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
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled)
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
            options={'manifest.sidecar.enabled': 'true',
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
                    self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled)
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

    def test_manifest_list_index_reference_compatibility(self):
        indexed = self.write_meta('indexed', [self.entry('data.parquet', 100)])
        indexed = replace(indexed, extra_files=['other-index'] + indexed.extra_files)
        self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, False)
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
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled)
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
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled)
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
            self.table.options.options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled)
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
                read_sidecar(self.table.file_io, path, metas[0], [Range(0, 0)])

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
                self.assertEqual([call[0][0] for call in opened.call_args_list], [index_path])
            else:
                entries = manager.read_entries_parallel([meta], row_ranges=[Range(100, 100)])
                self.assertEqual([entry.file.file_name for entry in entries], ['data.parquet'])
                read_body.assert_called_once()
                self.assertIsNone(read_body.call_args[1]['selected_blocks'])
                self.assertEqual([call[0][0] for call in opened.call_args_list], [index_path, body_path])
        if stream is not None:
            self.assertTrue(stream.closed)
