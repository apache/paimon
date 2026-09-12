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
from copy import deepcopy
from io import BytesIO

import fastavro
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.manifest.row_id_index import (
    Block, Builder, Selection, Settings, SUFFIX, MAX_ROW_ID, Query, select, read_index,
    read_selected_bytes, index_file_name,
)
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_file_meta import MANIFEST_FILE_META_SCHEMA
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.tests.manifest import manifest_entry_identifier_test as existing
from pypaimon.utils.range import Range


def fixture():
    path = (Path(__file__).resolve().parents[4] / 'paimon-core/src/test/resources' /
            'manifest-row-id-index-v2.txt')
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


class RowIdIndexReadTest(unittest.TestCase):
    def test_index_reads_use_bounded_bulk_requests(self):
        header = avro_header()
        for block_count in (5000, 25000):
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
                actual = read_index(file_io, '/manifest/manifest-large', meta,
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
                actual = read_index(file_io, '/manifest/manifest-golden', meta,
                                    [Range(20, 20)], settings)
                self.assertEqual(actual, select(data, meta, [Range(20, 20)], settings))
                self.assertTrue(stream.closed)

    def test_index_over_budget_stops_after_one_extra_byte(self):
        data, meta = golden(), golden_meta()
        meta.extra_files = [meta.file_name + SUFFIX]
        stream = CountingInput(data)
        file_io = SimpleNamespace(new_input_stream=lambda path: stream)
        self.assertIsNone(read_index(file_io, '/manifest/manifest-golden', meta,
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


class RowIdIndexFormatTest(unittest.TestCase):
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
        self.assertEqual(b.serialize(meta.file_name, meta.file_size, 7), golden())

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

    def test_rejected_and_early_hit_blocks_still_validate_every_interval(self):
        data = bytearray(golden())
        first_block_intervals = 68 + 4 + len(avro_header()) + 4 + 36
        struct.pack_into('>q', data, first_block_intervals + 16, 9)
        data[-32:] = hashlib.sha256(data[:-32]).digest()
        for point in (30, 0):
            with self.assertRaises(ValueError):
                select(data, golden_meta(), [Range(point, point)], Settings())

    def test_coverage_and_budgets(self):
        header = avro_header()
        for first, count in [(None, 1), (-1, 1), (10, 0), (10, -1), (MAX_ROW_ID, 2)]:
            b = Builder(Settings(), header)
            b.begin_block(len(header), 100, 1)
            b.add(first, count)
            self.assertIsNone(b.serialize('m', 1, 1))
        b = Builder(Settings(), header)
        b.begin_block(len(header), 100, 2)
        b.add(0, MAX_ROW_ID)
        b.add(MAX_ROW_ID, 1)
        b.end_block()
        self.assertLess(len(b.serialize('m', len(header) + 100, 2)), 512)
        b = Builder(Settings(max_ranges=1), header)
        b.begin_block(len(header), 100, 2)
        b.add(1, 1)
        b.add(1 << 32, 1)
        self.assertIsNone(b.serialize('m', 1, 2))
        b = Builder(Settings(max_bytes=128), header)
        self.assertIsNone(b.serialize('m', 1, 1))

    def test_invalid_envelopes(self):
        meta, data = golden_meta(), golden()
        for index in (0, 9, 11, 15, 16, 55, 63, 67, 75, len(data) - 1):
            bad = bytearray(data)
            bad[index] ^= 2
            with self.assertRaises(ValueError):
                select(bad, meta, [Range(10, 10)], Settings())
        for index in (9, 11, 15):
            bad = bytearray(data[:-32])
            bad[index] = 0
            bad.extend(hashlib.sha256(bad).digest())
            with self.assertRaises(ValueError):
                select(bad, meta, [Range(10, 10)], Settings())
        with self.assertRaises(ValueError):
            select(data[:-1], meta, [Range(10, 10)], Settings())
        meta.file_name = 'mismatch'
        with self.assertRaises(ValueError):
            select(data, meta, [Range(10, 10)], Settings())


class RowIdIndexScanTest(existing.ManifestEntryIdentifierTest):
    def setUp(self):
        super().setUp()
        self.table.options.options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_WRITE, True)
        self.table.options.options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_READ, True)

    def entry(self, name, first, count=10, kind=0):
        return ManifestEntry(kind, self._create_file_meta('unused').min_key, 0, 1,
                             replace(self._create_file_meta(name), first_row_id=first, row_count=count))

    def write_meta(self, name, entries):
        manager = self.manifest_file_manager
        return manager.write(name, entries)

    def test_explicit_reference_and_null_does_not_probe(self):
        manager = self.manifest_file_manager
        written = self.write_meta('explicit', [self.entry('data.parquet', 100)])
        self.assertEqual(index_file_name(written), written.file_name + SUFFIX)
        index_path = Path(manager.manifest_path, index_file_name(written))
        explicit_path = index_path.with_name('independent-index' + SUFFIX)
        index_path.rename(explicit_path)
        other_path = index_path.with_name('other-partition-index')
        other_path.write_bytes(b'not a row-id index')
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
        unindexed = self.write_meta('legacy-entry', [self.entry('old.parquet', None)])
        self.assertIsNone(index_file_name(unindexed))
        lists = ManifestListManager(self.table)
        lists.write('references', [indexed, unindexed])
        actual = lists.read('references')
        self.assertEqual([meta.extra_files for meta in actual], [indexed.extra_files, None])
        self.assertEqual([index_file_name(meta) for meta in actual], [index_file_name(indexed), None])
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
        self.assertTrue(all(index_file_name(meta) is None for meta in lists.read('old-list')))

    def test_skips_blocks_inside_a_matching_manifest(self):
        entries = [self.entry('file-%d.parquet' % i, i * 1000) for i in range(4000)]
        meta = self.write_meta('many-blocks', entries)
        outputs = []
        reader = fastavro.reader
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_READ, enabled)
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
            self.table.options.options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_READ, enabled)
            scanner = FileScanner(self.table, lambda: (metas, None))
            scanner.with_global_index_result(GlobalIndexResult.from_ranges([Range(50, 50)]))
            manager = scanner.manifest_file_manager
            with patch.object(manager, 'read', wraps=manager.read) as read_body, \
                    patch('pypaimon.manifest.manifest_file_manager.read_index', wraps=read_index) as read_sidecar:
                entries, _ = scanner._create_data_evolution_split_generator()
            results.append(sorted(e.file.file_name for e in entries))
            self.assertEqual(len(read_body.call_args_list), 2 if enabled else 72)
            self.assertEqual(len(read_sidecar.call_args_list), 72 if enabled else 0)
        self.assertEqual(results, [['hit.blob', 'hit.parquet']] * 2)

    def test_delete_union_no_resurrection_and_no_query_no_index_io(self):
        add = self.entry('data.parquet', 45)
        blob = self.entry('data.blob', 45)
        metas = [self.write_meta('add', [add, blob]),
                 self.write_meta('delete', [replace(add, kind=1), replace(blob, kind=1)]),
                 self.write_meta('gap', [self.entry('lo', 0), self.entry('hi', 100)])]
        manager = self.manifest_file_manager
        for enabled in (False, True):
            self.table.options.options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_READ, enabled)
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
                read_index(self.table.file_io, path, metas[0], [Range(0, 0)], Settings())

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
            self.assertIsNotNone(index_file_name(meta))
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
        manager.write('unknown', [self.entry('legacy', None)])
        self.assertFalse(Path(manager.manifest_path, 'unknown' + SUFFIX).exists())
        self.table.options.options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_RANGES, 1)
        manager.write('huge', [self.entry('one', 0, 10), self.entry('two', 100, 10)])
        self.assertFalse(Path(manager.manifest_path, 'huge' + SUFFIX).exists())
