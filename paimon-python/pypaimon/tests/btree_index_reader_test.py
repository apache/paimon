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

"""File-level tests for BTree reader versions and initialization cleanup."""

import io
import json
import os
import struct
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.btree.btree_file_footer import BTreeFileFooter
from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.btree.btree_index_writer import BTreeIndexWriter
from pypaimon.globalindex.btree.lazy_filtered_btree_reader import LazyFilteredBTreeReader
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import FieldRef
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.sorted_file_meta_selector import SortedFileMetaSelector
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
from pypaimon.schema.data_types import AtomicType


class _TrackingInput(io.BytesIO):

    def __init__(self, data, read_error=None, close_error=None):
        super().__init__(data)
        self.read_ranges = []
        self.read_error = read_error
        self.close_error = close_error

    def read(self, size=-1):
        self.read_ranges.append((self.tell(), size))
        if self.read_error is not None:
            raise self.read_error
        return super().read(size)

    def close(self):
        super().close()
        if self.close_error is not None:
            raise self.close_error


class _PreadInput(_TrackingInput):

    def read_at(self, size, offset):
        self.read_ranges.append((offset, size))
        if self.read_error is not None:
            raise self.read_error
        return self.getvalue()[offset:offset + size]


class _MemoryFileIO:

    def __init__(self, data, input_type, read_error=None, close_error=None):
        self.input_stream = input_type(data, read_error, close_error)

    def new_input_stream(self, path):
        return self.input_stream


class BTreeIndexReaderTest(unittest.TestCase):

    def setUp(self):
        self.serializer = create_serializer(AtomicType('INT'))
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO()
            writer = BTreeIndexWriter(
                file_io, directory, self.serializer, block_size=32)
            for key, row_id in (
                (10, 7), (20, 10), (20, 30), (20, 60),
                (30, 100), (30, 299), (None, 400),
            ):
                writer.write(key, row_id)
            entry = writer.finish()[0]
            with open(os.path.join(directory, entry.file_name), 'rb') as stream:
                self.data = stream.read()
            self.io_meta = GlobalIndexIOMeta(
                entry.file_name, len(self.data), entry.meta)

    def test_version_1_queries_and_close(self):
        for input_type in (_TrackingInput, _PreadInput):
            with self.subTest(input_type=input_type):
                file_io = self._file_io(self.data, input_type)
                reader = self._reader(file_io)
                try:
                    self.assertEqual(1, reader.footer.version)
                    self.assertEqual([7], reader.visit_equal(10).results().to_list())
                    self.assertEqual([10, 30, 60], reader.visit_equal(20).results().to_list())
                    self.assertEqual([], reader.visit_equal(25).results().to_list())
                    self.assertEqual(
                        [7, 10, 30, 60], reader.visit_between(10, 20).results().to_list())
                    self.assertEqual(
                        [10, 30, 60, 100, 299], reader.visit_between(20, 30).results().to_list())
                    self.assertEqual([7], reader.visit_less_than(20).results().to_list())
                    self.assertEqual([100, 299], reader.visit_greater_than(20).results().to_list())
                    self.assertEqual([400], reader.visit_is_null().results().to_list())
                    self.assertFalse(file_io.input_stream.closed)
                finally:
                    reader.close()
                self.assertTrue(file_io.input_stream.closed)

    def test_string_prefix_intervals_are_exact(self):
        values = [None, '', '\x00', 'ke', 'keep', 'keep', 'keeper', 'kf', 'missing-after',
                  '\x7f', '\x7fa', '\u0080', '\u00ff', '\u00ffa', '\u0100',
                  '\u07ff', '\u07ffa', '\u0800', '\uffff', '\uffffa', '\U00010000',
                  '\U0010ffff', '\U0010ffffa']
        serializer = create_serializer(AtomicType('STRING'))
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO()
            writer = BTreeIndexWriter(file_io, directory, serializer, block_size=32)
            for row_id, value in sorted(enumerate(values), key=lambda item: (item[1] is not None, item[1])):
                writer.write(value, row_id)
            entry = writer.finish()[0]
            with open(os.path.join(directory, entry.file_name), 'rb') as stream:
                data = stream.read()
        io_meta = GlobalIndexIOMeta(entry.file_name, len(data), entry.meta)
        for input_type in (_TrackingInput, _PreadInput):
            reader = BTreeIndexReader(serializer, self._file_io(data, input_type), '/unused', io_meta)
            try:
                for prefix in ('', '\x00', 'ke', 'keep', 'missing', 'absent', 'zz',
                               '\x7f', '\u00ff', '\u07ff', '\uffff', '\U0010ffff'):
                    with self.subTest(input_type=input_type, prefix=prefix):
                        result = reader.visit_starts_with(prefix)
                        self.assertTrue(result.is_exact())
                        self.assertEqual(
                            [row_id for row_id, value in enumerate(values)
                             if value is not None and value.startswith(prefix)],
                            result.results().to_list())
            finally:
                reader.close()

    def test_prefix_query_on_null_only_file_is_exact_and_empty(self):
        serializer = create_serializer(AtomicType('STRING'))
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO()
            writer = BTreeIndexWriter(file_io, directory, serializer, block_size=32)
            writer.write(None, 0)
            writer.write(None, 1)
            entry = writer.finish()[0]
            file_size = os.path.getsize(os.path.join(directory, entry.file_name))
            io_meta = GlobalIndexIOMeta(entry.file_name, file_size, entry.meta)
            reader = BTreeIndexReader(serializer, file_io, directory, io_meta)
            try:
                for prefix in ('', 'ke'):
                    with self.subTest(prefix=prefix):
                        result = reader.visit_starts_with(prefix)
                        self.assertTrue(result.is_exact())
                        self.assertTrue(result.results().is_empty())
            finally:
                reader.close()

    def test_string_prefix_file_selection_uses_utf8_byte_bounds(self):
        values = ['', '\x00', 'ke', 'kf', '\x7f', '\x7fa', '\u0080', '\u00ff', '\u00ffa', '\u0100',
                  '\u07ff', '\u07ffa', '\u0800', '\uffff', '\uffffa', '\U00010000', '\U0010ffff', None]
        serializer = create_serializer(AtomicType('STRING'))
        files = []
        for row_id, value in enumerate(values):
            key = None if value is None else serializer.serialize(value)
            meta = SortedIndexFileMeta(key, key, value is None)
            files.append((GlobalIndexIOMeta(str(row_id), 0, meta.serialize()), meta))
        selector = SortedFileMetaSelector(files, serializer)
        for prefix in ('', '\x00', 'ke', 'keep', '\x7f', '\u00ff', '\u07ff', '\uffff', '\U0010ffff'):
            with self.subTest(prefix=prefix):
                self.assertEqual(
                    [str(row_id) for row_id, value in enumerate(values)
                     if value is not None and value.startswith(prefix)],
                    [file.file_name for file in selector.select_starts_with(prefix)])

    def test_unsupported_versions_rejected_before_sst_read(self):
        for input_type in (_TrackingInput, _PreadInput):
            for version in (0, 3, 0x80000000, 0xffffffff):
                with self.subTest(input_type=input_type, version=version):
                    data = self.data[:-8] + struct.pack('<I', version) + self.data[-4:]
                    file_io = self._file_io(data, input_type)
                    with self.assertRaisesRegex(
                        ValueError, 'Unsupported BTree index file version: %s' % version,
                    ):
                        self._reader(file_io)
                    self.assertTrue(file_io.input_stream.closed)
                    self.assertEqual(
                        [(len(data) - BTreeFileFooter.ENCODED_LENGTH,
                          BTreeFileFooter.ENCODED_LENGTH)],
                        file_io.input_stream.read_ranges,
                    )

    def test_invalid_footer_closes_stream(self):
        bad_magic = self.data[:-4] + struct.pack('<I', 0)
        for input_type in (_TrackingInput, _PreadInput):
            for data, error in ((bad_magic, ValueError), (self.data[:-4], struct.error)):
                with self.subTest(input_type=input_type, error=error):
                    file_io = self._file_io(data, input_type)
                    with self.assertRaises(error):
                        self._reader(file_io)
                    self.assertTrue(file_io.input_stream.closed)

    def test_invalid_index_block_closes_stream(self):
        footer = BTreeFileFooter.read_footer(self.data[-BTreeFileFooter.ENCODED_LENGTH:])
        handle = footer.index_block_handle
        data = bytearray(self.data)
        data[handle.offset + handle.size + 1] ^= 1
        for input_type in (_TrackingInput, _PreadInput):
            with self.subTest(input_type=input_type):
                file_io = self._file_io(bytes(data), input_type)
                with self.assertRaisesRegex(ValueError, 'CRC32 mismatch'):
                    self._reader(file_io)
                self.assertTrue(file_io.input_stream.closed)

    def test_read_failure_closes_stream(self):
        for input_type in (_TrackingInput, _PreadInput):
            for error in (OSError('Footer read failed'), KeyboardInterrupt()):
                with self.subTest(input_type=input_type, error=type(error)):
                    file_io = self._file_io(self.data, input_type, read_error=error)
                    with self.assertRaises(type(error)) as raised:
                        self._reader(file_io)
                    self.assertIs(error, raised.exception)
                    self.assertTrue(file_io.input_stream.closed)

    def test_close_failure_preserves_initialization_error(self):
        for input_type in (_TrackingInput, _PreadInput):
            with self.subTest(input_type=input_type):
                error = OSError('Footer read failed')
                file_io = self._file_io(
                    self.data, input_type, read_error=error,
                    close_error=OSError('Stream close failed'))
                with self.assertRaises(OSError) as raised:
                    self._reader(file_io)
                self.assertIs(error, raised.exception)
                self.assertTrue(file_io.input_stream.closed)

    def _file_io(self, data, input_type, **kwargs):
        file_io = _MemoryFileIO(data, input_type, **kwargs)
        self.addCleanup(io.BytesIO.close, file_io.input_stream)
        return file_io

    def _reader(self, file_io):
        return BTreeIndexReader(
            self.serializer, file_io, '/unused', self.io_meta)


class JavaBTreeIndexReaderTest(unittest.TestCase):

    def setUp(self):
        self.directory = Path(__file__).parent / 'resources' / 'btree'
        self.serializer = create_serializer(AtomicType('INT'))
        self.rows = {int(k): v for k, v in json.loads(
            (self.directory / 'expected.json').read_text()).items()}

    def _meta(self, version, bloom=False, string=False):
        name = 'java-v%d%s%s.btree' % (version, '-string' if string else '', '-bloom' if bloom else '')
        data = (self.directory / name).read_bytes()
        return data, GlobalIndexIOMeta(
            name, len(data), (self.directory / (name + '.meta')).read_bytes())

    def test_java_file_queries(self):
        for version in (1, 2):
            for bloom in (False, True):
                for input_type in (_TrackingInput, _PreadInput):
                    with self.subTest(version=version, bloom=bloom, input_type=input_type):
                        data, meta = self._meta(version, bloom)
                        file_io = _MemoryFileIO(data, input_type)
                        reader = BTreeIndexReader(self.serializer, file_io, '/unused', meta)
                        try:
                            self.assertEqual(version, reader.footer.version)
                            self.assertEqual(bloom, reader.footer.bloom_filter_handle is not None)
                            for key, expected in self.rows.items():
                                self.assertEqual(expected, reader.visit_equal(key).results().to_list())
                            self.assertEqual([], reader.visit_equal(6).results().to_list())
                            self.assertEqual([], reader.visit_between(6, 7).results().to_list())
                            self.assertEqual(
                                sorted(self.rows[2] + self.rows[3] + self.rows[4]),
                                reader.visit_between(2, 4).results().to_list())
                            self.assertEqual(
                                self.rows[3], reader._range_query(2, 4, False, False).to_list())
                            self.assertEqual(
                                sorted(self.rows[1] + self.rows[3]),
                                reader.visit_in([1, 3, 3, 6, None]).results().to_list())
                            self.assertEqual(self.rows[1], reader.visit_less_than(2).results().to_list())
                            self.assertEqual(self.rows[5], reader.visit_greater_than(4).results().to_list())
                            self.assertEqual([999], reader.visit_is_null().results().to_list())
                            self.assertEqual(
                                sorted(sum(self.rows.values(), [])),
                                reader.visit_is_not_null().results().to_list())
                            if version == 2:
                                iterator = reader.reader.create_iterator()
                                iterator.seek_to(self.serializer.serialize(1))
                                blocks = 0
                                keys = []
                                while True:
                                    block = iterator.read_batch()
                                    if block is None:
                                        break
                                    blocks += 1
                                    while block.has_next():
                                        entry = next(block)
                                        key = self.serializer.deserialize(entry.key)
                                        keys.append(key)
                                        self.assertEqual(
                                            (self.directory / ('posting-%d.bin' % key)).read_bytes(),
                                            entry.value)
                                self.assertEqual([1, 2, 3, 4, 5], keys)
                                self.assertGreater(blocks, 1)
                        finally:
                            reader.close()
                        self.assertTrue(file_io.input_stream.closed)

    def test_java_string_prefix_queries(self):
        serializer = create_serializer(AtomicType('STRING'))
        keys = ['ke', 'keep', 'keeper', 'key', 'kf']
        for version in (1, 2):
            for bloom in (False, True):
                for input_type in (_TrackingInput, _PreadInput):
                    with self.subTest(version=version, bloom=bloom, input_type=input_type):
                        data, meta = self._meta(version, bloom, string=True)
                        file_io = _MemoryFileIO(data, input_type)
                        reader = BTreeIndexReader(serializer, file_io, '/unused', meta)
                        try:
                            for prefix in ('', 'ke', 'keep', 'keeper', 'key', 'kf', 'absent', 'kg'):
                                with self.subTest(prefix=prefix):
                                    expected = sorted(
                                        row_id for i, key in enumerate(keys, 1)
                                        if key.startswith(prefix) for row_id in self.rows[i])
                                    result = reader.visit_starts_with(prefix)
                                    self.assertTrue(result.is_exact())
                                    self.assertEqual(expected, result.results().to_list())
                            iterator = reader.reader.create_iterator()
                            iterator.seek_to(serializer.serialize(''))
                            blocks = 0
                            seen = []
                            while True:
                                block = iterator.read_batch()
                                if block is None:
                                    break
                                blocks += 1
                                while block.has_next():
                                    entry = next(block)
                                    key = serializer.deserialize(entry.key)
                                    seen.append(key)
                                    if version == 2:
                                        self.assertEqual(
                                            (self.directory / ('posting-%d.bin' % (keys.index(key) + 1))).read_bytes(),
                                            entry.value)
                            self.assertEqual(keys, seen)
                            self.assertGreater(blocks, 1)
                            self.assertEqual([999], reader.visit_is_null().results().to_list())
                        finally:
                            reader.close()
                        self.assertTrue(file_io.input_stream.closed)

    def test_v2_initialization_failure_preserves_error_and_closes_stream(self):
        data, meta = self._meta(2)
        footer = BTreeFileFooter.read_footer(data[-BTreeFileFooter.ENCODED_LENGTH:])
        damaged = bytearray(data)
        handle = footer.index_block_handle
        damaged[handle.offset + handle.size + 1] ^= 1
        for input_type in (_TrackingInput, _PreadInput):
            for close_error in (None, OSError('Close failed')):
                with self.subTest(input_type=input_type, close_error=close_error):
                    file_io = _MemoryFileIO(bytes(damaged), input_type, close_error=close_error)
                    with self.assertRaisesRegex(ValueError, 'CRC32 mismatch'):
                        BTreeIndexReader(self.serializer, file_io, '/unused', meta)
                    self.assertTrue(file_io.input_stream.closed)

    def test_malformed_posting_fails_query(self):
        data, meta = self._meta(2)
        reader = BTreeIndexReader(self.serializer, _MemoryFileIO(data, _TrackingInput), '/unused', meta)
        try:
            with patch.object(reader.reader, 'lookup', return_value=b'\x01\x02\x0a'):
                with self.assertRaises(IndexError):
                    reader.visit_equal(1)
        finally:
            reader.close()

    def test_mixed_versions_with_range_offset(self):
        metas = [self._meta(version)[1] for version in (1, 2)]
        with ThreadPoolExecutor(max_workers=2) as executor:
            reader = LazyFilteredBTreeReader(
                self.serializer, LocalFileIO(), str(self.directory), metas, executor,
                fallback_scan_max_size=1 << 20)
            offset_reader = OffsetGlobalIndexReader(reader, 100, (1 << 63) - 1)
            field = FieldRef(0, 'key', 'INT')
            try:
                result = offset_reader.visit_in(field, [1, 2, 3]).result(timeout=10)
                expected = sorted(v + 100 for v in self.rows[1] + self.rows[2] + self.rows[3])
                self.assertEqual(expected, result.results().to_list())
                self.assertEqual(
                    sorted(v + 100 for v in self.rows[2] + self.rows[3]),
                    offset_reader.visit_between(field, 2, 3).result(timeout=10).results().to_list())
                self.assertEqual(
                    [1099], offset_reader.visit_is_null(field).result(timeout=10).results().to_list())
                self.assertEqual({1, 2}, {r.footer.version for r in reader._reader_cache.values()})
            finally:
                offset_reader.close()


if __name__ == '__main__':
    unittest.main()
