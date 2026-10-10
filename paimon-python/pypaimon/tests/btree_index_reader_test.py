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
import os
import struct
import tempfile
import unittest

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.btree.btree_file_footer import BTreeFileFooter
from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.btree.btree_index_writer import BTreeIndexWriter
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.key_serializer import create_serializer
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

    def test_unsupported_versions_rejected_before_sst_read(self):
        for input_type in (_TrackingInput, _PreadInput):
            for version in (0, 2, 3, 0x80000000, 0xffffffff):
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


if __name__ == '__main__':
    unittest.main()
