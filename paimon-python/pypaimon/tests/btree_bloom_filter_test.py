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

"""Tests for the optional Bloom filter in Python BTree indexes."""

import io
import os
import tempfile
import unittest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.btree.bloom_filter import (
    BloomFilter,
    murmur_hash_bytes,
)
from pypaimon.globalindex.btree.btree_file_footer import BTreeFileFooter
from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.btree.btree_index_writer import BTreeIndexWriter
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.schema.data_types import AtomicType


class _CountingInput(io.BytesIO):

    def __init__(self, data):
        super().__init__(data)
        self.bytes_read = 0

    def read(self, size=-1):
        data = super().read(size)
        self.bytes_read += len(data)
        return data


class _MemoryFileIO:

    def __init__(self, data):
        self.data = data
        self.last_input = None

    def new_input_stream(self, path):
        self.last_input = _CountingInput(self.data)
        return self.last_input


class BTreeBloomFilterTest(unittest.TestCase):

    def setUp(self):
        self.serializer = create_serializer(AtomicType("INT"))

    def test_java_compatible_hash_and_bit_layout(self):
        values = [b"a", b"hello", b"world"]

        self.assertEqual(1485273170, murmur_hash_bytes(values[0]))
        self.assertEqual(-1008564952, murmur_hash_bytes(values[1]))
        self.assertEqual(-623458850, murmur_hash_bytes(values[2]))

        bloom_filter = BloomFilter.from_hashes(
            [murmur_hash_bytes(value) for value in values], 0.05)
        self.assertEqual("ea2299", bloom_filter.to_bytes().hex())

    def test_bloom_filter_option_defaults_to_disabled(self):
        self.assertFalse(
            CoreOptions(Options({})).btree_index_bloom_filter_enabled())
        self.assertTrue(
            CoreOptions(Options({
                "btree-index.bloom-filter.enabled": "true",
            })).btree_index_bloom_filter_enabled())

        data, _ = self._write_index(False, 10)
        footer = BTreeFileFooter.read_footer(
            data[-BTreeFileFooter.ENCODED_LENGTH:])
        self.assertIsNone(footer.bloom_filter_handle)

    def test_missing_point_lookups_read_only_bloom_filter(self):
        entry_count = 1000
        data, entry = self._write_index(True, entry_count)
        footer = BTreeFileFooter.read_footer(
            data[-BTreeFileFooter.ENCODED_LENGTH:])
        handle = footer.bloom_filter_handle

        self.assertIsNotNone(handle)
        self.assertEqual(entry_count, handle.expected_entries)

        bloom_filter = BloomFilter(
            handle.expected_entries,
            handle.size,
            data[handle.offset:handle.offset + handle.size],
        )
        missing_key = next(
            candidate
            for candidate in range(1, entry_count * 2, 2)
            if not bloom_filter.test_hash(
                murmur_hash_bytes(self.serializer.serialize(candidate)))
        )

        self._assert_missing_lookup_reads_only_bloom(
            data, entry, handle.size,
            lambda reader: reader.visit_equal(missing_key))
        self._assert_missing_lookup_reads_only_bloom(
            data, entry, handle.size,
            lambda reader: reader.visit_in([None, missing_key]))

    def test_present_point_lookup_returns_all_row_ids(self):
        data, entry = self._write_index(True, 10, duplicate_key=8)
        file_io = _MemoryFileIO(data)
        reader = self._reader(file_io, data, entry)
        try:
            result = reader.visit_equal(8)
            self.assertEqual([4, 10], result.results().to_list())
        finally:
            reader.close()

    def _write_index(self, enabled, entry_count, duplicate_key=None):
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO()
            writer = BTreeIndexWriter(
                file_io,
                directory,
                self.serializer,
                block_size=128,
                bloom_filter_enabled=enabled,
            )
            for index in range(entry_count):
                writer.write(index * 2, index)
                if index * 2 == duplicate_key:
                    writer.write(index * 2, entry_count)
            entry = writer.finish()[0]
            path = os.path.join(directory, entry.file_name)
            with open(path, "rb") as stream:
                return stream.read(), entry

    def _reader(self, file_io, data, entry):
        return BTreeIndexReader(
            self.serializer,
            file_io,
            "/unused",
            GlobalIndexIOMeta(
                file_name=entry.file_name,
                file_size=len(data),
                metadata=entry.meta,
            ),
        )

    def _assert_missing_lookup_reads_only_bloom(
        self, data, entry, bloom_size, lookup,
    ):
        file_io = _MemoryFileIO(data)
        reader = self._reader(file_io, data, entry)
        try:
            file_io.last_input.bytes_read = 0
            self.assertTrue(lookup(reader).results().is_empty())
            self.assertEqual(bloom_size, file_io.last_input.bytes_read)
        finally:
            reader.close()


if __name__ == '__main__':
    unittest.main()
