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
import unittest

from pypaimon.data.timestamp import Timestamp
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_message_serializer import (
    deserialize_commit_message, serialize_commit_message)


# Captured from Java CommitMessageSerializer v14 with an empty partition,
# bucket 3 and checkFromSnapshot 7. The second body has totalBuckets 5 and a
# data-increment IndexFileMeta("I", "index", 9, 2).
_JAVA_EMPTY = base64.b64decode(
    'AAAADAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw==')
_JAVA_INDEXED = base64.b64decode(
    'AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAABQAAAAAAAAAAAAAAAAAAAAEAAABAAHAAAAAAAABJAAAAAAAAgWluZGV4AACFCQAA'
    'AAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw==')
_JAVA_RICH_INDEXED = base64.b64decode(
    'AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAABQAAAAAAAAAAAAAAAAAAAAEAAADgAAAAAAAAAABEVgAAAAAAgmluZGV4AACFCQAA'
    'AAAAAAACAAAAAAAAAEgAAABAAAAACwAAAIgAAABIAAAAmAAAAAEAAAAAAAAAOAAAABAAAAAAAAAAAAAAAAwAAAAoAAAAAwAA'
    'AAAAAAAMAAAAAAAAAAIAAAAAAAAAZGF0YS5wYXJxdWV0AAAAAGZpbGU6L2luZGV4AAAAAAAAAAAAAAAAAAoAAAAAAAAAFAAA'
    'AAAAAAADAAAAAAAAABAAAAA4AAAAAQIAAAAAAIIDAAAAAAAAgQIAAAAAAAAAAQAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    'AAAAAAABAAAAAAAAAAc=')


class CommitMessageSerializerTest(unittest.TestCase):

    def test_java_v14_golden(self):
        for payload in (_JAVA_EMPTY, _JAVA_INDEXED, _JAVA_RICH_INDEXED):
            message = deserialize_commit_message(payload, [])
            self.assertEqual(serialize_commit_message(message, []), payload)
            self.assertEqual(message.check_from_snapshot, 7)
        indexed = deserialize_commit_message(_JAVA_INDEXED, [])
        self.assertEqual(indexed.total_buckets, 5)
        self.assertEqual(indexed.index_adds[0].index_file.file_name, 'index')
        self.assertEqual(serialize_commit_message(
            CommitMessage((), 3, [], check_from_snapshot=7), []), _JAVA_EMPTY)

    def test_rich_index_java_golden(self):
        meta = IndexFileMeta(
            'DV', 'index', 9, 2,
            {'data.parquet': DeletionVectorMeta('data.parquet', 3, 12, 2)},
            'file:/index', GlobalIndexMeta(10, 20, 3, [1, 4], b'\x01\x02', b'\x03'))
        entry = IndexManifestEntry(0, GenericRow([], []), 3, meta)
        message = CommitMessage(
            (), 3, [], check_from_snapshot=7, index_adds=[entry], total_buckets=5)
        self.assertEqual(serialize_commit_message(message, []), _JAVA_RICH_INDEXED)

    def test_files_and_compaction_stay_separate(self):
        stats = SimpleStats.empty_stats()
        file = DataFileMeta(
            'data.parquet', 100, 4, GenericRow([], []), GenericRow([], []),
            stats, stats, 1, 4, 2, 0, [], Timestamp(0), first_row_id=10,
            write_cols_sequences=[4])
        index = IndexManifestEntry(
            0, GenericRow([], []), 3, IndexFileMeta('I', 'index', 9, 2))
        message = CommitMessage(
            (), 3, [file], check_from_snapshot=7,
            compact_before=[file], compact_after=[file],
            compact_index_adds=[index])
        decoded = deserialize_commit_message(serialize_commit_message(message, []), [])
        self.assertEqual(decoded.new_files[0].file_name, 'data.parquet')
        self.assertEqual(decoded.new_files[0].first_row_id, 10)
        self.assertEqual(decoded.new_files[0].write_cols_sequences, [4])
        self.assertEqual(len(decoded.compact_before), 1)
        self.assertEqual(len(decoded.compact_after), 1)
        self.assertEqual(decoded.compact_index_adds[0].index_file.file_name, 'index')

    def test_bad_version_and_truncation(self):
        with self.assertRaises(ValueError):
            deserialize_commit_message(_JAVA_EMPTY, [], version=13)
        with self.assertRaises(ValueError):
            deserialize_commit_message(_JAVA_EMPTY[:-1], [])
        with self.assertRaises(ValueError):
            deserialize_commit_message(_JAVA_EMPTY + b'\x00', [])

    def test_primary_key_files_require_key_fields_for_decode(self):
        key_fields = [DataField(0, 'k', AtomicType('INT'))]
        stats = SimpleStats.empty_stats()
        file = DataFileMeta(
            'pk.parquet', 10, 1, GenericRow([1], key_fields),
            GenericRow([1], key_fields), stats, stats, 0, 0, 1, 0, [])
        payload = serialize_commit_message(CommitMessage((), 0, [file]), [])
        with self.assertRaisesRegex(ValueError, 'key_fields are required'):
            deserialize_commit_message(payload, [])
        decoded = deserialize_commit_message(payload, [], key_fields)
        self.assertEqual(decoded.new_files[0].min_key.values, [1])
        self.assertEqual(serialize_commit_message(decoded, []), payload)
