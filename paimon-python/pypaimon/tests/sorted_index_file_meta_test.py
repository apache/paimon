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

import struct
import unittest

from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta


def _meta_bytes(first_key, last_key, has_nulls, flags=None):
    first_bytes = first_key or b''
    last_bytes = last_key or b''
    data = (
        struct.pack('<I', len(first_bytes)) + first_bytes +
        struct.pack('<I', len(last_bytes)) + last_bytes +
        struct.pack('<B', 1 if has_nulls else 0)
    )
    if flags is not None:
        data += struct.pack('<B', SortedIndexFileMeta.FORMAT_VERSION_WITH_NULL_FLAGS)
        data += struct.pack('<B', flags)
    return data


class SortedIndexFileMetaTest(unittest.TestCase):

    def test_empty_string_key_is_not_null_with_format_flags(self):
        meta = SortedIndexFileMeta.deserialize(_meta_bytes(b'', b'k9', False, 0))

        self.assertEqual(b'', meta.first_key)
        self.assertEqual(b'k9', meta.last_key)
        self.assertFalse(meta.has_nulls)
        self.assertFalse(meta.only_nulls())

    def test_null_key_flags_are_honored(self):
        meta = SortedIndexFileMeta.deserialize(
            _meta_bytes(
                None,
                None,
                True,
                SortedIndexFileMeta.FIRST_KEY_IS_NULL
                | SortedIndexFileMeta.LAST_KEY_IS_NULL))

        self.assertIsNone(meta.first_key)
        self.assertIsNone(meta.last_key)
        self.assertTrue(meta.has_nulls)
        self.assertTrue(meta.only_nulls())

    def test_legacy_all_null_metadata(self):
        meta = SortedIndexFileMeta.deserialize(_meta_bytes(None, None, True))

        self.assertIsNone(meta.first_key)
        self.assertIsNone(meta.last_key)
        self.assertTrue(meta.has_nulls)
        self.assertTrue(meta.only_nulls())


if __name__ == '__main__':
    unittest.main()
