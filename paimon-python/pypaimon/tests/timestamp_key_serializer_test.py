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

"""Regression tests for timestamp global index keys with time zones."""

import datetime
import os
import tempfile
import unittest

from parameterized import parameterized

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.bitmap.bitmap_index_reader import BitmapIndexReader
from pypaimon.globalindex.bitmap.bitmap_index_writer import BitmapIndexWriter
from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.btree.btree_index_writer import BTreeIndexWriter
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.schema.data_types import AtomicType


class TimestampKeySerializerTest(unittest.TestCase):

    @parameterized.expand([
        ('TIMESTAMP_LTZ(3)', 123000),
        ('TIMESTAMP_LTZ(6)', 123456),
        ('TIMESTAMP(3) WITH LOCAL TIME ZONE', 123000),
        ('TIMESTAMP(6) WITH LOCAL TIME ZONE', 123456),
    ])
    def test_same_instant_has_identical_key(self, type_name, microsecond):
        serializer = create_serializer(AtomicType(type_name))
        compare = serializer.create_comparator()
        for year in (1969, 2026):
            utc = datetime.datetime(year, 1, 1, microsecond=microsecond, tzinfo=datetime.timezone.utc)
            expected = serializer.serialize(utc)
            for hours in (-7, 0, 8):
                offset = datetime.timezone(datetime.timedelta(hours=hours))
                local = utc.astimezone(offset)
                self.assertEqual(expected, serializer.serialize(local))
                self.assertEqual(0, compare(utc, local))
                self.assertEqual(utc.replace(tzinfo=None), serializer.deserialize(expected))
                self.assertEqual(expected, serializer.serialize(serializer.deserialize(expected)))
            earlier = utc.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
            self.assertLess(compare(earlier, utc), 0)

    def test_timestamp_without_zone_preserves_wall_time(self):
        serializer = create_serializer(AtomicType('TIMESTAMP(6)'))
        naive = datetime.datetime(2026, 1, 1, 8, 0, 0, 123456)
        aware = naive.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
        self.assertEqual(serializer.serialize(naive), serializer.serialize(aware))
        self.assertEqual(0, serializer.create_comparator()(naive, aware))
        self.assertEqual(naive, serializer.deserialize(serializer.serialize(aware)))

    @parameterized.expand([
        ('btree', BTreeIndexWriter, BTreeIndexReader),
        ('bitmap', BitmapIndexWriter, BitmapIndexReader),
    ])
    def test_index_queries_accept_equivalent_time_zones(self, name, writer_class, reader_class):
        serializer = create_serializer(AtomicType('TIMESTAMP_LTZ(6)'))
        utc = datetime.datetime(2026, 1, 1, microsecond=123456, tzinfo=datetime.timezone.utc)
        offset = datetime.timezone(datetime.timedelta(hours=8))
        local = utc.astimezone(offset)
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO()
            writer = writer_class(file_io, directory, serializer)
            # UTC is the representation used by Arrow for LTZ columns and by
            # existing indexes. Query literals may carry a different offset.
            for row_id in range(3):
                writer.write(utc + datetime.timedelta(seconds=row_id - 1), row_id)
            entry = writer.finish()[0]
            reader = reader_class(serializer, file_io, directory, GlobalIndexIOMeta(
                file_name=entry.file_name,
                file_size=os.path.getsize(os.path.join(directory, entry.file_name)),
                metadata=entry.meta,
            ))
            try:
                self.assertEqual([1], reader.visit_equal(local).results().to_list())
                self.assertEqual([1], reader.visit_in([local]).results().to_list())
                self.assertEqual([0], reader.visit_less_than(local).results().to_list())
                self.assertEqual([2], reader.visit_greater_than(local).results().to_list())
            finally:
                reader.close()
