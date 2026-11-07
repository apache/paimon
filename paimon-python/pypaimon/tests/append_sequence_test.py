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
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

# The ranges come from the Python writer; the native writer is paimon-rust's.
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]

PA_SCHEMA = pa.schema([('a', pa.int32())])


class AppendSequenceTest(unittest.TestCase):
    """Files of a bucketed append table carry the sequence range of their own
    rows, like Java's AppendOnlyWriter. Java's compaction picks a bucket's
    files in sequence order, so equal ranges reorder the rows."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='unittest_append_seq_')
        self.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(self.temp_dir, 'warehouse')})
        self.catalog.create_database('default', True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _table(self, name, options):
        self.catalog.create_table(
            f'default.{name}', Schema.from_pyarrow_schema(PA_SCHEMA, options=options), False)
        return self.catalog.get_table(f'default.{name}')

    def _commit(self, table, values):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({'a': values}, schema=PA_SCHEMA))
            messages = writer.prepare_commit()
            commit.commit(messages)
            return messages
        finally:
            writer.close()
            commit.close()

    def test_append_files_carry_advancing_sequence_ranges(self):
        table = self._table('seq', {'bucket': '1', 'bucket-key': 'a'})

        first = self._commit(table, [1])
        second = self._commit(table, [2, 3])
        third = self._commit(table, [4])

        ranges = []
        for messages in (first, second, third):
            for message in messages:
                for data_file in message.new_files:
                    ranges.append(
                        (data_file.min_sequence_number, data_file.max_sequence_number))

        # A bucket without files restores max sequence number 1.
        self.assertEqual([(2, 2), (3, 4), (5, 5)], ranges)

    def test_unaware_bucket_files_keep_zero_range(self):
        # Commit replaces the zero range with the snapshot id on row-tracking
        # tables, which only allow bucket = -1.
        for name, options in (
                ('seq_rt', {'row-tracking.enabled': 'true'}),
                ('seq_de', {'row-tracking.enabled': 'true',
                            'data-evolution.enabled': 'true'})):
            with self.subTest(table=name):
                table = self._table(name, options)
                first = self._commit(table, [1])
                second = self._commit(table, [2, 3])
                ranges = [
                    (f.min_sequence_number, f.max_sequence_number)
                    for messages in (first, second)
                    for m in messages
                    for f in m.new_files
                ]
                self.assertEqual([(0, 0), (0, 0)], ranges)
                rb = self.catalog.get_table(f'default.{name}').new_read_builder() \
                    .with_projection(['a', '_SEQUENCE_NUMBER'])
                rows = rb.new_read().to_arrow(rb.new_scan().plan().splits())
                self.assertEqual(
                    {1: 1, 2: 2, 3: 2},
                    dict(zip(rows.column('a').to_pylist(),
                             rows.column('_SEQUENCE_NUMBER').to_pylist())))


if __name__ == '__main__':
    unittest.main()
